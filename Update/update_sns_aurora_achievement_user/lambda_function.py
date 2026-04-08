import sys
import boto3
import json
import os
import gc
import numpy as np
import sqlalchemy as db
import pymongo
import pandas as pd
import logging

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timedelta
from pytz import timezone
from typing import Optional, Dict

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def connect_db(aurora_secret_name, mongo_secret_name= None):
    session = boto3.session.Session()
    aws_client = session.client(service_name='secretsmanager', region_name='us-east-1')
    credentials = json.loads(aws_client.get_secret_value(SecretId=aurora_secret_name)['SecretString'])
    
    user = credentials['user']
    password = credentials['password']
    host = credentials['host']
    port = credentials['port']
    database = 'postgres'

    connection_string = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}?sslmode=require"
    engine = create_engine(connection_string, pool_pre_ping=True, pool_recycle=3600, pool_size=10, max_overflow=20)

    ## ========= MONGO =========
    if mongo_secret_name != None:
        mongo_cred = json.loads(aws_client.get_secret_value(SecretId = mongo_secret_name)['SecretString'])
        STRING_MONGO = mongo_cred['STRING_MONGO']
        ## Connect
        mongo_client = pymongo.MongoClient(STRING_MONGO)
        mydb = mongo_client['production-01']
        conn = engine, mydb
    else:
        conn = engine
    
    return conn

# General Functions
def fetch_foreign_key(engine, query, trip_key):
    try:
        if not hasattr(engine, 'execute'):
            raise ValueError("Invalid engine object passed. Ensure it is an SQLAlchemy engine.")

        df = pd.read_sql(query, con=engine)
        result = df.loc[df['id_op'] == trip_key, 'id']
        if result.empty:
            raise ValueError(f"No foreign key found for trip_key: {trip_key}")
        return result.iloc[0]
    except Exception as e:
        print(f"Error fetching foreign key for {trip_key}: {e}")
        return None
    
def convert_types(data):
    if isinstance(data, list):
        for record in data:
            for key, value in record.items():
                if isinstance(value, np.generic):
                    record[key] = value.item()
                elif isinstance(value, pd.Timestamp):
                    record[key] = value.isoformat()
                elif isinstance(value, (int, float)) and 'date' in key.lower():
                    record[key] = datetime.fromtimestamp(value / 1000.0)
                elif value == "":
                    record[key] = None
    elif isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, np.generic):
                data[key] = value.item()
            elif isinstance(value, pd.Timestamp):
                data[key] = value.isoformat()
            elif isinstance(value, (int, float)) and 'date' in key.lower():
                data[key] = datetime.fromtimestamp(value / 1000.0)
            elif value == "":
                data[key] = None
    else:
        raise ValueError("Unsupported data type for conversion. Expected dict or list of dicts.")
    return data

def safe_json_loads(x):
    try:
        if isinstance(x, str) and x.strip():
            return json.loads(x)
        return x
    except json.JSONDecodeError:
        print(f"Invalid JSON: {x}")
        return None
    
def get_nested_value(data, key_path):
    """
    Retrieve the value from a nested dictionary using a dot-separated key path.
    """
    keys = key_path.split('.')
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key)
        else:
            return None
    return data

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat() 
        elif isinstance(obj, (np.generic, pd.Timestamp)): 
            return obj.item() if isinstance(obj, np.generic) else obj.isoformat()
        elif isinstance(obj, bytes):
            return obj.decode('utf-8')
        return super().default(obj)
    
##############################################
# Main Function
##############################################

def update_achv_user(engine, combined_df, achv_data, created_at, id_op_achievement_user):
    try:
        field_to_column_mapping = {
            'status': 'status'
                            }
        num_trip_achvusr = len(achv_data['trips'])
        order_medal = int(achv_data['order'])
        id_comp = f"{id_op_achievement_user}_{combined_df['id_user'][0]}_{combined_df['id_achievement'][0]}"
        #id_comp = "67d4a2b09a12eec18a4dc78c_41598714_67"

        # Build the dynamic UPDATE query for achv_user
        set_clauses = []
        params = {
            "updated_at": created_at,
            "num_trip_achvusr": num_trip_achvusr,
            "order_medal": order_medal
        }
        for field, column_name in field_to_column_mapping.items():
            value = get_nested_value(achv_data, field)
            if value is not None:
                set_clauses.append(f"{column_name} = :{field.replace('.', '_')}")
                params[field.replace('.', '_')] = value

        if set_clauses:
            set_clauses.extend([
                "updated_at = :updated_at",
                "num_trip_achvusr = :num_trip_achvusr",
                "order_medal = :order_medal"
            ])
            params["updated_at"] = datetime.now()
            params["num_trip_achvusr"] = num_trip_achvusr
        else:
            raise ValueError("No fields to update in achv_user")

        params = convert_types(params)

        update_query = f"""
            UPDATE achievement_user
            SET {', '.join(set_clauses)}
            WHERE id_comp = '{id_comp}'
        """
    
        # Execute the query
        with engine.connect() as connection:
            connection.execute(text(update_query), params)
            print(f"Achievement User updated successfully: {params}")
    except Exception as e:
        print(f"Error: {e}")
    return params

def push_trip(event):
    engine = connect_db('prod_rds_data_production_rw')

    for record in event:
        try:
            body = json.loads(record['body'])
            message = json.loads(body["Message"])
            id_user = message["user"]
            achv_data = message["achievement"]
            id_op_achievement = achv_data["id"]
            id_op_achievement_user = achv_data["relationId"]
            if id_user is None or id_op_achievement_user is None or id_op_achievement is None:
                raise ValueError("Missing 'id_user','id_op_achievement_user' or 'id_op_achievement' in message")
        except json.JSONDecodeError as e:
            print(f"JSON parsing error: {e}")
        except KeyError as e:
            print(f"Missing expected key: {e}")
        except Exception as e:
            print(f"Error processing record: {e}")
        continue

    created_at = body['Timestamp']

    if created_at is None:
        timeRef_dt = datetime.now()
        timeRef_dt = timeRef_dt - timedelta(days=10)
    else:
        if isinstance(created_at, int):
            timeRef_dt = datetime.fromtimestamp(created_at / 1000.0)
        elif isinstance(created_at, str):
            timeRef_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        else:
            raise TypeError(f"Unsupported type for 'createdAt': {type(created_at)}")

    query_achv_usr = f"""
        SELECT id_op_user, 
            id_user
        FROM users_person
        WHERE id_op_user = '{id_user}'
        LIMIT 1
    """
    query_achv = f"""
        SELECT id_op_achievement, 
            id_achievement
        FROM achievements
        WHERE id_op_achievement = '{id_op_achievement}'
        LIMIT 1
    """

    try:
        with engine.connect() as conn:
            df_user = pd.read_sql_query(query_achv_usr, con=conn, params={'id_user': id_user})
            df_achv = pd.read_sql_query(query_achv, con=conn, params={'id_op_achievement': id_op_achievement})
            combined_df = pd.concat([df_user, df_achv], axis=1)
    except Exception as e:
        print("Database query error:", e)

    if combined_df.empty:
        print('No data found')
    else: 
        params = update_achv_user(engine, combined_df, achv_data, created_at, id_op_achievement_user)
    return params


def lambda_handler(event, context):
    try:
        print('Procesing event', json.dumps(event))
        
        if 'Records' not in event:
            raise ValueError("Event does not contain 'Records' key")
        
        result = push_trip(event['Records'])
    
        logger.info(f"Processing result: {result}")
        
        response = {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Processing completed successfully",
                "result": result
            }, cls=CustomJSONEncoder)
        }
        
    except json.JSONDecodeError as e:
        logger.error(f"JSON decoding error: {e}")
        response = {
            "statusCode": 400,
            "body": json.dumps({
                "message": "Invalid JSON format in event",
                "error": str(e)
            }, cls=CustomJSONEncoder)
        }
    
    except KeyError as e:
        logger.error(f"Missing key in event: {e}")
        response = {
            "statusCode": 400,
            "body": json.dumps({
                "message": "Missing required key in event",
                "error": str(e)
            }, cls=CustomJSONEncoder)
        }
    
    except ValueError as e:
        logger.error(f"Value error: {e}")
        response = {
            "statusCode": 400,
            "body": json.dumps({
                "message": "Invalid value in event",
                "error": str(e)
            }, cls=CustomJSONEncoder)
        }
    
    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}")
        response = {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Database operation failed",
                "error": str(e)
            }, cls=CustomJSONEncoder)
        }
    
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        response = {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Internal server error",
                "error": str(e)
            }, cls=CustomJSONEncoder)
        }
    
    finally:
        gc.collect()
    return response
