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
from bson.objectid import ObjectId
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

def send_to_sqs(message_body, queue_url):
    sqs_client = boto3.client('sqs', region_name='us-east-1')
    response = sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message_body))
    return response

def fetch_foreign_key(engine, query, key):
    try:
        result = pd.read_sql(query, con=engine)
        return result[result['id_op'] == key].iloc[0]['id']
    except Exception as e:
        print(f"Error fetching foreign key for {key}: {e}")
        return None

def convert_types(data):
    if isinstance(data, list):
        for record in data:
            for key, value in record.items():
                if isinstance(value, np.generic):
                    record[key] = value.item()
                elif isinstance(value, pd.Timestamp):
                    record[key] = value.to_pydatetime()
                elif value == "":
                    record[key] = None
    elif isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, np.generic):
                data[key] = value.item()
            elif isinstance(value, pd.Timestamp):
                data[key] = value.to_pydatetime()
            elif value == "": 
                data[key] = None
    else:
        raise ValueError("Unsupported data type for conversion. Expected dict or list of dicts.")
    return data

def get_valid_field(data, *keys):
    for key in keys:
        if isinstance(key, tuple):
            value = data
            for subkey in key:
                value = value.get(subkey) if isinstance(value, dict) else None
                if value in ["", "null", None]:#"NA",
                    break
        else:
            value = data.get(key)

        if value not in ["", "null", None]: #"NA",
            return value
    return None

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat() 
        elif isinstance(obj, (np.generic, pd.Timestamp)): 
            return obj.item() if isinstance(obj, np.generic) else obj.isoformat()
        elif isinstance(obj, bytes):
            return obj.decode('utf-8')
        return super().default(obj)

def lambda_handler(event, context):
    engine = connect_db('prod_rds_data_production_rw')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/045511714637/MongoInsertSQSAllMessageFail'

    print('Process event:', event)

    insert_query = text("""
        INSERT INTO achievement_user (
            id_comp, id_op_achievement_user, id_user, id_achievement, start, "end", 
            status, created_usr_achv, num_trip_achvusr
        ) VALUES (
            :id_comp, :id_op_achievement_user, :id_user, :id_achievement, :start, :end, 
            :status, :created_usr_achv, :num_trip_achvusr
        )
    """)
    query_user = "SELECT id_user as id, id_op_user as id_op FROM users_person"
    query_achievement = "SELECT id_achievement as id, id_op_achievement as id_op FROM achievements"

    for record in event.get('Records', []):
        try:
            body = json.loads(record.get('body')) if isinstance(record.get('body'), str) else record.get('body')
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON body: {record.get('body')}, Error: {e}")
            continue

        detail = body.get('detail', {})
        full_document = detail.get('fullDocument')
        if not full_document:
            print("Skipping record due to missing 'detail' or 'fullDocument'")
            continue

        id_op_achv_usr = full_document.get('_id')
        user_key = full_document.get('user')
        achievements = full_document.get('achievements', [])
        if not achievements:
            print("Skipping record due to missing achievements")
            continue

        achv_data = achievements[0]
        achv_path_key = achv_data.get('achievement')
        achv_data_1 = achievements[1] if len(achievements) > 1 else None
        achv_path_key_1 = achv_data_1.get('achievement') if achv_data_1 else None

        # Fetch foreign keys
        id_user = fetch_foreign_key(engine, query_user, user_key)
        id_achievement = fetch_foreign_key(engine, query_achievement, achv_path_key)
        id_achievement_1 = (fetch_foreign_key(engine, query_achievement, achv_path_key_1)
                            if achv_path_key_1 else None)

        if not id_user or not id_achievement or (achv_data_1 and not id_achievement_1):
            message_body = {'record': record, 'reason': 'Missing or invalid foreign keys'}
            send_to_sqs(message_body, queue_url)
            continue

        # Prepare common values
        id_comp = f"{id_op_achv_usr}_{id_user}_{id_achievement}"
        try:
            created_usr_achv = ObjectId(id_op_achv_usr).generation_time
        except Exception as e:
            print(f"Error processing ObjectId: {id_op_achv_usr}, Error: {e}")
            continue

        achv_meta = achv_data.get('metadata', {}).get('metrics', {}).get('firstTrip', {})
        num_trip_achvusr = achv_meta.get('current')

        # Prepare insertion data for the first achievement
        data_to_insert = {
            'id_comp': id_comp,
            'id_op_achievement_user': id_op_achv_usr,
            'id_user': id_user,
            'id_achievement': id_achievement,
            'start': achv_data.get('start'),
            'end': achv_data.get('end'),
            'status': full_document.get('status'),
            'created_usr_achv': created_usr_achv,  
            'num_trip_achvusr': num_trip_achvusr
        }
        data_to_insert = convert_types(data_to_insert)

        try:
            with engine.connect() as connection:
                connection.execute(insert_query, **data_to_insert)
                print(f'Achievement Users data inserted successfully for first achievement: {data_to_insert}')
            
                if achv_data_1:
                    id_comp_1 = f"{id_op_achv_usr}_{id_user}_{id_achievement_1}"
                    data_to_insert_1 = {
                        'id_comp': id_comp_1,
                        'id_op_achievement_user': id_op_achv_usr,
                        'id_user': id_user,
                        'id_achievement': id_achievement_1,
                        'start': achv_data_1.get('start'),
                        'end': None,
                        'status': None,
                        'created_usr_achv': created_usr_achv,  
                        'num_trip_achvusr': None
                    }
                    data_to_insert_1 = convert_types(data_to_insert_1)
                    connection.execute(insert_query, **data_to_insert_1)
                    print(f'Achievement Users data inserted successfully for second achievement: {data_to_insert_1}')
                else:
                    print("No second achievement found")

                response = {
                    "statusCode": 200,
                    "body": json.dumps({
                        "message": "Processing completed successfully",
                    }, cls=CustomJSONEncoder)
                }
        except Exception as e:
            print(f"Error inserting data: {e}")
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
    