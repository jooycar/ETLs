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

# funciones generales
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


def send_to_sqs(message_body, queue_url):
    try:
        sqs_client = boto3.client('sqs')
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message_body, default=str)
        )
        print(f"Message sent to SQS. MessageId: {response.get('MessageId')}")
        return response
    except Exception as e:
        print(f"Error sending message to SQS: {e}")
        raise

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

def insert_anomaly(engine, id_trip, trip_key, update_data):
    try:
        insert_query = text("""
            INSERT INTO trip_anomalies (
                id_op_trip_anomalies, id_trip, created, reporter, status_anomaly, type_anomaly,
                            visibility, resolution_status, resolution_date, resolution_reporter
            ) VALUES (
                :id_op_trip_anomalies, :id_trip, :created, :reporter, :status_anomaly, :type_anomaly,
                            :visibility, :resolution_status, :resolution_date, :resolution_reporter
            )
        """)

        data_to_insert = {
            'id_op_trip_anomalies': trip_key, 
            'id_trip': id_trip,
            'created': datetime.fromtimestamp(update_data['resolutionDate'] / 1000.0),
            'reporter': update_data['reporter'],
            'status_anomaly': update_data['status'],
            'type_anomaly': update_data['type'],
            'visibility': update_data['visibility'],
            'resolution_status': update_data['resolutionStatus'],
            'resolution_date': datetime.fromtimestamp(update_data['resolutionDate'] / 1000.0),
            'resolution_reporter': update_data['resolutionReporter']
        }

        data_to_insert = convert_types(data_to_insert)

        try:
            with engine.connect() as connection:
                connection.execute(insert_query, **data_to_insert)
            print(f"Anomaly inserted: {data_to_insert}")
        except Exception as e:
            print(f"Error inserting Anomaly: {e}")
    except Exception as e:
        print(f"Error inserting Anomaly: {e}")

    return data_to_insert


def update_anomaly(engine, id_trip, trip_key, update_data):
    try:
        field_to_column_mapping = {
            'reporter': 'reporter',
            'status': 'status_anomaly',
            'type': 'type_anomaly',
            'visibility': 'visibility',
            'resolutionStatus': 'resolution_status',
            'resolutionDate': 'resolution_date',
            'resolutionReporter': 'resolution_reporter'
        }

        # Build the dynamic UPDATE query for trips
        set_clauses = []
        params = {"id_op_trip_anomalies": trip_key,
                  "id_trip": id_trip}
        
        for field, column_name in field_to_column_mapping.items():
            value = update_data.get(field)
            if value is not None:
                set_clauses.append(f"{column_name} = %({column_name})s")
                params[column_name] = value
        
        if set_clauses:
            set_clauses.extend(["updated_at = %(updated_at)s", "created = %(created)s"])
            params.update({"updated_at": datetime.now(), "created": datetime.now()})

        params = convert_types(params)

        update_query = f"""
            UPDATE trip_anomalies
            SET {', '.join(set_clauses)}
            WHERE id_op_trip_anomalies = %(id_op_trip_anomalies)s AND id_trip = %(id_trip)s
        """

        with engine.connect() as connection:
            connection.execute(update_query, params)
        print(f"Anomaly updated successfully: {params}")
    except Exception as e:
        print(f"Error updating Anomaly: {e}")
    return params

def push_trip(event):
    engine = connect_db('prod_rds_data_production_rw')
    summary = []

    for record in event:
        trip_key = None
        id_trip = None
        status = None

        try:
            body = json.loads(record['body'])
            record = body['message']

            trip_key = record.get('tripId')
            id_trip = record.get('id_trip')
            update_data = record.get('updateData') or record.get('anomaly')
            if update_data is None:
                raise ValueError("Missing 'updateData' (or 'anomaly') in message")

            if trip_key is None:
                raise ValueError("Missing 'tripId' in message")

            created_at = record.get('createdAt') or record.get('created')
            if created_at is None:
                timeRef_dt = datetime.now() - timedelta(days=10)
            else:
                if isinstance(created_at, int):
                    timeRef_dt = datetime.fromtimestamp(created_at / 1000.0)
                elif isinstance(created_at, str):
                    timeRef_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                else:
                    raise TypeError(f"Unsupported type for 'createdAt': {type(created_at)}")

            timeRef_date = timeRef_dt.date()

            if id_trip is None:
                query_trip_id = f"""
                    SELECT id_trip
                    FROM trips
                    WHERE id_op_trip = '{trip_key}' AND DATE(created) >= '{timeRef_date}'
                    LIMIT 1
                """
                df_trip = pd.read_sql(query_trip_id, con=engine)
                if df_trip.empty:
                    raise ValueError(f"No se encontró id_trip para trip_key: {trip_key}")
                id_trip = df_trip.iloc[0]['id_trip']

            query_trip_anomaly = f"""
                SELECT id_op_trip_anomalies as id_op
                FROM trip_anomalies
                WHERE id_op_trip_anomalies = '{trip_key}' AND DATE(created) >= '{timeRef_date}'
                LIMIT 1
            """
            df_anomaly = pd.read_sql(query_trip_anomaly, con=engine)

            if df_anomaly.empty:
                insert_anomaly(engine, id_trip, trip_key, update_data)
                status = 'inserted'
            else:
                update_anomaly(engine, id_trip, trip_key, update_data)
                status = 'updated'

        except Exception as e:
            status = f'error: {str(e)}'

        summary.append({
            "trip_key": trip_key,
            "id_trip": id_trip,
            "status": status
        })

    return summary


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