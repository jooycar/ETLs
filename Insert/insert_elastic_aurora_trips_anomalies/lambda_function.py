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

def fetch_foreign_key(engine, query, trip_key):
    try:
        if not hasattr(engine, 'execute'):
            raise ValueError("Invalid engine object passed. Ensure it is an SQLAlchemy engine.")
        with engine.connect() as connection:
            result = connection.execute(text(query), trip_key=trip_key).fetchone()
        if result is None:
            raise ValueError(f"No foreign key found for trip_key: {trip_key}")
        return result[0]
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
                elif isinstance(value, (int, float)) and 'date' or 'created' in key.lower():
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

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat() 
        elif isinstance(obj, (np.generic, pd.Timestamp)): 
            return obj.item() if isinstance(obj, np.generic) else obj.isoformat()
        elif isinstance(obj, bytes):
            return obj.decode('utf-8')
        return super().default(obj)


def push_trip(event):
    engine = connect_db('prod_rds_data_production_rw')
    try:
        for record in event: 
            body = json.loads(record['body'])
            trip_dict = body['message']
            trip_key = trip_dict['tripId']
            created = datetime.fromtimestamp(trip_dict['created'] / 1000.0)

            timeRef_date = created.date()

            query_trip = f"""
                SELECT id_trip
                FROM trips
                WHERE id_op_trip = '{trip_key}' AND DATE(created) >= '{timeRef_date}'
                LIMIT 1
            """

            id_trip = fetch_foreign_key(engine, query_trip, trip_key)
            if id_trip is None:
                raise ValueError(f"no match with: {trip_key}")
            else:
                id_trip = int(id_trip)
                print(f"id_trip: {id_trip}.")

            anomaly_data = trip_dict['anomaly']

            resolution_status = anomaly_data.get('resolutionStatus', None)
            resolution_reporter = anomaly_data.get('resolutionReporter', None)

            resolution_date = None
            if 'resolutionDate' in anomaly_data and anomaly_data['resolutionDate']:
                resolution_date = datetime.fromtimestamp(anomaly_data['resolutionDate'] / 1000.0)


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
                'created': created,
                 'reporter': anomaly_data.get('reporter', None),
                'status_anomaly': anomaly_data.get('status', None),
                'type_anomaly': anomaly_data.get('type', None),
                'visibility': anomaly_data.get('visibility', None),
                'resolution_status': resolution_status,
                'resolution_date': resolution_date,
                'resolution_reporter': resolution_reporter
            }

            data_to_insert = convert_types(data_to_insert)
            print(data_to_insert)

            try:
                with engine.connect() as connection:
                    connection.execute(insert_query, **data_to_insert)
                    print(f"Updated Trips {trip_key} with fields: {data_to_insert}")
            except Exception as e:
                print(f"Error updating trip_scores {trip_key}: {e}")
    except Exception as e:
        print(f"Error inserting Score: {e}")

    return data_to_insert


def lambda_handler(event, context):
    try:
        print('Procesing event', json.dumps(event))
        
        if 'Records' not in event:
            raise ValueError("Event does not contain 'Records' key")
        
        # Process the event records
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
