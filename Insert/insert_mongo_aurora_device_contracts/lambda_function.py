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

from bson.objectid import ObjectId
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
                if value in ["", "NA", "null", None]:
                    break
        else:
            value = data.get(key)

        if value not in ["", "NA", "null", None]:
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
    try:
        engine, mydb = connect_db('prod_rds_data_production_rw', 'mongo_production_ro')

        print('Process event:', event)

        for record in event['Records']:
            try:
                if isinstance(record['body'], str):
                    body = json.loads(record['body'])  
                else:
                    body = record['body']

                if 'Records' in body:
                    inner_record = body['Records'][0]  # Assuming single inner message
                    if isinstance(inner_record['body'], str):
                        body = json.loads(inner_record['body'])
                    else:
                        body = inner_record['body']    
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON body: {record['body']}, Error: {e}")
                break

            if 'detail' not in body or 'fullDocument' not in body['detail']:
                print("Skipping record due to missing 'detail' or 'fullDocument'")
                break

            full_document = body['detail']['fullDocument']

            # Queries to fetch foreign keys
            contract_key = full_document['_id']

            collection = mydb['servicecontracts']
            query = {'_id': ObjectId(contract_key)}
            projection = {
                '_id': 1,
                'device':1,
                'info.voucherCreatedAt': 1,
                'end': 1
            }

            contract_query = collection.find(query, projection)
            contract_data = []
            for doc in contract_query:
                data = {
                    'id': str(doc.get('_id', '')),
                    'device': str(doc.get('device', '')),
                    'created_voucher_start': str(doc.get('info', {}).get('voucherCreatedAt')),
                    'created_voucher_end': str(doc.get('end', ''))
                }
                contract_data.append(data)

            df = pd.DataFrame(contract_data)
            device_key = str(df['device'].iloc[0])

            query_contract = "SELECT id_contract as id, id_op_contract as id_op FROM contracts"
            query_device = "SELECT id_device as id, id_op_device as id_op FROM devices"

            id_contract = fetch_foreign_key(engine, query_contract, contract_key)
            id_device = fetch_foreign_key(engine, query_device, device_key)
            
            # Prepare the data for insertion
            insert_query = text("""
                INSERT INTO devices_contract (
                    id_device, id_contract, created_voucher_start, created_voucher_end
                ) VALUES (
                    :id_device, :id_contract, :created_voucher_start, :created_voucher_end
                )
            """)
            
            data_to_insert = {
                'id_device': id_device,
                'id_contract': id_contract,
                'created_voucher_start': full_document.get('info', {}).get('voucherCreatedAt', None),
                'created_voucher_end': get_valid_field(full_document,
                                    'end',
                                    'currentVersion.end')
            }

            data_to_insert = convert_types(data_to_insert)

            try:
                with engine.connect() as connection:
                    connection.execute(insert_query, **data_to_insert)
                    print(f"Inserted device_contract data: {data_to_insert}")
            except Exception as e:
                print(f"Error inserting into device_contract: {e}")

            response = {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Processing completed successfully",
                "result": data_to_insert
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

