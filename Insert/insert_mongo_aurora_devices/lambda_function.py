import sys
import boto3
import json
import gc
import numpy as np
import sqlalchemy as db
import pymongo
import pandas as pd

from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from bson.objectid import ObjectId
from pytz import timezone
from typing import Optional, Dict

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
    
def send_to_sqs(message_body, queue_url):
    sqs_client = boto3.client('sqs', region_name='us-east-1')
    response = sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message_body))
    return response

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

def insert_device_data(engine, full_document, id_warehouse):    
    try:
        if device_id := full_document.get('_id'):
            try:
                created_dvc_dt = ObjectId(device_id).generation_time
                created_dvc_date = created_dvc_dt.isoformat(timespec='seconds')
            except Exception as e:
                print(f"Error extracting or formatting creation time from device ObjectId: {e}")

        insert_query = text("""
        INSERT INTO devices (
            id_op_device, modif_date, device_status, imei, serial, iccid, 
            warehouse_name, id_device_warehouse, created_date
        ) VALUES (
            :id_op_device, :modif_date, :device_status, :imei, :serial, :iccid, 
            :warehouse_name, :id_device_warehouse, :created_date
        )
    """)
        data_to_insert = {
            'id_op_device': str(full_document['_id']), 
            'modif_date': full_document['modif'],
            'device_status' : full_document['status'],
            'imei': full_document['imei'],
            'serial': full_document['serial'],
            'iccid': full_document['iccid'],
            'warehouse_name': full_document['display']['warehouse'],
            'id_device_warehouse': id_warehouse,
            'created_date': created_dvc_date

        }

        data_to_insert = convert_types(data_to_insert)
        
        with engine.connect() as connection:
                connection.execute(insert_query, **data_to_insert)
                pass
        return True
    except Exception as e:
        print(f"Error inserting person data: {e}")
        return False

def lambda_handler(event, context):
    engine = connect_db('prod_rds_data_production_rw')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/045511714637/MongoInsertSQSAllMessageFail'

    print('Process event:', event)

    for record in event['Records']:
        try:
            if isinstance(record['body'], str):
                body = json.loads(record['body'])  
            else:
                body = record['body']

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON body: {record['body']}, Error: {e}")
            continue

        if 'detail' not in body or 'fullDocument' not in body['detail']:
            print("Skipping record due to missing 'detail' or 'fullDocument'")
            continue

        full_document = body['detail']['fullDocument']

        # Queries to fetch foreign keys
        warehouse_key = full_document['warehouse']

        query_warehouse = "SELECT id_device_warehouse as id, id_op_device_warehouse as id_op FROM device_warehouses"

        id_warehouse = fetch_foreign_key(engine, query_warehouse, warehouse_key)

        if not all([id_warehouse]) or 0 in [id_warehouse]:
            message_body = {
                'record': record,
                'reason': 'Missing or invalid foreign keys'
            }
            send_to_sqs(message_body, queue_url)
            continue

        if insert_device_data(engine, full_document, id_warehouse):
            print("Device data inserted successfully.")

    return {
        'statusCode': 200,
        'body': json.dumps('Processed successfully')
    }