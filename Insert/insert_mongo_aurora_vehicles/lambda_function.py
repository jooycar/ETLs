import sys
import boto3
import json
import gc
import re
import numpy as np
import sqlalchemy as db
import pymongo
import pandas as pd

from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
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

def insert_veh_phy_data(engine, full_document):
    try:
        insert_query = text("""
            INSERT INTO vehicles_physical (
                id_op_veh_phy, plate_number, vin
            ) VALUES (
                :id_op_veh_phy, :plate_number, :vin
            )
        """)

        data_to_insert = {
            'id_op_veh_phy': str(full_document['_id']), 
            'plate_number': full_document['patent'],
            'vin': full_document['vin']
        }

        data_to_insert = convert_types(data_to_insert)
        with engine.connect() as connection:
            connection.execute(insert_query, **data_to_insert)
            pass
        return True
    except Exception as e:
        print(f"Error inserting vehicle data: {e}")
        return False

    
def insert_data_aurora(engine, data, id_vehicle):
    try:
        with engine.connect() as connection:
            insert_query = text("""
                INSERT INTO vehicles_logic (
                    id_op_veh_log, id_veh_physical, modif_date, vehicle_brand, vehicle_model, vehicle_year,
                    plate_number, vin
                ) VALUES (
                    :id_op_veh_log, :id_veh_physical, :modif_date, :vehicle_brand, :vehicle_model, :vehicle_year,
                    :plate_number, :vin
                )
            """)

            vehicle_brand = get_valid_field(
                data, 
                'otherBrand', 
                ('display', 'brand')
                )

            vehicle_model = get_valid_field(
                data, 
                'otherModel', 
                ('display', 'model')
                )
            
            vehicle_year = get_valid_field(
                data, 
                'year', 
                ('display', 'year')
                )

            plate_number = get_valid_field(
                data, 
                'patent', 
                ('display', 'patent')
                )

            data_to_insert = {
                'id_op_veh_log': str(data['_id']), 
                'id_veh_physical': id_vehicle,
                'modif_date': data['modif'],
                'vehicle_brand': vehicle_brand,
                'vehicle_model': vehicle_model,
                'vehicle_year': vehicle_year,
                'plate_number': plate_number,
                'vin': data['vin']
            }

            data_to_insert = convert_types(data_to_insert)

            connection.execute(insert_query, **data_to_insert)
        return True
    except Exception as e:
        print(f"Error inserting data: {e}")
        return False
    
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

def document_exists_in_aurora(engine, document_id):
    query = """
        SELECT COUNT(1) 
        FROM vehicles_physical
        WHERE id_op_veh_phy = :document_id
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(text(query), {"document_id": document_id})
            count = result.scalar()  
            return count > 0 
    except Exception as e:
        print(f"Error checking document existence: {e}")
        return False



def lambda_handler(event, context):
    engine = connect_db('prod_rds_data_production_rw')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/045511714637/MongoInsertSQSAllMessageFail'

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
        document_id = full_document['_id']

        # Check if document_id already exists in Aurora
        if document_exists_in_aurora(engine, document_id):
            print(f"Document {document_id} already exists in Aurora. Send FK...")
            
            # Queries to fetch foreign keys
            query_vehicle_phy = "SELECT id_veh_physical as id, id_op_veh_phy as id_op FROM vehicles_physical"

            id_vehicle = fetch_foreign_key(engine, query_vehicle_phy, document_id)
            
            if insert_data_aurora(engine, full_document, id_vehicle):
                print('Vehicle Logic data inserted successfully')

        else:
            if insert_veh_phy_data(engine, full_document):
                # Queries to fetch foreign keys
                query_vehicle_phy = "SELECT id_veh_physical as id, id_op_veh_phy as id_op FROM vehicles_physical"

                id_vehicle = fetch_foreign_key(engine, query_vehicle_phy, document_id)

                if not all([id_vehicle]) or 0 in [id_vehicle]:
                    message_body = {
                        'record': record,
                        'reason': 'Missing or invalid foreign keys'
                    }
                    send_to_sqs(message_body, queue_url)
                    continue
                print("Vehicle Physical data inserted successfully.")

                if insert_data_aurora(engine, full_document, id_vehicle):
                    print('Vehicle Logic data inserted successfully')
            else:
                print("Failed to insert Vehicle data. Aborting process for this record.")

    return {
        'statusCode': 200,
        'body': json.dumps('Processed successfully')
    }