import sys
import boto3
import json
import os
import gc
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

def convert_types(data):
    """
    Convert data types such as numpy types and pandas timestamps to native Python types.
    Handles both dictionaries and lists of dictionaries.
    """
    if isinstance(data, list):
        for record in data:
            for key, value in record.items():
                if isinstance(value, (np.generic, np.int64, np.float64)):
                    record[key] = value.item()  
                elif isinstance(value, pd.Timestamp):
                    record[key] = value.to_pydatetime()
                elif value == "":
                    record[key] = None
    elif isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, (np.generic, np.int64, np.float64)):
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



def lambda_handler(event, context):
    engine = connect_db('prod_rds_data_production_rw')

    print('Process event:', event)

    field_to_column_mapping = {
        'modif': 'modif_date',
        'status': 'device_status',
        'imei': 'imei',
        'serial': 'serial',
        'iccid': 'iccid',
        'display.warehouse': 'warehouse_name'
    }

    for record in event['Records']:
        try:
            body = json.loads(record['body']) if isinstance(record['body'], str) else record['body']
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON body: {record['body']}, Error: {e}")
            continue

        if 'detail' not in body:
            print("Skipping record due to missing 'detail'")
            continue

        full_document = body['detail']
        device_key = full_document['documentKey']['_id']
        updated_fields = full_document.get('updateDescription', {}).get('updatedFields', {})

        if not updated_fields:
            print(f"No fields to update for device_key: {device_key}")
            continue

        # Fetch id_device where 'id_op_device' equals device_key
        id_device = None
        query_id_device = """
            SELECT id_device 
            FROM devices 
            WHERE id_op_device = :id_op_device
        """
        try:
            with engine.connect() as connection:
                result = connection.execute(text(query_id_device), {'id_op_device': device_key}).fetchone()
                if result:
                    id_device = result['id_device']
                    print(f"Fetched id_device: {id_device} for device_key: {device_key}")
        except Exception as e:
            print(f"Error fetching id_device for device_key {device_key}: {e}")
            continue

        # Fetch warehouse_id if warehouse is in updated_fields
        warehouse_id = None
        if 'warehouse' in updated_fields:
            warehouse_id = fetch_foreign_key(
                engine,
                'SELECT id_device_warehouse as id, id_op_device_warehouse as id_op FROM device_warehouses',
                updated_fields['warehouse']
            )

        # Build the dynamic UPDATE query for `devices`
        set_clauses = []
        params = {"id_op_device": device_key}

        for field, column_name in field_to_column_mapping.items():
            value = get_nested_value(updated_fields, field)
            if value is not None:
                set_clauses.append(f"{column_name} = :{field.replace('.', '_')}")
                params[field.replace('.', '_')] = value

        # Add warehouse_id dynamically if fetched
        if warehouse_id is not None:
            set_clauses.append("id_device_warehouse = :id_device_warehouse")
            params["id_device_warehouse"] = warehouse_id

        if set_clauses:
            set_clauses.append("updated_at = :updated_at")
            params["updated_at"] = datetime.now()

            params = convert_types(params)
            print(params)

            update_query = f"""
                UPDATE devices
                SET {', '.join(set_clauses)}
                WHERE id_op_device = :id_op_device
            """

            try:
                with engine.connect() as connection:
                    connection.execute(text(update_query), params)
                    print(f"Updated device {device_key} with fields: {params}") #subir el lunes 20
            except Exception as e:
                print(f"Error updating device {device_key}: {e}")

    return {
        'statusCode': 200,
        'body': json.dumps('Devices processed successfully')
    }
