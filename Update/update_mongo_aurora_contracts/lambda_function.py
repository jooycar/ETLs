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
        'status': 'policy_status',
        'installationStatus': 'installation_status',
        'tracking.installationDate': 'installation_date',
        'type': 'policy_type',
        'info.voucherId': 'proposal_id',
        'info.campaignCode': 'campaign_code',
        'provider': 'provider',
        'start': 'start_date',
        'end': 'end_date'
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
        contract_key = full_document['documentKey']['_id']
        updated_fields = full_document.get('updateDescription', {}).get('updatedFields', {})

        if not updated_fields:
            print(f"No fields to update for contract_key: {contract_key}")
            continue

        # Fetch id_contract where 'id_op_contract' equals contract_key
        id_contract = None
        if 'device' in updated_fields:
            query_id_contract = """
                SELECT id_contract 
                FROM contracts 
                WHERE id_op_contract = :id_op_contract
            """
            try:
                with engine.connect() as connection:
                    result = connection.execute(text(query_id_contract), {'id_op_contract': contract_key}).fetchone()
                    if result:
                        id_contract = result['id_contract']
                        print(f"Fetched id_contract: {id_contract} for contract_key: {contract_key}")
            except Exception as e:
                print(f"Error fetching id_contract for contract_key {contract_key}: {e}")
                continue

        # Prepare mappings for foreign key queries and corresponding database columns
        foreign_key_mappings = {
            'vehicle': {
                'query': 'SELECT id_veh_logic as id, id_op_veh_log as id_op FROM vehicles_logic',
                'column': 'id_veh_logic'
            },
            'user': [
                {
                    'query': 'SELECT id_payer as id, id_op_payer as id_op FROM payers_person',
                    'column': 'id_payer'
                },
                {
                    'query': 'SELECT id_user as id, id_op_user as id_op FROM users_person',
                    'column': 'id_user'
                }
            ],
            'tenants': {
                'query': 'SELECT id_tenant as id, id_op_tenant as id_op FROM tenants',
                'column': 'id_tenant'
            }
        }

        set_clauses = []
        params = {"id_op_contract": contract_key}

        # Dynamically fetch foreign keys and build the query
        for field, mapping in foreign_key_mappings.items():
            if field in updated_fields:
                if isinstance(mapping, list):
                    for sub_mapping in mapping:
                        foreign_key = fetch_foreign_key(engine, sub_mapping['query'], updated_fields[field])
                        if foreign_key is not None:
                            set_clauses.append(f"{sub_mapping['column']} = :{sub_mapping['column']}")
                            params[sub_mapping['column']] = foreign_key
                else:
                    foreign_key = fetch_foreign_key(engine, mapping['query'], updated_fields[field])
                    if foreign_key is not None:
                        set_clauses.append(f"{mapping['column']} = :{mapping['column']}")
                        params[mapping['column']] = foreign_key


        for field, column_name in field_to_column_mapping.items():
            value = get_nested_value(updated_fields, field)
            if value is not None:
                set_clauses.append(f"{column_name} = :{field.replace('.', '_')}")
                params[field.replace('.', '_')] = value

        if set_clauses:
            set_clauses.append("updated_at = :updated_at")
            params["updated_at"] = datetime.now()

            params = convert_types(params)

            update_query = f"""
                UPDATE contracts
                SET {', '.join(set_clauses)}
                WHERE id_op_contract = :id_op_contract
            """

            try:
                with engine.connect() as connection:
                    connection.execute(text(update_query), params)
                print(f"Updated contract {contract_key} with fields: {params}")
            except Exception as e:
                print(f"Error updating contract {contract_key}: {e}")

        # Update the `devices_contract` table
        if 'device' in updated_fields:

            device_value = updated_fields['device']

            if device_value is None:
                print(f"Device is None for contract {contract_key}. Skipping devices_contract update.")
                continue

            # Fetch the foreign key for the device
            device_id = fetch_foreign_key(
                engine,
                'SELECT id_device as id, id_op_device as id_op FROM devices',
                device_value
            )

            if device_id is None:
                print(f"Device {device_value} not found in devices table.")
                continue

            # Intentar extraer created_voucher_start de forma segura
            created_voucher_start = None
            if isinstance(updated_fields.get('info'), dict):
                created_voucher_start = updated_fields['info'].get('voucherCreatedAt')

            # Actualizar el último registro de devices_contract (si existe)
            last_device_query = text("""
                SELECT id_device_contract 
                FROM devices_contract
                WHERE id_device = :id_device
                ORDER BY created_voucher_start DESC
                LIMIT 1
            """)
            last_device_update = text("""
                UPDATE devices_contract
                SET created_voucher_end = :created_voucher_end
                WHERE id_device_contract = :id_device_contract
            """)

            try:
                with engine.connect() as connection:
                    id_device_python = int(device_id)

                    last_device = connection.execute(last_device_query, {'id_device': id_device_python}).fetchone()
                    if last_device:
                        connection.execute(last_device_update, {
                            'id_device_contract': last_device['id_device_contract'],
                            'created_voucher_end': datetime.now()  # ✅ aquí estaba el bug
                        })
                        print(f"Updated end date for previous device_contract ID: {last_device['id_device_contract']}")
                    else:
                        print("No previous device_contract found, skipping end date update.")
            except Exception as e:
                print(f"Error updating end date for last device record: {e}")

            # Insert or update the current record
            insert_query = text("""
                INSERT INTO devices_contract (id_device, id_contract, created_voucher_start, created_voucher_end, updated_at)
                VALUES (:id_device, :id_contract, :created_voucher_start, :created_voucher_end, :updated_at)
                ON CONFLICT (id_device, id_contract) DO UPDATE
                SET created_voucher_start = EXCLUDED.created_voucher_start,
                    created_voucher_end = EXCLUDED.created_voucher_end,
                    updated_at = EXCLUDED.updated_at
            """)

            data_to_insert = {
                'id_device': device_id,
                'id_contract': id_contract,
                'created_voucher_start': created_voucher_start,
                'created_voucher_end': updated_fields.get('end'),
                'updated_at': datetime.now()
            }

            data_to_insert = convert_types(data_to_insert)

            try:
                with engine.connect() as connection:
                    connection.execute(insert_query, **data_to_insert)
                print(f"Device-Contract updated or inserted: {data_to_insert}")
            except Exception as e:
                print(f"Error inserting/updating device-contract: {e}")
        else:
            print(f"No device valid for relation with contract {id_contract}. Skipping update.")
            return
    return {
        'statusCode': 200,
        'body': json.dumps('Contracts and Device-Contracts processed successfully')
    }
