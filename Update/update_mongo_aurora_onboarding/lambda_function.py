#import sys
import boto3
import json
#import os
import gc
import numpy as np
import sqlalchemy as db
import pymongo
import pandas as pd
import logging

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from pytz import timezone
#from typing import Optional, Dict
from dateutil import parser
from bson.objectid import ObjectId

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

def fetch_foreign_key(engine, query_str, key):
    try:
        print(f"Fetching foreign key for {key} using query: {query_str}")
        result = pd.read_sql_query(query_str, con=engine, params=key)
        return result.iloc[0]['id']
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

def parse_record(record):
    try:
        body = json.loads(record.get('body', '{}'))
        message_str = body.get('message', '[]')
        messages = json.loads(message_str)
        if not isinstance(messages, list):
            print("Parsed messages is not a list.")
            return None
        return messages
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON from record: {record.get('body')}. Error: {e}")
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

def parse_datetime_to_pg(value):
    if not value:
        return None
    try:
        dt = parser.isoparse(value)

        if dt.tzinfo:
            dt = dt.astimezone(tz=None)  # Esto convierte a naive UTC

        dt = dt.replace(tzinfo=None)

        return dt
    except Exception as e:
        print(f"Error parsing datetime: {e}")
        return None
    
def upsert_onboarding(engine, id_contract, term_record, created_user_date):
    document_type = term_record.get('documentType')
    permission = term_record.get('permission', False)

    ##created_voucher = created_voucher
    created_user_pg = parse_datetime_to_pg(created_user_date)

    timestamp_now = datetime.now()

    params = {
        'id_contract': id_contract,
        'document_type': document_type,
        'permission': permission,
        ##'created_voucher': created_voucher,
        'created_user': created_user_pg,
        'created_code': None,
        'activation_voucher': None,
        'updated_at': timestamp_now
    }

    set_clauses = []
    update_params = {'id_contract': id_contract, 'document_type': document_type, 'updated_at': timestamp_now}

    for field in ['permission',  'created_user', 'activation_voucher']:
        value = params[field]
        if value is not None:
            set_clauses.append(f"{field} = :{field}")
            update_params[field] = value

    set_clauses.append("updated_at = :updated_at")
    update_params = convert_types(update_params)

    update_query = f"""
        UPDATE onboarding
        SET {', '.join(set_clauses)}
        WHERE id_contract = :id_contract AND document_type = :document_type
    """

    try:
        with engine.connect() as connection:
            result = connection.execute(text(update_query), update_params)
            if result.rowcount == 0:
                insert_params = convert_types(params)
                insert_query = """
                    INSERT INTO onboarding (
                        id_contract, document_type, permission, created_user, created_code, 
                        activation_voucher, updated_at
                    ) VALUES (
                        :id_contract, :document_type, :permission, :created_user, :created_code, 
                        :activation_voucher,:updated_at
                    )
                """
                connection.execute(text(insert_query), insert_params)
                print(f"Inserted onboarding for id_contract={id_contract}, document_type={document_type}")
            else:
                print(f"Updated onboarding for id_contract={id_contract}, document_type={document_type}")
    except Exception as e:
        print(f"Error in upsert onboarding for id_contract={id_contract}, document_type={document_type}: {e}")

    return params

def lambda_handler(event, context):
    try:
        engine, mydb = connect_db('prod_rds_data_production_rw','mongo_production_rw')
        print('Process event:', event)

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

            id_contract = None
            query_id_contract = "SELECT id_contract FROM contracts WHERE id_op_contract = :id_op_contract"
            try:
                with engine.connect() as connection:
                    result = connection.execute(text(query_id_contract), {'id_op_contract': contract_key}).fetchone()
                    if result:
                        id_contract = result['id_contract']
                        print(f"Fetched id_contract: {id_contract} for contract_key: {contract_key}")
            except Exception as e:
                print(f"Error fetching id_contract for contract_key {contract_key}: {e}")
                continue

            if not id_contract:
                print(f"No id_contract found for {contract_key}, skipping record.")
                continue

            user_id = None
            query_id_op_user = """
                SELECT id_op_user
                FROM users_person
                INNER JOIN contracts ON contracts.id_user = users_person.id_user
                WHERE id_op_contract = :id_op_contract
            """
            try:
                with engine.connect() as connection:
                    result = connection.execute(text(query_id_op_user), {'id_op_contract': contract_key}).fetchone()
                    if result:
                        user_id = result['id_op_user']
                        print(f"Fetched id_op_user: {user_id} for contract_key: {contract_key}")
            except Exception as e:
                print(f"Error fetching user_id for contract_key {contract_key}: {e}")
                continue

            created_user_date = None
            if user_id:
                try:
                    created_user_dt = ObjectId(user_id).generation_time
                    created_user_date = created_user_dt.isoformat(timespec='seconds')
                except Exception as e:
                    print(f"Error extracting or formatting creation time from user ObjectId: {e}")

            # serv_con = mydb['servicecontracts']
            # created_v_data = serv_con.find_one({'_id': ObjectId(contract_key)},{'info.voucherCreatedAt': 1})
            # if created_v_data:
            #     created_voucher = created_v_data.get('info', {}).get('voucherCreatedAt')
            #     if isinstance(created_voucher, datetime):
            #         created_voucher = created_voucher.isoformat(timespec='seconds')
            #     elif isinstance(created_voucher, str):
            #         created_voucher = parse_datetime_to_pg(created_voucher)
            #     else:
            #         print(f"Invalid type for 'voucherCreatedAt': {type(created_voucher)}")
            # else:
            #     print(f"No data found for contract_key: {contract_key}")
            #     continue

            metadata = updated_fields.get('metadata', {})
            accepted_terms = metadata.get('acceptedTerms', [])
            for term_record in accepted_terms:
                value = term_record.get('documentType')
                term_record['permission'] = value not in [None, '', 'null']
                params = upsert_onboarding(engine, id_contract, term_record, created_user_date)#, created_voucher
        
        response = {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Processing completed successfully",
                "result": params
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
