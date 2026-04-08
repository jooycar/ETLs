import sys
import boto3
import json
import gc
import numpy as np
import sqlalchemy as db
import pymongo
import pandas as pd
import logging

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from dateutil import parser

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

def fetch_foreign_key(conn, query, key):
    try:
        df = pd.read_sql(query, con=conn)
        match = df[df['id_op'] == key]
        if not match.empty:
            return match.iloc[0]['id']
        print(f"No match found for key: {key}")
        return None
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
            dt = dt.astimezone(tz=None)
        dt = dt.replace(tzinfo=None)
        return dt
    except Exception as e:
        print(f"Error parsing datetime: {e}")
        return None
    
def extract_metadata(updated_fields):
    metadata = {}
    for k, v in updated_fields.items():
        if k.startswith("metadata."):
            keys = k.split(".")[1:]
            current = metadata
            for key in keys[:-1]:
                current = current.setdefault(key, {})
            current[keys[-1]] = v
    return metadata
    
def upsert_usr_rwd(engine, user_reward_key, updated_fields):
    status = updated_fields.get('status')
    amount = updated_fields.get('amount')

    timestamp_now = datetime.now()

    params = {
        'status_user_reward': status,
        'updated_at': timestamp_now
    }

    # Validar si amount es un integer válido distinto de 0 y None
    if isinstance(amount, int) and amount != 0:
        params['amount_user_reward'] = amount

    set_clauses = []
    update_params = {
        'id_op_user_reward': user_reward_key,
        'updated_at': timestamp_now
    }

    if status is not None:
        set_clauses.append("status_user_reward = :status_user_reward")
        update_params['status_user_reward'] = status

    if 'amount_user_reward' in params:
        set_clauses.append("amount_user_reward = :amount_user_reward")
        update_params['amount_user_reward'] = params['amount_user_reward']

    set_clauses.append("updated_at = :updated_at")

    update_params = convert_types(update_params)
    print(f"Update params: {update_params}")

    update_query = f"""
        UPDATE user_rewards
        SET {', '.join(set_clauses)}
        WHERE id_op_user_reward = :id_op_user_reward
    """

    try:
        with engine.connect() as connection:
            result = connection.execute(text(update_query), update_params)
        print(f"Updated user_rewards for id_op_user_reward={user_reward_key}, {update_params}")
        return result
    except Exception as e:
        logger.exception(f"Error in update user_rewards for id_op_user_reward={user_reward_key}")
        return None


def lambda_handler(event, context):
    try:
        queue_usr_rwdr_meta = 'https://sqs.us-east-1.amazonaws.com/045511714637/MongoUpdateSQSUserRewards_Metadata'
        engine = connect_db('prod_rds_data_production_rw')
        print('Processing incoming event:', event)

        for record in event['Records']:
            try:
                body = json.loads(record['body']) if isinstance(record['body'], str) else record['body']
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON body: {record['body']} - {e}")
                continue

            if 'detail' not in body:
                print("Skipping record due to missing 'detail'")
                continue

            full_document = body['detail']
            user_reward_key = full_document['documentKey']['_id']
            updated_fields = full_document.get('updateDescription', {}).get('updatedFields', {})

            if not updated_fields:
                print(f"No fields to update for user_reward_key: {user_reward_key}")
                continue

            params = upsert_usr_rwd(engine, user_reward_key, updated_fields)
            print(f"Upsert complete for user_reward_key={user_reward_key}")

            metadata = extract_metadata(updated_fields)
            print(f"Extracted metadata for user_reward_key={user_reward_key}: {metadata}")

            if metadata:
                send_to_sqs(full_document, queue_usr_rwdr_meta)
                print(f"Metadata detected. Sent to SQS: {queue_usr_rwdr_meta}")

        response = {
            "statusCode": 200,
            "body": json.dumps("Processing completed successfully")
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
        logger.exception("Database error occurred")
        response = {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Database operation failed",
                "error": str(e)
            }, cls=CustomJSONEncoder)
        }

    except Exception as e:
        logger.exception("Unexpected error occurred")
        response = {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Internal server error",
                "error": str(e)
            }, cls=CustomJSONEncoder)
        }

    finally:
        gc.collect()

    print(f"Lambda response: {response}")
    return response
