import sys
import boto3
import json
import gc
import numpy as np
import pymongo
import pandas as pd
import logging

from dateutil import parser
from bson.objectid import ObjectId
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

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

def lambda_handler(event, context):
    engine = connect_db('prod_rds_data_production_rw')

    for record in event['Records']:
        try:
            body = json.loads(record['body']) if isinstance(record['body'], str) else record['body']
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON body: {record['body']}, Error: {e}")
            continue

        if 'detail' not in body or 'fullDocument' not in body['detail']:
            print("Skipping record due to missing 'detail' or 'fullDocument'")
            continue

        full_document = body['detail']['fullDocument']
        print(f"Processing full document: {full_document}")

        try:
            user_key = full_document['user']
            campaign_key = full_document['campaign']
            reward_key = full_document['reward']
            id_op_user_reward = full_document['_id']
            status_user_reward = full_document.get('status')
            amount_user_reward = full_document.get('amount', 0)

            query_user = "SELECT id_user as id, id_op_user as id_op FROM users_person"
            query_reward = "SELECT id_reward as id, id_op_reward as id_op FROM rewards"
            query_campaign = "SELECT id_campaign as id, id_op_campaign as id_op FROM campaigns"

            with engine.connect() as conn:
                id_user = fetch_foreign_key(conn, query_user, user_key)
                id_reward = fetch_foreign_key(conn, query_reward, reward_key)
                id_campaign = fetch_foreign_key(conn, query_campaign, campaign_key)

                if not id_user or not id_reward or not id_campaign:
                    print("Missing foreign keys, skipping record.")
                    continue

                query_rw_cmp = text("""
                    SELECT id_reward_campaign 
                    FROM reward_campaigns 
                    WHERE id_campaign = :id_campaign AND id_reward = :id_reward
                """)
                result = conn.execute(query_rw_cmp, {
                    "id_campaign": int(id_campaign),
                    "id_reward": int(id_reward)
                }).fetchone()

                id_reward_campaign = result[0] if result else None
                if not id_reward_campaign:
                    print("No matching reward_campaign, skipping.")
                    continue

                print(f"reward_campaign key: {id_reward_campaign}")

                try:
                    created_at = ObjectId(id_op_user_reward).generation_time.isoformat(timespec='seconds')
                except Exception as e:
                    print(f"Error extracting date from ObjectId: {e}")
                    continue

                insert_query = text("""
                    INSERT INTO user_rewards (
                        id_op_user_reward, id_user, id_reward_campaign, 
                        status_user_reward, amount_user_reward, created_at
                    ) VALUES (
                        :id_op_user_reward, :id_user, :id_reward_campaign, 
                        :status_user_reward, :amount_user_reward, :created_at
                    )
                """)

                data_to_insert = {
                    'id_op_user_reward': id_op_user_reward,
                    'id_user': id_user,
                    'id_reward_campaign': id_reward_campaign,
                    'status_user_reward': status_user_reward,
                    'amount_user_reward': amount_user_reward,
                    'created_at': parse_datetime_to_pg(created_at)
                }

                data_to_insert = convert_types(data_to_insert)

                try:
                    conn.execute(insert_query, **data_to_insert)
                    print(f"Inserted user_reward: {data_to_insert}")
                except Exception as e:
                    print(f"Error inserting data: {e}")
                    continue
        
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
