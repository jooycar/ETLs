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
from dateutil.parser import parse as parse_date

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def connect_db(aurora_secret_name, mongo_secret_name=None):
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

    if mongo_secret_name is not None:
        mongo_cred = json.loads(aws_client.get_secret_value(SecretId=mongo_secret_name)['SecretString'])
        STRING_MONGO = mongo_cred['STRING_MONGO']
        mongo_client = pymongo.MongoClient(STRING_MONGO)
        mydb = mongo_client['production-01']
        return engine, mydb

    return engine


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
        logger.error(f"Error parsing datetime: {e}")
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


def upsert_usr_rwd_mtd(engine, id_user_reward, updated_fields):
    metadata = extract_metadata(updated_fields)
    if not metadata:
        print("No metadata found in updated_fields, skipping.")
        return None

    generation_att = metadata.get('cashbackListGenerationAttempts')
    payment_att = metadata.get('cashbackPaymentAttempts')
    payment_dates = metadata.get('cashbackPaymentAttemptDates', [])
    payment_order_created_at = metadata.get('paymentOrderCreatedAt')
    expires_at = metadata.get('expiresAt')
    type_value = metadata.get('type')
    error_reason = metadata.get('errorReason')

    init_date_att = None
    last_date_att = None

    if isinstance(payment_dates, list):
        try:
            if len(payment_dates) >= 1:
                init_date_att = parse_date(payment_dates[0])
            if len(payment_dates) >= 2:
                last_date_att = parse_date(payment_dates[-1])
            elif len(payment_dates) == 1:
                last_date_att = init_date_att
        except Exception as e:
            print(f"Error parsing payment dates: {payment_dates} - {e}")

    if not init_date_att and payment_order_created_at:
        try:
            init_date_att = parse_date(payment_order_created_at)
        except Exception as e:
            print(f"Error parsing paymentOrderCreatedAt: {payment_order_created_at} - {e}")

    if type_value == 'claimable' and expires_at:
        try:
            last_date_att = datetime.fromtimestamp(expires_at / 1000.0)
        except Exception as e:
            print(f"Error parsing expiresAt: {expires_at} - {e}")

    reason_parts = []
    if type_value:
        reason_parts.append(str(type_value))
    if error_reason:
        reason_parts.append(str(error_reason))
    reason = ' | '.join(reason_parts) if reason_parts else None

    if not any([generation_att, payment_att, init_date_att, last_date_att, reason]):
        print("No valid metadata values to persist. Skipping.")
        return None

    timestamp_now = datetime.now()

    params = {
        'id_user_reward': id_user_reward,
        'generation_att': generation_att,
        'payment_att': payment_att,
        'init_date_att': init_date_att,
        'last_date_att': last_date_att,
        'reason': reason,
        'created_at': init_date_att,
        'updated_at': timestamp_now
    }

    result = None
    update_query = """
        UPDATE user_rewards_metadata
        SET generation_att = :generation_att,
            payment_att = :payment_att,
            init_date_att = :init_date_att,
            last_date_att = :last_date_att,
            updated_at = :updated_at
        WHERE id_user_reward = :id_user_reward
          AND reason = :reason
    """

    insert_query = """
        INSERT INTO user_rewards_metadata (
            id_user_reward, generation_att, payment_att,
            init_date_att, last_date_att, reason, created_at, updated_at
        ) VALUES (
            :id_user_reward, :generation_att, :payment_att,
            :init_date_att, :last_date_att, :reason, :created_at, :updated_at
        )
    """

    try:
        with engine.connect() as connection:
            result = connection.execute(text(update_query), params)
            if result.rowcount == 0:
                connection.execute(text(insert_query), params)
                print(f"Inserted new metadata for id_user_reward={id_user_reward}")
            else:
                print(f"Updated metadata for id_user_reward={id_user_reward}")
    except Exception as e:
        logger.exception(f"Error upserting metadata for id_user_reward={id_user_reward}")

    return params


def lambda_handler(event, context):
    params = None 
    try:
        engine = connect_db('prod_rds_data_production_rw')
        print('Process event:', event)

        for record in event['Records']:
            try:
                body = json.loads(record['body']) if isinstance(record['body'], str) else record['body']
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON body: {record['body']}, Error: {e}")
                continue

            user_reward_key = body['documentKey']['_id']
            updated_fields = body.get('updateDescription', {}).get('updatedFields', {})

            id_user_reward = None
            query_id_usr_rwrd = "SELECT id_user_reward, id_user FROM user_rewards WHERE id_op_user_reward = :id_op_user_reward"
            try:
                with engine.connect() as connection:
                    result = connection.execute(text(query_id_usr_rwrd), {'id_op_user_reward': user_reward_key}).fetchone()
                    if result:
                        id_user_reward = result['id_user_reward']
                        print(f"Fetched id_user_reward: {id_user_reward} for user_reward_key: {user_reward_key} in Aurora")
            except Exception as e:
                logger.exception(f"Error fetching id_user_reward for user_reward_key {user_reward_key}")
                continue

            if not id_user_reward:
                print(f"No id_user_reward and id_user found for {user_reward_key}, skipping record.")
                continue

            params = upsert_usr_rwd_mtd(engine, id_user_reward, updated_fields)
            if params is None:
                logger.warning(f"No result returned for upsert_usr_rwd_mtd with id_user_reward={id_user_reward}")
            else:
                print(f"Updated user reward params: {params}")

        response = {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Processing completed successfully",
                "result": "ok"
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
