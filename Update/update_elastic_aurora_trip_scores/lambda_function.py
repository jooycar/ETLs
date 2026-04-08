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

# funciones generales
def fetch_foreign_key(engine, query, trip_key):
    try:
        if not hasattr(engine, 'execute'):
            raise ValueError("Invalid engine object passed. Ensure it is an SQLAlchemy engine.")

        df = pd.read_sql(query, con=engine)
        result = df.loc[df['id_op'] == trip_key, 'id']
        if result.empty:
            raise ValueError(f"No foreign key found for trip_key: {trip_key}")
        return result.iloc[0]
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
                elif isinstance(value, (int, float)) and 'date' in key.lower():
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


def send_to_sqs(message_body, queue_url):
    try:
        sqs_client = boto3.client('sqs')
        response = sqs_client.send_message(
            QueueUrl=queue_url,
            MessageBody=json.dumps(message_body, default=str)
        )
        print(f"Message sent to SQS. MessageId: {response.get('MessageId')}")
        return response
    except Exception as e:
        print(f"Error sending message to SQS: {e}")
        raise

def safe_json_loads(x):
    try:
        if isinstance(x, str) and x.strip():
            return json.loads(x)
        return x
    except json.JSONDecodeError:
        print(f"Invalid JSON: {x}")
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

def keep_if_not_null_or_zero(current, new):
    """
    Devuelve 'new' solo si es distinto de None y de 0.
    Si no, mantiene 'current'.
    """
    if new is None:
        return current
    if isinstance(new, (int, float)) and new == 0:
        return current
    return new

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat() 
        elif isinstance(obj, (np.generic, pd.Timestamp)): 
            return obj.item() if isinstance(obj, np.generic) else obj.isoformat()
        elif isinstance(obj, bytes):
            return obj.decode('utf-8')
        return super().default(obj)

def insert_score(engine, id_trip, trip_key, update_data, timeRef_dt):
    try:
        insert_query = text("""
            INSERT INTO trip_scores (
                id_op_trip_score, id_trip, created, type_sec, period, amount, 
                octo_score, octo_cornering, octo_hard_braking, octo_hard_acceleration, octo_speeding,
                octo_bh_score, octo_bh_cornering_score, octo_bh_smoothness_score, octo_bh_braking_score,
                octo_bh_phoneuse_score, octo_da_score, octo_da_distance_score, octo_da_roads_score, 
                octo_da_timeofday_score, octo_da_cornering_score, octo_da_smoothness_score, octo_da_braking_score,
                octo_da_weather_score, octo_da_phoneuse_score
            ) VALUES (
                :id_op_trip_score, :id_trip, :created, :type_sec, :period, :amount,
                :octo_score, :octo_cornering, :octo_hard_braking, :octo_hard_acceleration, :octo_speeding,
                :octo_bh_score, :octo_bh_cornering_score, :octo_bh_smoothness_score, :octo_bh_braking_score,
                :octo_bh_phoneuse_score, :octo_da_score, :octo_da_distance_score, :octo_da_roads_score, 
                :octo_da_timeofday_score, :octo_da_cornering_score, :octo_da_smoothness_score, :octo_da_braking_score,
                :octo_da_weather_score, :octo_da_phoneuse_score
            )
        """)

        score_data = update_data['scoring']['scores'][0]

        octo_score = None
        octo_cornering = None
        octo_hard_braking = None
        octo_hard_acceleration = None
        octo_speeding = None
        octo_bh_score = None
        octo_bh_cornering_score = None
        octo_bh_smoothness_score = None
        octo_bh_braking_score = None
        octo_bh_phoneuse_score = None
        octo_da_score = None
        octo_da_distance_score = None
        octo_da_roads_score = None
        octo_da_timeofday_score = None
        octo_da_cornering_score = None
        octo_da_smoothness_score = None
        octo_da_braking_score = None
        octo_da_weather_score = None
        octo_da_phoneuse_score = None

        for item in score_data['accumulableValues']:
            val = item.get('value', None)
            # Saltamos 0 y None
            if val is None or (isinstance(val, (int, float)) and val == 0):
                continue

            if item['name'] == 'octo_score':
                octo_score = val
            elif item['name'] == 'octo_cornering_score':
                octo_cornering = val
            elif item['name'] == 'octo_hard_braking_score':
                octo_hard_braking = val
            elif item['name'] == 'octo_harsh_acceleration_score':
                octo_hard_acceleration = val
            elif item['name'] == 'octo_speeding_score':
                octo_speeding = val
            elif item['name'] == 'octo_bh_score':
                octo_bh_score = val
            elif item['name'] == 'octo_bh_cornering_score':
                octo_bh_cornering_score = val
            elif item['name'] == 'octo_bh_smoothness_score':
                octo_bh_smoothness_score = val
            elif item['name'] == 'octo_bh_braking_score':
                octo_bh_braking_score = val
            elif item['name'] == 'octo_bh_phoneuse_score':
                octo_bh_phoneuse_score = val
            elif item['name'] == 'octo_da_score':
                octo_da_score = val
            elif item['name'] == 'octo_da_distance_score':
                octo_da_distance_score = val
            elif item['name'] == 'octo_da_roads_score':
                octo_da_roads_score = val
            elif item['name'] == 'octo_da_timeofday_score':
                octo_da_timeofday_score = val
            elif item['name'] == 'octo_da_cornering_score':
                octo_da_cornering_score = val
            elif item['name'] == 'octo_da_smoothness_score':
                octo_da_smoothness_score = val
            elif item['name'] == 'octo_da_braking_score':
                octo_da_braking_score = val
            elif item['name'] == 'octo_da_weather_score':
                octo_da_weather_score = val
            elif item['name'] == 'octo_da_phoneuse_score':
                octo_da_phoneuse_score = val

        # amount: también evitando 0/None
        amount = keep_if_not_null_or_zero(None, score_data.get('distance'))

        data_to_insert = {
            'id_op_trip_score': trip_key, 
            'id_trip': id_trip,
            'created': timeRef_dt,
            'type_sec': 'wings:assistance',
            'period': update_data['scoring']['periods'][0],
            'amount': amount,
            'octo_score': octo_score,
            'octo_cornering': octo_cornering,
            'octo_hard_braking': octo_hard_braking,
            'octo_hard_acceleration': octo_hard_acceleration,
            'octo_speeding': octo_speeding,
            'octo_bh_score': octo_bh_score,
            'octo_bh_cornering_score': octo_bh_cornering_score,
            'octo_bh_smoothness_score': octo_bh_smoothness_score,
            'octo_bh_braking_score': octo_bh_braking_score,
            'octo_bh_phoneuse_score': octo_bh_phoneuse_score,
            'octo_da_score': octo_da_score,
            'octo_da_distance_score': octo_da_distance_score,
            'octo_da_roads_score': octo_da_roads_score,
            'octo_da_timeofday_score': octo_da_timeofday_score,
            'octo_da_cornering_score': octo_da_cornering_score,
            'octo_da_smoothness_score': octo_da_smoothness_score,
            'octo_da_braking_score': octo_da_braking_score,
            'octo_da_weather_score': octo_da_weather_score,
            'octo_da_phoneuse_score': octo_da_phoneuse_score
        }

        data_to_insert = convert_types(data_to_insert)

        try:
            with engine.connect() as connection:
                connection.execute(insert_query, **data_to_insert)
            print(f"Score inserted: {data_to_insert}")
        except Exception as e:
            print(f"Error inserting Score: {e}")
    except Exception as e:
        print(f"Error inserting Score (outer): {e}")

    return data_to_insert


def update_score(engine, id_trip, trip_key, update_data, timeRef_dt):
    try:
        # =========================
        # 1) Parseo de los scores
        # =========================
        octo_score = None
        octo_cornering = None
        octo_hard_braking = None
        octo_hard_acceleration = None
        octo_speeding = None
        octo_bh_score = None
        octo_bh_cornering_score = None
        octo_bh_smoothness_score = None
        octo_bh_braking_score = None
        octo_bh_phoneuse_score = None
        octo_da_score = None
        octo_da_distance_score = None
        octo_da_roads_score = None
        octo_da_timeofday_score = None
        octo_da_cornering_score = None
        octo_da_smoothness_score = None
        octo_da_braking_score = None
        octo_da_weather_score = None
        octo_da_phoneuse_score = None

        amount = None
        period = None

        scoring = update_data.get('scoring', {})
        periods = scoring.get('periods', [])
        if periods:
            period = periods[0]

        # Recorremos cada bloque de score
        for score_block in scoring.get('scores', []):
            # posible distance (amount) solo desde bloque principal
            if score_block.get('defaultAccumulableValueName') == 'octo_score':
                amount = keep_if_not_null_or_zero(amount, score_block.get('distance'))

            values_dict = {item['name']: item['value'] for item in score_block.get('accumulableValues', [])}

            default_name = score_block.get('defaultAccumulableValueName')

            if default_name == "octo_score":
                octo_score = keep_if_not_null_or_zero(octo_score, values_dict.get("octo_score"))
                octo_cornering = keep_if_not_null_or_zero(octo_cornering, values_dict.get("octo_cornering_score"))
                octo_hard_braking = keep_if_not_null_or_zero(octo_hard_braking, values_dict.get("octo_hard_braking_score"))
                octo_hard_acceleration = keep_if_not_null_or_zero(octo_hard_acceleration, values_dict.get("octo_harsh_acceleration_score"))
                octo_speeding = keep_if_not_null_or_zero(octo_speeding, values_dict.get("octo_speeding_score"))

            elif default_name == "octo_bh_score":
                octo_bh_score = keep_if_not_null_or_zero(octo_bh_score, values_dict.get("octo_bh_score"))
                octo_bh_cornering_score = keep_if_not_null_or_zero(octo_bh_cornering_score, values_dict.get("octo_bh_cornering_score"))
                octo_bh_smoothness_score = keep_if_not_null_or_zero(octo_bh_smoothness_score, values_dict.get("octo_bh_smoothness_score"))
                octo_bh_braking_score = keep_if_not_null_or_zero(octo_bh_braking_score, values_dict.get("octo_bh_braking_score"))
                octo_bh_phoneuse_score = keep_if_not_null_or_zero(octo_bh_phoneuse_score, values_dict.get("octo_bh_phoneuse_score"))

            elif default_name == "octo_da_score":
                octo_da_score = keep_if_not_null_or_zero(octo_da_score, values_dict.get("octo_da_score"))
                octo_da_distance_score = keep_if_not_null_or_zero(octo_da_distance_score, values_dict.get("octo_da_distance_score"))
                octo_da_roads_score = keep_if_not_null_or_zero(octo_da_roads_score, values_dict.get("octo_da_roads_score"))
                octo_da_timeofday_score = keep_if_not_null_or_zero(octo_da_timeofday_score, values_dict.get("octo_da_timeofday_score"))
                octo_da_cornering_score = keep_if_not_null_or_zero(octo_da_cornering_score, values_dict.get("octo_da_cornering_score"))
                octo_da_smoothness_score = keep_if_not_null_or_zero(octo_da_smoothness_score, values_dict.get("octo_da_smoothness_score"))
                octo_da_braking_score = keep_if_not_null_or_zero(octo_da_braking_score, values_dict.get("octo_da_braking_score"))
                octo_da_weather_score = keep_if_not_null_or_zero(octo_da_weather_score, values_dict.get("octo_da_weather_score"))
                octo_da_phoneuse_score = keep_if_not_null_or_zero(octo_da_phoneuse_score, values_dict.get("octo_da_phoneuse_score"))

        # =========================
        # 2) Parámetros para UPDATE
        # =========================
        params = {
            'id_op_trip_score': trip_key, 
            'id_trip': id_trip,
            'period': period,
            'amount': amount,
            'octo_score': octo_score,
            'octo_cornering': octo_cornering,
            'octo_hard_braking': octo_hard_braking,
            'octo_hard_acceleration': octo_hard_acceleration,
            'octo_speeding': octo_speeding,
            'octo_bh_score': octo_bh_score,
            'octo_bh_cornering_score': octo_bh_cornering_score,
            'octo_bh_smoothness_score': octo_bh_smoothness_score,
            'octo_bh_braking_score': octo_bh_braking_score,
            'octo_bh_phoneuse_score': octo_bh_phoneuse_score,
            'octo_da_score': octo_da_score,
            'octo_da_distance_score': octo_da_distance_score,
            'octo_da_roads_score': octo_da_roads_score,
            'octo_da_timeofday_score': octo_da_timeofday_score,
            'octo_da_cornering_score': octo_da_cornering_score,
            'octo_da_smoothness_score': octo_da_smoothness_score,
            'octo_da_braking_score': octo_da_braking_score,
            'octo_da_weather_score': octo_da_weather_score,
            'octo_da_phoneuse_score': octo_da_phoneuse_score,
            'updated_at': datetime.now()
        }

        params = convert_types(params)

        # =========================
        # 3) UPDATE condicional
        # =========================

        # columnas numéricas donde queremos ignorar 0/None
        numeric_cols = [
            'amount',
            'octo_score', 'octo_cornering', 'octo_hard_braking', 'octo_hard_acceleration', 'octo_speeding',
            'octo_bh_score', 'octo_bh_cornering_score', 'octo_bh_smoothness_score', 'octo_bh_braking_score',
            'octo_bh_phoneuse_score',
            'octo_da_score', 'octo_da_distance_score', 'octo_da_roads_score', 'octo_da_timeofday_score',
            'octo_da_cornering_score', 'octo_da_smoothness_score', 'octo_da_braking_score',
            'octo_da_weather_score', 'octo_da_phoneuse_score'
        ]

        set_clauses = []

        # period: acá asumimos que no viene como 0, solo None o string
        set_clauses.append("period = COALESCE(%(period)s, period)")

        # todas las numéricas: solo se actualizan si el nuevo valor != 0 y != NULL
        for col in numeric_cols:
            set_clauses.append(
                f"{col} = COALESCE(NULLIF(%({col})s, 0), {col})"
            )

        # updated_at siempre
        set_clauses.append("updated_at = %(updated_at)s")

        update_query = f"""
            UPDATE trip_scores
            SET {', '.join(set_clauses)}
            WHERE id_op_trip_score = %(id_op_trip_score)s
              AND id_trip = %(id_trip)s
        """

        with engine.connect() as connection:
            connection.execute(update_query, params)
            print(f"Score updated successfully: {params}")

    except Exception as e:
        print(f"Score updating Anomaly: {e}")

    return params

def push_trip(event):
    engine = connect_db('prod_rds_data_production_rw')

    for record in event:
        try:
            body = json.loads(record['body'])
            record = body['message']
            id_trip = record['id_trip']
            trip_key = record['tripId']
            update_data = record['updateData']
            if trip_key is None or id_trip is None:
                raise ValueError("Missing 'tripId' or 'id_trip' in message")
        except json.JSONDecodeError as e:
            print(f"JSON parsing error: {e}")
        except KeyError as e:
            print(f"Missing expected key: {e}")
        except Exception as e:
            print(f"Error processing record: {e}")
        continue

    created_at = record.get('createdAt', None)

    if created_at is None:
        timeRef_dt = datetime.now()
        timeRef_dt = timeRef_dt - timedelta(days=10)
    else:
        if isinstance(created_at, int):
            timeRef_dt = datetime.fromtimestamp(created_at / 1000.0)
        elif isinstance(created_at, str):
            timeRef_dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
        else:
            raise TypeError(f"Unsupported type for 'createdAt': {type(created_at)}")

    timeRef_date = timeRef_dt.date()

    query_trip = f"""
        SELECT id_op_trip_score as id_op
        FROM trip_scores
        WHERE id_op_trip_score = '{trip_key}' AND DATE(created) >= '{timeRef_date}'
        LIMIT 1
    """

    df = pd.read_sql(query_trip, con=engine)
    if df.empty:
        df = insert_score(engine, id_trip, trip_key, update_data, timeRef_dt)
    else: 
        df = update_score(engine, id_trip, trip_key, update_data, timeRef_dt)
    return df

def lambda_handler(event, context):
    try:
        print('Procesing event', json.dumps(event))
        
        if 'Records' not in event:
            raise ValueError("Event does not contain 'Records' key")
        
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


