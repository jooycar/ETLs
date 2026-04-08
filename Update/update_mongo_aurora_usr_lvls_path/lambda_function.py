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
    
def parse_ts_safe(value):
    if not value:
        return None
    try:
        return parse_date(value).replace(tzinfo=None)
    except Exception:
        return None

def get_id_level(connection, id_op_level):
    """
    Busca el id_level en Aurora por el id de Mongo (id_op_level).
    """
    q = text("""
        SELECT id_level
        FROM levels
        WHERE id_op_level = :id_op_level
        LIMIT 1
    """)
    row = connection.execute(q, {"id_op_level": id_op_level}).fetchone()
    return row["id_level"] if row else None

def resolve_id_user(connection, id_op_user_levels_path, detail):
    """
    1) Reusa el id_user si ya existe al menos una fila en user_levels_path para este path.
    2) (Opcional/TO-DO) Intenta leerlo desde el evento si viene en fullDocument u otro campo.
    3) Si no lo encuentra, retorna None y el caller decide qué hacer.
    """
    # 1) ¿Ya existe?
    q = text("""
        SELECT id_user
        FROM user_levels_path
        WHERE id_op_user_levels_path = :id_op
        LIMIT 1
    """)
    row = connection.execute(q, {"id_op": id_op_user_levels_path}).fetchone()
    if row:
        return row["id_user"]


def upsert_user_level_item(connection, id_op_user_levels_path, id_user, level_item, now_ts):
    """
    Inserta/actualiza UNA fila de user_levels_path para (id_op_user_levels_path, id_level, iteration).
    Requiere que exista el índice único (id_op_user_levels_path, id_level, iteration_user_lvl).
    """

    id_op_level = level_item.get("level")
    if not id_op_level:
        print(f"Skipping level without 'level' (id_op_level) => {level_item}")
        return

    id_level = get_id_level(connection, id_op_level)
    if not id_level:
        print(f"No se encontró id_level para id_op_level={id_op_level}. Skipping.")
        return

    start_ts = parse_ts_safe(level_item.get("start"))
    end_ts = parse_ts_safe(level_item.get("end"))
    status = level_item.get("status")
    iteration = level_item.get("iteration")
    points = level_item.get("points", {}) or {}
    req_points = points.get("required")
    acc_points = points.get("accumulated")

    params = {
        "id_op_user_levels_path": id_op_user_levels_path,
        "id_user": id_user,
        "id_level": id_level,
        "start_user_lvl": start_ts,
        "end_user_lvl": end_ts,
        "status_user_lvl": status,
        "iteration_user_lvl": iteration,
        "point_lvl_required": req_points,
        "point_lvl_accumlated": acc_points,
        "created_at": now_ts,
        "updated_at": now_ts,
    }

    upsert_sql = text("""
        INSERT INTO user_levels_path (
            id_op_user_levels_path, id_user, id_level,
            start_user_lvl, end_user_lvl, status_user_lvl,
            iteration_user_lvl, point_lvl_required, point_lvl_accumlated,
            created_at, updated_at
        )
        VALUES (
            :id_op_user_levels_path, :id_user, :id_level,
            :start_user_lvl, :end_user_lvl, :status_user_lvl,
            :iteration_user_lvl, :point_lvl_required, :point_lvl_accumlated,
            :created_at, :updated_at
        )
        ON CONFLICT (id_op_user_levels_path, id_user, id_level, iteration_user_lvl)
        DO UPDATE SET
            start_user_lvl = EXCLUDED.start_user_lvl,
            end_user_lvl = EXCLUDED.end_user_lvl,
            status_user_lvl = EXCLUDED.status_user_lvl,
            point_lvl_required = EXCLUDED.point_lvl_required,
            point_lvl_accumlated = EXCLUDED.point_lvl_accumlated,
            updated_at = EXCLUDED.updated_at
    """)

    connection.execute(upsert_sql, params)
    print(f"Upserted user level item: {params}")
    

def lambda_handler(event, context):
    try:
        engine = connect_db('prod_rds_data_production_rw')
        print("Received event:", event)

        for record in event.get("Records", []):
            try:
                body = json.loads(record["body"]) if isinstance(record["body"], str) else record["body"]
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON body: {record.get('body')}, Error: {e}")
                continue

            # IMPORTANTE: en el evento de EventBridge, los datos reales vienen en 'detail'
            detail = body.get("detail", {})
            update_desc = detail.get("updateDescription", {})
            updated_fields = update_desc.get("updatedFields", {})
            levels = updated_fields.get("levels", [])

            id_op_user_levels_path = detail.get("documentKey", {}).get("_id")
            if not id_op_user_levels_path:
                print("No documentKey._id in event detail. Skipping record.")
                continue

            now_ts = datetime.now()

            with engine.begin() as connection:  # begin => hace commit/rollback automático
                # 1) Resolver id_user
                id_user = resolve_id_user(connection, id_op_user_levels_path, detail)
                if not id_user:
                    print(f"[WARN] No pude resolver id_user para id_op_user_levels_path={id_op_user_levels_path}. " 
                        "Necesitas proveerlo o tener un mapeo. Skipping.")
                    continue

                # 2) Upsert de cada level en el array
                for lvl in levels:
                    try:
                        upsert_user_level_item(connection, id_op_user_levels_path, id_user, lvl, now_ts)
                    except Exception as e:
                        print(f"Error upserting level item ({lvl}): {e}")

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
