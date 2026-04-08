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

        # Asegurarnos que esté en UTC
        if dt.tzinfo:
            dt = dt.astimezone(tz=None)  # Esto convierte a naive UTC

        # Eliminar zona horaria (Postgres odia tzinfo en timestamp normal)
        dt = dt.replace(tzinfo=None)

        return dt
    except Exception as e:
        print(f"Error parsing datetime: {e}")
        return None

def fetch_fk(conn, table: str, id_col: str, id_op_col: str, op_value: str):
    """
    Devuelve el id (entero) de la tabla relacional buscando por el id_op_* (string de Mongo).
    """
    q = text(f"SELECT {id_col} AS id FROM {table} WHERE {id_op_col} = :op_value")
    row = conn.execute(q, {"op_value": op_value}).fetchone()
    return row[0] if row else None

def lambda_handler(event, context):
    engine = connect_db('prod_rds_data_production_rw')

    for record in event.get('Records', []):
        # 1) Parsear el body
        try:
            body = json.loads(record['body']) if isinstance(record['body'], str) else record['body']
        except json.JSONDecodeError as e:
            print(f"[SKIP] Error decoding JSON body: {record.get('body')}, Error: {e}")
            continue

        # 2) Validar estructura
        if 'detail' not in body or 'fullDocument' not in body['detail']:
            print("[SKIP] Missing 'detail' or 'fullDocument'")
            continue

        full_document = body['detail']['fullDocument']
        print(f"[INFO] Processing full document: {full_document}")

        try:
            # 3) Extraer claves del documento
            id_op_user_levels_path = str(full_document.get('_id'))
            user_key = full_document.get('user')  # id_op_user
            levels_array = full_document.get('levels', [])
            modif = full_document.get('modif')  # para updated_at

            if not user_key or not levels_array:
                print("[SKIP] Missing user or levels[] in document.")
                continue

            # 4) Fechas globales
            #created_at = oid_to_datetime(id_op_user_levels_path)  # desde el ObjectId
            created_at = ObjectId(id_op_user_levels_path).generation_time.isoformat(timespec='seconds')
            updated_at = parse_datetime_to_pg(modif)

            with engine.connect() as conn:
                # 5) FK user
                id_user = fetch_fk(conn,
                                   table='users_person',
                                   id_col='id_user',
                                   id_op_col='id_op_user',
                                   op_value=user_key)

                if not id_user:
                    print(f"[SKIP] User FK not found for user_key={user_key}")
                    continue

                # 6) Insertar una fila por cada nivel del array
                for lvl in levels_array:
                    try:
                        level_key = lvl.get('level')  # id_op_level
                        if not level_key:
                            print("[WARN] Level item without 'level' key. Skipping item.")
                            continue

                        id_level = fetch_fk(conn,
                                            table='levels',
                                            id_col='id_level',
                                            id_op_col='id_op_level',
                                            op_value=level_key)

                        if not id_level:
                            print(f"[SKIP] Level FK not found for level_key={level_key}")
                            continue

                        start_user_lvl = parse_datetime_to_pg(lvl.get('start'))
                        end_user_lvl = parse_datetime_to_pg(lvl.get('end'))
                        status_user_lvl = lvl.get('status')
                        iteration_user_lvl = lvl.get('iteration')

                        points = lvl.get('points', {})
                        point_lvl_required = points.get('required')
                        point_lvl_accumlated = points.get('accumulated')

                        data_to_insert = {
                            "id_op_user_levels_path": id_op_user_levels_path,
                            "id_user": id_user,
                            "id_level": id_level,
                            "start_user_lvl": start_user_lvl,
                            "end_user_lvl": end_user_lvl,
                            "status_user_lvl": status_user_lvl,
                            "iteration_user_lvl": iteration_user_lvl,
                            "point_lvl_required": point_lvl_required,
                            "point_lvl_accumlated": point_lvl_accumlated,
                            "created_at": created_at,
                            "updated_at": updated_at or datetime.utcnow().replace(tzinfo=None)
                        }

                        data_to_insert = convert_types(data_to_insert)

                        insert_query = text("""
                            INSERT INTO user_levels_path (
                                id_op_user_levels_path, id_user, id_level,
                                start_user_lvl, end_user_lvl, status_user_lvl,
                                iteration_user_lvl, point_lvl_required, point_lvl_accumlated,
                                created_at, updated_at
                            ) VALUES (
                                :id_op_user_levels_path, :id_user, :id_level,
                                :start_user_lvl, :end_user_lvl, :status_user_lvl,
                                :iteration_user_lvl, :point_lvl_required, :point_lvl_accumlated,
                                :created_at, :updated_at
                            )
                        """)

                        conn.execute(insert_query, **data_to_insert)
                        print(f"[OK] Inserted user_levels_path: {data_to_insert}")

                    except Exception as e:
                        print(f"[ERROR] Level item insertion failed: {e}")
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
