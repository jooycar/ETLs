import sys
import boto3
import json
import os
import gc
import re
import numpy as np
import sqlalchemy as db
import pymongo
import pandas as pd
import logging

from sqlalchemy import create_engine, text
from bson.objectid import ObjectId
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime, timedelta
from pytz import timezone
from typing import Optional, Tuple, Literal, Dict, Any
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

# ====== Helpers existentes (tus versiones) ======
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
                if value in ["", "null", None]:
                    break
        else:
            value = data.get(key)
        if value not in ["", "null", None]:
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

# ====== Helpers nuevos mínimos ======
EXCLUDED_TYPE_COMM = {
    'notify-assistance-rac-email',
    'jooycar:support:notification',
    'sura:seguroxkm2:renewal:policy',
    'sura:assistance:views:refresh:scoring'
}

def normalize_status(status: Optional[str]) -> Optional[str]:
    if not status:
        return None
    s = status.strip().lower()
    if s == 'delivered':
        return 'sent'
    if s in ('processing', 'in_progress'):
        return 'pending'
    if s in ('failed', 'error'):
        return 'failed'
    return s

def normalize_type_comm(tc: Optional[str]) -> Optional[str]:
    if not tc:
        return None
    return tc.strip().lower()

def parse_db_coll(body: dict):
    """Extrae (db, coll) desde body['detail']['ns']"""
    detail = body.get('detail', {})
    ns = detail.get('ns', {})
    return ns.get('db'), ns.get('coll')

    
def lambda_handler(event, context):
    """
    Actualiza communications en Aurora para eventos de Mongo:
      - production-01.pushmessages  -> posibles updatedFields: status, viewed/viewAt, deleted/deleteAt, delivered/deliveredAt
      - email-sender.sentemails     -> posibles updatedFields: status, sentAt, viewAt, deleteAt, type
    """
    engine = connect_db('prod_rds_data_production_rw')  # tu helper existente
    print('Processing incoming event (truncated):', json.dumps(event)[:2000])

    # Procesamos 1 o N records; cada record termina en 200 propio
    for record in event.get('Records', []):
        # ---- Parse body ----
        try:
            body = json.loads(record['body']) if isinstance(record.get('body'), str) else (record.get('body') or {})
        except json.JSONDecodeError as e:
            print(f"[ERROR] JSON body inválido: {record.get('body')} - {e}")
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Invalid JSON format in event", "error": str(e)}, cls=CustomJSONEncoder)
            }

        detail = body.get('detail') or {}
        ns = detail.get('ns') or {}
        db, coll = ns.get('db'), ns.get('coll')
        coll_l = (coll or '').lower()

        if detail.get('operationType') != 'update':
            print(f"[INFO] Operación no soportada ({detail.get('operationType')}). Terminando armónicamente.")
            continue

        document_key = (detail.get('documentKey') or {}).get('_id')
        if not document_key:
            print("[WARN] documentKey._id ausente. Terminando armónicamente.")
            continue

        updated_fields = (detail.get('updateDescription') or {}).get('updatedFields') or {}
        if not isinstance(updated_fields, dict) or not updated_fields:
            print(f"[INFO] Sin updatedFields para id_op_comm={document_key}. Nada que hacer.")
            continue

        # ---- Preparar SET dinámico ----
        set_params = {}
        set_clauses = []

        # STATUS
        if 'status' in updated_fields:
            norm_status = normalize_status(updated_fields.get('status'))
            set_params['status'] = norm_status
            set_clauses.append("status = :status")

        # TIMESTAMPS (se mapean si vienen)
        # viewed / viewAt -> view_at
        for k in ('viewed', 'viewAt'):
            if k in updated_fields:
                set_params['view_at'] = parse_datetime_to_pg(updated_fields.get(k))
                set_clauses.append("view_at = :view_at")
                break

        # deleted / deleteAt -> delete_at
        for k in ('deleted', 'deleteAt'):
            if k in updated_fields:
                set_params['delete_at'] = parse_datetime_to_pg(updated_fields.get(k))
                set_clauses.append("delete_at = :delete_at")
                break

        # delivered / deliveredAt / sentAt -> sent_at
        for k in ('delivered', 'deliveredAt', 'sentAt'):
            if k in updated_fields:
                set_params['sent_at'] = parse_datetime_to_pg(updated_fields.get(k))
                set_clauses.append("sent_at = :sent_at")
                break

        # TYPE_COMM (solo si viene en el update)
        if coll_l == 'sentemails' and 'type' in updated_fields:
            tc = normalize_type_comm(updated_fields.get('type'))
            if tc in EXCLUDED_TYPE_COMM:
                msg = f"type_comm '{tc}' no permitido para sentemails. Ejecución terminada de forma controlada."
                print(f"[INFO] {msg}")
                return {
                    "statusCode": 200,
                    "body": json.dumps({"message": msg, "status": "ignored", "id_op_comm": str(document_key)}, cls=CustomJSONEncoder)
                }
            set_params['type_comm'] = tc
            set_clauses.append("type_comm = :type_comm")

        if coll_l == 'pushmessages' and 'profileType' in updated_fields:
            tc = normalize_type_comm(updated_fields.get('profileType'))
            if tc in EXCLUDED_TYPE_COMM:
                msg = f"type_comm '{tc}' no permitido para pushmessages. Ejecución terminada de forma controlada."
                print(f"[INFO] {msg}")
                return {
                    "statusCode": 200,
                    "body": json.dumps({"message": msg, "status": "ignored", "id_op_comm": str(document_key)}, cls=CustomJSONEncoder)
                }
            set_params['type_comm'] = tc
            set_clauses.append("type_comm = :type_comm")

        # Si no hay nada que actualizar, salir.
        if not set_clauses:
            print(f"[INFO] No hay columnas mapeables en updatedFields para id_op_comm={document_key}.")
            continue

        # updated_at siempre
        set_params['updated_at'] = datetime.now()
        set_clauses.append("updated_at = :updated_at")

        # Build UPDATE
        set_params['id_op_comm'] = str(document_key)
        update_sql = f"""
            UPDATE communications
            SET {', '.join(set_clauses)}
            WHERE id_op_comm = :id_op_comm
        """
        # ---- Ejecutar UPDATE ----
        try:
            with engine.begin() as conn:
                result = conn.execute(text(update_sql), convert_types(set_params))
                rowcount = getattr(result, "rowcount", 0)

            print(f"[OK] UPDATE communications id_op_comm={document_key}. Rowcount={rowcount}. SET={set_params}")

            if rowcount == 0:
                print(f"[WARN] No existe communications con id_op_comm={document_key}. No se actualizó.")

            # Construye y retorna una respuesta serializable
            return {
                "statusCode": 200,
                "body": json.dumps(
                    {
                        "message": "Processing completed successfully",
                        "rowcount": rowcount,
                        "id_op_comm": str(document_key),
                    },
                    cls=CustomJSONEncoder
                )
            }

        except SQLAlchemyError as e:
            print(f"[ERROR] Database error on UPDATE for id_op_comm={document_key}: {e}")
            return {
                "statusCode": 500,
                "body": json.dumps({"message": "Database operation failed", "error": str(e)}, cls=CustomJSONEncoder)
            }
        except Exception as e:
            print(f"[ERROR] Unexpected error on UPDATE for id_op_comm={document_key}: {e}")
            return {
                "statusCode": 500,
                "body": json.dumps({"message": "Internal server error", "error": str(e)}, cls=CustomJSONEncoder)
            }

