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

# ======= Constantes =======
REGION = "us-east-1"
TABLE_USERS = "users_person"
NULL_SET = {"", "null", None}

TagComm = Literal["email", "app_notif"]

DETAILTYPE_REGEX = re.compile(
    r"MongoDB Database Trigger for\s+([^.]+)\.([A-Za-z0-9_\-]+)\s*$"
)

# ======= Auxiliares existentes (conservados y/o mejorados) =======
def send_to_sqs(message_body, queue_url):
    sqs_client = boto3.client('sqs', region_name=REGION)
    response = sqs_client.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message_body)
    )
    return response

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

def parse_db_coll_from_detail_type(detail_type: str) -> Tuple[Optional[str], Optional[str]]:
    if detail_type in NULL_SET:
        return None, None
    m = DETAILTYPE_REGEX.search(detail_type)
    if m:
        db, coll = m.group(1), m.group(2)
        return db, coll
    return None, None

def parse_db_coll(event: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    detail_type = event.get("detail-type") or event.get("detailType")
    db, coll = parse_db_coll_from_detail_type(detail_type or "")
    if not db or not coll:
        ns = (event.get("detail") or {}).get("ns") or {}
        db = db or ns.get("db")
        coll = coll or ns.get("coll")
    return db, coll

def derive_tag_comm_from_collection(collection: Optional[str]) -> TagComm:
    if (collection or "").strip().lower() == "sentemails":
        return "email"
    return "app_notif"

def fetch_fk_by(engine, table: str, id_col: str, op_col: str, value: str, normalize: bool) -> Optional[int]:
    """
    Búsqueda eficiente de FK en SQL con parámetros.
    """
    if value in NULL_SET:
        return None

    v = value.strip()
    if normalize:
        v = v.lower()
        comparator = f"LOWER(TRIM({op_col})) = :v"
    else:
        comparator = f"TRIM({op_col}) = :v"

    sql = text(f"""
        SELECT {id_col} AS id
        FROM {table}
        WHERE {comparator}
        LIMIT 1
    """)
    with engine.begin() as conn:
        row = conn.execute(sql, {"v": v}).fetchone()
        return row[0] if row else None

def resolve_id_user(engine, full_document: dict, collection: Optional[str]) -> Tuple[Optional[int], Optional[str], str]:
    """
    1) Intento por id_op_user (full_document['user'])
    2) Fallback por email_user usando destinations[0] SOLO si collection == 'sentemails'
    Retorna (id_user, used_value, via)
    """
    # id_op_user (no normalizar)
    user_key = full_document.get("user")
    if user_key not in NULL_SET:
        id_user = fetch_fk_by(
            engine,
            table=TABLE_USERS,
            id_col="id_user",
            op_col="id_op_user",
            value=str(user_key),
            normalize=False
        )
        if id_user is not None:
            return id_user, str(user_key), "id_op_user"

    # Fallback por email solo si es sentemails
    if (collection or "").lower() == "sentemails":
        destinations = full_document.get("destinations") or []
        user_email = destinations[0] if destinations else None
        if user_email not in NULL_SET:
            id_user = fetch_fk_by(
                engine,
                table=TABLE_USERS,
                id_col="id_user",
                op_col="email_user",
                value=str(user_email),
                normalize=True
            )
            if id_user is not None:
                return id_user, str(user_email), "email_user"

    return None, None, "none"

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat() 
        elif isinstance(obj, (np.generic, pd.Timestamp)): 
            return obj.item() if isinstance(obj, np.generic) else obj.isoformat()
        elif isinstance(obj, bytes):
            return obj.decode('utf-8')
        return super().default(obj)

    
def lambda_handler(event, context):
    """
    Inserta en communications desde dos orígenes:
      - email-sender.sentemails  -> tag_comm='email', sent_at=sentAt, type_comm=type, view_at/delete_at posibles
      - production-01.pushmessages-> tag_comm='app_notif', sent_at=delivered (o deliveredAt), type_comm=profileType
    """
    engine = connect_db('prod_rds_data_production_rw')
    print('Process event:', json.dumps(event)[:2000])

    insert_query = text("""
        INSERT INTO communications (
            id_op_comm, id_user, tag_comm, sent_at, view_at, delete_at, 
            status, type_comm
        ) VALUES (
            :id_op_comm, :id_user, :tag_comm, :sent_at, :view_at, :delete_at, 
            :status, :type_comm
        )
    """)

    # Excluir ciertos type_comm (normalizados en minúscula y sin espacios)
    EXCLUDED_TYPE_COMM = {
        'notify-assistance-rac-email',
        'jooycar:support:notification',
        'sura:seguroxkm2:renewal:policy',
        'sura:assistance:views:refresh:scoring'
    }

    response = {
        "statusCode": 200,
        "body": json.dumps({"message": "Processing completed successfully"}, cls=CustomJSONEncoder)
    }

    records = event.get('Records', [])
    for rec in records:
        # ---- Parse body (SQS) ----
        raw_body = rec.get('body')
        try:
            body = json.loads(raw_body) if isinstance(raw_body, str) else (raw_body or {})
        except json.JSONDecodeError as e:
            print(f"[ERROR] JSON body inválido: {raw_body}. Error: {e}")
            return {
                "statusCode": 400,
                "body": json.dumps({"message": "Invalid JSON format in event", "error": str(e)}, cls=CustomJSONEncoder)
            }

        detail = body.get('detail', {})
        if not isinstance(detail, dict):
            print("[WARN] detail inexistente o malformado. Terminando armónicamente.")
            return response

        full_document = detail.get('fullDocument')
        if not isinstance(full_document, dict):
            print("[WARN] fullDocument inexistente o malformado. Terminando armónicamente.")
            return response

        # ---- Detecta colección y tag_comm ----
        db, collection = parse_db_coll(body)
        tag_comm: TagComm = derive_tag_comm_from_collection(collection)
        coll_l = (collection or "").lower()

        # ---- ID comunicación (id_op_comm) ----
        id_op_comm = full_document.get('_id')
        if id_op_comm in NULL_SET:
            print("[WARN] _id no presente en fullDocument. Terminando armónicamente.")
            return response

        # ---- Resolver id_user ----
        id_user, used_value, via = resolve_id_user(engine, full_document, collection)
        if id_user is None:
            print(f"[WARN] No se encontró id_user (via={via}). _id={id_op_comm}, collection={collection}, used_value={used_value}")
            # Si quieres omitir sin error:
            return {
                "statusCode": 200,
                "body": json.dumps({"message": "id_user no encontrado; registro omitido de forma controlada", "status": "ignored"}, cls=CustomJSONEncoder)
            }

        # ---- Mapear campos por tipo de colección ----
        if coll_l == "sentemails":
            sent_at   = get_valid_field(full_document, 'sentAt')
            type_comm = get_valid_field(full_document, 'type')
            view_at   = get_valid_field(full_document, 'viewAt')
            delete_at = get_valid_field(full_document, 'deleteAt')
        else:
            # pushmessages (u otras)
            sent_at   = get_valid_field(full_document, 'delivered', 'deliveredAt')
            type_comm = get_valid_field(full_document, 'profileType', 'type')
            view_at   = None
            delete_at = None

        # ---- Filtro de comunicaciones no permitidas (termina en 200) ----
        tc_norm = (type_comm or '').strip().lower()
        if tc_norm in EXCLUDED_TYPE_COMM:
            msg = f"type_comm '{tc_norm}' no permitido. Ejecución terminada de forma controlada."
            print(f"[INFO] {msg}")
            return {
                "statusCode": 200,
                "body": json.dumps({"message": msg, "excluded_type_comm": tc_norm, "status": "ignored"}, cls=CustomJSONEncoder)
            }

        # ---- Estandarizar status ----
        status = get_valid_field(full_document, 'status')
        if status:
            status = status.lower().strip()
            if status == 'delivered':
                status = 'sent'
            elif status in ('processing', 'in_progress'):
                status = 'pending'
            elif status in ('failed', 'error'):
                status = 'failed'

        data_to_insert = {
            'id_op_comm': str(id_op_comm),
            'id_user': id_user,
            'tag_comm': tag_comm,
            'sent_at': sent_at,
            'view_at': view_at,
            'delete_at': delete_at,
            'status': status,
            'type_comm': tc_norm or None,
        }
        data_to_insert = convert_types(data_to_insert)

        # ---- Insert ----
        try:
            with engine.begin() as conn:
                conn.execute(insert_query, **data_to_insert)
                print(f"[OK] Insert communications: {data_to_insert}")
                response = {
                    "statusCode": 200,
                    "body": json.dumps({"message": "Processing completed successfully"}, cls=CustomJSONEncoder)
                }
        except SQLAlchemyError as e:
            logger.error(f"Database error: {e}")
            return {
                "statusCode": 500,
                "body": json.dumps({"message": "Database operation failed", "error": str(e)}, cls=CustomJSONEncoder)
            }
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return {
                "statusCode": 500,
                "body": json.dumps({"message": "Internal server error", "error": str(e)}, cls=CustomJSONEncoder)
            }
        finally:
            gc.collect()

    return response
