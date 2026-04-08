import sys
import gc
import re
import json
import boto3
import numpy as np
import pandas as pd
import pymongo
import logging
import urllib.request
import urllib.parse

from datetime import datetime
from datetime import timezone as dt_timezone  # para usar dt_timezone.utc
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# =========================
# Logging
# =========================
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# =========================
# Conexión a Aurora (y opcional Mongo)
# =========================
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
    engine = create_engine(
        connection_string,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_size=10,
        max_overflow=20
        # OJO: no usar future=True para evitar conflictos con pandas en SA 1.4.x
    )

    if mongo_secret_name is not None:
        mongo_cred = json.loads(aws_client.get_secret_value(SecretId=mongo_secret_name)['SecretString'])
        STRING_MONGO = mongo_cred['STRING_MONGO']
        mongo_client = pymongo.MongoClient(STRING_MONGO)
        mydb = mongo_client['production-01']
        return engine, mydb
    else:
        return engine

# =========================
# Utilidades
# =========================
def send_to_sqs(message_body, queue_url):
    sqs_client = boto3.client('sqs', region_name='us-east-1')
    response = sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message_body))
    return response

def fetch_foreign_key(engine, query_str, key_params):
    """Lee un id (alias id) con pandas usando una Connection para evitar errores de SA 2.0 style."""
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query_str, conn, params=key_params)
        if df.empty:
            raise ValueError(f"No rows for params={key_params}")
        return df.iloc[0]['id']
    except Exception as e:
        print(f"Error fetching foreign key for {key_params}: {e}")
        return None

def convert_types(data):
    """Convierte np.generic, pd.Timestamp y strings vacíos a tipos JSON-friendly/None."""
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
    """Obtiene el primer valor no vacío entre varias rutas/keys posibles."""
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
    """Extrae y decodifica la lista de mensajes desde SQS (campo body.message)."""
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

def parse_created_any(created_val):
    """
    Acepta:
      - ISO string: '2025-10-20T15:32:47' (con/sin 'Z')
      - Epoch en millis: 1761070014751
      - Epoch en segundos: 1761070014
    Retorna: datetime naive (UTC)
    """
    if created_val is None:
        return None

    if isinstance(created_val, datetime):
        return created_val.replace(tzinfo=None)

    s = str(created_val).strip()

    iso_pat = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"
    if re.match(iso_pat, s):
        try:
            if s.endswith('Z'):
                dt = datetime.fromisoformat(s[:-1]).replace(tzinfo=dt_timezone.utc)
            else:
                dt = datetime.fromisoformat(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=dt_timezone.utc)
            return dt.astimezone(dt_timezone.utc).replace(tzinfo=None)
        except Exception:
            pass

    if s.isdigit():
        n = int(s)
        if n > 10**10:
            return datetime.fromtimestamp(n / 1000, tz=dt_timezone.utc).replace(tzinfo=None)
        else:
            return datetime.fromtimestamp(n, tz=dt_timezone.utc).replace(tzinfo=None)

    try:
        return datetime.fromisoformat(s).replace(tzinfo=None)
    except Exception:
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

# =========================
# Lógica principal por mensaje
# =========================
def process_message(msg, record, engine, insert_query, query_trip):
    """
    Procesa un mensaje (dict) y realiza el INSERT en trip_phones si corresponde.
    Retorna True si insertó, False en caso contrario.
    """
    trip_id = msg.get('tripId')
    imei = msg.get('imei')

    # 1) Parse created robusto
    created_time = parse_created_any(msg.get('created'))
    if not created_time:
        print(f"[WARN] created inválido: {msg.get('created')}. Record skip.")
        return False
    
    # 2) Buscar id_trip en Aurora
    params = {"created_time": created_time, "trip_id": trip_id}
    id_trip = fetch_foreign_key(engine, query_trip, params)
    print(f"Fetched foreign key for tripId {trip_id}: {id_trip}")

    # 3) Validaciones
    if id_trip is None:
        print(f"[WARN] id_trip no encontrado. Skip. tripId={trip_id}")
        return False
    if not imei or not re.search(r'octo', str(imei), re.IGNORECASE):
        print(f"[WARN] IMEI inválido/no octo. Skip. imei={imei}")
        return False

    id_comp = f"{imei}_{id_trip}"

    # 4) Llamada a API local para info de teléfono
    url = f"http://device-track-api.local/api/v1/heartbeats/imei/{urllib.parse.quote(str(imei))}"
    data = None
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            status_code = response.getcode()
            if status_code == 200:
                response_body = response.read().decode('utf-8')
                data = json.loads(response_body)
            else:
                print(f"[WARN] Failed to fetch data for IMEI {imei}: HTTP {status_code}")
    except Exception as e:
        print(f"[WARN] Error accessing {url}: {str(e)}")

    if not data or 'data' not in data or 'info' not in data['data']:
        print(f"[WARN] Payload inesperado o vacío para IMEI {imei}. Skip insert.")
        return False

    info = data["data"]["info"]
    phone_model   = info.get("phoneModel")
    phone_os      = info.get("osPhone")
    v_os          = info.get("osVersion")
    v_app         = info.get("appVersion")
    v_sdk         = info.get("sdkVersion")
    created_phone = info.get("updatedAt")  # si la columna es timestamp y necesitas castear, avísame

    # 5) Armar payload de insert
    data_to_insert = {
        'id_comp_trip_phone': id_comp,
        'id_trip': id_trip,
        'phone_model': phone_model,
        'phone_os': phone_os,
        'v_os': v_os,
        'v_app': v_app,
        'v_sdk': v_sdk,
        'created_phone': created_phone
    }
    data_to_insert = convert_types(data_to_insert)

    # 6) Insert (transaccional)
    try:
        with engine.begin() as connection:
            connection.execute(insert_query, data_to_insert)
            print(f"[OK] Inserted Trip_Phone data: {data_to_insert}")
            return True
    except Exception as e:
        print(f"[ERROR] Error inserting Trip_Phone data: {e}")
        return False

# =========================
# Lambda handler
# =========================
def lambda_handler(event, context=None):
    try:
        engine = connect_db('prod_rds_data_production_rw')
        print("Processing event:", event)

        # Query de inserción (agrega ON CONFLICT si lo necesitas)
        insert_query = text("""
            INSERT INTO trip_phones (
                id_comp_trip_phone, id_trip, phone_model, phone_os, v_os, v_app, v_sdk, created_phone
            ) VALUES (
                :id_comp_trip_phone, :id_trip, :phone_model, :phone_os, :v_os, :v_app, :v_sdk, :created_phone
            )
        """)
        # Si quieres idempotencia:
        # insert_query = text("""
        #     INSERT INTO trip_phones (
        #         id_comp_trip_phone, id_trip, phone_model, phone_os, v_os, v_app, v_sdk, created_phone
        #     ) VALUES (
        #         :id_comp_trip_phone, :id_trip, :phone_model, :phone_os, :v_os, :v_app, :v_sdk, :created_phone
        #     )
        #     ON CONFLICT (id_comp_trip_phone) DO NOTHING
        # """)

        processed = 0
        inserted = 0

        for record in event.get('Records', []):
            messages = parse_record(record)
            if messages is None:
                continue

            for msg in messages:
                query_trip = """
                    SELECT id_trip as id, id_op_trip as id_op 
                    FROM trips 
                    WHERE created >= %(created_time)s AND id_op_trip = %(trip_id)s
                """
                ok = process_message(msg, record, engine, insert_query, query_trip)
                processed += 1
                inserted += int(bool(ok))

        response = {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Processing completed successfully",
                "stats": {"processed": processed, "inserted": inserted}
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
