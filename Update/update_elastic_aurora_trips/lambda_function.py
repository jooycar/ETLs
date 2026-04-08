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

# ----------------------------
# Helpers nuevos (pequeños)
# ----------------------------
def normalize_trip_key(trip_key: str) -> str:
    # por padding base64 '='
    if not trip_key:
        return trip_key
    return str(trip_key).rstrip("=")

def extract_time_ref(message: dict):
    """
    timeRef NO es obligatorio.
    Se usa solo si existe para construir filtros o logging.
    """
    scores = (message.get("updateData") or {}).get("scoring", {}).get("scores", [])
    if isinstance(scores, list):
        for s in scores:
            if isinstance(s, dict) and s.get("timeRef") is not None:
                return s["timeRef"]

    # anomaly
    upd = message.get("updateData") or {}
    if upd.get("resolutionDate") is not None:
        return upd["resolutionDate"]

    # fallback
    if message.get("updatedAt") is not None:
        return message["updatedAt"]
    if message.get("createdAt") is not None:
        return message["createdAt"]

    return None

def to_datetime_utc_naive(value):
    """
    Convierte epoch(ms/s)/ISO a datetime naive UTC.
    (No obliga a usarlo, lo dejo por si quieres usar timeRef para filtros)
    """
    if value is None:
        return None
    if isinstance(value, int):
        # asume ms
        return datetime.utcfromtimestamp(value / 1000.0) if value > 1e12 else datetime.utcfromtimestamp(value)
    if isinstance(value, float):
        return datetime.utcfromtimestamp(value / 1000.0) if value > 1e12 else datetime.utcfromtimestamp(value)
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    return None


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

    # Cambio pequeño pro-Aurora: evita “tormenta” de conexiones por invocación
    # (si no quieres tocarlo, vuelve a tu config original)
    engine = create_engine(
        connection_string,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_size=2,
        max_overflow=2,
        pool_timeout=10,
        connect_args={"connect_timeout": 5},
    )

    ## ========= MONGO =========
    if mongo_secret_name is not None:
        mongo_cred = json.loads(aws_client.get_secret_value(SecretId=mongo_secret_name)['SecretString'])
        STRING_MONGO = mongo_cred['STRING_MONGO']
        mongo_client = pymongo.MongoClient(STRING_MONGO)
        mydb = mongo_client['production-01']
        return engine, mydb

    return engine


# ----------------------------
# FIX CLAVE: FK sin pandas + normalización '='
# ----------------------------
def fetch_trip_id(engine, trip_key: str, timeRef_dt: Optional[datetime] = None) -> Optional[int]:
    """
    Devuelve id_trip o None.
    - NO usa pandas (menos carga)
    - Normaliza '='
    - timeRef es opcional. Si lo usas, el filtro correcto es created <= timeRef
      (tu código lo tenía al revés: >=, eso te generaba "no match")
    """
    trip_key_norm = normalize_trip_key(trip_key)

    base_sql = """
        SELECT id_trip
        FROM trips
        WHERE rtrim(id_op_trip, '=') = :trip_key
    """

    params = {"trip_key": trip_key_norm}

    # Si quieres filtrar por fecha, hazlo bien (opcional)
    # Nota: para updates, el trip "created" normalmente es ANTES o igual al timeRef.
    if timeRef_dt is not None:
        base_sql += " AND created <= :timeRef_ts"
        params["timeRef_ts"] = timeRef_dt

    base_sql += " ORDER BY created DESC LIMIT 1"

    try:
        with engine.begin() as conn:
            row = conn.execute(text(base_sql), params).fetchone()
        if not row:
            return None
        return int(row[0])
    except Exception as e:
        print(f"Error fetching trip_id for {trip_key}: {e}")
        return None


def convert_types(data):
    if isinstance(data, list):
        for record in data:
            for key, value in record.items():
                if isinstance(value, np.generic):
                    record[key] = value.item()
                elif isinstance(value, pd.Timestamp):
                    record[key] = value.isoformat()
                elif value == "":
                    record[key] = None
    elif isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, np.generic):
                data[key] = value.item()
            elif isinstance(value, pd.Timestamp):
                data[key] = value.isoformat()
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


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, (np.generic, pd.Timestamp)):
            return obj.item() if isinstance(obj, np.generic) else obj.isoformat()
        elif isinstance(obj, bytes):
            return obj.decode('utf-8')
        return super().default(obj)


def process_trip_data(df_list, engine, trip_key, message):
    """
    Cambio mínimo: usar engine.begin() para commit y no colgar conexiones.
    """
    try:
        queue_url_score = 'https://sqs.us-east-1.amazonaws.com/045511714637/ESUpdateSQSScores'
        queue_url_anomaly = 'https://sqs.us-east-1.amazonaws.com/045511714637/ESUpdateSQSAnomalies'

        field_to_column_mapping = {
            'created': 'created',
            'updateData.visibility': 'anomaly_visibility',
            'status': 'source_value',
            'distance': 'distance',
            'start.location.lat': 'start_lat',
            'start.location.lon': 'start_lon',
            'start.time': 'start_time',
            'end.location.lat': 'end_lat',
            'end.location.lon': 'end_lon',
            'end.time': 'end_time',
            'duration.value': 'duration_trip'
        }

        df_fltr = pd.concat(df_list, ignore_index=True) if df_list else pd.DataFrame()

        set_clauses = []
        params = {"id_op_trip": trip_key}

        df_scores = None  # <- evita referencia antes de asignación

        if 'updatedProperty' in df_fltr.columns and (df_fltr['updatedProperty'] == 'aggregations').any():
            if 'updateData.scoring.scores' in df_fltr.columns:
                df_fltr['scores'] = df_fltr['updateData.scoring.scores'].apply(safe_json_loads)
                df_scores_tmp = df_fltr.explode('scores')
                df_scores_tmp = pd.json_normalize(df_scores_tmp['scores'])

                # Validación segura (evita crash si viene vacío)
                ac = df_scores_tmp.get('accumulableValues', [None])[0]
                if ac and isinstance(ac, list) and len(ac) > 0 and ac[0].get('name') == 'octo_score':
                    message_body = {'message': message, 'reason': 'Insert in table trip_scores'}
                    send_to_sqs(message_body, queue_url_score)
                else:
                    print("There is not enough data to process")

                df_scores = df_scores_tmp  # para updates si aplica

            else:
                print("No updateData.scoring.scores field found in DataFrame.")
                return params  # <- no devuelvas DF

        elif 'updatedProperty' in df_fltr.columns and (df_fltr['updatedProperty'] == 'anomaly').any():
            if 'updateData.visibility' in df_fltr.columns:
                df_scores = df_fltr[['updateData.visibility']]
                message_body = {'message': message, 'reason': 'Insert in table trip_anomalies'}
                send_to_sqs(message_body, queue_url_anomaly)
            else:
                print("No updateData.visibility field found in DataFrame.")
                return params

        else:
            print("No valid updatedProperty found in DataFrame.")
            return params

        if df_scores is None or df_scores.empty:
            return params

        for field, column_name in field_to_column_mapping.items():
            if field in df_scores.columns:
                value = df_scores[field].iloc[0]
                if value is not None:
                    set_clauses.append(f"{column_name} = :{column_name}")
                    params[column_name] = value

        if set_clauses:
            set_clauses.append("updated_at = :updated_at")
            params["updated_at"] = datetime.utcnow()
            params = convert_types(params)

            update_query = f"""
                UPDATE trips
                SET {', '.join(set_clauses)}
                WHERE id_op_trip = :id_op_trip
            """

            try:
                with engine.begin() as connection:
                    connection.execute(text(update_query), params)
                print(f"Updated Trips {trip_key} with fields: {params}")
            except Exception as e:
                print(f"Error updating trip {trip_key}: {e}")

    except Exception as e:
        print(f"Error processing trip data: {e}")

    return params


def push_trip(event):
    engine = connect_db('prod_rds_data_production_rw')

    df_list = []
    trip_key = None
    timeRef = None
    message = None

    for record in event:
        try:
            body = json.loads(record['body'])
            message = json.loads(body['Message'])
            trip_key = message.get('tripId', None)

            if trip_key is None:
                raise ValueError("Missing 'tripId' in message")

            scores = message.get('updateData', {}).get('scoring', {}).get('scores', [])

            # timeRef opcional
            timeRef = None
            if isinstance(scores, list) and scores:
                for score in scores:
                    if isinstance(score, dict) and score.get('timeRef') is not None:
                        timeRef = score['timeRef']
                        break
            else:
                # solo existe en anomaly; en aggregations normalmente no
                timeRef = message.get('updateData', {}).get('resolutionDate')

            df_list.append(pd.json_normalize(message))

        except json.JSONDecodeError as e:
            print(f"JSON parsing error: {e}")
        except KeyError as e:
            print(f"Missing expected key: {e}")
        except Exception as e:
            print(f"Error processing record: {e}")
        continue

    # ---- AQUI EL CAMBIO CLAVE ----
    if not trip_key:
        raise ValueError("Missing trip_key.")

    # timeRef NO obligatorio: solo lo convertimos si existe
    timeRef_dt = None
    if timeRef is not None:
        try:
            if isinstance(timeRef, int):
                timeRef_dt = datetime.fromtimestamp(timeRef / 1000.0)
            elif isinstance(timeRef, str):
                timeRef_dt = datetime.fromisoformat(timeRef.replace("Z", "+00:00"))
        except Exception:
            timeRef_dt = None

    # ---- FK lookup: NO filtres por fecha (evita no-match y baja carga) ----
    query_trip = text("""
        SELECT id_trip
        FROM trips
        WHERE id_op_trip = :trip_key
        LIMIT 1
    """)

    with engine.begin() as conn:
        row = conn.execute(query_trip, {"trip_key": trip_key}).fetchone()

    if not row:
        raise ValueError(f"no match with: {trip_key}")

    id_trip = int(row[0])
    message['id_trip'] = id_trip

    trip_df = process_trip_data(df_list, engine, trip_key, message)
    return trip_df



def lambda_handler(event, context):
    try:
        print('Procesing event', json.dumps(event, default=str))

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
            "body": json.dumps({"message": "Invalid JSON format in event", "error": str(e)}, cls=CustomJSONEncoder)
        }

    except KeyError as e:
        logger.error(f"Missing key in event: {e}")
        response = {
            "statusCode": 400,
            "body": json.dumps({"message": "Missing required key in event", "error": str(e)}, cls=CustomJSONEncoder)
        }

    except ValueError as e:
        logger.error(f"Value error: {e}")
        response = {
            "statusCode": 400,
            "body": json.dumps({"message": "Invalid value in event", "error": str(e)}, cls=CustomJSONEncoder)
        }

    except SQLAlchemyError as e:
        logger.error(f"Database error: {e}")
        response = {
            "statusCode": 500,
            "body": json.dumps({"message": "Database operation failed", "error": str(e)}, cls=CustomJSONEncoder)
        }

    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        response = {
            "statusCode": 500,
            "body": json.dumps({"message": "Internal server error", "error": str(e)}, cls=CustomJSONEncoder)
        }

    finally:
        gc.collect()

    return response
