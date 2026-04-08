# import sys
# import boto3
# import json
# import os
# import gc
# import numpy as np
# import sqlalchemy as db
# import pymongo
# import pandas as pd
# import logging
# import re

# from sqlalchemy import create_engine, text
# from sqlalchemy.exc import SQLAlchemyError
# from datetime import datetime, timedelta
# from pytz import timezone
# from typing import Optional, Dict

import sys
import re
import gc
import json
import boto3
import numpy as np
import pandas as pd
import sqlalchemy as db
import pymongo
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError
from datetime import datetime
from pytz import timezone
from typing import Optional, Dict, List, Tuple

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
    return sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message_body))

def check_and_send(trip_dict, key_path, message, queue_url, table_name):
    current = trip_dict
    for key in key_path:
        if isinstance(current, dict) and key in current:
            current = current[key]
        else:
            # No existe el nodo; no bloquea el flujo
            return
    message_body = {'message': message, 'reason': f'Insert in table {table_name}'}
    send_to_sqs(message_body, queue_url)

def safe_json_loads_maybe_double(s):
    """Intenta decodificar JSON anidado (string de JSON dentro de otro string)."""
    try:
        obj = json.loads(s)
        if isinstance(obj, str):
            try:
                obj = json.loads(obj)
            except Exception:
                pass
        return obj
    except Exception:
        return s

def fetch_device_map(engine, device_keys: List[str]) -> Dict[str, int]:
    """Obtiene un dict {id_op_device -> id_device} en un solo query."""
    if not device_keys:
        return {}
    unique_keys = list({str(k) for k in device_keys if k})
    # Evita SQL injection: usamos bindparam
    sql = text("""
        SELECT id_device, id_op_device
        FROM devices
        WHERE id_op_device = ANY(:keys)
    """)
    df = pd.read_sql(sql, con=engine, params={'keys': unique_keys})
    return {str(r['id_op_device']): int(r['id_device']) for _, r in df.iterrows()}

def _to_datetime_from_millis(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series // 1000, unit='s', utc=True).dt.tz_localize(None)

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat() 
        elif isinstance(obj, (np.generic, pd.Timestamp)): 
            return obj.item() if isinstance(obj, np.generic) else obj.isoformat()
        elif isinstance(obj, bytes):
            return obj.decode('utf-8')
        return super().default(obj)


def process_trip_data(df_list: List[pd.DataFrame], engine, device_map: Dict[str, int]):
    """
    df_list: lista de dataframes normalizados (uno por mensaje seleccionado).
    device_map: {device_key -> id_device}
    """
    if not df_list:
        return pd.DataFrame()

    df_all = pd.concat(df_list, ignore_index=True)

    cols_needed = [
        'id', 'tripId', 'created', 'associations.device', 'anomaly.visibility',
        'metadata.source.value',
        'accumulations.distance.value',
        'aggregations.start.location.lat', 'aggregations.start.location.lon',
        'aggregations.start.time',
        'aggregations.end.location.lat', 'aggregations.end.location.lon',
        'aggregations.end.time',
        'accumulations.duration.value',
        'dataSource.value',
        'device_key'
    ]

    # Asegura columnas faltantes (si un msg no trae todas)
    for c in cols_needed:
        if c not in df_all.columns:
            df_all[c] = np.nan

    df = df_all.copy()

    df['accumulations.duration.value'] = pd.to_numeric(df['accumulations.duration.value'], errors='coerce') / 1000.0

    for tcol in ['created', 'aggregations.start.time', 'aggregations.end.time']:
        df[tcol] = pd.to_numeric(df[tcol], errors='coerce')
        df[tcol] = _to_datetime_from_millis(df[tcol])

    df['id_device'] = df['device_key'].map(device_map).astype('Int64')

    flt_cols = [
        'accumulations.distance.value',
        'aggregations.start.location.lat', 'aggregations.start.location.lon',
        'aggregations.end.location.lat', 'aggregations.end.location.lon'
    ]
    for c in flt_cols:
        df[c] = pd.to_numeric(df[c], errors='coerce').round(6)

    df.replace(['-', '', 'nan', 'NAN', '<NA>', 'NONE', 'None'], np.nan, inplace=True)

    if 'id' in df.columns and 'created' in df.columns:
        df.sort_values(by=['id', 'created'], ascending=[False, False], inplace=True, ignore_index=True)

    ordered = df[[
        'tripId', 'created', 'id_device', 'anomaly.visibility',
        'metadata.source.value', 'accumulations.distance.value',
        'aggregations.start.location.lat', 'aggregations.start.location.lon',
        'aggregations.start.time',
        'aggregations.end.location.lat', 'aggregations.end.location.lon',
        'aggregations.end.time',
        'accumulations.duration.value',
        'dataSource.value' 
    ]].copy()

    # Prepara params para bulk insert
    params_list = []
    for _, row in ordered.iterrows():
        if pd.isna(row['id_device']):
            continue
        params_list.append({
            'id_op_trip': row['tripId'],
            'created': row['created'],
            'id_device': int(row['id_device']),
            'anomaly_visibility': row['anomaly.visibility'],
            'source_value': row['metadata.source.value'],
            'distance': row['accumulations.distance.value'],
            'start_lat': row['aggregations.start.location.lat'],
            'start_lon': row['aggregations.start.location.lon'],
            'start_time': row['aggregations.start.time'],
            'end_lat': row['aggregations.end.location.lat'],
            'end_lon': row['aggregations.end.location.lon'],
            'end_time': row['aggregations.end.time'],
            'duration_trip': row['accumulations.duration.value'],
            '_imei_value': row['dataSource.value']
        })

    if not params_list:
        return pd.DataFrame()

    insert_query = text("""
        INSERT INTO trips (
            id_op_trip, created, id_device, anomaly_visibility, source_value, distance,
            start_lat, start_lon, start_time, end_lat, end_lon, end_time, duration_trip
        )
        VALUES (
            :id_op_trip, :created, :id_device, :anomaly_visibility, :source_value, :distance,
            :start_lat, :start_lon, :start_time, :end_lat, :end_lon, :end_time, :duration_trip
        )
    """)

    try:
        with engine.begin() as connection:
            connection.execute(insert_query, params_list)
    except IntegrityError as e:
        print(f"[WARN] IntegrityError en bulk insert: {e}")
    except SQLAlchemyError as e:
        print(f"[ERROR] Database error en bulk insert: {e}")

    out_df = pd.DataFrame([{
        'id_op_trip': p['id_op_trip'],
        'id_device': p['id_device'],
        'created': p['created'],
        'source_value': p['source_value'],
        'distance': p['distance'],
    } for p in params_list])

    return out_df, params_list

def push_trip(event_records):
    engine = connect_db('prod_rds_data_production_rw')
    queue_url_score     = 'https://sqs.us-east-1.amazonaws.com/045511714637/ESInsertSQSTripsScores'
    queue_url_event     = 'https://sqs.us-east-1.amazonaws.com/045511714637/ESInsertSQSTripsEvents'
    queue_url_anomaly   = 'https://sqs.us-east-1.amazonaws.com/045511714637/ESInsertSQSTripsAnomalies'
    queue_url_anomaly_up = 'https://sqs.us-east-1.amazonaws.com/045511714637/ESUpdateSQSAnomalies'
    queue_url_scores_up  = 'https://sqs.us-east-1.amazonaws.com/045511714637/ESUpdateSQSScores'
    queue_url_trip_phone = 'https://sqs.us-east-1.amazonaws.com/045511714637/InsertSQSTripPhone'

    df_list: List[pd.DataFrame] = []
    device_keys_for_fk: List[str] = []
    post_send_events: List[Tuple[dict, str]] = []  # (trip_dict, tipo_envio)

    CONFIRM_REQUIRED_TAG = "device:trip-confirm-required"
    SCAFFOLDING_TAG      = "device:scaffolding-stage"

    for record in event_records:
        try:
            body_obj = safe_json_loads_maybe_double(record.get('body', ''))
            trip_dict = body_obj if isinstance(body_obj, dict) else safe_json_loads_maybe_double(body_obj)
            if not isinstance(trip_dict, dict):
                continue

            anomaly_status = (trip_dict.get('anomaly', {}) or {}).get('status', '')
            status_l = str(anomaly_status).strip().lower()

            tags = trip_dict.get('tags') or []
            if not isinstance(tags, list):
                tags = []
            tags_l = {str(t).strip().lower() for t in tags}

            has_scaffolding      = (SCAFFOLDING_TAG in tags_l)
            has_confirm_required = (CONFIRM_REQUIRED_TAG in tags_l)
            is_evaluated         = (status_l == 'evaluated')

            associations = trip_dict.get('associations') or {}
            device_key = str(associations.get('device')) if isinstance(associations, dict) and associations.get('device') else None
            device_present = device_key is not None and len(device_key) > 0

            def get_trip_key(td: dict) -> str:
                return str(td.get('tripId') or td.get('id'))

            trip_key = get_trip_key(trip_dict)

            select_row = False
            insert_reason = None

            if (status_l == 'notevaluated') and has_confirm_required:
                select_row = True
                insert_reason = 'notEvaluated+trip-confirm-required:first-arrival'
                # encolamos actualizaciones asociadas
                post_send_events.append((trip_dict, 'anomaly_up'))
                post_send_events.append((trip_dict, 'scores_up'))

            elif is_evaluated and not has_confirm_required:
                select_row = True
                insert_reason = 'evaluated+scaffolding-stage'
                post_send_events.append((trip_dict, 'anomaly_up'))
                post_send_events.append((trip_dict, 'scores_up'))

            elif is_evaluated and has_confirm_required:
                select_row = False

            elif device_present:
                select_row = True
                insert_reason = 'device_present'

            if select_row:
                row = pd.json_normalize(trip_dict)
                row['device_key'] = device_key
                row['insert_reason'] = insert_reason
                df_list.append(row)
                if device_key:
                    device_keys_for_fk.append(device_key)

                # encolamos otros side-effects
                post_send_events.append((trip_dict, 'events'))
                post_send_events.append((trip_dict, 'scores'))
                post_send_events.append((trip_dict, 'anomaly'))

        except Exception as e:
            print(f"[ERROR] procesando record: {e}")
            continue

    # Mapear FKs en bloque
    device_map = fetch_device_map(engine, device_keys_for_fk)

    result_df, params_list = process_trip_data(df_list, engine, device_map) if df_list else (pd.DataFrame(), [])

    for p in params_list:
        imei_val = str(p.get('_imei_value') or '')
        if re.search(r'octo', imei_val, flags=re.IGNORECASE):
            msg_df = pd.DataFrame([{
                'imei': imei_val,
                'tripId': p['id_op_trip'],
                'created': p['created'].isoformat() if isinstance(p['created'], datetime) else p['created']
            }])
            send_to_sqs({'message': msg_df.to_json(orient='records'),
                         'reason': 'Data for trip_phone table'}, queue_url_trip_phone)

    for trip_dict, action in post_send_events:
        if action == 'events':
            check_and_send(trip_dict, ['accumulations', 'behaviourStats'], trip_dict, queue_url_event, 'trip_events')
        elif action == 'scores':
            check_and_send(trip_dict, ['aggregations', 'scoring'], trip_dict, queue_url_score, 'trip_scores')
        elif action == 'anomaly':
            check_and_send(trip_dict, ['anomaly', 'reporter'], trip_dict, queue_url_anomaly, 'trip_anomalies')
        elif action == 'anomaly_up':
            check_and_send(trip_dict, ['anomaly', 'status'], trip_dict, queue_url_anomaly_up, 'trip_anomalies')
        elif action == 'scores_up':
            check_and_send(trip_dict, ['aggregations', 'scoring'], trip_dict, queue_url_scores_up, 'trip_scores')

    return result_df

def lambda_handler(event, context):
    try:
        print('Processing event', str(event)[:800])
        result = push_trip(event.get('Records', []))

        if isinstance(result, pd.DataFrame) and not result.empty:
            result_data = result.to_dict(orient='records')
            for r in result_data:
                for k, v in list(r.items()):
                    if isinstance(v, np.generic):
                        r[k] = v.item()
                    elif isinstance(v, pd.Timestamp) or isinstance(v, datetime):
                        r[k] = v.isoformat()
        else:
            result_data = []
        
        response = {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Processing completed successfully",
                "result": result_data
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
