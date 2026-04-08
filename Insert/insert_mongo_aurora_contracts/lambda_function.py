import sys
import boto3
import json
import numpy as np
import sqlalchemy as db
import pymongo
import pandas as pd

from sqlalchemy import create_engine, text
from pytz import timezone
from datetime import datetime, timedelta

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

    if mongo_secret_name:
        mongo_cred = json.loads(aws_client.get_secret_value(SecretId=mongo_secret_name)['SecretString'])
        STRING_MONGO = mongo_cred['STRING_MONGO']
        mongo_client = pymongo.MongoClient(STRING_MONGO)
        mydb = mongo_client['production-01']
        return engine, mydb

    return engine

def fetch_foreign_key(engine, query, key):
    try:
        result = pd.read_sql(query, con=engine)
        return result[result['id_op'] == key].iloc[0]['id']
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

def send_to_sqs(message_body, queue_url):
    sqs_client = boto3.client('sqs', region_name='us-east-1')
    response = sqs_client.send_message(QueueUrl=queue_url, MessageBody=json.dumps(message_body))
    return response

def get_valid_field(data, *keys):
    for key in keys:
        if isinstance(key, tuple):
            value = data
            for subkey in key:
                value = value.get(subkey) if isinstance(value, dict) else None
                if value in ["", "NA", "null", None]:
                    break
        else:
            value = data.get(key)

        if value not in ["", "NA", "null", None]:
            return value
    return None

def lambda_handler(event, context):
    engine = connect_db('prod_rds_data_production_rw')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/045511714637/MongoInsertSQSAllMessageFail'
    queue_device_contract = 'https://sqs.us-east-1.amazonaws.com/045511714637/MongoInsertSQSDeviceContracts'

    print('Process event:', event)

    for record in event['Records']:
        try:
            body = json.loads(record['body']) if isinstance(record['body'], str) else record['body']
        except json.JSONDecodeError as e:
            print(f"Error decoding JSON body: {record['body']}, Error: {e}")
            break

        if 'detail' not in body or 'fullDocument' not in body['detail']:
            print("Skipping record due to missing 'detail' or 'fullDocument'")
            break

        full_document = body['detail']['fullDocument']

        vehicle_key = full_document['vehicle']
        user_key = full_document['user']
        payer_key = full_document['user']
        tenant_key = full_document['tenant']

        query_vehicle = "SELECT id_veh_logic as id, id_op_veh_log as id_op FROM vehicles_logic"
        query_user = "SELECT id_user as id, id_op_user as id_op FROM users_person"
        query_payer = "SELECT id_payer as id, id_op_payer as id_op FROM payers_person"
        query_tenant = "SELECT id_tenant as id, id_op_tenant as id_op FROM tenants"

        id_veh_logic = fetch_foreign_key(engine, query_vehicle, vehicle_key)
        id_user = fetch_foreign_key(engine, query_user, user_key)
        id_payer = fetch_foreign_key(engine, query_payer, payer_key)
        id_tenant = fetch_foreign_key(engine, query_tenant, tenant_key)

        if not all([id_veh_logic, id_user, id_payer, id_tenant]) or 0 in [id_veh_logic, id_user, id_payer, id_tenant]:
            message_body = {'record': record, 'reason': 'Missing or invalid foreign keys'}
            send_to_sqs(message_body, queue_url)
            continue

        insert_query = text("""
            INSERT INTO contracts (
                id_op_contract, id_tenant, id_payer, id_user, id_veh_logic, modif_date, 
                policy_status, installation_status, installation_date, policy_type, 
                renewal_status, proposal_id, start_date, end_date, foreign_id, 
                period_strategy, updated_at, created_mongo, provider, campaign_code
            ) VALUES (
                :id_op_contract, :id_tenant, :id_payer, :id_user, :id_veh_logic, :modif_date, 
                :policy_status, :installation_status, :installation_date, :policy_type, 
                :renewal_status, :proposal_id, :start_date, :end_date, :foreign_id, 
                :period_strategy, CURRENT_TIMESTAMP, :created_mongo, :provider, :campaign_code
            )
            RETURNING id_contract
        """)


        proposal_id = get_valid_field(full_document, ('info', 'voucherId'))
        end_date = get_valid_field(full_document, 'end')
        foreign_id = get_valid_field(full_document, ('info', 'externalId'))
        campaign_code = get_valid_field(full_document, ('info', 'campaignCode'))

        data_to_insert = {
            'id_op_contract': full_document['_id'],
            'id_tenant': id_tenant,
            'id_payer': id_payer,
            'id_user': id_user,
            'id_veh_logic': id_veh_logic,
            'modif_date': full_document['modif'],
            'policy_status': full_document['status'],
            'installation_status': full_document['installationStatus'],
            'installation_date': None,
            'policy_type': full_document['type'],
            'renewal_status': None,
            'proposal_id': proposal_id,
            'start_date': full_document['start'],
            'end_date': end_date,
            'foreign_id': foreign_id,
            'period_strategy': None,
            'created_mongo': full_document['modif'],
            'provider': full_document['provider'],
            'campaign_code': campaign_code
        }

        data_to_insert = convert_types(data_to_insert)

        try:
            with engine.connect() as connection:
                # ===== INSERT CONTRACT & fetch id_contract =====
                result = connection.execute(insert_query, **data_to_insert)
                row = result.fetchone()
                id_contract = row[0] if row else None
                if not id_contract:
                    print("Could not fetch id_contract after insert; skipping device relation.")
                else:
                    print(f"Contracts data inserted successfully. id_contract={id_contract}")

                # ===== Update DNI en users_person (tal cual tenías) =====
                id_value = get_valid_field(full_document, ('info', 'idValue'))
                if id_value and user_key:
                    check_user_query = text("""
                        SELECT id_user FROM users_person
                        WHERE id_op_user = :id_op_user
                        LIMIT 1
                    """)
                    result_user = connection.execute(check_user_query, {'id_op_user': user_key}).fetchone()
                    if result_user:
                        update_query = text("""
                            UPDATE users_person
                            SET dni = :dni
                            WHERE id_op_user = :id_op_user
                        """)
                        connection.execute(update_query, {'dni': id_value, 'id_op_user': user_key})
                        print(f"DNI {id_value} update for user_key {user_key}")
                    else:
                        print(f"Warning: User not found with id_op_user: {user_key}")
                else:
                    print("info.idValue or user_key not found. Update aborted for DNI.")

                # ===== NUEVO: Insert/Upsert en devices_contract si viene 'device' =====
                device_value = full_document.get('device', None)
                if device_value and id_contract:
                    # Buscar id_device
                    device_id = fetch_foreign_key(
                        engine,
                        'SELECT id_device as id, id_op_device as id_op FROM devices',
                        device_value
                    )

                    if device_id is None:
                        print(f"Device {device_value} not found in devices table. Skipping devices_contract.")
                    else:
                        # Cerrar el último registro de ese device (si existe)
                        last_device_query = text("""
                            SELECT id_device_contract 
                            FROM devices_contract
                            WHERE id_device = :id_device
                            ORDER BY created_voucher_start DESC
                            LIMIT 1
                        """)
                        last_device_update = text("""
                            UPDATE devices_contract
                            SET created_voucher_end = :created_voucher_end
                            WHERE id_device_contract = :id_device_contract
                        """)

                        try:
                            id_device_python = int(device_id)
                            last_device = connection.execute(
                                last_device_query, {'id_device': id_device_python}
                            ).fetchone()
                            if last_device:
                                connection.execute(last_device_update, {
                                    'id_device_contract': last_device['id_device_contract'],
                                    'created_voucher_end': datetime.now()
                                })
                                print(f"Updated end date for previous device_contract ID: {last_device['id_device_contract']}")
                            else:
                                print("No previous device_contract found, skipping end date update.")
                        except Exception as e:
                            print(f"Error updating end date for last device record: {e}")

                        # Insert/Upsert actual
                        insert_dc = text("""
                            INSERT INTO devices_contract (
                                id_device, id_contract, created_voucher_start, created_voucher_end, updated_at
                            )
                            VALUES (:id_device, :id_contract, :created_voucher_start, :created_voucher_end, :updated_at)
                            ON CONFLICT (id_device, id_contract) DO UPDATE
                            SET created_voucher_start = EXCLUDED.created_voucher_start,
                                created_voucher_end = EXCLUDED.created_voucher_end,
                                updated_at = EXCLUDED.updated_at
                        """)

                        created_voucher_start = get_valid_field(full_document, ('info', 'voucherCreatedAt'))
                        created_voucher_end = get_valid_field(full_document, 'end')

                        dc_payload = {
                            'id_device': device_id,
                            'id_contract': id_contract,
                            'created_voucher_start': created_voucher_start,
                            'created_voucher_end': created_voucher_end,
                            'updated_at': datetime.now()
                        }

                        dc_payload = convert_types(dc_payload)

                        try:
                            connection.execute(insert_dc, **dc_payload)
                            print(f"Device-Contract upserted: {dc_payload}")
                        except Exception as e:
                            print(f"Error inserting/updating device-contract: {e}")
                else:
                    if not device_value:
                        print("No 'device' in full_document; skipping devices_contract relation.")
                    if not id_contract:
                        print("id_contract not available; skipping devices_contract relation.")

                send_to_sqs(event, queue_device_contract)
                continue
        except Exception as e:
            print(f"Error inserting data: {e}")

    return {
        'statusCode': 200,
        'body': json.dumps('Processed successfully')
    }
