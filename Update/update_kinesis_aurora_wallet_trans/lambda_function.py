import sys
import boto3
import json
import gc
import numpy as np
import sqlalchemy as db
import pymongo
import pandas as pd
import logging
import base64

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime
from pytz import timezone

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

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat() 
        elif isinstance(obj, (np.generic, pd.Timestamp)): 
            return obj.item() if isinstance(obj, np.generic) else obj.isoformat()
        elif isinstance(obj, bytes):
            return obj.decode('utf-8')
        return super().default(obj)
    
##############################################
# Main Function
##############################################

def extract_fields(msg):
    try:
        dynamo = msg["dynamodb"]
        keys = dynamo["Keys"]
        new_image = dynamo.get("NewImage", {})

        id_user_ext = keys["PK"]["S"].split("#")[1]
        sk_raw = keys["SK"]["S"]

        sk_type = sk_raw.split("#")[0]
        created_at = sk_raw.split("#")[1] if "#" in sk_raw else None

        amount = int(new_image["amount"]["N"])
        action = new_image.get("action", {}).get("S")
        title = new_image.get("title", {}).get("S")
        description = new_image.get("description", {}).get("S")

        return {
            "sk_type": sk_type,
            "created_at": created_at,
            "updated_at": datetime.utcnow(),
            "amount_wallet": amount,
            "action_wallet": action,
            "title_wallet": title,
            "description_wallet": description
        }, id_user_ext

    except Exception as e:
        print(f"Error extracting fields: {e}")
        return None, None


def lambda_handler(event, context):
    engine = connect_db('prod_rds_data_production_rw')
    try:
        print('Procesing event', json.dumps(event))
        
        with engine.connect() as conn:
            for record in event.get("Records", []):
                try:
                    encoded_data = record['kinesis']['data']
                    decoded = base64.b64decode(encoded_data).decode('utf-8')
                    message = json.loads(decoded)
                    print("Mensaje decodificado:", message)

                    parsed, id_user_ext = extract_fields(message)
                    if not parsed or not id_user_ext:
                        continue

                    # Obtener id_user interno desde users_person
                    id_user_query = text("SELECT id_user FROM users_person WHERE id_op_user = :id_user_ext")
                    result = conn.execute(id_user_query, {"id_user_ext": id_user_ext}).fetchone()
                    if not result:
                        print(f"No user found for external id: {id_user_ext}")
                        continue

                    parsed["id_user"] = result[0]

                    check_query = text("""
                        SELECT id_wallet_transaction FROM wallet_transaction
                        WHERE id_user = :id_user AND sk_type = :sk_type
                        AND (created_at = :created_at OR (:created_at IS NULL AND created_at IS NULL))
                    """)

                    existing = conn.execute(check_query, {
                        "id_user": parsed["id_user"],
                        "sk_type": parsed["sk_type"],
                        "created_at": parsed["created_at"]
                    }).fetchone()

                    if existing:
                        update_query = text("""
                            UPDATE wallet_transaction
                            SET amount_wallet = :amount_wallet,
                                action_wallet = :action_wallet,
                                title_wallet = :title_wallet,
                                description_wallet = :description_wallet,
                                updated_at = :updated_at
                            WHERE id_wallet_transaction = :id_wallet_transaction
                        """)
                        conn.execute(update_query, {
                            **parsed,
                            "id_wallet_transaction": existing[0]
                        })
                        print(f"Updated wallet_transaction ID {existing[0]} with data: {parsed}")
                    else:
                        insert_query = text("""
                            INSERT INTO wallet_transaction (
                                sk_type, created_at, updated_at,
                                action_wallet, amount_wallet,
                                title_wallet, description_wallet,
                                id_user
                            ) VALUES (
                                :sk_type, :created_at, :updated_at,
                                :action_wallet, :amount_wallet,
                                :title_wallet, :description_wallet,
                                :id_user
                            )
                        """)
                        conn.execute(insert_query, parsed)
                        print(f"Inserted new wallet_transaction {parsed}")

                except Exception as e:
                    print(f"Error processing record: {e}")

        response = {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Processing completed successfully",
                "result": parsed
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
