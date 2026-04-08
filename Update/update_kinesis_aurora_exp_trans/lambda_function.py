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
from dateutil.parser import parse as parse_date

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

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
    else:
        return engine

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, (np.generic, pd.Timestamp)):
            return obj.item() if isinstance(obj, np.generic) else obj.isoformat()
        elif isinstance(obj, bytes):
            return obj.decode('utf-8')
        return super().default(obj)

def extract_fields(msg):
    try:
        dynamo = msg["dynamodb"]
        keys = dynamo["Keys"]
        new_image = dynamo.get("NewImage", {})

        id_user_ext = keys["PK"]["S"].split("#")[1]
        sk_raw = keys["SK"]["S"]

        if sk_raw.startswith("BALANCE"):
            sk_type = sk_raw.split("#")[0]
            level_part = sk_raw.split("#")[1]
            id_op_level = level_part.split("-")[1]
            date_xp = None

        elif sk_raw.startswith("TRANSACTION"):
            sk_type = sk_raw.split("##")[0]
            details = sk_raw.split("##")[1]
            level_part, date_str = details.split("#")
            id_op_level = level_part.split("-")[1]

            # Soporta tanto #ADD-<fecha> como #REMOVE-<fecha>
            if date_str.startswith("ADD-") or date_str.startswith("REMOVE-"):
                date_str = date_str.split("-", 1)[1]

            if date_str.endswith("Z"):
                date_str = date_str.replace("Z", "+00:00")
            date_xp = parse_date(date_str)
        else:
            raise ValueError("Formato SK no reconocido")

        # Inicializar campos
        points = None
        amount = None

        if "points" in new_image:
            points = int(new_image["points"]["N"])
        if "amount" in new_image:
            amount = int(new_image["amount"]["N"])
        if points is None and amount is None:
            raise ValueError("No se encontró ni 'points' ni 'amount' en el mensaje")

        parsed = {
            "sk_type": sk_type,
            "id_op_level": id_op_level,
            "date_xp": date_xp,
            "updated_at": datetime.utcnow()
        }

        if points is not None:
            parsed["point_xp"] = points  # si viene -100 en REMOVE, se mantiene tal cual
        if amount is not None:
            parsed["amount_xp"] = amount

        return parsed, id_user_ext

    except Exception as e:
        print(f"[ERROR extract_fields] {e}")
        return None, None


# def extract_fields(msg):
#     try:
#         dynamo = msg["dynamodb"]
#         keys = dynamo["Keys"]
#         new_image = dynamo.get("NewImage", {})

#         id_user_ext = keys["PK"]["S"].split("#")[1]
#         sk_raw = keys["SK"]["S"]

#         if sk_raw.startswith("BALANCE"):
#             sk_type = sk_raw.split("#")[0]
#             level_part = sk_raw.split("#")[1]
#             id_op_level = level_part.split("-")[1]
#             date_xp = None

#         elif sk_raw.startswith("TRANSACTION"):
#             sk_type = sk_raw.split("##")[0]
#             details = sk_raw.split("##")[1]
#             level_part, date_str = details.split("#")
#             id_op_level = level_part.split("-")[1]

#             if date_str.startswith("ADD-"):
#                 date_str = date_str.replace("ADD-", "")
#             if date_str.endswith("Z"):
#                 date_str = date_str.replace("Z", "+00:00")
#             date_xp = parse_date(date_str)
#         else:
#             raise ValueError("Formato SK no reconocido")

#         # Inicializar campos
#         points = None
#         amount = None

#         if "points" in new_image:
#             points = int(new_image["points"]["N"])
#         if "amount" in new_image:
#             amount = int(new_image["amount"]["N"])
#         if points is None and amount is None:
#             raise ValueError("No se encontró ni 'points' ni 'amount' en el mensaje")

#         parsed = {
#             "sk_type": sk_type,
#             "id_op_level": id_op_level,
#             "date_xp": date_xp,
#             "updated_at": datetime.utcnow()
#         }

#         if points is not None:
#             parsed["point_xp"] = points
#         if amount is not None:
#             parsed["amount_xp"] = amount

#         return parsed, id_user_ext

#     except Exception as e:
#         print(f"[ERROR extract_fields] {e}")
#         return None, None

def lambda_handler(event, context):
    engine = connect_db('prod_rds_data_production_rw')

    try:
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

                    user_result = conn.execute(
                        text("SELECT id_user FROM users_person WHERE id_op_user = :id_user_ext"),
                        {"id_user_ext": id_user_ext}
                    ).fetchone()
                    if not user_result:
                        print(f"No user found for external id: {id_user_ext}")
                        continue
                    parsed["id_user"] = user_result[0]

                    level_result = conn.execute(
                        text("SELECT id_level FROM levels WHERE id_op_level = :id_op_level"),
                        {"id_op_level": parsed["id_op_level"]}
                    ).fetchone()
                    if not level_result:
                        print(f"No level found for external id: {parsed['id_op_level']}")
                        continue
                    parsed["id_level"] = level_result[0]
                    parsed.pop("id_op_level", None)

                    check_query = text(
                        "SELECT id_xp_transaction FROM xp_transaction "
                        "WHERE id_user = :id_user AND id_level = :id_level "
                        "AND (date_xp = :date_xp OR (:date_xp IS NULL AND date_xp IS NULL))"
                    )
                    existing = conn.execute(check_query, {
                        "id_user": parsed["id_user"],
                        "id_level": parsed["id_level"],
                        "date_xp": parsed["date_xp"]
                    }).fetchone()

                    if existing:
                        # Update dinámico
                        update_fields = ["updated_at = :updated_at"]
                        update_params = {
                            "updated_at": parsed["updated_at"],
                            "id_xp_transaction": existing[0],
                        }

                        if "amount_xp" in parsed:
                            update_fields.append("amount_xp = :amount_xp")
                            update_params["amount_xp"] = parsed["amount_xp"]

                        if "point_xp" in parsed:
                            update_fields.append("point_xp = :point_xp")
                            update_params["point_xp"] = parsed["point_xp"]

                        update_query = text(
                            f"UPDATE xp_transaction SET {', '.join(update_fields)} "
                            "WHERE id_xp_transaction = :id_xp_transaction"
                        )
                        conn.execute(update_query, update_params)
                        print(f"Updated xp_transaction ID {existing[0]}")

                    else:
                        insert_fields = [
                            "sk_type", "id_user", "id_level", "date_xp", "updated_at"
                        ]
                        insert_values = [
                            ":sk_type", ":id_user", ":id_level", ":date_xp", ":updated_at"
                        ]

                        if "amount_xp" in parsed:
                            insert_fields.append("amount_xp")
                            insert_values.append(":amount_xp")
                        if "point_xp" in parsed:
                            insert_fields.append("point_xp")
                            insert_values.append(":point_xp")

                        insert_query = text(
                            f"INSERT INTO xp_transaction ({', '.join(insert_fields)}) "
                            f"VALUES ({', '.join(insert_values)})"
                        )
                        conn.execute(insert_query, parsed)
                        print(f"Inserted new xp_transaction {parsed}")

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