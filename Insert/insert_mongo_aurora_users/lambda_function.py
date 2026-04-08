import sys
import boto3
import json
import gc
import re
import numpy as np
import sqlalchemy as db
import pymongo
import pandas as pd

from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from bson.objectid import ObjectId
from pytz import timezone
from typing import Optional, Dict

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

def get_address_single(field, item):
    if item.get('stages').get('insuredAddress') is None:
        out = None
    elif item.get('stages').get('insuredAddress').get(field) is not None:
        out = item.get('stages').get('insuredAddress').get(field)
    return out

def get_name_single(field, item):
    if item.get('stages').get('insuredPerson') is None:
        out = None
    elif item.get('stages').get('insuredPerson').get(field) is None:
        out = None
    elif item.get('stages').get('insuredPerson').get(field) is not None:
        out = item.get('stages').get('insuredPerson').get(field)
    return out

def remove_accents(old):
    """
    Removes common accent characters, lower form.
    """
    new = old.lower()
    new = re.sub(r'[àáâãäå]', 'a', new)
    new = re.sub(r'[èéêë]', 'e', new)
    new = re.sub(r'[ìíîï]', 'i', new)
    new = re.sub(r'[òóôõö]', 'o', new)
    new = re.sub(r'[ùúûü]', 'u', new)
    
    return new

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


def insert_data_to_aurora(engine, data, data_type, id_person):
    try:
        with engine.connect() as connection:
            if data_type == 'full_document':
                insert_query = text("""
                    INSERT INTO users_person (
                        id_op_user, id_person, dni, email_user, timezone, country,
                                    phone_user, confirmed_phone_user
                    ) VALUES (
                        :id_op_user, :id_person, :dni, :email_user, :timezone, :country,
                                    :phone_user, :confirmed_phone_user
                    )
                """)

                dni = get_valid_field(
                    data, 
                    ('identifiers', 'usernameChilena'),
                    ('identifiers', 'rutSura'),
                    ('identifiers', 'usernameCNS'),
                    ('identifiers', 'dniRimac'),
                    ('identifiers', 'rutBT'),
                    ('identifiers', 'rutHdi')
                )

                email_user = get_valid_field(
                    data, 
                    'email', 
                    ('identifiers', 'emailChilena'),
                    ('identifiers', 'emailSura'),
                    ('identifiers', 'emailCNS'),
                    ('identifiers', 'emailRimac'),
                    ('identifiers', 'emailBT'),
                    ('identifiers', 'emailHdi'),
                    ('identifiers', 'emailRenta')
                )

                confirmed_phone_user = get_valid_field(
                    data, 
                    'confirmedPhone', 
                    None
                )

                data_to_insert = {
                    'id_op_user': str(data['_id']), 
                    'id_person': id_person,
                    'dni': dni, 
                    'email_user': email_user,
                    'timezone': data['timezone'],
                    'country': data['country'],
                    'phone_user': data['phone'],
                    'confirmed_phone_user': confirmed_phone_user
                }

            elif data_type == 'payer_person':
                insert_query = text("""
                    INSERT INTO payers_person (
                        id_op_payer, id_person, dni_payer, email_payer, strategy_payer, auth_code_payer, card_type,
                                    last_4_card_digits, tbk_payer
                    ) VALUES (
                        :id_op_payer, :id_person, :dni_payer, :email_payer, :strategy_payer, :auth_code_payer, :card_type,
                                    :last_4_card_digits, :tbk_payer
                    )
                """)


                data_to_insert = {
                    'id_op_payer': str(data['id_op_payer']), 
                    'id_person': data['id_person'],
                    'dni_payer': data['dni_payer'],
                    'email_payer': data['email_payer'],
                    'strategy_payer': data['strategy_payer'],
                    'auth_code_payer': data['auth_code_payer'],
                    'card_type': data['card_type'],
                    'last_4_card_digits': data['last_4_card_digits'],
                    'tbk_payer': data['tbk_payer']
                }


            data_to_insert = convert_types(data_to_insert)

            connection.execute(insert_query, **data_to_insert)
        return True
    except Exception as e:
        print(f"Error inserting data: {e}")
        return False
    
def prepare_payer_person_data(data, id_person):
    return {
        'id_op_payer': str(data['user']), 
        'id_person': id_person,
        'dni_payer': get_valid_field(
            data, 
            ('info', 'DNI'),
            ('info', 'rut'),
            ('info', 'RUC'),
            ('info', 'CE'),
            ('info', 'idValue')
        ),
        'email_payer': get_valid_field(
            data, 
            'email', 
            ('info', 'email')
        ),
        'strategy_payer': get_valid_field(
            data, 
            ('subscription', 'strategy'),
            None
        ),
        'auth_code_payer': None,
        'card_type': get_valid_field(
            data, 
            ('subscription', 'subscriptionResult','creditCardType'),
            None
        ),
        'last_4_card_digits': get_valid_field(
            data, 
            'email', 
            ('subscription', 'subscriptionResult','last4CardDigits'),
            None
        ),
        'tbk_payer': get_valid_field(
            data,  
            ('subscription', 'subscriptionResult','customerId'),
            None
        )
    }

def fetch_payer_person_data(mydb, user_id, id_person):
    subscriptions = mydb['servicecontracts']
    payer_person_data = subscriptions.find_one({'user': ObjectId(user_id)})
    return prepare_payer_person_data(payer_person_data, id_person)

def person_presale_data(mydb, user_id):
    collection = mydb['presaleleads']
    query = {'user': ObjectId(user_id)}
    projection = {
        'stages.insuredPerson.firstName': 1,
        'stages.insuredPerson.idValue':1,
        'stages.insuredPerson.lastName1': 1,
        'stages.insuredPerson.birthDate': 1,
        'stages.insuredPerson.gender': 1,
        'stages.insuredPerson.email': 1,
        'stages.insuredAddress.street': 1,
        'user': 1
    }

    presale_query = collection.find(query, projection)
    presale_data = []
    for doc in presale_query:
        data = {
            'id_op_person': str(doc.get('user', '')),
            'first_name_person': get_name_single('firstName', doc),
            'last_name_person': get_name_single('lastName1', doc),
            'birth_date': get_name_single('birthDate', doc) or None,
            'gender': get_name_single('gender', doc),
            'dni': get_name_single('idValue', doc),
            'address': get_address_single('street', doc),
            'email_person': get_name_single('email', doc)
        }
        presale_data.append(data)

    df = pd.DataFrame(presale_data)
    
    df['first_name_person'] = df['first_name_person'].str.lower().map(remove_accents)
    df['last_name_person'] = df['last_name_person'].str.lower().map(remove_accents)
    df['email_person'] = df['email_person'].str.lower()

    variables = ['first_name_person', 'last_name_person']
    df[variables] = df[variables].apply(lambda x: x.str.strip().str.title())

    return df.to_dict('records')

def insert_person_data(engine, mydb, user_id):
    presale_data = person_presale_data(mydb, user_id)
    if not presale_data:
        return False
    
    insert_query = text("""
        INSERT INTO persons (
            id_op_person, first_name_person, last_name_person, birth_date, gender, dni, 
            address, email_person
        ) VALUES (
            :id_op_person, :first_name_person, :last_name_person, :birth_date, :gender, :dni, 
            :address, :email_person
        )
    """)

    data_to_insert = convert_types(presale_data)
    
    try:
        with engine.connect() as connection:
            for data in data_to_insert:
                connection.execute(insert_query, **data)
                pass
        return True
    except Exception as e:
        print(f"Error inserting person data: {e}")
        return False


def lambda_handler(event, context):
    engine, mydb = connect_db('prod_rds_data_production_rw', 'mongo_production_ro')
    queue_url = 'https://sqs.us-east-1.amazonaws.com/045511714637/MongoInsertSQSAllMessageFail'

    for record in event['Records']:
        try:
            if isinstance(record['body'], str):
                body = json.loads(record['body'])  
            else:
                body = record['body']

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON body: {record['body']}, Error: {e}")
            continue

        if 'detail' not in body or 'fullDocument' not in body['detail']:
            print("Skipping record due to missing 'detail' or 'fullDocument'")
            continue

        full_document = body['detail']['fullDocument']

        user_id = full_document['_id']

        if insert_person_data(engine, mydb, user_id):
            print("Person data inserted successfully.")

            # Queries to fetch foreign keys
            person_key = full_document['_id']

            query_person = "SELECT id_person as id, id_op_person as id_op FROM persons"

            id_person = fetch_foreign_key(engine, query_person, person_key)

            if not all([id_person]) or 0 in [id_person]:
                message_body = {
                    'record': record,
                    'reason': 'Missing or invalid foreign keys'
                }
                send_to_sqs(message_body, queue_url)
                continue
            
            # Collect data for the user_person and payer_person
            payer_person_data = fetch_payer_person_data(mydb, user_id, id_person)

            #Insert data to Aurora and check if insertion is successful
            if insert_data_to_aurora(engine, full_document, 'full_document', id_person):
                print('User Person data inserted succesfully')
                pass

            if insert_data_to_aurora(engine, payer_person_data, 'payer_person', id_person):
                print('Payer Person data inserted succesfully')
                pass
        else:
            print("Failed to insert person data. Aborting process for this record.")

    return {
        'statusCode': 200,
        'body': json.dumps('Processed successfully')
    }