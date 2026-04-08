import sys
import boto3
import json
import os
import gc
import numpy as np
import sqlalchemy as db
import pandas as pd

from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
from pytz import timezone
from typing import Optional, Dict

def connect_all(aurora_secret_name):
    ## ========= AWS SDK =========
    session = boto3.session.Session()
    ## AWS Secret Manager - Get credentials
    aws_client = session.client(service_name = 'secretsmanager',region_name = 'us-east-1')

   ## ========= Postgres BD ========
    credentials = json.loads(aws_client.get_secret_value(SecretId= aurora_secret_name)['SecretString'])
    user = credentials['user']
    password = credentials['password']
    host = credentials['host']
    puerto = credentials['port']
    db = 'postgres'
    
    # Set SSL mode to require
    sslmode = 'require'
    
    # Create engine with SSL settings
    engine = create_engine(
        f'postgresql+psycopg2://{user}:{password}@{host}:{puerto}/{db}',
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_size=10, 
        max_overflow=20,
        connect_args={'sslmode': sslmode}  # Pass SSL mode as connect argument
    )
    return engine


def lambda_handler(event, context):
    print('Process event:', event)

    engine = connect_all(aurora_secret_name='prod_rds_data_production_rw')

    # Process each record from SQS
    for record in event['Records']:
        print('Processing record:', record)

        body = record['body']
        print('Type of body:', type(body))

        # If the body is a string, parse it as JSON
        if isinstance(body, str):
            body = json.loads(body)
        elif isinstance(body, dict):
            pass
        else:
            continue

       # Access the _id from the nested detail.documentKey object
        record_id = body['detail']['documentKey']['_id']
        collection_name = body['detail']['ns']['coll']
        print(f'Deleting record with ID: {record_id} (Type: {type(record_id)})')

        if collection_name not in ['insurancepolicies', 'servicecontracts', 'devices']:
            print(f'Skipping deletion for collection: {collection_name}')
            continue
        elif collection_name == 'insurancepolicies' or collection_name == 'servicecontracts':
            delete_query = text("DELETE FROM contracts WHERE id_op_contract = :record_id")
        elif collection_name == 'devices':
            delete_query = text("DELETE FROM devices WHERE id_op_device = :record_id")
        
        try:
            with engine.connect() as connection:
                with connection.begin():
                    connection.execute(delete_query, {"record_id": record_id})
                    print(f"Deleted record with ID: {record_id}")
        except Exception as e:
            print(f"Error executing delete query: {e}")

    return {
        'statusCode': 200,
        'body': json.dumps('Processed successfully')
    }

