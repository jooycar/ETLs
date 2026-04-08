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

engine = None

def connect_all(aurora_secret_name):
    """
    Function to create and return a SQLAlchemy engine.
    This is a costly operation, so caching the engine can improve performance.
    """
    global engine

    if engine:
        return engine

    # AWS SDK to get credentials from Secrets Manager
    session = boto3.session.Session()
    aws_client = session.client(service_name='secretsmanager', region_name='us-east-1')
    credentials = json.loads(aws_client.get_secret_value(SecretId=aurora_secret_name)['SecretString'])

    user = credentials['user']
    password = credentials['password']
    host = credentials['host']
    puerto = credentials['port']
    db = 'postgres'

    sslmode = 'require'

    # Create SQLAlchemy engine with SSL connection
    engine = create_engine(
        f'postgresql+psycopg2://{user}:{password}@{host}:{puerto}/{db}',
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_size=10,
        max_overflow=20,
        connect_args={'sslmode': sslmode}
    )

    return engine


def handle_delete_query(engine, record_id, collection_name):

    collection_to_query = {
        'userachievementpaths': "DELETE FROM achievement_user WHERE id_op_achievement_user = :record_id"
    }

    # Dynamically select the delete query based on collection name
    delete_query = collection_to_query.get(collection_name)
    if delete_query:
        try:
            with engine.connect() as connection:
                with connection.begin():
                    connection.execute(text(delete_query), {"record_id": record_id})
                    print(f"Deleted record with ID: {record_id}")
        except Exception as e:
            print(f"Error executing delete query for collection {collection_name}: {e}")
    else:
        print(f"No delete query found for collection: {collection_name}")


def lambda_handler(event, context):
    #print('Processing event:', event)

    engine = connect_all(aurora_secret_name='prod_rds_data_production_rw')

    # Process each record from SQS
    for record in event['Records']:
        print('Processing record:', record)

        body = record.get('body', {})

        # Parse body if necessary
        if isinstance(body, str):
            body = json.loads(body)

        record_id = body.get('detail', {}).get('documentKey', {}).get('_id', None)

        if not record_id:
            print("No record_id found, skipping...")
            continue

        print(f'Deleting record with ID: {record_id}')

        collection_name = body.get('detail', {}).get('ns', {}).get('coll', None)

        if collection_name:
            print(f'Collection name: {collection_name}')
            handle_delete_query(engine, record_id, collection_name)
        else:
            print("Invalid message structure or no collection name found, skipping...")

    return {
        'statusCode': 200,
        'body': json.dumps('Processed successfully')
    }
