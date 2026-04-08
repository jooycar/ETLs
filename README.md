# ETLs

Collection of AWS Lambda functions that synchronize data from MongoDB and other sources (Kinesis, SNS, Elasticsearch) into Aurora PostgreSQL. Each Lambda is triggered by event sources (SQS, Kinesis streams, SNS topics) and handles a specific entity or data flow.

## Structure

```
ETLs/
├── Insert/   # New records from MongoDB/Elastic/Aurora into Aurora
├── Update/   # Updates from MongoDB/Kinesis/SNS/Elastic into Aurora
└── Delete/   # Deletions from MongoDB into Aurora
```

## Lambdas

### Insert

| Lambda | Description |
|--------|-------------|
| `insert_mongo_aurora_users` | Inserts users, persons and payer data from MongoDB into Aurora |
| `insert_mongo_aurora_vehicles` | Inserts vehicle records from MongoDB into Aurora |
| `insert_mongo_aurora_devices` | Inserts device records from MongoDB into Aurora |
| `insert_mongo_aurora_contracts` | Inserts contract records from MongoDB into Aurora |
| `insert_mongo_aurora_device_contracts` | Inserts device-contract relationships from MongoDB into Aurora |
| `insert_mongo_aurora_communications` | Inserts communication records from MongoDB into Aurora |
| `insert_mongo_aurora_achievement_user` | Inserts user achievement records from MongoDB into Aurora |
| `insert_mongo_usr_lvls_path` | Inserts user levels and path data from MongoDB into Aurora |
| `insert_mongo_usr_rwdr` | Inserts user reward records from MongoDB into Aurora |
| `insert_elastic_aurora_trips` | Inserts trip records from Elasticsearch into Aurora |
| `insert_elastic_aurora_trips_anomalies` | Inserts trip anomaly records from Elasticsearch into Aurora |
| `insert_elastic_aurora_trips_events` | Inserts trip event records from Elasticsearch into Aurora |
| `insert_elastic_aurora_trips_scores` | Inserts trip score records from Elasticsearch into Aurora |
| `insert_aurora_trips_phone` | Inserts trip phone data into Aurora |

### Update

| Lambda | Description |
|--------|-------------|
| `update_mongo_aurora_users` | Updates user records from MongoDB into Aurora |
| `update_mongo_aurora_devices` | Updates device records from MongoDB into Aurora |
| `update_mongo_aurora_contracts` | Updates contract records from MongoDB into Aurora |
| `update_mongo_aurora_communications` | Updates communication records from MongoDB into Aurora |
| `update_mongo_aurora_onboarding` | Updates onboarding records from MongoDB into Aurora |
| `update_mongo_aurora_usr_lvls_path` | Updates user levels and path data from MongoDB into Aurora |
| `update_mongo_aurora_usr_rwrd` | Updates user reward records from MongoDB into Aurora |
| `update_mongo_aurora_usr_rwrd_mtdt` | Updates user reward metadata from MongoDB into Aurora |
| `update_elastic_aurora_trips` | Updates trip records from Elasticsearch into Aurora |
| `update_elastic_aurora_trip_anomalies` | Updates trip anomaly records from Elasticsearch into Aurora |
| `update_elastic_aurora_trip_scores` | Updates trip score records from Elasticsearch into Aurora |
| `update_kinesis_aurora_wallet_trans` | Updates wallet transactions from Kinesis into Aurora |
| `update_kinesis_aurora_exp_trans` | Updates expense transactions from Kinesis into Aurora |
| `update_sns_aurora_achievement_user` | Updates user achievement records from SNS into Aurora |

### Delete

| Lambda | Description |
|--------|-------------|
| `delete_mongo_aurora` | Deletes records from Aurora based on MongoDB events |
| `delete_mongo_aurora_idcomp` | Deletes records from Aurora by composite ID based on MongoDB events |

## Each Lambda contains

- `lambda_function.py` — main handler and business logic
- `requirements.txt` — Python dependencies
- `Dockerfile` — container image definition for deployment
- `deploy.sh` — deployment script to AWS Lambda

## Tech stack

- **Runtime**: Python (AWS Lambda)
- **Sources**: MongoDB, Elasticsearch, Kinesis, SNS
- **Destination**: Aurora PostgreSQL
- **Queue**: SQS (dead-letter / error handling)
- **Secrets**: AWS Secrets Manager
- **Dependencies**: `sqlalchemy`, `pymongo`, `pandas`, `boto3`
