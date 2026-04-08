#!/bin/bash

set -eo pipefail

aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 045511714637.dkr.ecr.us-east-1.amazonaws.com
docker build -t insert_mongo_users .
docker tag insert_mongo_users:latest 045511714637.dkr.ecr.us-east-1.amazonaws.com/insert_mongo_users:latest
docker push 045511714637.dkr.ecr.us-east-1.amazonaws.com/insert_mongo_users:latest