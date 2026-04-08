#!/bin/bash

set -eo pipefail

aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 045511714637.dkr.ecr.us-east-1.amazonaws.com
docker build -t delete_mongo_aurora .
docker tag delete_mongo_aurora:latest 045511714637.dkr.ecr.us-east-1.amazonaws.com/delete_mongo_aurora:latest
docker push 045511714637.dkr.ecr.us-east-1.amazonaws.com/delete_mongo_aurora:latest