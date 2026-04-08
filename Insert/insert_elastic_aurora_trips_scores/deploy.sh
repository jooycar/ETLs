#!/bin/bash

set -eo pipefail

aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 045511714637.dkr.ecr.us-east-1.amazonaws.com
docker build -t insert_elasticsearch_trips_scores .
docker tag insert_elasticsearch_trips_scores:latest 045511714637.dkr.ecr.us-east-1.amazonaws.com/insert_elasticsearch_trips_scores:latest
docker push 045511714637.dkr.ecr.us-east-1.amazonaws.com/insert_elasticsearch_trips_scores:latest