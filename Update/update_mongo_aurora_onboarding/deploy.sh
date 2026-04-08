#!/bin/bash

set -eo pipefail

aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 045511714637.dkr.ecr.us-east-1.amazonaws.com
docker build -t update_aurora_onboarding .
docker tag update_aurora_onboarding:latest 045511714637.dkr.ecr.us-east-1.amazonaws.com/update_aurora_onboarding:latest
docker push 045511714637.dkr.ecr.us-east-1.amazonaws.com/update_aurora_onboarding:latest