#!/bin/bash

set -e

docker compose up -d kafka kafka-ui

echo "Waiting for Kafka..."
sleep 15

./scripts/create_topics.sh

docker compose run --rm secure-station-app
