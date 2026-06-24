#!/bin/bash

set -e

docker compose up -d kafka

echo "Waiting for Kafka..."
sleep 15

./scripts/create_topics.sh

docker compose run --rm tests
