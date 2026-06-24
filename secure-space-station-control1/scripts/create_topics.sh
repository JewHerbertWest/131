#!/bin/bash

set -e

KAFKA_CONTAINER="secure-space-station-control1-kafka"

TOPICS=(
  "station.commands.raw"
  "station.commands.filtered"
  "station.commands.validated"
  "station.commands.authorized"
  "station.commands.rejected"
  "station.gateway.commands"
  "station.gateway.events"
  "station.access.commands"
  "station.access.events"
  "station.docking.commands"
  "station.docking.events"
  "station.compartment.telemetry"
  "station.sensor.telemetry"
  "station.equipment.telemetry"
  "station.monitoring.events"
  "station.security.events"
  "station.emergency.events"
  "station.safe_mode.commands"
  "station.state.write"
  "station.state.events"
  "station.journal.events"
)

for TOPIC in "${TOPICS[@]}"; do
  docker exec "$KAFKA_CONTAINER" kafka-topics \
    --bootstrap-server kafka:9092 \
    --create \
    --if-not-exists \
    --topic "$TOPIC" \
    --partitions 1 \
    --replication-factor 1
done

echo "Kafka topics created"