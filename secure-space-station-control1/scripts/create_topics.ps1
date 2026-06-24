$KafkaContainer = "secure-space-station-control1-kafka"

$Topics = @(
  "station.commands.raw",
  "station.commands.filtered",
  "station.commands.validated",
  "station.commands.authorized",
  "station.commands.rejected",
  "station.gateway.commands",
  "station.gateway.events",
  "station.access.commands",
  "station.access.events",
  "station.docking.commands",
  "station.docking.events",
  "station.compartment.telemetry",
  "station.sensor.telemetry",
  "station.equipment.telemetry",
  "station.monitoring.events",
  "station.security.events",
  "station.emergency.events",
  "station.safe_mode.commands",
  "station.state.write",
  "station.state.events",
  "station.journal.events"
)

foreach ($Topic in $Topics) {
  docker exec $KafkaContainer kafka-topics `
    --bootstrap-server kafka:9092 `
    --create `
    --if-not-exists `
    --topic $Topic `
    --partitions 1 `
    --replication-factor 1
}

Write-Host "Kafka topics created"