RAW_COMMANDS_TOPIC = "station.commands.raw"
FILTERED_COMMANDS_TOPIC = "station.commands.filtered"
VALIDATED_COMMANDS_TOPIC = "station.commands.validated"
AUTHORIZED_COMMANDS_TOPIC = "station.commands.authorized"
REJECTED_COMMANDS_TOPIC = "station.commands.rejected"

GATEWAY_COMMANDS_TOPIC = "station.gateway.commands"
GATEWAY_EVENTS_TOPIC = "station.gateway.events"

ACCESS_COMMANDS_TOPIC = "station.access.commands"
ACCESS_EVENTS_TOPIC = "station.access.events"

DOCKING_COMMANDS_TOPIC = "station.docking.commands"
DOCKING_EVENTS_TOPIC = "station.docking.events"

COMPARTMENT_TELEMETRY_TOPIC = "station.compartment.telemetry"
SENSOR_TELEMETRY_TOPIC = "station.sensor.telemetry"
EQUIPMENT_TELEMETRY_TOPIC = "station.equipment.telemetry"

MONITORING_EVENTS_TOPIC = "station.monitoring.events"
SECURITY_EVENTS_TOPIC = "station.security.events"
EMERGENCY_EVENTS_TOPIC = "station.emergency.events"
SAFE_MODE_COMMANDS_TOPIC = "station.safe_mode.commands"

STATE_WRITE_TOPIC = "station.state.write"
STATE_EVENTS_TOPIC = "station.state.events"
JOURNAL_EVENTS_TOPIC = "station.journal.events"


ALL_TOPICS = [
    RAW_COMMANDS_TOPIC,
    FILTERED_COMMANDS_TOPIC,
    VALIDATED_COMMANDS_TOPIC,
    AUTHORIZED_COMMANDS_TOPIC,
    REJECTED_COMMANDS_TOPIC,
    GATEWAY_COMMANDS_TOPIC,
    GATEWAY_EVENTS_TOPIC,
    ACCESS_COMMANDS_TOPIC,
    ACCESS_EVENTS_TOPIC,
    DOCKING_COMMANDS_TOPIC,
    DOCKING_EVENTS_TOPIC,
    COMPARTMENT_TELEMETRY_TOPIC,
    SENSOR_TELEMETRY_TOPIC,
    EQUIPMENT_TELEMETRY_TOPIC,
    MONITORING_EVENTS_TOPIC,
    SECURITY_EVENTS_TOPIC,
    EMERGENCY_EVENTS_TOPIC,
    SAFE_MODE_COMMANDS_TOPIC,
    STATE_WRITE_TOPIC,
    STATE_EVENTS_TOPIC,
    JOURNAL_EVENTS_TOPIC,
]