from python.kafka.message_schema import create_event
from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import FILTERED_COMMANDS_TOPIC, REJECTED_COMMANDS_TOPIC, SECURITY_EVENTS_TOPIC


class CommandFilter:
    def __init__(self):
        self.producer = KafkaProducerClient()
        self.allowed_sources = ["OPERATOR-1", "EARTH-CONTROL-1", "SHIP-1"]
        self.allowed_types = [
            "OPEN_GATEWAY",
            "CLOSE_GATEWAY",
            "BLOCK_GATEWAY",
            "REQUEST_DOCKING",
            "CANCEL_DOCKING",
            "SEND_TELEMETRY",
            "EMERGENCY_MODE",
            "CANCEL_OPERATION"
        ]

    def filter(self, command: dict) -> dict:
        if command.get("source") not in self.allowed_sources:
            return self._reject(command, "unknown_source")

        if command.get("type") not in self.allowed_types:
            return self._reject(command, "unknown_command_type")

        if command.get("payload") is None:
            return self._reject(command, "empty_payload")

        self.producer.send(FILTERED_COMMANDS_TOPIC, command)

        return {
            "status": "passed",
            "command": command
        }

    def _reject(self, command: dict, reason: str) -> dict:
        event = create_event(
            event_type="COMMAND_FILTER_REJECTED",
            source="command_filter",
            payload={
                "reason": reason,
                "command": command
            }
        )

        self.producer.send(REJECTED_COMMANDS_TOPIC, command)
        self.producer.send(SECURITY_EVENTS_TOPIC, event)

        return {
            "status": "rejected",
            "reason": reason,
            "command": command
        }