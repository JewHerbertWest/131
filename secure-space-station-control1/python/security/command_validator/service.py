from python.kafka.message_schema import mark_validated, create_event
from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import VALIDATED_COMMANDS_TOPIC, REJECTED_COMMANDS_TOPIC, SECURITY_EVENTS_TOPIC


class CommandValidator:
    def __init__(self):
        self.producer = KafkaProducerClient()
        self.required_fields = ["id", "type", "source", "target", "payload", "timestamp"]

    def validate(self, command: dict) -> dict:
        for field in self.required_fields:
            if field not in command:
                return self._reject(command, f"missing_field_{field}")

        if command.get("payload", {}).get("tampered") is True:
            return self._reject(command, "tampered_command_detected")

        command = mark_validated(command)
        self.producer.send(VALIDATED_COMMANDS_TOPIC, command)

        return {
            "status": "validated",
            "command": command
        }

    def _reject(self, command: dict, reason: str) -> dict:
        event = create_event(
            event_type="COMMAND_VALIDATION_FAILED",
            source="command_validator",
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