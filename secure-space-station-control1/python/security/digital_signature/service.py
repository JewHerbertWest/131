import hashlib
import json

from python.kafka.message_schema import mark_signature_valid, create_event
from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import VALIDATED_COMMANDS_TOPIC, REJECTED_COMMANDS_TOPIC, SECURITY_EVENTS_TOPIC


class DigitalSignatureService:
    def __init__(self):
        self.producer = KafkaProducerClient()
        self.secret = "station-demo-secret"

    def sign(self, command: dict) -> dict:
        command["signature"] = self._calculate_signature(command)
        return command

    def verify(self, command: dict) -> dict:
        signature = command.get("signature")

        if signature is None:
            return self._reject(command, "signature_missing")

        expected = self._calculate_signature(command)

        if signature != expected:
            return self._reject(command, "invalid_signature")

        command = mark_signature_valid(command)
        self.producer.send(VALIDATED_COMMANDS_TOPIC, command)

        return {
            "status": "signature_valid",
            "command": command
        }

    def _calculate_signature(self, command: dict) -> str:
        data = {
            "id": command.get("id"),
            "type": command.get("type"),
            "source": command.get("source"),
            "target": command.get("target"),
            "payload": command.get("payload"),
            "timestamp": command.get("timestamp"),
        }

        raw = json.dumps(data, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256((raw + self.secret).encode("utf-8")).hexdigest()

    def _reject(self, command: dict, reason: str) -> dict:
        event = create_event(
            event_type="SIGNATURE_CHECK_FAILED",
            source="digital_signature",
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