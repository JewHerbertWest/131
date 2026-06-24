from python.kafka.message_schema import create_event
from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import REJECTED_COMMANDS_TOPIC, SECURITY_EVENTS_TOPIC


class DockingValidator:
    def __init__(self):
        self.producer = KafkaProducerClient()

    def validate(self, command: dict, docking_node_status: str) -> dict:
        payload = command.get("payload", {})
        ship_id = payload.get("ship_id")
        ship_node_status = payload.get("docking_node_status")

        if ship_id != command.get("source"):
            return self._reject(command, "ship_identity_mismatch")

        if command.get("source") == "FAKE-SHIP":
            return self._reject(command, "fake_ship_detected")

        if docking_node_status != "normal":
            return self._reject(command, "station_docking_node_damaged")

        if ship_node_status != "normal":
            return self._reject(command, "ship_docking_node_damaged")

        return {
            "status": "docking_allowed",
            "command": command
        }

    def _reject(self, command: dict, reason: str) -> dict:
        event = create_event(
            event_type="DOCKING_VALIDATION_FAILED",
            source="docking_validator",
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