import json

from python.core.config.settings import CONFIG_FILE
from python.kafka.message_schema import create_event
from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import REJECTED_COMMANDS_TOPIC, SECURITY_EVENTS_TOPIC


class SecurityPolicyService:
    def __init__(self):
        self.producer = KafkaProducerClient()

    def check_policy(self, command: dict, context: dict | None = None) -> dict:
        context = context or {}
        config = self._load_config()
        command_type = command.get("type")

        if command_type == "OPEN_GATEWAY":
            compartment = context.get("compartment", {})

            if compartment.get("pressure_normal") is not True:
                return self._reject(command, "pressure_not_normal")

            if compartment.get("sealed") is not True:
                return self._reject(command, "compartment_unsealed")

        if command_type == "OPEN_INNER_AND_OUTER_GATEWAY":
            if config["gateways"]["GATEWAY-1"]["allow_simultaneous_doors_open"] is False:
                return self._reject(command, "simultaneous_gateway_doors_forbidden")

        return {
            "status": "policy_passed",
            "command": command
        }

    def _load_config(self) -> dict:
        with open(CONFIG_FILE, "r", encoding="utf-8") as file:
            return json.load(file)

    def _reject(self, command: dict, reason: str) -> dict:
        event = create_event(
            event_type="SECURITY_POLICY_FAILED",
            source="security_policy_service",
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