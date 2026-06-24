import json

from python.core.config.settings import USERS_ROLES_FILE
from python.kafka.message_schema import mark_authorized, create_event
from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import AUTHORIZED_COMMANDS_TOPIC, REJECTED_COMMANDS_TOPIC, SECURITY_EVENTS_TOPIC


class AuthorizationService:
    def __init__(self):
        self.producer = KafkaProducerClient()

    def authorize(self, command: dict) -> dict:
        source = command.get("source")
        command_type = command.get("type")

        users_roles = self._load_users_roles()
        user = users_roles["users"].get(source)

        if user is None:
            return self._reject(command, "user_not_found")

        if command_type not in user["allowed_commands"]:
            return self._reject(command, "command_not_allowed")

        command = mark_authorized(command)
        self.producer.send(AUTHORIZED_COMMANDS_TOPIC, command)

        return {
            "status": "authorized",
            "command": command
        }

    def _load_users_roles(self) -> dict:
        with open(USERS_ROLES_FILE, "r", encoding="utf-8") as file:
            return json.load(file)

    def _reject(self, command: dict, reason: str) -> dict:
        event = create_event(
            event_type="AUTHORIZATION_FAILED",
            source="authorization_service",
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