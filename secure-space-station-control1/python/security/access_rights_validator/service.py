import json

from python.core.config.settings import USERS_ROLES_FILE
from python.kafka.message_schema import create_event
from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import SECURITY_EVENTS_TOPIC


class AccessRightsValidator:
    def __init__(self):
        self.producer = KafkaProducerClient()

    def validate_access(self, user_id: str, sector_id: str) -> dict:
        users_roles = self._load_users_roles()
        user = users_roles["users"].get(user_id)

        if user is None:
            return self._reject(user_id, sector_id, "user_not_found")

        if sector_id == "REACTOR-SECTOR":
            role = users_roles["roles"].get(user["role"], {})

            if role.get("can_access_reactor_sector") is not True:
                return self._reject(user_id, sector_id, "reactor_access_denied")

        if sector_id not in user.get("allowed_compartments", []) and sector_id != "REACTOR-SECTOR":
            return self._reject(user_id, sector_id, "sector_not_allowed")

        return {
            "status": "access_granted",
            "user_id": user_id,
            "sector_id": sector_id
        }

    def _load_users_roles(self) -> dict:
        with open(USERS_ROLES_FILE, "r", encoding="utf-8") as file:
            return json.load(file)

    def _reject(self, user_id: str, sector_id: str, reason: str) -> dict:
        event = create_event(
            event_type="ACCESS_RIGHTS_VALIDATION_FAILED",
            source="access_rights_validator",
            payload={
                "user_id": user_id,
                "sector_id": sector_id,
                "reason": reason
            }
        )

        self.producer.send(SECURITY_EVENTS_TOPIC, event)

        return {
            "status": "rejected",
            "reason": reason,
            "user_id": user_id,
            "sector_id": sector_id
        }