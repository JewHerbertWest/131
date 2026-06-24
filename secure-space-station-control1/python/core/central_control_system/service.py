from python.core.command_router.service import CommandRouter
from python.core.logging.logger import log
from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import (
    AUTHORIZED_COMMANDS_TOPIC,
    REJECTED_COMMANDS_TOPIC,
    SECURITY_EVENTS_TOPIC,
    JOURNAL_EVENTS_TOPIC,
)
from python.kafka.message_schema import create_event


class CentralControlSystem:
    def __init__(self):
        self.router = CommandRouter()
        self.producer = KafkaProducerClient()

    def process_authorized_command(self, command: dict) -> dict:
        command_type = command.get("type")

        log("central_control_system", f"Получена разрешённая команда: {command_type}")

        journal_event = create_event(
            event_type="AUTHORIZED_COMMAND_RECEIVED",
            source="central_control_system",
            payload=command,
        )
        self.producer.send(JOURNAL_EVENTS_TOPIC, journal_event)

        return self.router.route(command)

    def reject_command(self, command: dict, reason: str) -> dict:
        log("central_control_system", f"Команда отклонена: {reason}")

        rejected_event = create_event(
            event_type="COMMAND_REJECTED",
            source="central_control_system",
            payload={
                "reason": reason,
                "command": command,
            },
        )

        self.producer.send(REJECTED_COMMANDS_TOPIC, command)
        self.producer.send(SECURITY_EVENTS_TOPIC, rejected_event)
        self.producer.send(JOURNAL_EVENTS_TOPIC, rejected_event)

        return {
            "status": "rejected",
            "reason": reason,
            "command": command,
        }

    def handle_command(self, command: dict) -> dict:
        if command.get("authorized") is not True:
            return self.reject_command(command, "command_not_authorized")

        if command.get("validated") is not True:
            return self.reject_command(command, "command_not_validated")

        if command.get("signature_valid") is not True:
            return self.reject_command(command, "invalid_signature")

        self.producer.send(AUTHORIZED_COMMANDS_TOPIC, command)

        return self.process_authorized_command(command)