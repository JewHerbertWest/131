from python.core.logging.logger import log
from python.kafka.topics import (
    GATEWAY_COMMANDS_TOPIC,
    ACCESS_COMMANDS_TOPIC,
    DOCKING_COMMANDS_TOPIC,
    REJECTED_COMMANDS_TOPIC,
    SECURITY_EVENTS_TOPIC,
)
from python.kafka.producer import KafkaProducerClient
from python.kafka.message_schema import create_event


class CommandRouter:
    def __init__(self):
        self.producer = KafkaProducerClient()

    def route(self, command: dict) -> dict:
        command_type = command.get("type")

        if command_type in ["OPEN_GATEWAY", "CLOSE_GATEWAY", "BLOCK_GATEWAY"]:
            topic = GATEWAY_COMMANDS_TOPIC

        elif command_type in ["CHECK_ACCESS", "LOCK_ACCESS", "UNLOCK_ACCESS"]:
            topic = ACCESS_COMMANDS_TOPIC

        elif command_type in ["REQUEST_DOCKING", "CANCEL_DOCKING"]:
            topic = DOCKING_COMMANDS_TOPIC

        else:
            event = create_event(
                event_type="UNKNOWN_COMMAND_TYPE",
                source="command_router",
                payload=command,
            )

            self.producer.send(REJECTED_COMMANDS_TOPIC, command)
            self.producer.send(SECURITY_EVENTS_TOPIC, event)

            log("command_router", f"Команда отклонена: {command_type}")

            return {
                "status": "rejected",
                "reason": "unknown_command_type",
                "command": command,
            }

        self.producer.send(topic, command)

        log("command_router", f"Команда {command_type} отправлена в {topic}")

        return {
            "status": "routed",
            "topic": topic,
            "command": command,
        }