from python.core.logging.logger import log
from python.kafka.producer import KafkaProducerClient
from python.kafka.message_schema import create_event
from python.kafka.topics import SAFE_MODE_COMMANDS_TOPIC, EMERGENCY_EVENTS_TOPIC


class SafeModeController:
    def __init__(self):
        self.producer = KafkaProducerClient()
        self.safe_mode = False

    def enable_safe_mode(self, reason: str) -> dict:
        self.safe_mode = True

        event = create_event(
            event_type="SAFE_MODE_ENABLED",
            source="safe_mode_controller",
            payload={
                "safe_mode": self.safe_mode,
                "reason": reason,
            },
        )

        self.producer.send(SAFE_MODE_COMMANDS_TOPIC, event)
        self.producer.send(EMERGENCY_EVENTS_TOPIC, event)

        log("safe_mode_controller", f"Безопасный режим включён: {reason}")

        return event

    def disable_safe_mode(self) -> dict:
        self.safe_mode = False

        event = create_event(
            event_type="SAFE_MODE_DISABLED",
            source="safe_mode_controller",
            payload={
                "safe_mode": self.safe_mode,
            },
        )

        self.producer.send(SAFE_MODE_COMMANDS_TOPIC, event)

        log("safe_mode_controller", "Безопасный режим отключён")

        return event

    def get_status(self) -> dict:
        return {
            "safe_mode": self.safe_mode,
        }