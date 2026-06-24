from python.core.logging.logger import log
from python.kafka.message_schema import create_event
from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import EMERGENCY_EVENTS_TOPIC, JOURNAL_EVENTS_TOPIC, STATE_WRITE_TOPIC


class LifeSupportControl:
    def __init__(self):
        self.producer = KafkaProducerClient()
        self.mode = "normal"

    def switch_to_safe_mode(self, reason: str) -> dict:
        self.mode = "safe"

        event = create_event(
            event_type="LIFE_SUPPORT_SAFE_MODE_ENABLED",
            source="life_support_control",
            payload={
                "mode": self.mode,
                "reason": reason,
            },
        )

        self.producer.send(EMERGENCY_EVENTS_TOPIC, event)
        self.producer.send(STATE_WRITE_TOPIC, event)
        self.producer.send(JOURNAL_EVENTS_TOPIC, event)

        log("life_support_control", f"Жизнеобеспечение переведено в безопасный режим: {reason}")

        return event

    def restore_normal_mode(self) -> dict:
        self.mode = "normal"

        event = create_event(
            event_type="LIFE_SUPPORT_NORMAL_MODE_RESTORED",
            source="life_support_control",
            payload={
                "mode": self.mode,
            },
        )

        self.producer.send(STATE_WRITE_TOPIC, event)
        self.producer.send(JOURNAL_EVENTS_TOPIC, event)

        log("life_support_control", "Жизнеобеспечение возвращено в нормальный режим")

        return event

    def get_status(self) -> dict:
        return {
            "mode": self.mode,
        }