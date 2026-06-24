from python.core.logging.logger import log
from python.emergency.safe_mode_controller.service import SafeModeController
from python.kafka.producer import KafkaProducerClient
from python.kafka.message_schema import create_event
from python.kafka.topics import EMERGENCY_EVENTS_TOPIC, JOURNAL_EVENTS_TOPIC


class EmergencyModule:
    def __init__(self):
        self.producer = KafkaProducerClient()
        self.safe_mode_controller = SafeModeController()
        self.emergency_mode = False
        self.reason = None

    def activate(self, reason: str) -> dict:
        self.emergency_mode = True
        self.reason = reason

        event = create_event(
            event_type="EMERGENCY_MODE_ACTIVATED",
            source="emergency_module",
            payload={
                "emergency_mode": self.emergency_mode,
                "reason": self.reason,
            },
        )

        self.producer.send(EMERGENCY_EVENTS_TOPIC, event)
        self.producer.send(JOURNAL_EVENTS_TOPIC, event)

        safe_mode_event = self.safe_mode_controller.enable_safe_mode(reason)

        log("emergency_module", f"Аварийный режим активирован: {reason}")

        return {
            "status": "emergency_enabled",
            "event": event,
            "safe_mode": safe_mode_event,
        }

    def deactivate(self) -> dict:
        self.emergency_mode = False
        self.reason = None

        event = create_event(
            event_type="EMERGENCY_MODE_DISABLED",
            source="emergency_module",
            payload={
                "emergency_mode": self.emergency_mode,
            },
        )

        self.producer.send(EMERGENCY_EVENTS_TOPIC, event)
        self.producer.send(JOURNAL_EVENTS_TOPIC, event)

        log("emergency_module", "Аварийный режим отключён")

        return event

    def get_status(self) -> dict:
        return {
            "emergency_mode": self.emergency_mode,
            "reason": self.reason,
        }