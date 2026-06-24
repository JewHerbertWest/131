from python.core.logging.logger import log
from python.kafka.message_schema import create_event
from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import EMERGENCY_EVENTS_TOPIC, MONITORING_EVENTS_TOPIC, SECURITY_EVENTS_TOPIC


class EventAnalyzer:
    def __init__(self):
        self.producer = KafkaProducerClient()
        self.events = []

    def register_event(self, event: dict) -> dict:
        self.events.append(event)

        log("event_analyzer", f"Событие добавлено в анализ: {event.get('type')}")

        return {
            "status": "registered",
            "event": event,
        }

    def detect_missing_journal_record(self, expected_event_type: str, journal_events: list[dict]) -> dict:
        found = any(event.get("type") == expected_event_type for event in journal_events)

        if not found:
            event = create_event(
                event_type="MISSING_JOURNAL_RECORD_DETECTED",
                source="event_analyzer",
                payload={
                    "expected_event_type": expected_event_type,
                },
            )

            self.producer.send(SECURITY_EVENTS_TOPIC, event)
            self.producer.send(MONITORING_EVENTS_TOPIC, event)

            log("event_analyzer", "Обнаружено отсутствие записи в журнале")

            return {
                "status": "missing_record_detected",
                "event": event,
            }

        return {
            "status": "ok",
            "expected_event_type": expected_event_type,
        }

    def detect_false_state(self, real_state: dict, stored_state: dict) -> dict:
        if real_state != stored_state:
            event = create_event(
                event_type="FALSE_STATE_DETECTED",
                source="event_analyzer",
                payload={
                    "real_state": real_state,
                    "stored_state": stored_state,
                },
            )

            self.producer.send(SECURITY_EVENTS_TOPIC, event)
            self.producer.send(MONITORING_EVENTS_TOPIC, event)
            self.producer.send(EMERGENCY_EVENTS_TOPIC, event)

            log("event_analyzer", "Обнаружено ложное состояние в хранилище")

            return {
                "status": "false_state_detected",
                "event": event,
            }

        return {
            "status": "ok",
        }