from python.core.logging.logger import log
from python.kafka.message_schema import create_event
from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import MONITORING_EVENTS_TOPIC, SECURITY_EVENTS_TOPIC


class CommunicationMonitor:
    def __init__(self):
        self.producer = KafkaProducerClient()

    def check_earth_connection(self, status: str) -> dict:
        if status != "connected":
            return self._event(
                "EARTH_CONNECTION_FAILURE_DETECTED",
                {
                    "connection": "earth",
                    "status": status,
                },
            )

        log("communication_monitor", "Связь с Землёй в норме")

        return {
            "status": "ok",
            "connection": "earth",
        }

    def check_ship_connection(self, ship_id: str, status: str) -> dict:
        if status != "connected":
            return self._event(
                "SHIP_CONNECTION_FAILURE_DETECTED",
                {
                    "ship_id": ship_id,
                    "status": status,
                },
            )

        log("communication_monitor", f"Связь с кораблём {ship_id} в норме")

        return {
            "status": "ok",
            "ship_id": ship_id,
        }

    def _event(self, event_type: str, payload: dict) -> dict:
        event = create_event(
            event_type=event_type,
            source="communication_monitor",
            payload=payload,
        )

        self.producer.send(MONITORING_EVENTS_TOPIC, event)
        self.producer.send(SECURITY_EVENTS_TOPIC, event)

        log("communication_monitor", f"Обнаружена проблема связи: {event_type}")

        return {
            "status": "detected",
            "event": event,
        }