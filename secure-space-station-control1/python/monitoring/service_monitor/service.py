from python.core.logging.logger import log
from python.kafka.message_schema import create_event
from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import MONITORING_EVENTS_TOPIC


class ServiceMonitor:
    def __init__(self):
        self.producer = KafkaProducerClient()

    def check_service_status(self, service_name: str, status: str) -> dict:
        if status != "running":
            event = create_event(
                event_type="SERVICE_FAILURE_DETECTED",
                source="service_monitor",
                payload={
                    "service_name": service_name,
                    "status": status,
                },
            )

            self.producer.send(MONITORING_EVENTS_TOPIC, event)

            log("service_monitor", f"Сбой сервиса {service_name}")

            return {
                "status": "failure_detected",
                "event": event,
            }

        return {
            "status": "ok",
            "service_name": service_name,
        }