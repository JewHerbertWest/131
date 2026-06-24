from python.core.logging.logger import log
from python.kafka.message_schema import create_event
from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import EMERGENCY_EVENTS_TOPIC, MONITORING_EVENTS_TOPIC, SECURITY_EVENTS_TOPIC


class SensorMonitor:
    def __init__(self):
        self.producer = KafkaProducerClient()

    def check_pressure(self, compartment_id: str, pressure: int, sealed: bool) -> dict:
        if pressure < 95 or pressure > 105 or sealed is False:
            event = create_event(
                event_type="DANGEROUS_COMPARTMENT_STATE_DETECTED",
                source="sensor_monitor",
                payload={
                    "compartment_id": compartment_id,
                    "pressure": pressure,
                    "sealed": sealed,
                },
            )

            self.producer.send(MONITORING_EVENTS_TOPIC, event)
            self.producer.send(EMERGENCY_EVENTS_TOPIC, event)
            self.producer.send(SECURITY_EVENTS_TOPIC, event)

            log("sensor_monitor", f"Опасное состояние отсека {compartment_id}")

            return {
                "status": "danger_detected",
                "event": event,
            }

        return {
            "status": "ok",
            "compartment_id": compartment_id,
            "pressure": pressure,
            "sealed": sealed,
        }

    def check_temperature(self, compartment_id: str, temperature: int) -> dict:
        if temperature < 10 or temperature > 35:
            event = create_event(
                event_type="ABNORMAL_TEMPERATURE_DETECTED",
                source="sensor_monitor",
                payload={
                    "compartment_id": compartment_id,
                    "temperature": temperature,
                },
            )

            self.producer.send(MONITORING_EVENTS_TOPIC, event)

            log("sensor_monitor", f"Ненормальная температура в отсеке {compartment_id}")

            return {
                "status": "abnormal_detected",
                "event": event,
            }

        return {
            "status": "ok",
            "compartment_id": compartment_id,
            "temperature": temperature,
        }