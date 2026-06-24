from python.kafka.message_schema import create_event
from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import SECURITY_EVENTS_TOPIC, EMERGENCY_EVENTS_TOPIC


class SensorValueVerifier:
    def __init__(self):
        self.producer = KafkaProducerClient()

    def verify_compartment(self, reported: dict, real: dict) -> dict:
        if reported.get("pressure_normal") != real.get("pressure_normal"):
            return self._detect("false_pressure_status", reported, real)

        if reported.get("sealed") != real.get("sealed"):
            return self._detect("false_sealing_status", reported, real)

        return {
            "status": "verified",
            "reported": reported,
            "real": real
        }

    def _detect(self, reason: str, reported: dict, real: dict) -> dict:
        event = create_event(
            event_type="FALSE_SENSOR_VALUE_DETECTED",
            source="sensor_value_verifier",
            payload={
                "reason": reason,
                "reported": reported,
                "real": real
            }
        )

        self.producer.send(SECURITY_EVENTS_TOPIC, event)
        self.producer.send(EMERGENCY_EVENTS_TOPIC, event)

        return {
            "status": "detected",
            "reason": reason,
            "event": event
        }