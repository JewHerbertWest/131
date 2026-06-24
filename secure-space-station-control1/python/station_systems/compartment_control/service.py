import json

from python.core.config.settings import TELEMETRY_FILE
from python.core.logging.logger import log
from python.kafka.message_schema import create_event
from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import COMPARTMENT_TELEMETRY_TOPIC, JOURNAL_EVENTS_TOPIC


class CompartmentControl:
    def __init__(self):
        self.producer = KafkaProducerClient()

    def get_compartment_state(self, compartment_id: str) -> dict:
        telemetry = self._load_telemetry()
        sensor_data = telemetry["sensors"].get(compartment_id)

        if sensor_data is None:
            state = {
                "compartment_id": compartment_id,
                "pressure_normal": False,
                "sealed": False,
                "temperature_normal": False,
                "reason": "compartment_not_found",
            }
        else:
            pressure = sensor_data["pressure"]
            temperature = sensor_data["temperature"]

            state = {
                "compartment_id": compartment_id,
                "pressure": pressure,
                "pressure_normal": 95 <= pressure <= 105,
                "sealed": sensor_data["sealed"],
                "temperature": temperature,
                "temperature_normal": 10 <= temperature <= 35,
            }

        event = create_event(
            event_type="COMPARTMENT_STATE_REPORTED",
            source="compartment_control",
            payload=state,
        )

        self.producer.send(COMPARTMENT_TELEMETRY_TOPIC, event)
        self.producer.send(JOURNAL_EVENTS_TOPIC, event)

        log("compartment_control", f"Передано состояние отсека {compartment_id}")

        return state

    def _load_telemetry(self) -> dict:
        with open(TELEMETRY_FILE, "r", encoding="utf-8") as file:
            return json.load(file)