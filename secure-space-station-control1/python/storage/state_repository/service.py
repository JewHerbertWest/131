import json

from python.core.config.settings import STATE_FILE
from python.kafka.message_schema import create_event
from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import STATE_EVENTS_TOPIC


class StateRepository:
    def __init__(self):
        self.producer = KafkaProducerClient()

    def read_state(self) -> dict:
        with open(STATE_FILE, "r", encoding="utf-8") as file:
            return json.load(file)

    def write_state(self, section: str, item_id: str, value: dict) -> dict:
        state = self.read_state()

        if section not in state:
            state[section] = {}

        state[section][item_id] = value

        with open(STATE_FILE, "w", encoding="utf-8") as file:
            json.dump(state, file, ensure_ascii=False, indent=2)

        event = create_event(
            event_type="STATE_UPDATED",
            source="state_repository",
            payload={
                "section": section,
                "item_id": item_id,
                "value": value,
            },
        )

        self.producer.send(STATE_EVENTS_TOPIC, event)

        return {
            "status": "saved",
            "section": section,
            "item_id": item_id,
            "value": value,
        }

    def get_gateway_state(self, gateway_id: str) -> dict | None:
        state = self.read_state()
        return state.get("gateways", {}).get(gateway_id)

    def get_compartment_state(self, compartment_id: str) -> dict | None:
        state = self.read_state()
        return state.get("compartments", {}).get(compartment_id)