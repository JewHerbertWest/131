from python.core.logging.logger import log
from python.devices.docking_node.service import DockingNode
from python.kafka.message_schema import create_event
from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import DOCKING_EVENTS_TOPIC, STATE_WRITE_TOPIC, JOURNAL_EVENTS_TOPIC


class DockingControl:
    def __init__(self):
        self.producer = KafkaProducerClient()
        self.docking_node = DockingNode()

    def start_docking(self, command: dict) -> dict:
        payload = command.get("payload", {})
        ship_id = payload.get("ship_id")

        result = self.docking_node.start_docking(ship_id)

        event = create_event(
            event_type="DOCKING_RESULT",
            source="docking_control",
            payload={
                "command": command,
                "result": result,
            },
        )

        self.producer.send(DOCKING_EVENTS_TOPIC, event)
        self.producer.send(STATE_WRITE_TOPIC, event)
        self.producer.send(JOURNAL_EVENTS_TOPIC, event)

        log("docking_control", f"Результат стыковки: {result['status']}")

        return event

    def cancel_docking(self, ship_id: str) -> dict:
        event = create_event(
            event_type="DOCKING_CANCELLED",
            source="docking_control",
            payload={
                "ship_id": ship_id,
            },
        )

        self.producer.send(DOCKING_EVENTS_TOPIC, event)
        self.producer.send(STATE_WRITE_TOPIC, event)
        self.producer.send(JOURNAL_EVENTS_TOPIC, event)

        log("docking_control", f"Стыковка отменена: {ship_id}")

        return event