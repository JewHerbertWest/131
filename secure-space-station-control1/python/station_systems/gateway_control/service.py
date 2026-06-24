from python.core.logging.logger import log
from python.devices.gateway_drives.service import GatewayDrives
from python.kafka.message_schema import create_event
from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import GATEWAY_EVENTS_TOPIC, STATE_WRITE_TOPIC, JOURNAL_EVENTS_TOPIC


class GatewayControl:
    def __init__(self):
        self.producer = KafkaProducerClient()
        self.drives = GatewayDrives()
        self.blocked = False

    def open_gateway(self, command: dict) -> dict:
        if self.blocked:
            return self._event("GATEWAY_OPEN_BLOCKED", {
                "reason": "gateway_blocked",
                "command": command,
            })

        inner_result = self.drives.open_inner_door()

        event = self._event("GATEWAY_OPENED", {
            "command": command,
            "drive_result": inner_result,
        })

        self.producer.send(STATE_WRITE_TOPIC, event)
        self.producer.send(JOURNAL_EVENTS_TOPIC, event)

        log("gateway_control", "Команда открытия шлюза выполнена")

        return event

    def close_gateway(self, command: dict) -> dict:
        result = self.drives.close_all_doors()

        event = self._event("GATEWAY_CLOSED", {
            "command": command,
            "drive_result": result,
        })

        self.producer.send(STATE_WRITE_TOPIC, event)
        self.producer.send(JOURNAL_EVENTS_TOPIC, event)

        log("gateway_control", "Команда закрытия шлюза выполнена")

        return event

    def block_gateway(self, reason: str) -> dict:
        self.blocked = True
        result = self.drives.block_drives()

        event = self._event("GATEWAY_BLOCKED", {
            "reason": reason,
            "drive_result": result,
        })

        self.producer.send(STATE_WRITE_TOPIC, event)
        self.producer.send(JOURNAL_EVENTS_TOPIC, event)

        log("gateway_control", f"Шлюз заблокирован: {reason}")

        return event

    def _event(self, event_type: str, payload: dict) -> dict:
        event = create_event(
            event_type=event_type,
            source="gateway_control",
            payload=payload,
        )

        self.producer.send(GATEWAY_EVENTS_TOPIC, event)

        return event