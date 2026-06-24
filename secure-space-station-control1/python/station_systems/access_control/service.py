from python.core.logging.logger import log
from python.devices.access_locks.service import AccessLocks
from python.kafka.message_schema import create_event
from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import ACCESS_EVENTS_TOPIC, STATE_WRITE_TOPIC, JOURNAL_EVENTS_TOPIC


class AccessControl:
    def __init__(self):
        self.producer = KafkaProducerClient()
        self.locks = AccessLocks()

    def lock_sector(self, sector_id: str) -> dict:
        result = self.locks.lock(sector_id)

        event = self._event("SECTOR_LOCKED", result)

        self.producer.send(STATE_WRITE_TOPIC, event)
        self.producer.send(JOURNAL_EVENTS_TOPIC, event)

        log("access_control", f"Сектор заблокирован: {sector_id}")

        return event

    def unlock_sector(self, sector_id: str) -> dict:
        result = self.locks.unlock(sector_id)

        event = self._event("SECTOR_UNLOCKED", result)

        self.producer.send(STATE_WRITE_TOPIC, event)
        self.producer.send(JOURNAL_EVENTS_TOPIC, event)

        log("access_control", f"Сектор разблокирован: {sector_id}")

        return event

    def emergency_lock_all(self) -> dict:
        result = self.locks.emergency_lock_all()

        event = self._event("ALL_SECTORS_LOCKED", result)

        self.producer.send(STATE_WRITE_TOPIC, event)
        self.producer.send(JOURNAL_EVENTS_TOPIC, event)

        log("access_control", "Все сектора аварийно заблокированы")

        return event

    def _event(self, event_type: str, payload: dict) -> dict:
        event = create_event(
            event_type=event_type,
            source="access_control",
            payload=payload,
        )

        self.producer.send(ACCESS_EVENTS_TOPIC, event)

        return event