from datetime import datetime

from python.core.config.settings import JOURNAL_FILE
from python.kafka.message_schema import create_event
from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import JOURNAL_EVENTS_TOPIC


class EventJournal:
    def __init__(self):
        self.producer = KafkaProducerClient()

    def write_event(self, component: str, message: str) -> dict:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        record = f"[{timestamp}] [{component}] {message}"

        with open(JOURNAL_FILE, "a", encoding="utf-8") as file:
            file.write(record + "\n")

        event = create_event(
            event_type="JOURNAL_RECORD_WRITTEN",
            source="event_journal",
            payload={
                "component": component,
                "message": message,
                "record": record,
            },
        )

        self.producer.send(JOURNAL_EVENTS_TOPIC, event)

        return {
            "status": "written",
            "record": record,
        }

    def read_events(self) -> list[str]:
        with open(JOURNAL_FILE, "r", encoding="utf-8") as file:
            return file.read().splitlines()