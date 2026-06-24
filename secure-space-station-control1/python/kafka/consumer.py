import json

from kafka import KafkaConsumer

from python.core.config.settings import KAFKA_BOOTSTRAP_SERVERS
from python.core.logging.logger import log


class KafkaConsumerClient:
    def __init__(self, topic: str, group_id: str):
        self.topic = topic
        self.group_id = group_id

        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            key_deserializer=lambda value: value.decode("utf-8") if value else None,
        )

    def consume_forever(self, handler):
        log("kafka_consumer", f"Ожидание сообщений из топика {self.topic}")

        for record in self.consumer:
            message = record.value
            log("kafka_consumer", f"Получено сообщение из {self.topic}: {message}")
            handler(message)

    def consume_once(self) -> dict | None:
        records = self.consumer.poll(timeout_ms=3000)

        for _, messages in records.items():
            for record in messages:
                return record.value

        return None