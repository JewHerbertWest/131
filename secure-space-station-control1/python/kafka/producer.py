import json

from kafka import KafkaProducer

from python.core.config.settings import KAFKA_BOOTSTRAP_SERVERS
from python.core.logging.logger import log


class KafkaProducerClient:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
            key_serializer=lambda value: value.encode("utf-8") if value else None,
        )

    def send(self, topic: str, message: dict, key: str | None = None) -> dict:
        self.producer.send(topic, key=key, value=message)
        self.producer.flush()

        log("kafka_producer", f"Сообщение отправлено в топик {topic}")

        return {
            "status": "sent",
            "topic": topic,
            "message": message,
        }