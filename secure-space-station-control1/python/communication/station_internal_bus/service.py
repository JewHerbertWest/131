from python.kafka.producer import KafkaProducerClient


class StationInternalBus:
    def __init__(self):
        self.producer = KafkaProducerClient()

    def publish(self, topic: str, message: dict) -> dict:
        self.producer.send(topic, message)

        return {
            "status": "published",
            "topic": topic,
            "message": message,
        }

    def publish_many(self, topic: str, messages: list[dict]) -> list[dict]:
        results = []

        for message in messages:
            results.append(self.publish(topic, message))

        return results