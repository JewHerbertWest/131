import pytest


class FakeKafkaProducer:
    def __init__(self, *args, **kwargs):
        self.messages = []

    def send(self, topic, key=None, value=None):
        self.messages.append(
            {
                "topic": topic,
                "key": key,
                "value": value,
            }
        )

    def flush(self):
        return None


@pytest.fixture(autouse=True)
def use_fake_kafka_producer(monkeypatch):
    monkeypatch.setattr("python.kafka.producer.KafkaProducer", FakeKafkaProducer)
