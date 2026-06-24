from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import RAW_COMMANDS_TOPIC, SECURITY_EVENTS_TOPIC
from python.kafka.message_schema import create_command, create_event


class EarthLinkInterface:
    def __init__(self):
        self.producer = KafkaProducerClient()
        self.source = "earth_link_interface"
        self.connection_status = "connected"

    def receive_earth_command(self, command_type: str, payload: dict) -> dict:
        command = create_command(
            command_type=command_type,
            source="EARTH-CONTROL-1",
            target="central_control_system",
            payload=payload,
        )

        self.producer.send(RAW_COMMANDS_TOPIC, command)
        return command

    def receive_tampered_command(self) -> dict:
        command = create_command(
            command_type="OPEN_GATEWAY",
            source="EARTH-CONTROL-1",
            target="central_control_system",
            payload={
                "gateway_id": "FAKE-GATEWAY",
                "compartment_id": "COMPARTMENT-1",
                "tampered": True,
            },
        )

        self.producer.send(RAW_COMMANDS_TOPIC, command)

        event = create_event(
            event_type="EARTH_COMMAND_TAMPERED",
            source=self.source,
            payload=command,
        )
        self.producer.send(SECURITY_EVENTS_TOPIC, event)

        return command

    def lose_connection(self) -> dict:
        self.connection_status = "lost"

        event = create_event(
            event_type="EARTH_CONNECTION_LOST",
            source=self.source,
            payload={
                "connection_status": self.connection_status,
            },
        )

        self.producer.send(SECURITY_EVENTS_TOPIC, event)
        return event

    def restore_connection(self) -> dict:
        self.connection_status = "connected"

        event = create_event(
            event_type="EARTH_CONNECTION_RESTORED",
            source=self.source,
            payload={
                "connection_status": self.connection_status,
            },
        )

        self.producer.send(SECURITY_EVENTS_TOPIC, event)
        return event