from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import RAW_COMMANDS_TOPIC, SECURITY_EVENTS_TOPIC
from python.kafka.message_schema import create_command, create_event


class ShipLinkInterface:
    def __init__(self):
        self.producer = KafkaProducerClient()
        self.source = "ship_link_interface"
        self.connection_status = "idle"

    def receive_docking_request(self, ship_id: str) -> dict:
        command = create_command(
            command_type="REQUEST_DOCKING",
            source=ship_id,
            target="central_control_system",
            payload={
                "ship_id": ship_id,
                "docking_port_id": "DOCKING-PORT-1",
                "docking_node_status": "normal",
            },
        )

        self.connection_status = "connected"
        self.producer.send(RAW_COMMANDS_TOPIC, command)
        return command

    def receive_fake_docking_request(self) -> dict:
        command = create_command(
            command_type="REQUEST_DOCKING",
            source="FAKE-SHIP",
            target="central_control_system",
            payload={
                "ship_id": "FAKE-SHIP",
                "docking_port_id": "DOCKING-PORT-1",
                "docking_node_status": "unknown",
            },
        )

        self.producer.send(RAW_COMMANDS_TOPIC, command)

        event = create_event(
            event_type="FAKE_SHIP_DOCKING_REQUEST",
            source=self.source,
            payload=command,
        )
        self.producer.send(SECURITY_EVENTS_TOPIC, event)

        return command

    def receive_damaged_node_telemetry(self) -> dict:
        command = create_command(
            command_type="SEND_TELEMETRY",
            source="SHIP-1",
            target="central_control_system",
            payload={
                "ship_id": "SHIP-1",
                "docking_node_status": "damaged",
                "telemetry_valid": False,
            },
        )

        self.producer.send(RAW_COMMANDS_TOPIC, command)
        return command

    def lose_connection(self, ship_id: str) -> dict:
        self.connection_status = "lost"

        event = create_event(
            event_type="SHIP_CONNECTION_LOST",
            source=self.source,
            payload={
                "ship_id": ship_id,
                "connection_status": self.connection_status,
            },
        )

        self.producer.send(SECURITY_EVENTS_TOPIC, event)
        return event