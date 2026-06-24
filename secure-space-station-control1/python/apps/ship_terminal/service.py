from python.kafka.producer import KafkaProducerClient
from python.kafka.message_schema import create_command
from python.kafka.topics import RAW_COMMANDS_TOPIC


class ShipTerminal:
    def __init__(self):
        self.producer = KafkaProducerClient()
        self.source = "SHIP-1"

    def send_docking_request(self):
        command = create_command(
            command_type="REQUEST_DOCKING",
            source=self.source,
            target="central_control_system",
            payload={
                "ship_id": "SHIP-1",
                "docking_port_id": "DOCKING-PORT-1",
                "docking_node_status": "normal"
            }
        )

        self.producer.send(RAW_COMMANDS_TOPIC, command)
        return command

    def send_fake_docking_request(self):
        command = create_command(
            command_type="REQUEST_DOCKING",
            source="FAKE-SHIP",
            target="central_control_system",
            payload={
                "ship_id": "FAKE-SHIP",
                "docking_port_id": "DOCKING-PORT-1",
                "docking_node_status": "unknown"
            }
        )

        self.producer.send(RAW_COMMANDS_TOPIC, command)
        return command

    def send_damaged_node_telemetry(self):
        command = create_command(
            command_type="SEND_TELEMETRY",
            source=self.source,
            target="central_control_system",
            payload={
                "ship_id": "SHIP-1",
                "docking_node_status": "damaged"
            }
        )

        self.producer.send(RAW_COMMANDS_TOPIC, command)
        return command