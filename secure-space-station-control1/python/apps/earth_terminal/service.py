from python.kafka.producer import KafkaProducerClient
from python.kafka.message_schema import create_command
from python.kafka.topics import RAW_COMMANDS_TOPIC


class EarthTerminal:
    def __init__(self):
        self.producer = KafkaProducerClient()
        self.source = "EARTH-CONTROL-1"

    def send_open_gateway_command(self):
        command = create_command(
            command_type="OPEN_GATEWAY",
            source=self.source,
            target="central_control_system",
            payload={
                "gateway_id": "GATEWAY-1",
                "compartment_id": "COMPARTMENT-1"
            }
        )

        self.producer.send(RAW_COMMANDS_TOPIC, command)
        return command

    def send_cancel_operation_command(self):
        command = create_command(
            command_type="CANCEL_OPERATION",
            source=self.source,
            target="central_control_system",
            payload={
                "operation_id": "OPERATION-1"
            }
        )

        self.producer.send(RAW_COMMANDS_TOPIC, command)
        return command

    def send_tampered_gateway_command(self):
        command = create_command(
            command_type="OPEN_GATEWAY",
            source=self.source,
            target="central_control_system",
            payload={
                "gateway_id": "FAKE-GATEWAY",
                "compartment_id": "COMPARTMENT-1",
                "tampered": True
            }
        )

        self.producer.send(RAW_COMMANDS_TOPIC, command)
        return command