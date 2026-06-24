from python.kafka.producer import KafkaProducerClient
from python.kafka.message_schema import create_command
from python.kafka.topics import RAW_COMMANDS_TOPIC


class OperatorTerminal:
    def __init__(self):
        self.producer = KafkaProducerClient()
        self.source = "OPERATOR-1"

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

    def send_close_gateway_command(self):
        command = create_command(
            command_type="CLOSE_GATEWAY",
            source=self.source,
            target="central_control_system",
            payload={
                "gateway_id": "GATEWAY-1"
            }
        )

        self.producer.send(RAW_COMMANDS_TOPIC, command)
        return command

    def send_emergency_command(self):
        command = create_command(
            command_type="EMERGENCY_MODE",
            source=self.source,
            target="central_control_system",
            payload={
                "reason": "operator_emergency_command"
            }
        )

        self.producer.send(RAW_COMMANDS_TOPIC, command)
        return command