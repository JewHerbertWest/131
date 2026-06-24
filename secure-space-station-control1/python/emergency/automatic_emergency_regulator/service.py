from python.core.logging.logger import log
from python.emergency.emergency_module.service import EmergencyModule
from python.kafka.producer import KafkaProducerClient
from python.kafka.message_schema import create_event
from python.kafka.topics import EMERGENCY_EVENTS_TOPIC, SECURITY_EVENTS_TOPIC


class AutomaticEmergencyRegulator:
    def __init__(self):
        self.producer = KafkaProducerClient()
        self.emergency_module = EmergencyModule()

    def handle_decompression(self, compartment_id: str) -> dict:
        reason = f"Разгерметизация отсека {compartment_id}"

        event = create_event(
            event_type="DECOMPRESSION_DETECTED",
            source="automatic_emergency_regulator",
            payload={
                "compartment_id": compartment_id,
                "reason": reason,
            },
        )

        self.producer.send(EMERGENCY_EVENTS_TOPIC, event)

        emergency_result = self.emergency_module.activate(reason)

        log("automatic_emergency_regulator", reason)

        return {
            "status": "emergency_processed",
            "event": event,
            "emergency_result": emergency_result,
        }

    def handle_gateway_power_failure(self, gateway_id: str) -> dict:
        reason = f"Отказ питания шлюзового контура {gateway_id}"

        event = create_event(
            event_type="GATEWAY_POWER_FAILURE_DETECTED",
            source="automatic_emergency_regulator",
            payload={
                "gateway_id": gateway_id,
                "reason": reason,
            },
        )

        self.producer.send(EMERGENCY_EVENTS_TOPIC, event)
        self.producer.send(SECURITY_EVENTS_TOPIC, event)

        emergency_result = self.emergency_module.activate(reason)

        log("automatic_emergency_regulator", reason)

        return {
            "status": "emergency_processed",
            "event": event,
            "emergency_result": emergency_result,
        }

    def handle_delayed_emergency_command(self, command: dict) -> dict:
        reason = "Задержка аварийной команды"

        event = create_event(
            event_type="DELAYED_EMERGENCY_COMMAND_DETECTED",
            source="automatic_emergency_regulator",
            payload={
                "reason": reason,
                "command": command,
            },
        )

        self.producer.send(EMERGENCY_EVENTS_TOPIC, event)
        self.producer.send(SECURITY_EVENTS_TOPIC, event)

        emergency_result = self.emergency_module.activate(reason)

        log("automatic_emergency_regulator", reason)

        return {
            "status": "emergency_processed",
            "event": event,
            "emergency_result": emergency_result,
        }