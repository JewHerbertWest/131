from python.core.logging.logger import log
from python.kafka.message_schema import create_event
from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import EMERGENCY_EVENTS_TOPIC, MONITORING_EVENTS_TOPIC


class EquipmentMonitor:
    def __init__(self):
        self.producer = KafkaProducerClient()

    def check_gateway_power(self, gateway_id: str, power_status: str) -> dict:
        if power_status != "normal":
            event = create_event(
                event_type="GATEWAY_POWER_FAILURE_DETECTED",
                source="equipment_monitor",
                payload={
                    "gateway_id": gateway_id,
                    "power_status": power_status,
                },
            )

            self.producer.send(MONITORING_EVENTS_TOPIC, event)
            self.producer.send(EMERGENCY_EVENTS_TOPIC, event)

            log("equipment_monitor", f"Отказ питания шлюза {gateway_id}")

            return {
                "status": "failure_detected",
                "event": event,
            }

        return {
            "status": "ok",
            "gateway_id": gateway_id,
        }

    def check_docking_node(self, node_id: str, node_status: str) -> dict:
        if node_status != "normal":
            event = create_event(
                event_type="DOCKING_NODE_FAILURE_DETECTED",
                source="equipment_monitor",
                payload={
                    "node_id": node_id,
                    "node_status": node_status,
                },
            )

            self.producer.send(MONITORING_EVENTS_TOPIC, event)
            self.producer.send(EMERGENCY_EVENTS_TOPIC, event)

            log("equipment_monitor", f"Неисправность стыковочного узла {node_id}")

            return {
                "status": "failure_detected",
                "event": event,
            }

        return {
            "status": "ok",
            "node_id": node_id,
        }