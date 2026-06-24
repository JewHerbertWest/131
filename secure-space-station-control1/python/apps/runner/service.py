from python.kafka.producer import KafkaProducerClient
from python.kafka.message_schema import create_command, create_event, mark_validated, mark_authorized
from python.kafka.topics import (
    RAW_COMMANDS_TOPIC,
    FILTERED_COMMANDS_TOPIC,
    VALIDATED_COMMANDS_TOPIC,
    AUTHORIZED_COMMANDS_TOPIC,
    REJECTED_COMMANDS_TOPIC,
    GATEWAY_COMMANDS_TOPIC,
    GATEWAY_EVENTS_TOPIC,
    DOCKING_COMMANDS_TOPIC,
    DOCKING_EVENTS_TOPIC,
    COMPARTMENT_TELEMETRY_TOPIC,
    SENSOR_TELEMETRY_TOPIC,
    EQUIPMENT_TELEMETRY_TOPIC,
    MONITORING_EVENTS_TOPIC,
    SECURITY_EVENTS_TOPIC,
    EMERGENCY_EVENTS_TOPIC,
    SAFE_MODE_COMMANDS_TOPIC,
    STATE_WRITE_TOPIC,
    STATE_EVENTS_TOPIC,
    JOURNAL_EVENTS_TOPIC,
)
from python.core.logging.logger import log


class RunnerApp:
    def __init__(self):
        self.producer = KafkaProducerClient()

    def run(self):
        while True:
            print("\n=== SECURE SPACE STATION CONTROL ===")
            print("1 — оператор: открыть шлюз")
            print("2 — оператор: закрыть шлюз")
            print("3 — оператор: аварийный режим")
            print("4 — Земля: открыть шлюз")
            print("5 — Земля: отменить операцию")
            print("6 — Земля: подменённая команда открытия шлюза")
            print("7 — корабль: запрос стыковки")
            print("8 — корабль: поддельный запрос стыковки")
            print("9 — корабль: телеметрия повреждённого узла")
            print("0 — выход")

            choice = input("\nВведите команду: ").strip()

            if choice == "1":
                self.operator_open_gateway()
            elif choice == "2":
                self.operator_close_gateway()
            elif choice == "3":
                self.operator_emergency_mode()
            elif choice == "4":
                self.earth_open_gateway()
            elif choice == "5":
                self.earth_cancel_operation()
            elif choice == "6":
                self.earth_tampered_command()
            elif choice == "7":
                self.ship_docking_request()
            elif choice == "8":
                self.fake_ship_docking_request()
            elif choice == "9":
                self.damaged_ship_telemetry()
            elif choice == "0":
                print("Выход")
                break
            else:
                print("Неизвестная команда")

    def _send(self, topic: str, message: dict):
        self.producer.send(topic, message)
        log("runner", f"Отправлено в Kafka: {topic}")

    def _journal(self, event_type: str, payload: dict):
        event = create_event(event_type, "runner", payload)
        self._send(JOURNAL_EVENTS_TOPIC, event)

    def _success_pipeline(self, command: dict, system_topic: str, event_topic: str, event_type: str):
        self._send(RAW_COMMANDS_TOPIC, command)

        self._send(FILTERED_COMMANDS_TOPIC, command)

        command = mark_validated(command)
        command["signature_valid"] = True
        self._send(VALIDATED_COMMANDS_TOPIC, command)

        command = mark_authorized(command)
        self._send(AUTHORIZED_COMMANDS_TOPIC, command)

        self._send(system_topic, command)

        event = create_event(
            event_type,
            "central_control_system",
            {
                "command_id": command["id"],
                "command_type": command["type"],
                "status": "executed",
                "target": command["target"],
                "payload": command["payload"],
            },
        )
        self._send(event_topic, event)

        state_event = create_event(
            "STATE_UPDATED",
            "state_repository",
            {
                "command_id": command["id"],
                "status": "updated",
                "payload": command["payload"],
            },
        )
        self._send(STATE_WRITE_TOPIC, state_event)
        self._send(STATE_EVENTS_TOPIC, state_event)

        monitoring_event = create_event(
            "COMMAND_EXECUTED",
            "command_security_monitor",
            {
                "command_id": command["id"],
                "command_type": command["type"],
                "result": "success",
            },
        )
        self._send(MONITORING_EVENTS_TOPIC, monitoring_event)

        self._journal("COMMAND_EXECUTED", {
            "command_id": command["id"],
            "command_type": command["type"],
            "result": "success",
        })

    def _reject_pipeline(self, command: dict, reason: str):
        self._send(RAW_COMMANDS_TOPIC, command)
        self._send(FILTERED_COMMANDS_TOPIC, command)

        rejected = {
            "status": "rejected",
            "reason": reason,
            "message": command,
        }

        self._send(REJECTED_COMMANDS_TOPIC, rejected)

        security_event = create_event(
            "SECURITY_VIOLATION",
            "security_policy_service",
            {
                "command_id": command["id"],
                "command_type": command["type"],
                "source": command["source"],
                "reason": reason,
                "result": "blocked",
            },
        )
        self._send(SECURITY_EVENTS_TOPIC, security_event)

        monitoring_event = create_event(
            "COMMAND_BLOCKED",
            "command_security_monitor",
            {
                "command_id": command["id"],
                "reason": reason,
            },
        )
        self._send(MONITORING_EVENTS_TOPIC, monitoring_event)

        self._journal("COMMAND_REJECTED", {
            "command_id": command["id"],
            "reason": reason,
        })

    def operator_open_gateway(self):
        command = create_command(
            "OPEN_GATEWAY",
            "OPERATOR-1",
            "gateway_control",
            {
                "gateway_id": "GATEWAY-1",
                "compartment_id": "COMPARTMENT-1",
            },
        )

        telemetry = create_event(
            "COMPARTMENT_TELEMETRY",
            "sensor_monitor",
            {
                "compartment_id": "COMPARTMENT-1",
                "pressure": 100,
                "sealed": True,
                "status": "normal",
            },
        )
        self._send(COMPARTMENT_TELEMETRY_TOPIC, telemetry)
        self._send(SENSOR_TELEMETRY_TOPIC, telemetry)

        self._success_pipeline(
            command,
            GATEWAY_COMMANDS_TOPIC,
            GATEWAY_EVENTS_TOPIC,
            "GATEWAY_OPENED",
        )

    def operator_close_gateway(self):
        command = create_command(
            "CLOSE_GATEWAY",
            "OPERATOR-1",
            "gateway_control",
            {
                "gateway_id": "GATEWAY-1",
            },
        )

        self._success_pipeline(
            command,
            GATEWAY_COMMANDS_TOPIC,
            GATEWAY_EVENTS_TOPIC,
            "GATEWAY_CLOSED",
        )

    def operator_emergency_mode(self):
        command = create_command(
            "EMERGENCY_MODE",
            "OPERATOR-1",
            "emergency_module",
            {
                "reason": "operator_emergency_command",
            },
        )

        self._send(RAW_COMMANDS_TOPIC, command)
        self._send(FILTERED_COMMANDS_TOPIC, command)

        command = mark_validated(command)
        command["signature_valid"] = True
        command = mark_authorized(command)

        self._send(VALIDATED_COMMANDS_TOPIC, command)
        self._send(AUTHORIZED_COMMANDS_TOPIC, command)

        emergency_event = create_event(
            "EMERGENCY_MODE_ENABLED",
            "emergency_module",
            {
                "reason": "operator_emergency_command",
                "safe_mode": True,
            },
        )

        self._send(EMERGENCY_EVENTS_TOPIC, emergency_event)
        self._send(SAFE_MODE_COMMANDS_TOPIC, emergency_event)
        self._send(STATE_EVENTS_TOPIC, emergency_event)
        self._journal("EMERGENCY_MODE_ENABLED", emergency_event)

    def earth_open_gateway(self):
        command = create_command(
            "OPEN_GATEWAY",
            "EARTH-CONTROL-1",
            "gateway_control",
            {
                "gateway_id": "GATEWAY-1",
                "compartment_id": "COMPARTMENT-1",
            },
        )

        self._success_pipeline(
            command,
            GATEWAY_COMMANDS_TOPIC,
            GATEWAY_EVENTS_TOPIC,
            "GATEWAY_OPENED_BY_EARTH_COMMAND",
        )

    def earth_cancel_operation(self):
        command = create_command(
            "CANCEL_OPERATION",
            "EARTH-CONTROL-1",
            "central_control_system",
            {
                "operation_id": "OPERATION-1",
            },
        )

        self._success_pipeline(
            command,
            MONITORING_EVENTS_TOPIC,
            STATE_EVENTS_TOPIC,
            "OPERATION_CANCELLED",
        )

    def earth_tampered_command(self):
        command = create_command(
            "OPEN_GATEWAY",
            "EARTH-CONTROL-1",
            "gateway_control",
            {
                "gateway_id": "GATEWAY-1",
                "compartment_id": "COMPARTMENT-1",
                "tampered": True,
                "changed_after_signature": True,
            },
        )

        self._reject_pipeline(
            command,
            "Обнаружена подмена команды или нарушение цифровой подписи",
        )

    def ship_docking_request(self):
        command = create_command(
            "REQUEST_DOCKING",
            "SHIP-1",
            "docking_control",
            {
                "ship_id": "SHIP-1",
                "docking_port_id": "DOCKING-PORT-1",
                "telemetry_valid": True,
                "docking_node_status": "normal",
            },
        )

        equipment_event = create_event(
            "EQUIPMENT_TELEMETRY",
            "equipment_monitor",
            {
                "docking_port_id": "DOCKING-PORT-1",
                "node_status": "normal",
                "power": "normal",
            },
        )
        self._send(EQUIPMENT_TELEMETRY_TOPIC, equipment_event)

        self._success_pipeline(
            command,
            DOCKING_COMMANDS_TOPIC,
            DOCKING_EVENTS_TOPIC,
            "DOCKING_ALLOWED",
        )

    def fake_ship_docking_request(self):
        command = create_command(
            "REQUEST_DOCKING",
            "UNKNOWN-SHIP",
            "docking_control",
            {
                "ship_id": "UNKNOWN-SHIP",
                "docking_port_id": "DOCKING-PORT-1",
                "telemetry_valid": False,
            },
        )

        self._reject_pipeline(
            command,
            "Поддельный корабль не прошёл проверку источника",
        )

    def damaged_ship_telemetry(self):
        command = create_command(
            "REQUEST_DOCKING",
            "SHIP-1",
            "docking_control",
            {
                "ship_id": "SHIP-1",
                "docking_port_id": "DOCKING-PORT-1",
                "telemetry_valid": False,
                "docking_node_status": "broken",
            },
        )

        telemetry_event = create_event(
            "DAMAGED_DOCKING_NODE",
            "sensor_monitor",
            {
                "ship_id": "SHIP-1",
                "docking_node_status": "broken",
                "telemetry_valid": False,
            },
        )
        self._send(SENSOR_TELEMETRY_TOPIC, telemetry_event)
        self._send(EQUIPMENT_TELEMETRY_TOPIC, telemetry_event)

        self._reject_pipeline(
            command,
            "Обнаружена повреждённая телеметрия или неисправный стыковочный узел",
        )

        emergency_event = create_event(
            "DOCKING_RISK_DETECTED",
            "emergency_module",
            {
                "ship_id": "SHIP-1",
                "reason": "damaged_docking_node",
            },
        )
        self._send(EMERGENCY_EVENTS_TOPIC, emergency_event)