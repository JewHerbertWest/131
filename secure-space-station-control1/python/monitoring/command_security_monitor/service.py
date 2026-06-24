from datetime import datetime, timezone

from python.core.logging.logger import log
from python.kafka.message_schema import create_event
from python.kafka.producer import KafkaProducerClient
from python.kafka.topics import MONITORING_EVENTS_TOPIC, SECURITY_EVENTS_TOPIC


class CommandSecurityMonitor:
    def __init__(self):
        self.producer = KafkaProducerClient()
        self.last_commands = {}

    def check_replay(self, command: dict) -> dict:
        command_id = command.get("id")

        if command_id in self.last_commands:
            return self._security_event("REPLAY_COMMAND_DETECTED", command)

        self.last_commands[command_id] = command

        return {
            "status": "ok",
            "reason": None,
            "command": command,
        }

    def check_conflict(self, first_command: dict, second_command: dict) -> dict:
        first_type = first_command.get("type")
        second_type = second_command.get("type")

        conflict_pairs = [
            ("OPEN_GATEWAY", "BLOCK_GATEWAY"),
            ("OPEN_GATEWAY", "CLOSE_GATEWAY"),
            ("UNLOCK_ACCESS", "LOCK_ACCESS"),
        ]

        if (first_type, second_type) in conflict_pairs or (second_type, first_type) in conflict_pairs:
            event = create_event(
                event_type="CONFLICTING_COMMANDS_DETECTED",
                source="command_security_monitor",
                payload={
                    "first_command": first_command,
                    "second_command": second_command,
                },
            )

            self.producer.send(SECURITY_EVENTS_TOPIC, event)
            self.producer.send(MONITORING_EVENTS_TOPIC, event)

            log("command_security_monitor", "Обнаружены взаимоисключающие команды")

            return {
                "status": "conflict_detected",
                "event": event,
            }

        return {
            "status": "ok",
            "reason": None,
        }

    def check_delay(self, command: dict, max_delay_seconds: int = 5) -> dict:
        timestamp = command.get("timestamp")

        if timestamp is None:
            return self._security_event("COMMAND_TIMESTAMP_MISSING", command)

        command_time = datetime.fromisoformat(timestamp)
        now = datetime.now(timezone.utc)
        delay = (now - command_time).total_seconds()

        if delay > max_delay_seconds:
            return self._security_event("DELAYED_COMMAND_DETECTED", command)

        return {
            "status": "ok",
            "delay_seconds": delay,
        }

    def _security_event(self, event_type: str, command: dict) -> dict:
        event = create_event(
            event_type=event_type,
            source="command_security_monitor",
            payload=command,
        )

        self.producer.send(SECURITY_EVENTS_TOPIC, event)
        self.producer.send(MONITORING_EVENTS_TOPIC, event)

        log("command_security_monitor", f"Событие безопасности: {event_type}")

        return {
            "status": "detected",
            "event": event,
        }