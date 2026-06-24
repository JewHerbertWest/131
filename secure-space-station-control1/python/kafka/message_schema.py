from datetime import datetime, timezone
from uuid import uuid4


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def create_command(
    command_type: str,
    source: str,
    target: str,
    payload: dict | None = None,
) -> dict:
    return {
        "id": str(uuid4()),
        "type": command_type,
        "source": source,
        "target": target,
        "payload": payload or {},
        "timestamp": utc_now(),
        "validated": False,
        "authorized": False,
        "signature_valid": False,
    }


def create_event(
    event_type: str,
    source: str,
    payload: dict | None = None,
) -> dict:
    return {
        "id": str(uuid4()),
        "type": event_type,
        "source": source,
        "payload": payload or {},
        "timestamp": utc_now(),
    }


def mark_validated(command: dict) -> dict:
    command["validated"] = True
    return command


def mark_authorized(command: dict) -> dict:
    command["authorized"] = True
    return command


def mark_signature_valid(command: dict) -> dict:
    command["signature_valid"] = True
    return command


def reject_message(message: dict, reason: str) -> dict:
    return {
        "status": "rejected",
        "reason": reason,
        "message": message,
        "timestamp": utc_now(),
    }