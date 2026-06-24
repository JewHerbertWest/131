from datetime import datetime, timedelta, timezone

from python.kafka.message_schema import create_command
from python.security.command_filter.service import CommandFilter
from python.security.command_validator.service import CommandValidator
from python.security.digital_signature.service import DigitalSignatureService
from python.security.authorization_service.service import AuthorizationService
from python.security.access_rights_validator.service import AccessRightsValidator
from python.security.security_policy_service.service import SecurityPolicyService
from python.security.docking_validator.service import DockingValidator
from python.security.sensor_value_verifier.service import SensorValueVerifier
from python.monitoring.command_security_monitor.service import CommandSecurityMonitor
from python.monitoring.equipment_monitor.service import EquipmentMonitor
from python.monitoring.event_analyzer.service import EventAnalyzer
from python.emergency.automatic_emergency_regulator.service import AutomaticEmergencyRegulator


def test_ns_01_earth_command_tampering_attack_successful():
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

    result = CommandValidator().validate(command)

    assert result["status"] == "validated"


def test_ns_02_fake_ship_docking_attack_successful():
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

    result = CommandFilter().filter(command)

    assert result["status"] == "passed"


def test_ns_03_gateway_without_pressure_check_attack_successful():
    command = create_command(
        command_type="OPEN_GATEWAY",
        source="OPERATOR-1",
        target="central_control_system",
        payload={
            "gateway_id": "GATEWAY-1",
            "compartment_id": "COMPARTMENT-1",
        },
    )

    context = {
        "compartment": {
            "pressure_normal": False,
            "sealed": True,
        }
    }

    result = SecurityPolicyService().check_policy(command, context)

    assert result["status"] == "policy_passed"


def test_ns_04_double_gateway_open_attack_successful():
    command = create_command(
        command_type="OPEN_INNER_AND_OUTER_GATEWAY",
        source="OPERATOR-1",
        target="gateway_control",
        payload={
            "gateway_id": "GATEWAY-1",
        },
    )

    result = SecurityPolicyService().check_policy(command)

    assert result["status"] == "policy_passed"


def test_ns_05_unauthorized_reactor_access_attack_successful():
    result = AccessRightsValidator().validate_access(
        user_id="OPERATOR-1",
        sector_id="REACTOR-SECTOR",
    )

    assert result["status"] == "access_granted"


def test_ns_06_false_pressure_data_attack_successful():
    reported = {
        "pressure_normal": True,
        "sealed": True,
    }

    real = {
        "pressure_normal": False,
        "sealed": False,
    }

    result = SensorValueVerifier().verify_compartment(reported, real)

    assert result["status"] == "verified"


def test_ns_07_broken_docking_node_attack_successful():
    command = create_command(
        command_type="REQUEST_DOCKING",
        source="SHIP-1",
        target="central_control_system",
        payload={
            "ship_id": "SHIP-1",
            "docking_port_id": "DOCKING-PORT-1",
            "docking_node_status": "normal",
        },
    )

    result = DockingValidator().validate(
        command=command,
        docking_node_status="damaged",
    )

    assert result["status"] == "docking_allowed"


def test_ns_08_command_substitution_attack_successful():
    command = create_command(
        command_type="CLOSE_GATEWAY",
        source="OPERATOR-1",
        target="central_control_system",
        payload={
            "gateway_id": "GATEWAY-1",
        },
    )

    signature_service = DigitalSignatureService()
    signed_command = signature_service.sign(command)

    signed_command["type"] = "OPEN_GATEWAY"

    result = signature_service.verify(signed_command)

    assert result["status"] == "signature_valid"


def test_ns_09_false_database_state_attack_successful():
    real_state = {
        "gateway_id": "GATEWAY-1",
        "state": "open",
    }

    stored_state = {
        "gateway_id": "GATEWAY-1",
        "state": "closed",
    }

    result = EventAnalyzer().detect_false_state(real_state, stored_state)

    assert result["status"] == "ok"


def test_ns_10_missing_journal_record_attack_successful():
    journal_events = []

    result = EventAnalyzer().detect_missing_journal_record(
        expected_event_type="EMERGENCY_GATEWAY_OPENED",
        journal_events=journal_events,
    )

    assert result["status"] == "ok"


def test_ns_11_gateway_power_failure_attack_successful():
    result = EquipmentMonitor().check_gateway_power(
        gateway_id="GATEWAY-1",
        power_status="failed",
    )

    assert result["status"] == "ok"


def test_ns_12_unauthorized_unlock_attack_successful():
    command = create_command(
        command_type="UNLOCK_REACTOR_SECTOR",
        source="OPERATOR-1",
        target="access_control",
        payload={
            "sector_id": "REACTOR-SECTOR",
        },
    )

    result = CommandFilter().filter(command)

    assert result["status"] == "passed"


def test_ns_13_emergency_safe_mode_not_enabled_attack_successful():
    result = AutomaticEmergencyRegulator().handle_decompression("COMPARTMENT-1")

    assert result["emergency_result"]["status"] != "emergency_enabled"


def test_ns_14_replayed_earth_command_attack_successful():
    monitor = CommandSecurityMonitor()

    command = create_command(
        command_type="OPEN_GATEWAY",
        source="EARTH-CONTROL-1",
        target="central_control_system",
        payload={
            "gateway_id": "GATEWAY-1",
        },
    )

    first_result = monitor.check_replay(command)
    second_result = monitor.check_replay(command)

    assert first_result["status"] == "ok"
    assert second_result["status"] == "ok"


def test_ns_15_fake_ship_telemetry_attack_successful():
    command = create_command(
        command_type="REQUEST_DOCKING",
        source="SHIP-1",
        target="central_control_system",
        payload={
            "ship_id": "SHIP-1",
            "docking_node_status": "damaged",
        },
    )

    result = DockingValidator().validate(
        command=command,
        docking_node_status="normal",
    )

    assert result["status"] == "docking_allowed"


def test_ns_16_conflicting_commands_attack_successful():
    monitor = CommandSecurityMonitor()

    open_command = create_command(
        command_type="OPEN_GATEWAY",
        source="OPERATOR-1",
        target="central_control_system",
        payload={
            "gateway_id": "GATEWAY-1",
        },
    )

    block_command = create_command(
        command_type="BLOCK_GATEWAY",
        source="OPERATOR-1",
        target="central_control_system",
        payload={
            "gateway_id": "GATEWAY-1",
        },
    )

    result = monitor.check_conflict(open_command, block_command)

    assert result["status"] == "ok"


def test_ns_17_delayed_emergency_command_attack_successful():
    command = create_command(
        command_type="CLOSE_GATEWAY",
        source="OPERATOR-1",
        target="central_control_system",
        payload={
            "gateway_id": "GATEWAY-1",
        },
    )

    command["timestamp"] = (
        datetime.now(timezone.utc) - timedelta(seconds=60)
    ).isoformat()

    result = CommandSecurityMonitor().check_delay(
        command,
        max_delay_seconds=5,
    )

    assert result["status"] == "ok"
