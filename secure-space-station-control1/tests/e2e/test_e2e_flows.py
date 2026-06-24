from python.kafka.message_schema import create_command
from python.security.command_filter.service import CommandFilter
from python.security.command_validator.service import CommandValidator
from python.security.digital_signature.service import DigitalSignatureService
from python.security.authorization_service.service import AuthorizationService
from python.security.security_policy_service.service import SecurityPolicyService
from python.security.docking_validator.service import DockingValidator
from python.core.central_control_system.service import CentralControlSystem
from python.station_systems.gateway_control.service import GatewayControl
from python.station_systems.docking_control.service import DockingControl
from python.emergency.automatic_emergency_regulator.service import AutomaticEmergencyRegulator


def pass_security_pipeline(command: dict, context: dict | None = None) -> dict:
    signature_service = DigitalSignatureService()

    signed = signature_service.sign(command)

    filtered = CommandFilter().filter(signed)
    assert filtered["status"] == "passed"

    validated = CommandValidator().validate(filtered["command"])
    assert validated["status"] == "validated"

    signature_checked = signature_service.verify(validated["command"])
    assert signature_checked["status"] == "signature_valid"

    authorized = AuthorizationService().authorize(signature_checked["command"])
    assert authorized["status"] == "authorized"

    policy = SecurityPolicyService().check_policy(
        authorized["command"],
        context or {},
    )
    assert policy["status"] == "policy_passed"

    routed = CentralControlSystem().handle_command(policy["command"])
    assert routed["status"] == "routed"

    return policy["command"]


def test_e2e_operator_open_gateway_success():
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
            "pressure_normal": True,
            "sealed": True,
        }
    }

    checked_command = pass_security_pipeline(command, context)

    result = GatewayControl().open_gateway(checked_command)

    assert result["type"] == "GATEWAY_OPENED"
    assert result["payload"]["drive_result"]["status"] == "opened"


def test_e2e_ship_docking_success():
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

    checked_command = pass_security_pipeline(command)

    docking_check = DockingValidator().validate(
        command=checked_command,
        docking_node_status="normal",
    )
    assert docking_check["status"] == "docking_allowed"

    result = DockingControl().start_docking(docking_check["command"])

    assert result["type"] == "DOCKING_RESULT"
    assert result["payload"]["result"]["status"] == "docking_completed"


def test_e2e_decompression_enables_safe_mode():
    result = AutomaticEmergencyRegulator().handle_decompression("COMPARTMENT-1")

    assert result["status"] == "emergency_processed"
    assert result["emergency_result"]["status"] == "emergency_enabled"
    assert result["emergency_result"]["safe_mode"]["type"] == "SAFE_MODE_ENABLED"
