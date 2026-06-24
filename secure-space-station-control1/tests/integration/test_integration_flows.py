from python.kafka.message_schema import create_command
from python.security.command_filter.service import CommandFilter
from python.security.command_validator.service import CommandValidator
from python.security.digital_signature.service import DigitalSignatureService
from python.security.authorization_service.service import AuthorizationService
from python.security.security_policy_service.service import SecurityPolicyService
from python.core.central_control_system.service import CentralControlSystem
from python.station_systems.gateway_control.service import GatewayControl
from python.station_systems.docking_control.service import DockingControl
from python.emergency.automatic_emergency_regulator.service import AutomaticEmergencyRegulator


def test_gateway_open_security_pipeline():
    command = create_command(
        command_type="OPEN_GATEWAY",
        source="OPERATOR-1",
        target="central_control_system",
        payload={
            "gateway_id": "GATEWAY-1",
            "compartment_id": "COMPARTMENT-1",
        },
    )

    command = DigitalSignatureService().sign(command)

    filtered = CommandFilter().filter(command)
    assert filtered["status"] == "passed"

    validated = CommandValidator().validate(filtered["command"])
    assert validated["status"] == "validated"

    signature = DigitalSignatureService().verify(validated["command"])
    assert signature["status"] == "signature_valid"

    authorized = AuthorizationService().authorize(signature["command"])
    assert authorized["status"] == "authorized"

    context = {
        "compartment": {
            "pressure_normal": True,
            "sealed": True,
        }
    }

    policy = SecurityPolicyService().check_policy(authorized["command"], context)
    assert policy["status"] == "policy_passed"

    result = CentralControlSystem().handle_command(policy["command"])
    assert result["status"] == "routed"


def test_gateway_control_executes_open_command():
    command = create_command(
        command_type="OPEN_GATEWAY",
        source="OPERATOR-1",
        target="gateway_control",
        payload={
            "gateway_id": "GATEWAY-1",
        },
    )

    result = GatewayControl().open_gateway(command)

    assert result["type"] == "GATEWAY_OPENED"


def test_docking_control_executes_docking_command():
    command = create_command(
        command_type="REQUEST_DOCKING",
        source="SHIP-1",
        target="docking_control",
        payload={
            "ship_id": "SHIP-1",
            "docking_port_id": "DOCKING-PORT-1",
            "docking_node_status": "normal",
        },
    )

    result = DockingControl().start_docking(command)

    assert result["type"] == "DOCKING_RESULT"


def test_emergency_decompression_flow():
    result = AutomaticEmergencyRegulator().handle_decompression("COMPARTMENT-1")

    assert result["status"] == "emergency_processed"
    assert result["emergency_result"]["status"] == "emergency_enabled"