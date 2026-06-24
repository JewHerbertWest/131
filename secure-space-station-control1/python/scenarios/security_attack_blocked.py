from python.apps.earth_terminal.service import EarthTerminal
from python.apps.ship_terminal.service import ShipTerminal


def run_earth_command_tampering():
    terminal = EarthTerminal()

    command = terminal.send_tampered_gateway_command()

    return {
        "scenario": "earth_command_tampering",
        "expected_result": "blocked_by_security_pipeline",
        "command": command,
    }


def run_fake_ship_docking():
    terminal = ShipTerminal()

    command = terminal.send_fake_docking_request()

    return {
        "scenario": "fake_ship_docking",
        "expected_result": "blocked_by_docking_validator",
        "command": command,
    }


def run_damaged_ship_telemetry():
    terminal = ShipTerminal()

    command = terminal.send_damaged_node_telemetry()

    return {
        "scenario": "damaged_ship_telemetry",
        "expected_result": "blocked_by_docking_validator",
        "command": command,
    }