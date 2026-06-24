from python.apps.ship_terminal.service import ShipTerminal


def run_normal_docking():
    terminal = ShipTerminal()

    command = terminal.send_docking_request()

    return {
        "scenario": "normal_docking",
        "status": "command_sent",
        "command": command,
    }