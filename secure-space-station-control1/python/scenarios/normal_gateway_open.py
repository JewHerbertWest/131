from python.apps.operator_terminal.service import OperatorTerminal


def run_normal_gateway_open():
    terminal = OperatorTerminal()

    command = terminal.send_open_gateway_command()

    return {
        "scenario": "normal_gateway_open",
        "status": "command_sent",
        "command": command,
    }