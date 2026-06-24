from python.communication.earth_link_interface.service import EarthLinkInterface


def run_communication_loss():
    earth_link = EarthLinkInterface()

    result = earth_link.lose_connection()

    return {
        "scenario": "communication_loss",
        "expected_result": "security_event_created",
        "result": result,
    }