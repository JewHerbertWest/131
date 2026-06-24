from python.emergency.automatic_emergency_regulator.service import AutomaticEmergencyRegulator


def run_emergency_decompression():
    regulator = AutomaticEmergencyRegulator()

    result = regulator.handle_decompression("COMPARTMENT-1")

    return {
        "scenario": "emergency_decompression",
        "expected_result": "safe_mode_enabled",
        "result": result,
    }