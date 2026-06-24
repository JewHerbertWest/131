import json

from python.core.config.settings import TELEMETRY_FILE


class TelemetryRepository:
    def read_telemetry(self) -> dict:
        with open(TELEMETRY_FILE, "r", encoding="utf-8") as file:
            return json.load(file)

    def get_compartment_sensor_data(self, compartment_id: str) -> dict | None:
        telemetry = self.read_telemetry()
        return telemetry.get("sensors", {}).get(compartment_id)

    def get_equipment_data(self, equipment_id: str) -> dict | None:
        telemetry = self.read_telemetry()
        return telemetry.get("equipment", {}).get(equipment_id)

    def get_ship_telemetry(self, ship_id: str) -> dict | None:
        telemetry = self.read_telemetry()
        return telemetry.get("ships", {}).get(ship_id)

    def get_earth_link_status(self) -> dict | None:
        telemetry = self.read_telemetry()
        return telemetry.get("communication", {}).get("earth_link")

    def get_ship_link_status(self) -> dict | None:
        telemetry = self.read_telemetry()
        return telemetry.get("communication", {}).get("ship_link")