import json

from python.core.config.settings import CONFIG_FILE


class ConfigRepository:
    def read_config(self) -> dict:
        with open(CONFIG_FILE, "r", encoding="utf-8") as file:
            return json.load(file)

    def get_gateway_config(self, gateway_id: str) -> dict | None:
        config = self.read_config()
        return config.get("gateways", {}).get(gateway_id)

    def get_compartment_config(self, compartment_id: str) -> dict | None:
        config = self.read_config()
        return config.get("compartments", {}).get(compartment_id)

    def get_security_config(self) -> dict:
        config = self.read_config()
        return config.get("security", {})