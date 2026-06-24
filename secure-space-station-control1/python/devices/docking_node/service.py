from python.core.logging.logger import log


class DockingNode:
    def __init__(self):
        self.node_id = "DOCKING-PORT-1"
        self.status = "free"
        self.node_status = "normal"
        self.power = "normal"

    def start_docking(self, ship_id: str) -> dict:
        if self.node_status != "normal":
            log("docking_node", f"Стыковка запрещена: узел неисправен, корабль {ship_id}")
            return {
                "status": "blocked",
                "reason": "docking_node_not_normal",
                "ship_id": ship_id,
                "node_status": self.node_status,
            }

        self.status = "docking_completed"

        log("docking_node", f"Стыковка корабля {ship_id} завершена")

        return {
            "status": self.status,
            "ship_id": ship_id,
            "node_id": self.node_id,
            "node_status": self.node_status,
        }

    def set_node_damaged(self) -> dict:
        self.node_status = "damaged"

        log("docking_node", "Стыковочный узел повреждён")

        return {
            "node_id": self.node_id,
            "node_status": self.node_status,
        }

    def set_power_failure(self) -> dict:
        self.power = "failed"

        log("docking_node", "Отказ питания стыковочного узла")

        return {
            "node_id": self.node_id,
            "power": self.power,
        }

    def get_status(self) -> dict:
        return {
            "node_id": self.node_id,
            "status": self.status,
            "node_status": self.node_status,
            "power": self.power,
        }