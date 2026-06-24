from python.core.logging.logger import log


class GatewayDrives:
    def __init__(self):
        self.gateway_id = "GATEWAY-1"
        self.inner_door = "closed"
        self.outer_door = "closed"
        self.power = "normal"

    def open_inner_door(self) -> dict:
        if self.outer_door == "open":
            log("gateway_drives", "Открытие внутренней створки запрещено: внешняя створка уже открыта")
            return {
                "status": "blocked",
                "reason": "outer_door_already_open",
                "inner_door": self.inner_door,
                "outer_door": self.outer_door,
            }

        self.inner_door = "open"
        log("gateway_drives", "Внутренняя створка шлюза открыта")

        return {
            "status": "opened",
            "door": "inner",
            "inner_door": self.inner_door,
            "outer_door": self.outer_door,
        }

    def open_outer_door(self) -> dict:
        if self.inner_door == "open":
            log("gateway_drives", "Открытие внешней створки запрещено: внутренняя створка уже открыта")
            return {
                "status": "blocked",
                "reason": "inner_door_already_open",
                "inner_door": self.inner_door,
                "outer_door": self.outer_door,
            }

        self.outer_door = "open"
        log("gateway_drives", "Внешняя створка шлюза открыта")

        return {
            "status": "opened",
            "door": "outer",
            "inner_door": self.inner_door,
            "outer_door": self.outer_door,
        }

    def close_all_doors(self) -> dict:
        self.inner_door = "closed"
        self.outer_door = "closed"

        log("gateway_drives", "Все створки шлюза закрыты")

        return {
            "status": "closed",
            "inner_door": self.inner_door,
            "outer_door": self.outer_door,
        }

    def block_drives(self) -> dict:
        self.inner_door = "blocked"
        self.outer_door = "blocked"

        log("gateway_drives", "Приводы шлюза заблокированы")

        return {
            "status": "blocked",
            "inner_door": self.inner_door,
            "outer_door": self.outer_door,
        }

    def set_power_failure(self) -> dict:
        self.power = "failed"

        log("gateway_drives", "Отказ питания привода шлюза")

        return {
            "status": "failed",
            "power": self.power,
        }

    def get_status(self) -> dict:
        return {
            "gateway_id": self.gateway_id,
            "inner_door": self.inner_door,
            "outer_door": self.outer_door,
            "power": self.power,
        }