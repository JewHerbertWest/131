from python.core.logging.logger import log


class OtherEquipment:
    def __init__(self):
        self.equipment = {
            "life_support": "normal",
            "sensors": "normal",
            "communication_unit": "normal",
        }

    def set_failure(self, equipment_id: str) -> dict:
        self.equipment[equipment_id] = "failed"

        log("other_equipment", f"Оборудование {equipment_id} отказало")

        return {
            "equipment_id": equipment_id,
            "status": self.equipment[equipment_id],
        }

    def restore(self, equipment_id: str) -> dict:
        self.equipment[equipment_id] = "normal"

        log("other_equipment", f"Оборудование {equipment_id} восстановлено")

        return {
            "equipment_id": equipment_id,
            "status": self.equipment[equipment_id],
        }

    def get_status(self) -> dict:
        return {
            "equipment": self.equipment,
        }