from python.core.logging.logger import log


class AccessLocks:
    def __init__(self):
        self.locks = {
            "COMPARTMENT-1": "locked",
            "REACTOR-SECTOR": "locked",
        }

    def unlock(self, sector_id: str) -> dict:
        self.locks[sector_id] = "unlocked"

        log("access_locks", f"Сектор {sector_id} разблокирован")

        return {
            "sector_id": sector_id,
            "status": self.locks[sector_id],
        }

    def lock(self, sector_id: str) -> dict:
        self.locks[sector_id] = "locked"

        log("access_locks", f"Сектор {sector_id} заблокирован")

        return {
            "sector_id": sector_id,
            "status": self.locks[sector_id],
        }

    def emergency_lock_all(self) -> dict:
        for sector_id in self.locks:
            self.locks[sector_id] = "locked"

        log("access_locks", "Все сектора аварийно заблокированы")

        return {
            "status": "all_locked",
            "locks": self.locks,
        }

    def get_status(self) -> dict:
        return {
            "locks": self.locks,
        }