from datetime import datetime
from python.core.config.settings import JOURNAL_FILE


def log(component: str, message: str) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] [{component}] {message}"

    print(line)

    JOURNAL_FILE.parent.mkdir(parents=True, exist_ok=True)

    with open(JOURNAL_FILE, "a", encoding="utf-8") as file:
        file.write(line + "\n")