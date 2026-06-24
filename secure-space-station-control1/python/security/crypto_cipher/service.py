import base64
import json


class CryptoCipher:
    def encrypt(self, message: dict) -> dict:
        raw = json.dumps(message, ensure_ascii=False)
        encrypted = base64.b64encode(raw.encode("utf-8")).decode("utf-8")

        return {
            "encrypted": True,
            "data": encrypted
        }