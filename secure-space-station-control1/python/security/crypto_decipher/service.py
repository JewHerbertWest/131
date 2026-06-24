import base64
import json


class CryptoDecipher:
    def decrypt(self, encrypted_message: dict) -> dict:
        raw = base64.b64decode(encrypted_message["data"].encode("utf-8")).decode("utf-8")
        return json.loads(raw)