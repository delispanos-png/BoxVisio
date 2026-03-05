import hashlib
import hmac


def verify_hmac_signature(secret: str, body: bytes, incoming_signature: str) -> bool:
    expected = hmac.new(secret.encode('utf-8'), body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, incoming_signature)
