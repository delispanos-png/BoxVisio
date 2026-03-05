from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Any

from cryptography.fernet import Fernet

from app.core.config import settings


@dataclass
class SqlServerSecret:
    host: str
    port: int
    database: str
    username: str
    password: str
    options: dict[str, str]


def _master_fernet() -> Fernet:
    return Fernet(settings.bi_secret_key.encode('utf-8'))


def _normalize_options(raw: dict[str, Any] | None) -> dict[str, str]:
    if not raw:
        return {}
    out: dict[str, str] = {}
    for key, value in raw.items():
        if value is None:
            continue
        out[str(key)] = str(value)
    return out


def encrypt_sqlserver_secret(*, host: str, port: int, database: str, username: str, password: str, options: dict[str, Any] | None = None) -> str:
    payload = {
        'host': host.strip(),
        'port': int(port),
        'database': database.strip(),
        'username': username.strip(),
        'password': password,
        'options': _normalize_options(options),
    }
    plaintext = json.dumps(payload, ensure_ascii=True, separators=(',', ':')).encode('utf-8')

    dek = Fernet.generate_key()
    ciphertext = Fernet(dek).encrypt(plaintext).decode('utf-8')
    wrapped_dek = _master_fernet().encrypt(dek).decode('utf-8')
    envelope = {
        'v': 1,
        'alg': 'fernet-envelope-v1',
        'wrapped_dek': wrapped_dek,
        'ciphertext': ciphertext,
    }
    return base64.urlsafe_b64encode(json.dumps(envelope, separators=(',', ':')).encode('utf-8')).decode('utf-8')


def decrypt_sqlserver_secret(enc_payload: str) -> SqlServerSecret:
    raw = base64.urlsafe_b64decode(enc_payload.encode('utf-8'))
    envelope = json.loads(raw.decode('utf-8'))
    if envelope.get('v') != 1:
        raise ValueError('Unsupported encrypted payload version')

    wrapped_dek = str(envelope['wrapped_dek'])
    ciphertext = str(envelope['ciphertext'])
    dek = _master_fernet().decrypt(wrapped_dek.encode('utf-8'))
    plaintext = Fernet(dek).decrypt(ciphertext.encode('utf-8'))
    payload = json.loads(plaintext.decode('utf-8'))

    return SqlServerSecret(
        host=str(payload['host']),
        port=int(payload.get('port') or settings.sqlserver_default_port),
        database=str(payload['database']),
        username=str(payload['username']),
        password=str(payload['password']),
        options=_normalize_options(payload.get('options')),
    )


def build_odbc_connection_string(secret: SqlServerSecret) -> str:
    parts = [
        'DRIVER={ODBC_DRIVER}',
        f'SERVER={secret.host},{secret.port}',
        f'DATABASE={secret.database}',
        f'UID={secret.username}',
        f'PWD={secret.password}',
    ]
    for key, value in secret.options.items():
        parts.append(f'{key}={value}')
    return ';'.join(parts)
