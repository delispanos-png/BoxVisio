"""Per-tenant Co-Pilot configuration helpers.

The tenant supplies its OWN LLM API key (billed to the tenant's own account, never
through us) from its admin panel. The key is stored encrypted with the same Fernet
envelope used for connection secrets and is only ever decrypted in-memory at call time.
Never log or return the plaintext key to a template.
"""
from __future__ import annotations

from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.control import CopilotConfig
from app.services.connection_secrets import decrypt_json_secret, encrypt_json_secret

DEFAULT_MODEL = 'claude-opus-5'
ALLOWED_MODELS = ('claude-opus-5', 'claude-sonnet-5', 'claude-haiku-4-5')
ALLOWED_DATA_SCOPES = ('aggregates', 'row_level')


async def get_copilot_config(db: AsyncSession, tenant_id: int) -> CopilotConfig | None:
    return (
        await db.execute(select(CopilotConfig).where(CopilotConfig.tenant_id == int(tenant_id)))
    ).scalar_one_or_none()


def copilot_api_key(config: CopilotConfig | None) -> str | None:
    """Decrypt and return the tenant's API key (in-memory only), or None."""
    if config is None or not config.api_key_enc:
        return None
    try:
        payload = decrypt_json_secret(config.api_key_enc)
    except Exception:
        return None
    key = str(payload.get('api_key') or '').strip()
    return key or None


def copilot_is_ready(config: CopilotConfig | None) -> bool:
    """True when the tenant has enabled the Co-Pilot AND provided a key."""
    return bool(config is not None and config.enabled and copilot_api_key(config))


def copilot_settings_view(config: CopilotConfig | None) -> dict[str, Any]:
    """Template-safe view — never exposes the key, only whether one is stored."""
    if config is None:
        return {
            'enabled': False,
            'provider': 'anthropic',
            'model': DEFAULT_MODEL,
            'has_key': False,
            'max_monthly_tokens': 0,
            'data_scope': 'row_level',
            'ready': False,
        }
    return {
        'enabled': bool(config.enabled),
        'provider': config.provider or 'anthropic',
        'model': config.model or DEFAULT_MODEL,
        'has_key': bool(config.api_key_enc),
        'max_monthly_tokens': int(config.max_monthly_tokens or 0),
        'data_scope': config.data_scope or 'row_level',
        'ready': copilot_is_ready(config),
    }


async def upsert_copilot_config(
    db: AsyncSession,
    tenant_id: int,
    *,
    enabled: bool,
    model: str,
    api_key: str | None,
    clear_key: bool = False,
    max_monthly_tokens: int = 0,
    data_scope: str = 'row_level',
) -> CopilotConfig:
    """Create or update the tenant's Co-Pilot config.

    - api_key: a new plaintext key to store (encrypted); None/'' leaves the existing key
      untouched so the admin doesn't have to re-enter it on every save.
    - clear_key: wipe the stored key entirely.
    """
    config = await get_copilot_config(db, tenant_id)
    if config is None:
        config = CopilotConfig(tenant_id=int(tenant_id))
        db.add(config)

    config.enabled = bool(enabled)
    config.provider = 'anthropic'
    config.model = model if model in ALLOWED_MODELS else DEFAULT_MODEL
    config.data_scope = data_scope if data_scope in ALLOWED_DATA_SCOPES else 'row_level'
    config.max_monthly_tokens = int(max_monthly_tokens or 0) or None

    if clear_key:
        config.api_key_enc = None
    elif api_key and str(api_key).strip():
        config.api_key_enc = encrypt_json_secret({'api_key': str(api_key).strip()})
    # else: keep the existing encrypted key

    await db.flush()
    return config
