from __future__ import annotations

import smtplib
from email.message import EmailMessage
from pathlib import Path

from app.core.config import settings


_ENV_PATH = Path(__file__).resolve().parents[3] / '.env'


def _parse_env_value(raw_value: str) -> str:
    value = str(raw_value or '').strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        value = value[1:-1]
        if raw_value.strip().startswith('"'):
            value = value.replace('\\n', '\n').replace('\\"', '"').replace('\\\\', '\\')
    return value


def _refresh_smtp_settings_from_env() -> None:
    try:
        lines = _ENV_PATH.read_text(encoding='utf-8').splitlines()
    except FileNotFoundError:
        return
    except Exception:
        return
    values: dict[str, str] = {}
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith('#') or '=' not in line:
            continue
        key, raw_value = line.split('=', 1)
        values[key.strip()] = _parse_env_value(raw_value)
    if not values:
        return
    settings.smtp_host = values.get('SMTP_HOST', settings.smtp_host)
    settings.smtp_port = int(values.get('SMTP_PORT') or settings.smtp_port or 587)
    settings.smtp_username = values.get('SMTP_USERNAME', settings.smtp_username)
    settings.smtp_password = values.get('SMTP_PASSWORD', settings.smtp_password)
    settings.smtp_from_email = values.get('SMTP_FROM_EMAIL', settings.smtp_from_email)
    settings.smtp_from_name = values.get('SMTP_FROM_NAME', settings.smtp_from_name or 'CloudOn BI')
    settings.smtp_use_tls = str(values.get('SMTP_USE_TLS', settings.smtp_use_tls)).strip().lower() in {'1', 'true', 'yes', 'y', 'on'}
    settings.app_public_base_url = values.get('APP_PUBLIC_BASE_URL', settings.app_public_base_url)


def email_configured() -> bool:
    _refresh_smtp_settings_from_env()
    return bool(settings.smtp_host and (settings.smtp_from_email or settings.smtp_username))


def send_email(*, to_email: str, subject: str, text_body: str, html_body: str | None = None) -> dict[str, object]:
    if not email_configured():
        return {'status': 'skipped', 'reason': 'smtp_not_configured'}

    sender = settings.smtp_from_email or settings.smtp_username
    msg = EmailMessage()
    msg['Subject'] = subject
    msg['From'] = f'{settings.smtp_from_name} <{sender}>'
    msg['To'] = to_email
    msg.set_content(text_body)
    if html_body:
        msg.add_alternative(html_body, subtype='html')

    with smtplib.SMTP(settings.smtp_host, int(settings.smtp_port), timeout=20) as smtp:
        if settings.smtp_use_tls:
            smtp.starttls()
        if settings.smtp_username:
            smtp.login(settings.smtp_username, settings.smtp_password)
        refused = smtp.send_message(msg)
    if refused:
        return {'status': 'error', 'reason': 'recipient_refused', 'refused': {str(k): str(v) for k, v in refused.items()}}
    return {'status': 'sent'}


def build_tenant_invite_url(*, slug: str, token: str) -> str:
    base = str(settings.app_public_base_url or '').strip().rstrip('/')
    if not base:
        host = settings.tenant_portal_host
        if settings.tenant_domain_root:
            host = f'{slug}.{settings.tenant_domain_root.strip(".")}'
        base = f'https://{host}'
    return f'{base}/invite?token={token}'


def build_tenant_login_url(*, slug: str) -> str:
    base = str(settings.app_public_base_url or '').strip().rstrip('/')
    if not base:
        host = settings.tenant_portal_host
        if settings.tenant_domain_root:
            host = f'{slug}.{settings.tenant_domain_root.strip(".")}'
        base = f'https://{host}'
    return f'{base}/login'


def send_tenant_welcome_email(
    *,
    tenant_name: str,
    tenant_slug: str,
    admin_email: str,
    invite_token: str,
    temporary_password: str = '',
) -> dict[str, object]:
    invite_url = build_tenant_invite_url(slug=tenant_slug, token=invite_token)
    login_url = build_tenant_login_url(slug=tenant_slug)
    password_line = f'Προσωρινός κωδικός: {temporary_password}\n' if temporary_password else ''
    password_html = f'<br><strong>Προσωρινός κωδικός:</strong> {temporary_password}' if temporary_password else ''
    subject = f'Πρόσβαση στο CloudOn BI - {tenant_name}'
    text_body = (
        f'Καλώς ήρθατε στο CloudOn BI.\n\n'
        f'Πελάτης: {tenant_name}\n'
        f'Tenant: {tenant_slug}\n'
        f'BI URL: {login_url}\n'
        f'Username: {admin_email}\n'
        f'{password_line}'
        f'Ορισμός / αλλαγή κωδικού: {invite_url}\n\n'
        f'Ο σύνδεσμος αλλαγής κωδικού λήγει σε 48 ώρες.'
    )
    html_body = (
        '<p>Καλώς ήρθατε στο <strong>CloudOn BI</strong>.</p>'
        f'<p><strong>Πελάτης:</strong> {tenant_name}<br>'
        f'<strong>Tenant:</strong> {tenant_slug}<br>'
        f'<strong>BI URL:</strong> <a href="{login_url}">{login_url}</a><br>'
        f'<strong>Username:</strong> {admin_email}'
        f'{password_html}</p>'
        f'<p><a href="{invite_url}">Ορισμός / αλλαγή κωδικού</a></p>'
        '<p>Ο σύνδεσμος αλλαγής κωδικού λήγει σε 48 ώρες.</p>'
    )
    result = send_email(to_email=admin_email, subject=subject, text_body=text_body, html_body=html_body)
    return {**result, 'invite_url': invite_url, 'login_url': login_url}
