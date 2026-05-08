from __future__ import annotations

import smtplib
from email.message import EmailMessage

from app.core.config import settings


def email_configured() -> bool:
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
        smtp.send_message(msg)
    return {'status': 'sent'}


def build_tenant_invite_url(*, slug: str, token: str) -> str:
    base = str(settings.app_public_base_url or '').strip().rstrip('/')
    if not base:
        host = settings.tenant_portal_host
        if settings.tenant_domain_root:
            host = f'{slug}.{settings.tenant_domain_root.strip(".")}'
        base = f'https://{host}'
    return f'{base}/invite?token={token}'


def send_tenant_welcome_email(*, tenant_name: str, tenant_slug: str, admin_email: str, invite_token: str) -> dict[str, object]:
    invite_url = build_tenant_invite_url(slug=tenant_slug, token=invite_token)
    subject = f'Πρόσβαση στο CloudOn BI - {tenant_name}'
    text_body = (
        f'Καλώς ήρθατε στο CloudOn BI.\n\n'
        f'Πελάτης: {tenant_name}\n'
        f'Tenant: {tenant_slug}\n'
        f'Email εισόδου: {admin_email}\n'
        f'Ορισμός κωδικού / πρόσκληση: {invite_url}\n\n'
        f'Ο σύνδεσμος πρόσκλησης λήγει σε 48 ώρες.'
    )
    html_body = (
        '<p>Καλώς ήρθατε στο <strong>CloudOn BI</strong>.</p>'
        f'<p><strong>Πελάτης:</strong> {tenant_name}<br>'
        f'<strong>Tenant:</strong> {tenant_slug}<br>'
        f'<strong>Email εισόδου:</strong> {admin_email}</p>'
        f'<p><a href="{invite_url}">Ορισμός κωδικού / άνοιγμα πρόσβασης</a></p>'
        '<p>Ο σύνδεσμος πρόσκλησης λήγει σε 48 ώρες.</p>'
    )
    result = send_email(to_email=admin_email, subject=subject, text_body=text_body, html_body=html_body)
    return {**result, 'invite_url': invite_url}
