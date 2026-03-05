import asyncio
import secrets
import string
import subprocess
import os
import sys
from datetime import datetime, timedelta

import psycopg
from psycopg import sql
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.security import get_password_hash
from app.db.tenant_manager import tenant_db_name
from app.models.control import (
    AuditLog,
    Plan,
    PlanName,
    RoleName,
    Subscription,
    SubscriptionLimit,
    SubscriptionStatus,
    Tenant,
    TenantApiKey,
    TenantStatus,
    User,
)
from app.services.subscriptions import infer_default_features_for_plan

TENANT_MIGRATION_HEAD = '20260228_0011_tenant'


def _rand_secret(size: int = 40) -> str:
    alphabet = string.ascii_letters + string.digits
    return ''.join(secrets.choice(alphabet) for _ in range(size))


def _admin_dsn() -> str:
    return (
        f"host={settings.tenant_db_host} port={settings.tenant_db_port} "
        f"dbname=postgres user={settings.tenant_db_superuser} password={settings.tenant_db_superpass}"
    )


def _create_db_and_role(db_name: str, db_user: str, db_password: str) -> None:
    with psycopg.connect(_admin_dsn(), autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (db_user,))
            if cur.fetchone() is None:
                cur.execute(
                    sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(
                        sql.Identifier(db_user),
                        sql.Literal(db_password),
                    )
                )
            else:
                cur.execute(
                    sql.SQL("ALTER ROLE {} WITH PASSWORD {}").format(
                        sql.Identifier(db_user),
                        sql.Literal(db_password),
                    )
                )

            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
            if cur.fetchone() is None:
                cur.execute(
                    sql.SQL("CREATE DATABASE {} OWNER {}").format(sql.Identifier(db_name), sql.Identifier(db_user))
                )


def _drop_db_and_role(db_name: str, db_user: str) -> None:
    with psycopg.connect(_admin_dsn(), autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s AND pid <> pg_backend_pid()
                """,
                (db_name,),
            )
            cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name)))
            cur.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(db_user)))


def _run_tenant_migrations(db_name: str, db_user: str, db_password: str) -> None:
    tenant_url = settings.tenant_database_url_template_sync.format(
        user=db_user,
        password=db_password,
        db_name=db_name,
    )
    env = {
        **dict(os.environ),
        'MIGRATION_TARGET': 'tenant',
        'TENANT_MIGRATION_URL': tenant_url,
    }
    subprocess.run(
        [sys.executable, '-m', 'alembic', '-c', '/app/alembic.ini', 'upgrade', TENANT_MIGRATION_HEAD],
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )


async def run_tenant_provisioning_wizard(
    db: AsyncSession,
    *,
    name: str,
    slug: str,
    admin_email: str,
    plan: PlanName,
    source: str,
    subscription_status: SubscriptionStatus,
    trial_days: int | None,
) -> dict:
    steps: list[dict] = []
    db_name = tenant_db_name(slug)
    db_user = f"u_{slug[:30]}"
    db_password = _rand_secret(24)
    api_key_secret = _rand_secret(40)
    api_key_id = secrets.token_urlsafe(16)
    invite_token = secrets.token_urlsafe(24)
    trial_days_eff = settings.default_trial_days if trial_days is None else trial_days
    created_db = False

    def mark(step_no: int, title: str, status: str, message: str = ''):
        steps.append({'step': step_no, 'title': title, 'status': status, 'message': message})

    try:
        # STEP 1: Tenant details
        existing = (await db.execute(select(Tenant).where(Tenant.slug == slug))).scalar_one_or_none()
        if existing:
            raise ValueError(f'tenant slug already exists: {slug}')
        mark(1, 'Tenant details', 'ok')

        # STEP 2: Plan selection
        plan_row = (await db.execute(select(Plan).where(Plan.code == plan.value))).scalar_one_or_none()
        if not plan_row:
            raise ValueError(f'plan not found: {plan.value}')
        mark(2, 'Plan selection', 'ok', plan.value)

        # STEP 3: Data source selection
        if source not in {'pharmacyone', 'external', 'files'}:
            raise ValueError(f'invalid source: {source}')
        mark(3, 'Data source selection', 'ok', source)

        # STEP 4: Create tenant DB
        await asyncio.to_thread(_create_db_and_role, db_name, db_user, db_password)
        created_db = True
        mark(4, 'Create tenant DB', 'ok', db_name)

        # STEP 5: Run tenant migrations
        await asyncio.to_thread(_run_tenant_migrations, db_name, db_user, db_password)
        mark(5, 'Run tenant migrations', 'ok')

        # STEP 6: Create tenant admin user + subscription
        features = infer_default_features_for_plan(plan)
        trial_ends = datetime.utcnow() + timedelta(days=trial_days_eff) if subscription_status == SubscriptionStatus.trial else None
        period_end = datetime.utcnow() + timedelta(days=30) if subscription_status in {SubscriptionStatus.active, SubscriptionStatus.past_due} else None

        tenant = Tenant(
            name=name,
            slug=slug,
            plan=plan,
            status=TenantStatus.active,
            source=source,
            subscription_status=subscription_status,
            trial_ends_at=trial_ends,
            current_period_end=period_end,
            db_name=db_name,
            db_user=db_user,
            db_password=db_password,
            feature_flags=features,
        )
        db.add(tenant)
        await db.flush()

        user = User(
            tenant_id=tenant.id,
            email=admin_email,
            password_hash=get_password_hash(_rand_secret(32)),
            role=RoleName.tenant_admin,
            reset_token=invite_token,
            reset_token_expires_at=datetime.utcnow() + timedelta(days=2),
            is_active=True,
        )
        db.add(user)

        sub = Subscription(
            tenant_id=tenant.id,
            plan=plan,
            status=subscription_status,
            trial_starts_at=datetime.utcnow() if subscription_status == SubscriptionStatus.trial else None,
            trial_ends_at=trial_ends,
            current_period_start=datetime.utcnow() if subscription_status in {SubscriptionStatus.active, SubscriptionStatus.past_due} else None,
            current_period_end=period_end,
            feature_flags=features,
        )
        db.add(sub)
        await db.flush()
        db.add(SubscriptionLimit(subscription_id=sub.id, limit_key='max_users', limit_value=plan_row.max_users, used_value=0))
        db.add(SubscriptionLimit(subscription_id=sub.id, limit_key='max_branches', limit_value=plan_row.max_branches, used_value=0))
        mark(6, 'Create tenant admin user', 'ok', admin_email)

        # STEP 7: Generate API keys
        api_key = TenantApiKey(
            tenant_id=tenant.id,
            key_id=api_key_id,
            key_secret=api_key_secret,
            is_active=True,
        )
        db.add(api_key)
        db.add(
            AuditLog(
                tenant_id=tenant.id,
                action='tenant_provisioned_wizard',
                entity_type='tenant',
                entity_id=str(tenant.id),
                payload={'steps': 7, 'source': source, 'plan': plan.value},
            )
        )
        mark(7, 'Generate API keys', 'ok')
        await db.commit()

        return {
            'status': 'ok',
            'tenant_id': tenant.id,
            'slug': slug,
            'plan': plan.value,
            'subscription_status': subscription_status.value,
            'invite_token': invite_token,
            'api_key_id': api_key_id,
            'api_key_secret': api_key_secret,
            'steps': steps,
        }
    except Exception as exc:
        await db.rollback()
        if created_db:
            try:
                await asyncio.to_thread(_drop_db_and_role, db_name, db_user)
            except Exception:
                pass
        mark(0, 'Rollback', 'ok', 'rolled back control DB + tenant DB resources')
        return {
            'status': 'error',
            'error': str(exc),
            'steps': steps,
        }
