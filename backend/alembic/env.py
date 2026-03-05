from __future__ import with_statement

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from app.db.base import ControlBase, TenantBase
from app.models import control, tenant  # noqa: F401

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target = os.getenv('MIGRATION_TARGET', 'control')

if target == 'tenant':
    target_metadata = TenantBase.metadata
    db_url = os.getenv('TENANT_MIGRATION_URL')
else:
    target_metadata = ControlBase.metadata
    db_url = os.getenv('CONTROL_DATABASE_URL_SYNC')

if db_url:
    config.set_main_option('sqlalchemy.url', db_url)


def run_migrations_offline() -> None:
    url = config.get_main_option('sqlalchemy.url')
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={'paramstyle': 'named'},
        version_table=f'alembic_version_{target}',
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    configuration = config.get_section(config.config_ini_section)
    connectable = engine_from_config(
        configuration,
        prefix='sqlalchemy.',
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            version_table=f'alembic_version_{target}',
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
