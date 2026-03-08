"""control configurable rules model

Revision ID: 20260307_0003_control
Revises: 20260307_0002_control
Create Date: 2026-03-07
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql

revision: str = '20260307_0003_control'
down_revision: Union[str, Sequence[str], None] = '20260307_0002_control'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(bind, table_name: str) -> bool:
    return inspect(bind).has_table(table_name)


def _index_names(bind, table_name: str) -> set[str]:
    if not _table_exists(bind, table_name):
        return set()
    return {idx['name'] for idx in inspect(bind).get_indexes(table_name)}


def upgrade() -> None:
    bind = op.get_bind()

    rule_domain = postgresql.ENUM(
        'document_type_rules',
        'source_mapping',
        'kpi_participation_rules',
        'intelligence_threshold_rules',
        name='ruledomain',
        create_type=False,
    )
    stream_enum = postgresql.ENUM(
        'sales_documents',
        'purchase_documents',
        'inventory_documents',
        'cash_transactions',
        'supplier_balances',
        'customer_balances',
        name='operationalstream',
        create_type=False,
    )
    override_mode = postgresql.ENUM('merge', 'replace', 'disable', name='overridemode', create_type=False)

    rule_domain.create(bind, checkfirst=True)
    stream_enum.create(bind, checkfirst=True)
    override_mode.create(bind, checkfirst=True)

    if not _table_exists(bind, 'global_rule_sets'):
        op.create_table(
            'global_rule_sets',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('code', sa.String(length=128), nullable=False),
            sa.Column('name', sa.String(length=255), nullable=False),
            sa.Column('description', sa.Text(), nullable=True),
            sa.Column('is_active', sa.Boolean(), nullable=False, server_default=sa.text('true')),
            sa.Column('priority', sa.Integer(), nullable=False, server_default='100'),
            sa.Column('created_at', sa.DateTime(), nullable=False),
            sa.Column('updated_at', sa.DateTime(), nullable=False),
            sa.UniqueConstraint('code', name='uq_global_rule_sets_code'),
        )

    if 'ix_global_rule_sets_active_priority' not in _index_names(bind, 'global_rule_sets'):
        op.create_index(
            'ix_global_rule_sets_active_priority',
            'global_rule_sets',
            ['is_active', 'priority'],
        )

    if not _table_exists(bind, 'global_rule_entries'):
        op.create_table(
            'global_rule_entries',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('ruleset_id', sa.Integer(), sa.ForeignKey('global_rule_sets.id'), nullable=False),
            sa.Column('domain', rule_domain, nullable=False),
            sa.Column('stream', stream_enum, nullable=False),
            sa.Column('rule_key', sa.String(length=128), nullable=False),
            sa.Column('payload_json', sa.JSON(), nullable=False, server_default=sa.text("'{}'::json")),
            sa.Column('is_active', sa.Boolean(), nullable=False, server_default=sa.text('true')),
            sa.Column('created_at', sa.DateTime(), nullable=False),
            sa.Column('updated_at', sa.DateTime(), nullable=False),
            sa.UniqueConstraint('ruleset_id', 'domain', 'stream', 'rule_key', name='uq_global_rule_entries_scope'),
        )

    idx_global = _index_names(bind, 'global_rule_entries')
    if 'ix_global_rule_entries_domain_stream' not in idx_global:
        op.create_index('ix_global_rule_entries_domain_stream', 'global_rule_entries', ['domain', 'stream'])
    if 'ix_global_rule_entries_ruleset_id' not in idx_global:
        op.create_index('ix_global_rule_entries_ruleset_id', 'global_rule_entries', ['ruleset_id'])

    if not _table_exists(bind, 'tenant_rule_overrides'):
        op.create_table(
            'tenant_rule_overrides',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('tenant_id', sa.Integer(), sa.ForeignKey('tenants.id'), nullable=False),
            sa.Column('domain', rule_domain, nullable=False),
            sa.Column('stream', stream_enum, nullable=False),
            sa.Column('rule_key', sa.String(length=128), nullable=False),
            sa.Column('override_mode', override_mode, nullable=False, server_default='merge'),
            sa.Column('payload_json', sa.JSON(), nullable=False, server_default=sa.text("'{}'::json")),
            sa.Column('is_active', sa.Boolean(), nullable=False, server_default=sa.text('true')),
            sa.Column('created_at', sa.DateTime(), nullable=False),
            sa.Column('updated_at', sa.DateTime(), nullable=False),
            sa.UniqueConstraint('tenant_id', 'domain', 'stream', 'rule_key', name='uq_tenant_rule_override_scope'),
        )

    idx_tenant = _index_names(bind, 'tenant_rule_overrides')
    if 'ix_tenant_rule_overrides_tenant_id' not in idx_tenant:
        op.create_index('ix_tenant_rule_overrides_tenant_id', 'tenant_rule_overrides', ['tenant_id'])
    if 'ix_tenant_rule_overrides_tenant_domain_stream' not in idx_tenant:
        op.create_index(
            'ix_tenant_rule_overrides_tenant_domain_stream',
            'tenant_rule_overrides',
            ['tenant_id', 'domain', 'stream'],
        )

    bind.execute(
        sa.text(
            """
            INSERT INTO global_rule_sets (code, name, description, is_active, priority, created_at, updated_at)
            SELECT CAST(:code AS varchar(128)), CAST(:name AS varchar(255)), CAST(:description AS text), true, 100, now(), now()
            WHERE NOT EXISTS (
                SELECT 1 FROM global_rule_sets WHERE code = CAST(:code AS varchar(128))
            )
            """
        ),
        {
            'code': 'softone_default_v1',
            'name': 'SoftOne Default Rules v1',
            'description': 'Default operational stream and KPI/business rule behavior for SoftOne tenants.',
        },
    )


def downgrade() -> None:
    bind = op.get_bind()

    if _table_exists(bind, 'tenant_rule_overrides'):
        idx = _index_names(bind, 'tenant_rule_overrides')
        if 'ix_tenant_rule_overrides_tenant_domain_stream' in idx:
            op.drop_index('ix_tenant_rule_overrides_tenant_domain_stream', table_name='tenant_rule_overrides')
        if 'ix_tenant_rule_overrides_tenant_id' in idx:
            op.drop_index('ix_tenant_rule_overrides_tenant_id', table_name='tenant_rule_overrides')
        op.drop_table('tenant_rule_overrides')

    if _table_exists(bind, 'global_rule_entries'):
        idx = _index_names(bind, 'global_rule_entries')
        if 'ix_global_rule_entries_ruleset_id' in idx:
            op.drop_index('ix_global_rule_entries_ruleset_id', table_name='global_rule_entries')
        if 'ix_global_rule_entries_domain_stream' in idx:
            op.drop_index('ix_global_rule_entries_domain_stream', table_name='global_rule_entries')
        op.drop_table('global_rule_entries')

    if _table_exists(bind, 'global_rule_sets'):
        idx = _index_names(bind, 'global_rule_sets')
        if 'ix_global_rule_sets_active_priority' in idx:
            op.drop_index('ix_global_rule_sets_active_priority', table_name='global_rule_sets')
        op.drop_table('global_rule_sets')

    sa.Enum(name='overridemode').drop(bind, checkfirst=True)
    sa.Enum(name='operationalstream').drop(bind, checkfirst=True)
    sa.Enum(name='ruledomain').drop(bind, checkfirst=True)
