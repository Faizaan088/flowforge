"""add_auth_tokens_api_keys_and_ownership

Revision ID: c1a2b3d4e5f6
Revises: 94318d5e07b4
Create Date: 2026-10-23 19:52:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'c1a2b3d4e5f6'
down_revision: Union[str, Sequence[str], None] = '94318d5e07b4'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # 1. Add user_id ownership columns
    op.add_column('job_definitions', sa.Column('user_id', sa.Integer(), nullable=True))
    op.create_index(op.f('ix_job_definitions_user_id'), 'job_definitions', ['user_id'], unique=False)
    op.create_foreign_key('fk_job_definitions_user_id', 'job_definitions', 'users', ['user_id'], ['id'], ondelete='SET NULL')

    op.add_column('schedule_definitions', sa.Column('user_id', sa.Integer(), nullable=True))
    op.create_index(op.f('ix_schedule_definitions_user_id'), 'schedule_definitions', ['user_id'], unique=False)
    op.create_foreign_key('fk_schedule_definitions_user_id', 'schedule_definitions', 'users', ['user_id'], ['id'], ondelete='SET NULL')

    op.add_column('workflow_definitions', sa.Column('user_id', sa.Integer(), nullable=True))
    op.create_index(op.f('ix_workflow_definitions_user_id'), 'workflow_definitions', ['user_id'], unique=False)
    op.create_foreign_key('fk_workflow_definitions_user_id', 'workflow_definitions', 'users', ['user_id'], ['id'], ondelete='SET NULL')

    # 2. Add is_verified to users
    op.add_column('users', sa.Column('is_verified', sa.Boolean(), server_default=sa.text('false'), nullable=False))

    # 3. Create password_reset_tokens table
    op.create_table(
        'password_reset_tokens',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('token_hash', sa.String(length=255), nullable=False),
        sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('used_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_password_reset_tokens_id'), 'password_reset_tokens', ['id'], unique=False)
    op.create_index(op.f('ix_password_reset_tokens_token_hash'), 'password_reset_tokens', ['token_hash'], unique=True)
    op.create_index(op.f('ix_password_reset_tokens_user_id'), 'password_reset_tokens', ['user_id'], unique=False)

    # 4. Create email_verification_tokens table
    op.create_table(
        'email_verification_tokens',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('token_hash', sa.String(length=255), nullable=False),
        sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('used_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_email_verification_tokens_id'), 'email_verification_tokens', ['id'], unique=False)
    op.create_index(op.f('ix_email_verification_tokens_token_hash'), 'email_verification_tokens', ['token_hash'], unique=True)
    op.create_index(op.f('ix_email_verification_tokens_user_id'), 'email_verification_tokens', ['user_id'], unique=False)

    # 5. Create api_keys table
    op.create_table(
        'api_keys',
        sa.Column('id', sa.Integer(), nullable=False),
        sa.Column('user_id', sa.Integer(), nullable=False),
        sa.Column('name', sa.String(length=100), nullable=False),
        sa.Column('key_prefix', sa.String(length=16), nullable=False),
        sa.Column('key_hash', sa.String(length=255), nullable=False),
        sa.Column('is_active', sa.Boolean(), server_default=sa.text('true'), nullable=False),
        sa.Column('expires_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.text('now()'), nullable=False),
        sa.Column('last_used_at', sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(['user_id'], ['users.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index(op.f('ix_api_keys_id'), 'api_keys', ['id'], unique=False)
    op.create_index(op.f('ix_api_keys_key_prefix'), 'api_keys', ['key_prefix'], unique=False)
    op.create_index(op.f('ix_api_keys_user_id'), 'api_keys', ['user_id'], unique=False)


def downgrade() -> None:
    op.drop_index(op.f('ix_api_keys_user_id'), table_name='api_keys')
    op.drop_index(op.f('ix_api_keys_key_prefix'), table_name='api_keys')
    op.drop_index(op.f('ix_api_keys_id'), table_name='api_keys')
    op.drop_table('api_keys')

    op.drop_index(op.f('ix_email_verification_tokens_user_id'), table_name='email_verification_tokens')
    op.drop_index(op.f('ix_email_verification_tokens_token_hash'), table_name='email_verification_tokens')
    op.drop_index(op.f('ix_email_verification_tokens_id'), table_name='email_verification_tokens')
    op.drop_table('email_verification_tokens')

    op.drop_index(op.f('ix_password_reset_tokens_user_id'), table_name='password_reset_tokens')
    op.drop_index(op.f('ix_password_reset_tokens_token_hash'), table_name='password_reset_tokens')
    op.drop_index(op.f('ix_password_reset_tokens_id'), table_name='password_reset_tokens')
    op.drop_table('password_reset_tokens')

    op.drop_column('users', 'is_verified')

    op.drop_constraint('fk_workflow_definitions_user_id', 'workflow_definitions', type_='foreignkey')
    op.drop_index(op.f('ix_workflow_definitions_user_id'), table_name='workflow_definitions')
    op.drop_column('workflow_definitions', 'user_id')

    op.drop_constraint('fk_schedule_definitions_user_id', 'schedule_definitions', type_='foreignkey')
    op.drop_index(op.f('ix_schedule_definitions_user_id'), table_name='schedule_definitions')
    op.drop_column('schedule_definitions', 'user_id')

    op.drop_constraint('fk_job_definitions_user_id', 'job_definitions', type_='foreignkey')
    op.drop_index(op.f('ix_job_definitions_user_id'), table_name='job_definitions')
    op.drop_column('job_definitions', 'user_id')
