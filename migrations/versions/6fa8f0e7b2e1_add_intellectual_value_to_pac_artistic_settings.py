"""add intellectual value to pac artistic settings

Revision ID: 6fa8f0e7b2e1
Revises: 009fc5fd2522
Create Date: 2026-04-06 22:40:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "6fa8f0e7b2e1"
down_revision = "009fc5fd2522"
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())

    if "pac_artistic_settings" in existing_tables:
        column_names = {column["name"] for column in inspector.get_columns("pac_artistic_settings")}
        if "intellectual_value" not in column_names:
            op.add_column(
                "pac_artistic_settings",
                sa.Column("intellectual_value", sa.Float(), nullable=False, server_default="0"),
            )
            op.alter_column("pac_artistic_settings", "intellectual_value", server_default=None)


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())

    if "pac_artistic_settings" in existing_tables:
        column_names = {column["name"] for column in inspector.get_columns("pac_artistic_settings")}
        if "intellectual_value" in column_names:
            op.drop_column("pac_artistic_settings", "intellectual_value")
