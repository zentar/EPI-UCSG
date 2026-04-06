"""add equivalence and exclusion tables

Revision ID: 009fc5fd2522
Revises: 
Create Date: 2026-04-06 21:25:09.316004

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '009fc5fd2522'
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())

    if "publication_type_labels" not in existing_tables:
        op.create_table(
            "publication_type_labels",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("type_code", sa.String(length=10), nullable=False),
            sa.Column("label", sa.String(length=120), nullable=False),
            sa.Column("description", sa.Text(), nullable=True),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.PrimaryKeyConstraint("id"),
        )
        op.create_index(op.f("ix_publication_type_labels_type_code"), "publication_type_labels", ["type_code"], unique=True)

    if "base_labels" not in existing_tables:
        op.create_table(
            "base_labels",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("base_code", sa.String(length=32), nullable=False),
            sa.Column("label", sa.String(length=120), nullable=False),
            sa.Column("description", sa.Text(), nullable=True),
            sa.Column("updated_at", sa.DateTime(), nullable=False),
            sa.PrimaryKeyConstraint("id"),
        )
        op.create_index(op.f("ix_base_labels_base_code"), "base_labels", ["base_code"], unique=True)

    if "publication_type_excluded" not in existing_tables:
        op.create_table(
            "publication_type_excluded",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("type_code", sa.String(length=10), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.PrimaryKeyConstraint("id"),
        )
        op.create_index(op.f("ix_publication_type_excluded_type_code"), "publication_type_excluded", ["type_code"], unique=True)

    if "base_excluded" not in existing_tables:
        op.create_table(
            "base_excluded",
            sa.Column("id", sa.Integer(), nullable=False),
            sa.Column("base_code", sa.String(length=32), nullable=False),
            sa.Column("created_at", sa.DateTime(), nullable=False),
            sa.PrimaryKeyConstraint("id"),
        )
        op.create_index(op.f("ix_base_excluded_base_code"), "base_excluded", ["base_code"], unique=True)


def downgrade():
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    existing_tables = set(inspector.get_table_names())

    if "base_excluded" in existing_tables:
        op.drop_index(op.f("ix_base_excluded_base_code"), table_name="base_excluded")
        op.drop_table("base_excluded")

    if "publication_type_excluded" in existing_tables:
        op.drop_index(op.f("ix_publication_type_excluded_type_code"), table_name="publication_type_excluded")
        op.drop_table("publication_type_excluded")

    if "base_labels" in existing_tables:
        op.drop_index(op.f("ix_base_labels_base_code"), table_name="base_labels")
        op.drop_table("base_labels")

    if "publication_type_labels" in existing_tables:
        op.drop_index(op.f("ix_publication_type_labels_type_code"), table_name="publication_type_labels")
        op.drop_table("publication_type_labels")
