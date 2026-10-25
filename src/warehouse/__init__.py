"""Warehouse module for PostgreSQL and SQLite Kimball Star Schema."""
from .connection import get_engine
from .loader import init_warehouse_schema, populate_dimensions, load_facts
