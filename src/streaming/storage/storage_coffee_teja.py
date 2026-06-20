"""src/streaming/storage/storage_coffee_teja.py.

DuckDB storage for the COFFEE SHOP scenario (Phase 5).

This is my copy of storage_case.py, adapted to store and query a different
set of fields. The case example grouped valid sales by region; this version
persists coffee order fields (store, drink, size, loyalty, derived money
fields) and runs coffee-specific summary queries:

  - revenue and order count per store
  - revenue per drink size
  - loyalty vs non-loyalty revenue and total loyalty discount given

Author: Teja (adapted from Denise Case's storage_case.py)
Date: 2026-06
"""

# === DECLARE IMPORTS ===

from pathlib import Path
from typing import Any, Final

from datafun_streaming.core.types import DataRecordDict
from datafun_streaming.storage.duckdb_sql import (
    build_clear_table_sql,
    build_create_table_sql,
    build_insert_sql,
)
from datafun_toolkit.logger import get_logger
import duckdb

from streaming.data_validation.data_contract_coffee_teja import (
    CONSUMED_FIELDNAMES,
    REJECTED_ORDER_FIELDNAMES,
)
from streaming.data_validation.data_validation_case import add_validation_errors

# === DECLARE EXPORTS ===

__all__ = [
    "clear_storage_tables",
    "create_storage_tables",
    "init_db",
    "log_storage_summary",
    "write_rejected_record",
    "write_valid_record",
]

# === CONFIGURE LOGGER ONCE PER PYTHON FILE (MODULE) ===

LOG = get_logger("C06-STORAGE-TEJA", level="DEBUG")

# === DECLARE GLOBAL CONSTANTS FOR TABLES ===

VALID_TABLE_NAME: Final[str] = "consumed_valid_orders"
REJECTED_TABLE_NAME: Final[str] = "consumed_rejected_orders"

# The valid table stores the full enriched order, including the Phase 4
# loyalty_discount derived field and the Kafka metadata fields.
CONSUMED_VALID_FIELDNAMES: Final[list[str]] = [*CONSUMED_FIELDNAMES]

CONSUMED_REJECTED_FIELDNAMES: Final[list[str]] = [
    *REJECTED_ORDER_FIELDNAMES,
    "_kafka_key",
    "_kafka_partition",
    "_kafka_offset",
]


# === DEFINE HELPER FUNCTIONS ===


def clean_valid_consumed_record(record: dict[str, Any]) -> dict[str, Any]:
    """Keep only the fields written to the valid consumed table."""
    return {field: record.get(field, "") for field in CONSUMED_VALID_FIELDNAMES}


def clean_rejected_consumed_record(record: dict[str, Any]) -> dict[str, Any]:
    """Keep only the fields written to the rejected consumed table."""
    return {field: record.get(field, "") for field in CONSUMED_REJECTED_FIELDNAMES}


def create_storage_tables(db_path: Path) -> None:
    """Create the consumed order tables if they do not exist.

    Arguments:
        db_path: Path to the DuckDB database file.
    """
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(
            build_create_table_sql(VALID_TABLE_NAME, CONSUMED_VALID_FIELDNAMES)
        )
        conn.execute(
            build_create_table_sql(REJECTED_TABLE_NAME, CONSUMED_REJECTED_FIELDNAMES)
        )


def clear_storage_tables(db_path: Path) -> None:
    """Clear prior consumed order rows for a fresh run.

    Arguments:
        db_path: Path to the DuckDB database file.
    """
    with duckdb.connect(str(db_path)) as conn:
        conn.execute(build_clear_table_sql(VALID_TABLE_NAME))
        conn.execute(build_clear_table_sql(REJECTED_TABLE_NAME))


def init_db(db_path: Path) -> None:
    """Initialize the DuckDB database: create tables and clear old rows.

    Arguments:
        db_path: Path to the DuckDB database file.
    """
    create_storage_tables(db_path)
    clear_storage_tables(db_path)


def write_valid_record(db_path: Path, record: DataRecordDict) -> None:
    """Write one valid consumed coffee order to DuckDB.

    Arguments:
        db_path: Path to the DuckDB database file.
        record: A valid consumed Kafka message record.
    """
    clean_record = clean_valid_consumed_record(record)
    insert_sql = build_insert_sql(VALID_TABLE_NAME, CONSUMED_VALID_FIELDNAMES)
    insert_values = [clean_record[field] for field in CONSUMED_VALID_FIELDNAMES]

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(insert_sql, insert_values)


def write_rejected_record(
    db_path: Path, record: DataRecordDict, errors: list[str]
) -> None:
    """Write one rejected consumed coffee order to DuckDB.

    Arguments:
        db_path: Path to the DuckDB database file.
        record: A rejected consumed Kafka message record.
        errors: Validation errors explaining why the record was rejected.
    """
    rejected_record = add_validation_errors(record=record, errors=errors)
    clean_record = clean_rejected_consumed_record(rejected_record)
    insert_sql = build_insert_sql(REJECTED_TABLE_NAME, CONSUMED_REJECTED_FIELDNAMES)
    insert_values = [clean_record[field] for field in CONSUMED_REJECTED_FIELDNAMES]

    with duckdb.connect(str(db_path)) as conn:
        conn.execute(insert_sql, insert_values)


def log_storage_summary(db_path: Path) -> None:
    """Log coffee-specific DuckDB query results after consuming messages.

    Money fields are stored as text, so each aggregate CASTs to DOUBLE.
    Table names are module constants (not user input), so the f-strings are
    safe from SQL injection (flagged with noqa: S608).

    Arguments:
        db_path: Path to the DuckDB database file.
    """
    sql_valid_count = f"SELECT COUNT(*) FROM {VALID_TABLE_NAME}"  # noqa: S608
    sql_rejected_count = f"SELECT COUNT(*) FROM {REJECTED_TABLE_NAME}"  # noqa: S608

    sql_by_store = f"""
        SELECT store_id,
               COUNT(*) AS order_count,
               ROUND(SUM(CAST(total AS DOUBLE)), 2) AS revenue
        FROM {VALID_TABLE_NAME}
        GROUP BY store_id
        ORDER BY revenue DESC
        """  # noqa: S608

    sql_by_size = f"""
        SELECT size,
               COUNT(*) AS order_count,
               ROUND(SUM(CAST(total AS DOUBLE)), 2) AS revenue
        FROM {VALID_TABLE_NAME}
        GROUP BY size
        ORDER BY revenue DESC
        """  # noqa: S608

    sql_loyalty = f"""
        SELECT is_loyalty,
               COUNT(*) AS order_count,
               ROUND(SUM(CAST(total AS DOUBLE)), 2) AS revenue,
               ROUND(SUM(CAST(loyalty_discount AS DOUBLE)), 2) AS discount_given
        FROM {VALID_TABLE_NAME}
        GROUP BY is_loyalty
        ORDER BY is_loyalty DESC
        """  # noqa: S608

    with duckdb.connect(str(db_path)) as conn:
        valid_result = conn.execute(sql_valid_count).fetchone()
        valid_count = valid_result[0] if valid_result else 0

        rejected_result = conn.execute(sql_rejected_count).fetchone()
        rejected_count = rejected_result[0] if rejected_result else 0

        by_store = conn.execute(sql_by_store).fetchall()
        by_size = conn.execute(sql_by_size).fetchall()
        by_loyalty = conn.execute(sql_loyalty).fetchall()

    LOG.info(f"DuckDB valid order(s): {valid_count}")
    LOG.info(f"DuckDB rejected order(s): {rejected_count}")

    LOG.info("Revenue by store:")
    for store_id, order_count, revenue in by_store:
        LOG.info(f"  {store_id}: {order_count} order(s), ${revenue:,.2f}")

    LOG.info("Revenue by drink size:")
    for size, order_count, revenue in by_size:
        LOG.info(f"  {size}: {order_count} order(s), ${revenue:,.2f}")

    LOG.info("Loyalty vs non-loyalty:")
    for is_loyalty, order_count, revenue, discount_given in by_loyalty:
        label = "loyalty" if str(is_loyalty).lower() == "true" else "non-loyalty"
        LOG.info(
            f"  {label}: {order_count} order(s), ${revenue:,.2f} revenue, "
            f"${discount_given:,.2f} discount given"
        )
