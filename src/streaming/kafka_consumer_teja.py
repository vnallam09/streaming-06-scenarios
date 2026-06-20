"""src/streaming/kafka_consumer_teja.py.

Kafka consumer for the COFFEE SHOP scenario - full pipeline.

This is my copy of kafka_consumer_case.py (Phase 4 rename) extended to apply
the streaming pattern to a new dataset (Phase 5).

Phase 4 - technical modification:
  - Adds a NEW derived field, loyalty_discount, computed in
    derived_fields_teja.enrich_message().

Phase 5 - apply the skills to a new problem (coffee shop orders):
  - Different dataset: coffee orders + stores/drinks reference tables.
  - Different live chart: a cumulative-revenue-by-store BAR chart
    (live_visualizations_teja) instead of the case line chart.
  - Different stored/queried fields: coffee order fields summarized by
    store, drink size, and loyalty status (storage_teja).

Reads coffee orders from a Kafka topic and runs the full pipeline:
  - Validates each message against the coffee data contract
  - Computes derived fields (subtotal, loyalty_discount, tax_amount, total)
  - Updates a live bar chart of revenue by store
  - Stores each valid order in a DuckDB database

Author: Teja (adapted from Denise Case's kafka_consumer_case.py)
Date: 2026-06

Terminal command to run this file from the root project folder:

    uv run python -m streaming.kafka_consumer_teja
"""

# === DECLARE IMPORTS ===

import os
from pathlib import Path
from typing import Any, Final

from confluent_kafka.cimpl import OFFSET_BEGINNING, TopicPartition
from datafun_streaming.data_validation.validation_utils import validate_required_fields
from datafun_streaming.io.io_utils import append_csv_row, read_csv_as_lookup
from datafun_streaming.kafka.kafka_admin_utils import (
    create_admin_client,
    get_topic_message_count,
    topic_exists,
)
from datafun_streaming.kafka.kafka_connection_utils import verify_kafka_connection
from datafun_streaming.kafka.kafka_consumer_utils import (
    consume_kafka_message,
    create_consumer,
)
from datafun_streaming.kafka.kafka_settings import KafkaSettings
from datafun_streaming.stats.stats_utils import RunningStats
from datafun_toolkit.logger import get_logger, log_header, log_path
from dotenv import load_dotenv

from streaming.core.utils import log_env_vars
from streaming.data_engineering.derived_fields_teja import enrich_message
from streaming.data_validation.data_contract_teja import (
    CONSUMED_FIELDNAMES,
    ORDER_REQUIRED_FIELDS,
)
from streaming.storage.storage_teja import (
    init_db,
    log_storage_summary,
    write_rejected_record,
    write_valid_record,
)
from streaming.visualizations.live_visualizations_teja import (
    close_live_chart,
    init_live_chart,
    save_live_chart,
    update_live_chart,
)

# === CONFIGURE LOGGER ===

LOG = get_logger("C06-TEJA", level="DEBUG")

# === LOAD ENVIRONMENT VARIABLES ===

load_dotenv(override=True)
log_env_vars(LOG)

# === DECLARE GLOBAL CONSTANTS ===

COURSE_NAME: Final[str] = "Streaming Data"
TIMEOUT_SECONDS: Final[float] = float(os.getenv("CONSUMER_TIMEOUT_SECONDS", "10.0"))
MAX_MESSAGES: Final[int] = int(os.getenv("CONSUMER_MAX_MESSAGES", "1000"))

# === DECLARE CONSTANT PATHS ===

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
OUTPUT_DIR: Final[Path] = DATA_DIR / "output"

OUTPUT_CSV: Final[Path] = OUTPUT_DIR / "consumed_coffee_orders.csv"
OUTPUT_DB: Final[Path] = OUTPUT_DIR / "coffee.duckdb"
OUTPUT_CHART: Final[Path] = OUTPUT_DIR / "coffee_revenue_by_store_teja.png"

STORES_CSV: Final[Path] = DATA_DIR / "stores.csv"
DRINKS_CSV: Final[Path] = DATA_DIR / "drinks.csv"


# ==========================================================
# DEFINE SECTION A. ACQUIRE RESOURCES AND GET READY HELPERS
# ==========================================================


def log_paths() -> None:
    """Log run header and all paths."""
    log_header(LOG, "C06-TEJA")
    LOG.info("========================")
    LOG.info("START coffee consumer main()")
    LOG.info("========================")
    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "DATA_DIR", DATA_DIR)
    log_path(LOG, "OUTPUT_CSV", OUTPUT_CSV)
    log_path(LOG, "OUTPUT_DB", OUTPUT_DB)
    log_path(LOG, "OUTPUT_CHART", OUTPUT_CHART)
    log_path(LOG, "STORES_CSV", STORES_CSV)
    log_path(LOG, "DRINKS_CSV", DRINKS_CSV)


def load_settings() -> KafkaSettings:
    """Load settings from .env and log them.

    Returns:
        A KafkaSettings instance populated from environment variables.
    """
    LOG.info("Loading settings from .env...")
    settings = KafkaSettings.from_env()
    LOG.info(f"KAFKA_BOOTSTRAP_SERVERS  = {settings.bootstrap_servers}")
    LOG.info(f"KAFKA_TOPIC              = {settings.topic}")
    LOG.info(f"KAFKA_GROUP_ID           = {settings.group_id}")
    LOG.info(f"CONSUMER_TIMEOUT_SECONDS = {TIMEOUT_SECONDS}")
    LOG.info(f"CONSUMER_MAX_MESSAGES    = {MAX_MESSAGES}")
    return settings


def verify_connection(settings: KafkaSettings) -> None:
    """Verify Kafka is reachable before doing anything else.

    Raises:
        SystemExit: If Kafka is not reachable.
    """
    LOG.info("Verifying Kafka connection...")
    try:
        verify_kafka_connection(settings)
        LOG.info("Kafka port is reachable.")
    except ConnectionError as error:
        LOG.error(str(error))
        raise SystemExit(1) from error


def verify_topic(settings: KafkaSettings) -> None:
    """Verify the topic exists and has messages.

    Raises:
        SystemExit: If the topic does not exist or is empty.
    """
    LOG.info("Verifying Kafka topic...")
    admin = create_admin_client(settings)

    if not topic_exists(admin, settings.topic):
        LOG.error(f"Topic {settings.topic!r} does not exist.")
        LOG.error("Run the producer first.")
        raise SystemExit(1)

    message_count = get_topic_message_count(admin, settings.topic, settings)
    LOG.info(f"Topic {settings.topic!r} exists.")
    LOG.info(f"Found {message_count} message(s) available.")

    if message_count == 0:
        LOG.error("Topic is empty. Run the producer first.")
        raise SystemExit(1)


def get_kafka_consumer(settings: KafkaSettings) -> Any:
    """Create a Kafka consumer subscribed to the topic.

    Resets offsets to the beginning so this example reads all available messages.

    Returns:
        A confluent_kafka.Consumer instance subscribed to the topic.
    """
    LOG.info("Creating Kafka consumer...")
    consumer = create_consumer(settings)
    consumer.subscribe(
        [settings.topic],
        on_assign=lambda c, partitions: c.assign(
            [
                TopicPartition(
                    partition.topic,
                    partition.partition,
                    OFFSET_BEGINNING,
                )
                for partition in partitions
            ]
        ),
    )
    LOG.info(f"Subscribed to topic: {settings.topic!r} (reading from beginning)")
    return consumer


# ===========================================================================
# DEFINE SECTION C. CONSUME AND PROCESS MESSAGES HELPERS
# ===========================================================================


def initialize_output() -> tuple[Any, Any, dict[str, float], RunningStats]:
    """Initialize output resources.

    Returns:
        A tuple of (figure, axis, store_totals, stats).
    """
    LOG.info("Initializing output...")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if OUTPUT_CSV.exists():
        OUTPUT_CSV.unlink()
    LOG.info(f"Output CSV cleared: {OUTPUT_CSV.name}")

    init_db(OUTPUT_DB)
    LOG.info(f"Database initialized: {OUTPUT_DB.name}")

    figure, axis, store_totals = init_live_chart()
    LOG.info("Live chart initialized.")

    stats = RunningStats()
    return figure, axis, store_totals, stats


def load_reference_data() -> dict[str, float]:
    """Load reference data used for message enrichment.

    Returns:
        A dictionary mapping store_id to tax rate as a float.
    """
    LOG.info("Loading enrichment reference data...")
    store_lookup: dict[str, float] = {
        store_id: float(tax_rate_pct)
        for store_id, tax_rate_pct in read_csv_as_lookup(
            STORES_CSV,
            key_field="store_id",
            value_field="tax_rate_pct",
        ).items()
    }
    LOG.info(f"Found {len(store_lookup)} store tax rates.")
    return store_lookup


def process_message(
    row: dict[str, Any],
    *,
    store_lookup: dict[str, float],
    stats: RunningStats,
    figure: Any,
    axis: Any,
    store_totals: dict[str, float],
) -> dict[str, Any] | None:
    """Process one consumed coffee order.

    Steps:
      - Validate required fields
      - Enrich with derived fields (incl. the Phase 4 loyalty_discount)
      - Update running statistics
      - Update live chart
      - Return the enriched order (or None if validation failed)

    Arguments:
        row: A raw consumed Kafka message row.
        store_lookup: Tax rates by store_id.
        stats: Running statistics accumulator.
        figure: Matplotlib figure.
        axis: Matplotlib axis.
        store_totals: Running revenue per store_id (updated by the chart).

    Returns:
        The enriched row, or None if validation failed.
    """
    errors = validate_required_fields(record=row, required_fields=ORDER_REQUIRED_FIELDS)
    if errors:
        LOG.warning(f"Validation failed for order {row.get('order_id', '?')}")
        LOG.warning(f"errors={errors}")
        write_rejected_record(OUTPUT_DB, row, errors)
        return None

    enriched = enrich_message(row, store_lookup)
    LOG.info(
        f"subtotal={enriched['subtotal']}  "
        f"loyalty_discount={enriched['loyalty_discount']}  "
        f"tax={enriched['tax_amount']}  "
        f"total={enriched['total']}  "
        f"running_total={stats.total + enriched['total']:.2f}"
    )

    stats.update(enriched["total"])

    update_live_chart(
        figure=figure,
        axis=axis,
        store_totals=store_totals,
        message=enriched,
    )

    return enriched


def consume_messages(
    consumer: Any,
    *,
    store_lookup: dict[str, float],
    stats: RunningStats,
    figure: Any,
    axis: Any,
    store_totals: dict[str, float],
) -> tuple[int, int]:
    """Consume and process messages from the Kafka topic.

    Runs until MAX_MESSAGES is reached or TIMEOUT_SECONDS elapses
    with no new message.

    All arguments after the asterisk must be passed as keyword arguments.

    Arguments:
        consumer: An open Kafka consumer subscribed to the topic.
        store_lookup: Tax rates by store_id.
        stats: Running statistics accumulator.
        figure: Matplotlib figure.
        axis: Matplotlib axis.
        store_totals: Running revenue per store_id.

    Returns:
        A tuple of (consumed_count, skipped_count).
    """
    LOG.info("Consuming messages...")
    LOG.info(f"Waiting for up to {MAX_MESSAGES} message(s).")
    LOG.info("Press CTRL+C to stop early.\n")

    consumed_count = 0
    skipped_count = 0

    while consumed_count + skipped_count < MAX_MESSAGES:
        row = consume_kafka_message(
            consumer=consumer,
            timeout_seconds=TIMEOUT_SECONDS,
        )

        if row is None:
            LOG.info(f"No message received within {TIMEOUT_SECONDS}s timeout.")
            LOG.info("Producer finished or paused. Stopping consumer.")
            break

        LOG.info(row)

        enriched = process_message(
            row,
            store_lookup=store_lookup,
            stats=stats,
            figure=figure,
            axis=axis,
            store_totals=store_totals,
        )

        if enriched is None:
            skipped_count += 1
            LOG.warning("MESSAGE REJECTED")
            LOG.warning(f"order={row.get('order_id', '?')}")
            LOG.warning(f"skipped={skipped_count}")
            continue

        # Write the valid record to the DuckDB database.
        write_valid_record(OUTPUT_DB, enriched)
        LOG.info("Wrote valid record to DuckDB:")
        LOG.info(f"  order={enriched['order_id']}")

        append_csv_row(
            path=OUTPUT_CSV,
            row={field: enriched.get(field, "") for field in CONSUMED_FIELDNAMES},
            fieldnames=CONSUMED_FIELDNAMES,
        )

        consumed_count += 1
        LOG.info("MESSAGE ACCEPTED")
        LOG.info(f"order={enriched['order_id']}")
        LOG.info(f"total=${enriched['total']:.2f}")
        LOG.info(f"consumed={consumed_count}")
        LOG.info("RUNNING STATS")
        LOG.info(f"total_revenue=${stats.total:,.2f}")
        LOG.info(f"average=${stats.mean:,.2f}")
        LOG.info(f"min=${stats.minimum:,.2f}")
        LOG.info(f"max=${stats.maximum:,.2f}")

    return consumed_count, skipped_count


def save_artifacts(figure: Any) -> None:
    """Save output artifacts or note their location.

    Arguments:
        figure: Matplotlib figure to save as an image.
    """
    LOG.info("Saving artifacts...")

    save_live_chart(figure=figure, chart_path=OUTPUT_CHART)

    log_path(LOG, "WROTE OUTPUT_CHART", OUTPUT_CHART)
    log_path(LOG, "WROTE OUTPUT_CSV", OUTPUT_CSV)
    log_path(LOG, "WROTE OUTPUT_DB", OUTPUT_DB)


# ===========================================================================
# DEFINE SECTION E. EXIT AND CLEANUP HELPERS
# ===========================================================================


def log_summary(
    consumed_count: int,
    skipped_count: int,
    stats: RunningStats,
    settings: KafkaSettings,
) -> None:
    """Log final summary statistics."""
    LOG.info("Summary:")
    LOG.info(f"Consumed {consumed_count} message(s) from topic {settings.topic!r}.")
    LOG.info(f"Skipped  {skipped_count} message(s).")
    log_path(LOG, "OUTPUT_CSV", OUTPUT_CSV)

    if stats.count > 0:
        LOG.info(f"  Total revenue: ${stats.total:,.2f}")
        LOG.info(f"  Average order: ${stats.mean:,.2f}")
        LOG.info(f"  Minimum order: ${stats.minimum:,.2f}")
        LOG.info(f"  Maximum order: ${stats.maximum:,.2f}")

    # Run the coffee-specific DuckDB summary queries.
    log_storage_summary(OUTPUT_DB)

    LOG.info("========================")
    LOG.info("Coffee consumer executed successfully!")
    LOG.info("========================")


# ===========================================================================
# MAIN FUNCTION
# ===========================================================================


def main() -> None:
    """Main entry point for the Kafka consumer."""
    log_paths()

    LOG.info("========================")
    LOG.info("SECTION A. Acquire")
    LOG.info("========================")

    settings = load_settings()
    verify_connection(settings)
    verify_topic(settings)
    consumer = get_kafka_consumer(settings)

    LOG.info("========================")
    LOG.info("SECTION C. Consume and Process Messages")
    LOG.info("========================")

    figure, axis, store_totals, stats = initialize_output()
    store_lookup = load_reference_data()

    consumed_count = 0
    skipped_count = 0

    try:
        try:
            consumed_count, skipped_count = consume_messages(
                consumer,
                store_lookup=store_lookup,
                stats=stats,
                figure=figure,
                axis=axis,
                store_totals=store_totals,
            )
        finally:
            consumer.close()
            LOG.info("Kafka consumer closed.")

        save_artifacts(figure)

    finally:
        close_live_chart()
        LOG.info("Live chart closed.")

    LOG.info("========================")
    LOG.info("SECTION E. Exit")
    LOG.info("========================")

    log_summary(consumed_count, skipped_count, stats, settings)


# === CONDITIONAL EXECUTION GUARD ===

if __name__ == "__main__":
    main()
