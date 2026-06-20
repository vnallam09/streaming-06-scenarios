"""src/streaming/data_validation/data_contract_teja.py.

Data contract for the COFFEE SHOP scenario (Phase 5).

This is my copy of data_contract_case.py, adapted for a different dataset:
a stream of coffee shop drink orders instead of online course sales.

Each event is a single coffee order. Reference tables provide the store
tax rates (stores.csv) and the drink catalog (drinks.csv).

Defines what a valid coffee order looks like for this project:
required fields, allowed values, reference table fields, and output field order.

Author: Teja (adapted from Denise Case's data_contract_case.py)
Date: 2026-06
"""

# === DECLARE IMPORTS ===

from typing import Any, Final

from datafun_streaming.core.types import DataRecordDict
from datafun_streaming.data_validation.types import ValidationResult
from datafun_streaming.data_validation.validation_utils import (
    validate_boolean_text,
    validate_datetime,
    validate_positive_integer,
    validate_required_fields,
)

# === EVENT TABLE FIELDS ===

# A coffee order event sent by the producer.
ORDER_REQUIRED_FIELDS: Final[list[str]] = [
    "order_id",
    "datetime",
    "store_id",
    "drink_id",
    "size",
    "unit_price",
    "quantity",
    "is_loyalty",
    "customer_id",
    "payment_method",
]

ORDER_OPTIONAL_FIELDS: Final[list[str]] = [
    "customer_note",
]

VALID_ORDER_FIELDNAMES: Final[list[str]] = [
    *ORDER_REQUIRED_FIELDS,
    *ORDER_OPTIONAL_FIELDS,
]

# === REFERENCE TABLE FIELDS ===

STORES_REQUIRED_FIELDS: Final[list[str]] = [
    "store_id",
    "store_name",
    "city",
    "state_code",
    "tax_rate_pct",
    "timezone",
]

DRINKS_REQUIRED_FIELDS: Final[list[str]] = [
    "drink_id",
    "drink_name",
    "category",
    "base_price_usd",
    "is_seasonal",
]

# === ALLOWED VALUES ===

ALLOWED_SIZES: Final[set[str]] = {"small", "medium", "large"}
ALLOWED_PAYMENT_METHODS: Final[set[str]] = {
    "credit_card",
    "apple_pay",
    "gift_card",
    "cash",
}

# === OUTPUT FIELD ORDER ===

# Derived fields added by derived_fields_teja.enrich_message():
#   subtotal, loyalty_discount (NEW in Phase 4), tax_amount, total
CONSUMED_FIELDNAMES: Final[list[str]] = [
    *ORDER_REQUIRED_FIELDS,
    "subtotal",
    "loyalty_discount",
    "tax_amount",
    "total",
    "_kafka_key",
    "_kafka_partition",
    "_kafka_offset",
]

REJECTED_ORDER_FIELDNAMES: Final[list[str]] = [
    *ORDER_REQUIRED_FIELDS,
    "validation_errors",
]


# === DOMAIN-SPECIFIC VALIDATION ===


def validate_order_record(
    *,
    record: DataRecordDict,
    valid_store_ids: set[str],
    valid_drink_ids: set[str],
) -> ValidationResult:
    """Validate one coffee order against this project's data contract.

    All arguments after the asterisk must be passed as keyword arguments.

    Arguments:
        record: The message to validate.
        valid_store_ids: The set of valid store_id values from stores.csv.
        valid_drink_ids: The set of valid drink_id values from drinks.csv.

    Returns:
        A ValidationResult indicating whether the record is valid and any errors.
    """
    errors: list[str] = []

    # Required fields must all be present and non-empty.
    errors.extend(
        validate_required_fields(record=record, required_fields=ORDER_REQUIRED_FIELDS)
    )

    if errors:
        # Stop early: the value checks below assume the fields exist.
        return ValidationResult(is_valid=False, errors=errors)

    # Reference-table checks. The !r in the f-string shows the value with quotes,
    # which makes an empty or whitespace value easy to spot in the logs.
    if record["store_id"] not in valid_store_ids:
        errors.append(f"Unknown store_id: {record['store_id']!r}")

    if record["drink_id"] not in valid_drink_ids:
        errors.append(f"Unknown drink_id: {record['drink_id']!r}")

    # Allowed-value checks.
    if record["size"] not in ALLOWED_SIZES:
        errors.append(f"Invalid size: {record['size']!r}")

    if record["payment_method"] not in ALLOWED_PAYMENT_METHODS:
        errors.append(f"Invalid payment_method: {record['payment_method']!r}")

    # Type / format checks from the shared validation helpers.
    errors.extend(validate_datetime(record["datetime"]))
    errors.extend(validate_positive_integer(record["quantity"]))
    errors.extend(validate_boolean_text(record["is_loyalty"], field_name="is_loyalty"))

    is_result_valid = not bool(errors)
    return ValidationResult(is_valid=is_result_valid, errors=errors)


# === OUTPUT HELPERS ===


def keep_order_fields(row: dict[str, Any]) -> dict[str, Any]:
    """Return only required order fields in standard order.

    Arguments:
        row: The original message as a dict.

    Returns:
        A new dict with only the required fields in the standard order.
    """
    return {field: row.get(field, "") for field in ORDER_REQUIRED_FIELDS}
