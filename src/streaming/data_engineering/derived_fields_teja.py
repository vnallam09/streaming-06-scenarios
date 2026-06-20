"""src/streaming/data_engineering/derived_fields_teja.py.

Derived field calculations for COFFEE SHOP orders.

This is my copy of derived_fields.py, adapted for the coffee shop scenario.

The producer sends raw order fields only (store, drink, size, unit_price,
quantity, is_loyalty, ...). The consumer is responsible for all derived
calculations below.

Derived fields added here:
  - subtotal          quantity * unit_price (before discount and tax)
  - loyalty_discount  *** NEW FIELD ADDED IN PHASE 4 ***
                      10% off the subtotal for loyalty members, else 0.00
  - tax_amount        store tax rate applied to (subtotal - loyalty_discount)
  - total             discounted subtotal + tax

Author: Teja (adapted from Denise Case's derived_fields.py)
Date: 2026-06
"""

# === DECLARE IMPORTS ===

import logging
from typing import Any, Final

# === DECLARE EXPORTS ===

__all__ = [
    "LOYALTY_DISCOUNT_RATE",
    "TAX_RATE_DEFAULT",
    "compute_loyalty_discount",
    "compute_subtotal",
    "compute_tax_amount",
    "enrich_message",
    "get_tax_rate",
]

# === DECLARE CONSTANTS ===

# Fallback tax rate used when a store is not found in the lookup table.
TAX_RATE_DEFAULT: Final[float] = 0.08

# Loyalty members receive this fraction off the subtotal (Phase 4 field).
LOYALTY_DISCOUNT_RATE: Final[float] = 0.10

# Text values that count as a "true" loyalty flag.
_TRUE_TEXT: Final[set[str]] = {"true", "1", "yes", "y", "t"}

# === CONFIGURE LOGGER ONCE PER PYTHON FILE (MODULE) ===

LOG = logging.getLogger(__name__)

# === DEFINE DERIVED FIELD FUNCTIONS ===


def compute_subtotal(quantity: int, unit_price: float) -> float:
    """Compute the order subtotal before discount and tax.

    Arguments:
        quantity: Number of drinks ordered.
        unit_price: Price per drink in USD.

    Returns:
        Subtotal rounded to 2 decimal places.
    """
    return round(quantity * unit_price, 2)


def compute_loyalty_discount(subtotal: float, is_loyalty: bool) -> float:
    """Compute the loyalty discount for an order (NEW Phase 4 derived field).

    Loyalty members get LOYALTY_DISCOUNT_RATE off the subtotal.
    Non-members get no discount.

    Arguments:
        subtotal: Order subtotal before discount.
        is_loyalty: Whether the customer is a loyalty member.

    Returns:
        Discount amount rounded to 2 decimal places (0.00 for non-members).
    """
    if not is_loyalty:
        return 0.0
    return round(subtotal * LOYALTY_DISCOUNT_RATE, 2)


def compute_tax_amount(taxable_amount: float, tax_rate: float) -> float:
    """Compute the tax amount for an order.

    Arguments:
        taxable_amount: Amount after the loyalty discount is applied.
        tax_rate: Tax rate as a decimal (e.g. 0.086 for 8.6%).

    Returns:
        Tax amount rounded to 2 decimal places.
    """
    return round(taxable_amount * tax_rate, 2)


def enrich_message(
    row: dict[str, Any],
    store_lookup: dict[str, float],
) -> dict[str, Any]:
    """Add all derived fields to a raw coffee order row.

    Arguments:
        row: A validated raw order row.
        store_lookup: A dict mapping store_id to tax_rate_pct.

    Returns:
        A new dict containing all original fields plus derived fields.
    """
    quantity = int(row.get("quantity", 0))
    unit_price = float(row.get("unit_price", 0.0))
    store_id = str(row.get("store_id", ""))
    is_loyalty = str(row.get("is_loyalty", "")).strip().lower() in _TRUE_TEXT

    tax_rate = get_tax_rate(store_id, store_lookup)

    subtotal = compute_subtotal(quantity, unit_price)
    loyalty_discount = compute_loyalty_discount(subtotal, is_loyalty)
    taxable_amount = round(subtotal - loyalty_discount, 2)
    tax_amount = compute_tax_amount(taxable_amount, tax_rate)

    total = round(taxable_amount + tax_amount, 2)
    return {
        **row,
        "subtotal": subtotal,
        "loyalty_discount": loyalty_discount,
        "tax_amount": tax_amount,
        "total": total,
    }


def get_tax_rate(store_id: str, store_lookup: dict[str, float]) -> float:
    """Look up the tax rate for a store.

    The tax rate is stored as a percentage in stores.csv (e.g. 8.6 for 8.6%).
    This function converts it to a decimal for use in calculations.

    Arguments:
        store_id: The store identifier from the message (e.g. "S-KC").
        store_lookup: A dict mapping store_id to tax_rate_pct (as a float).

    Returns:
        The tax rate as a decimal (e.g. 0.086 for 8.6%).
    """
    if store_id in store_lookup:
        return store_lookup[store_id] / 100.0

    LOG.warning(
        f"Store {store_id!r} not in lookup table. "
        f"Using default tax rate {TAX_RATE_DEFAULT}."
    )

    return TAX_RATE_DEFAULT
