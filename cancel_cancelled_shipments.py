#!/usr/bin/env python3
"""
Cancel in Kibo the shipments the warehouse has already cancelled.

The cancellation list is driven by `tlg-business-intelligence-prd`.til.orders_cancellation_table:
nothing is cancelled unless that table carries a row for the shipment with
cancelled_on_kibo = false, and the reason sent to Kibo is the table's own.

Pipeline
--------
1. Read the active shipments from BigQuery (BI project):

       `tlg-business-intelligence-prd`.til.oms_active_shipments

   keeping only the rows whose shipmentStatus is READY (--oms-status, empty to
   disable), and taking from them shipment_order_number, the shipment number,
   tenant_id and site_id.

2. For each of those orders, read the warehouse events in the WLFS project:

       `tlg-wlfs-prd`.til.prd_shipmentStatus

   The LAST event of the order has to be a CANCELLED one. Not "a CANCELLED event
   exists somewhere in the history": the most recent event of any type, across every
   generation of the order, must be the cancellation. A later PICKED, SHIPPED or
   anything else means the warehouse has moved on and the shipment is left alone.

   The events are read per ORDER rather than per shipmentOrderNumber, because the
   active shipment usually carries no events of its own: GXO cancels generation F,
   Kibo reassigns into a brand new G, and G is what sits READY in oms_active_shipments
   while every event still names A to F. prd_shipmentStatus has no order number
   column, so the order is the shipmentOrderNumber with its 6th character — the
   generation letter — wildcarded:

       SELECT * FROM `tlg-wlfs-prd`.til.prd_shipmentStatus
       WHERE shipmentOrderNumber LIKE 'DJ-US%ST0000124550'

   The items of that last CANCELLED event are what the rest of the run matches on.

3. Ask Kibo for every shipment of the order:

       GET https://t{tenantId}.{host}/api/commerce/shipments?filter=externalOrderId=={id}

   The externalOrderId is the shipment's own field, falling back to the
   shipmentOrderNumber with its 6th character put back to a hyphen
   ("DJ-USAST0000124550" -> "DJ-US-ST0000124550").

   Out of the shipments of that order, the target is the one that

     - has shipmentStatus BACKORDER or CUSTOMER_CARE (--cancel-target-statuses)
     - carries the exact same items as the CANCELLED event: every product of the
       event is on the shipment and every item of the shipment is in the event
       (--product-match overlap only asks for one product in common, any skips it)
     - is present in `tlg-business-intelligence-prd`.til.orders_cancellation_table,
       in column shipment_order_number, with cancelled_on_kibo = false, and whose
       shipment_items match those same items

   The table is matched on the shipment's shipmentOrderNumber AND on its
   shipmentNumber, so the column holds either one.

   Several matches -> `ambiguous_cancel_target`, nothing is touched
   (--on-multiple-cancel-targets newest takes the most recent one instead).
   No match -> `no_cancel_target`, nothing is touched.

4. Cancel that shipment in Kibo:

       PUT https://t{tenantId}.{host}/api/commerce/shipments/{shipmentNumber}/canceled

       {"canceledReason": {"reasonCode": "<reason_code>", "moreInfo": "<more_info>"}}

   reasonCode comes from the table's reason_code column and moreInfo from its
   more_info column. The reason has to be nested under canceledReason: Kibo answers
   200 to a flat {"reasonCode": ..., "moreInfo": ...} body but ignores it and records
   its own default reason. After a successful cancellation the shipment is read back
   and the stored shipmentStatusReason.reasonCode is reported.

5. The cancelled rows are flagged in one statement at the end of the run:

       UPDATE `tlg-business-intelligence-prd`.til.orders_cancellation_table
       SET cancelled_on_kibo = TRUE
       WHERE shipment_order_number IN UNNEST(@numbers)

   so a later run skips them. --no-mark-cancelled leaves the table untouched, and a
   dry run never writes. Should a run die between the cancellation and this update,
   the next run finds the shipment CANCELED in Kibo rather than BACKORDER, so it is
   no longer a target and nothing is cancelled twice.

Nothing is ever sent to the warehouse: GXO has already cancelled, which is what put
the shipment in the table. Nothing is reassigned either; the ecommerce branch of the
earlier version is gone, and the table alone decides what gets cancelled.

Credentials come from environment variables (GitHub repository secrets):
    BI_BIGQUERY_CREDS   service account JSON for tlg-business-intelligence-prd
    TIL_BIGQUERY_CREDS  service account JSON for tlg-wlfs-prd
    KIBO_API_CREDS      {"client_id": "...", "client_secret": "..."}
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import random
import re
import sys
import time
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import quote

import requests
from google.cloud import bigquery
from google.oauth2 import service_account

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BI_PROJECT = "tlg-business-intelligence-prd"
BI_DATASET = "til"
BI_TABLE = "oms_active_shipments"
# The cancellation list. A shipment is cancelled in Kibo only when it has a row here.
CANCELLATION_TABLE = "orders_cancellation_table"

WLFS_PROJECT = "tlg-wlfs-prd"
WLFS_DATASET = "til"
SHIPMENT_STATUS_TABLE = "prd_shipmentStatus"


# The Kibo tenant and site of each brand. oms_active_shipments carries a tenant_id and
# a site_id of its own, but they are not always the ones the Shipment API expects: a
# wrong x-vol-site scopes a filtered query to a site that holds none of the order's
# shipments, and Kibo answers 200 with an empty page rather than an error, which looks
# exactly like "this order has no shipments". This table is the authority, and the
# brand is taken from the start of the shipmentOrderNumber.
BRAND_KIBO_CONFIG: Dict[str, Tuple[str, str]] = {
    # brand: (kiboSiteId, kiboTenantId)
    "DG": ("49791", "29916"),
    "LN": ("49801", "29917"),
    "MO": ("53100", "29988"),
    "SC": ("53620", "29989"),
    "CO": ("53190", "29990"),
    "RC": ("53192", "29990"),
    "WL": ("53613", "29991"),
    "SW": ("52246", "29992"),
    "CA": ("53602", "29993"),
    "HB": ("60955", "30014"),
    "BA": ("53088", "30014"),
    "CV": ("60958", "30014"),
    "OW": ("53145", "30016"),
    "RL": ("53145", "30016"),
    "VW": ("53605", "30017"),
    "FA": ("53605", "30017"),
    "DJ": ("53592", "30018"),
    "AT": ("53155", "30019"),
    "LA": ("53157", "30019"),
    "CL": ("53139", "30020"),
    "PL": ("53139", "30020"),
    "HE": ("53148", "30021"),
    "FE": ("53097", "30022"),
    "CR": ("53095", "30022"),
    "EFOT": ("53166", "30023"),
    "EF": ("53166", "30023"),
    "FO": ("53163", "30023"),
    "PJ": ("53179", "30475"),
    "TM": ("53493", "32145"),
    "FU": ("53493", "32145"),
    "VE": ("53636", "32147"),
    "ZH": ("53636", "32147"),
    "WF": ("53636", "32147"),
    "BO": ("54979", "33692"),
    "MA": ("55105", "33693"),
    "FL": ("55105", "33693"),
    "PA": ("55105", "33693"),
    "AL": ("55105", "33693"),
    "BB": ("54981", "33694"),
}


DEFAULT_HOST = "tp3.mozu.com"
DEFAULT_AUTH_URL = "https://home.mozu.com/api/platform/applications/authtickets/oauth"

BQ_SCOPES = ["https://www.googleapis.com/auth/bigquery"]
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Both spellings exist in the data.
CANCELLED_EVENT_TYPES = ("CANCELLED", "CANCELED")

# How the items of the CANCELLED event, of the Kibo shipment and of the cancellation
# table are compared.
PRODUCT_MATCH_EXACT = "exact"
PRODUCT_MATCH_OVERLAP = "overlap"
PRODUCT_MATCH_ANY = "any"
DEFAULT_PRODUCT_MATCH = PRODUCT_MATCH_EXACT

# Product identifiers looked for anywhere in a CANCELLED requestPayload, on a Kibo
# shipment item, and in the shipment_items column of the cancellation table.
DEFAULT_PRODUCT_KEYS = (
    "upc", "sku", "productCode", "variationProductCode", "itemCode", "partNumber",
    "productId", "barcode", "ean", "styleCode",
)

# prd_shipmentStatus carries no order number, so the shipments of one order are found by
# wildcarding the 6th character of the shipmentOrderNumber, the generation letter:
#
#     SELECT * FROM `tlg-wlfs-prd`.til.prd_shipmentStatus
#     WHERE shipmentOrderNumber LIKE 'DJ-US%ST0000124550'
#
# The pattern is what identifies the order throughout this script.
ORDER_PATTERN_WILDCARD = "%"
ORDER_PATTERN_POSITION = 6

# The 6th character of the shipmentOrderNumber (counting the hyphen, so the 'E' of
# "DJ-USE00041696") is the generation letter. It is reported and used to identify the
# order, but it does not restrict the run: every letter is in scope. Narrow a single
# run with --allowed-sixth-characters if ever needed.
DEFAULT_ALLOWED_SIXTH_CHARACTERS = ""

# Kibo statuses that mean "nothing left to cancel".
TERMINAL_STATUSES = {
    "CANCELED", "CANCELLED", "CLOSED", "COMPLETE", "COMPLETED",
    "FULFILLED", "SHIPPED", "PICKEDUP", "PICKED_UP",
}

# Statuses Kibo will not accept a cancellation for. REASSIGNED is one of them: such a
# shipment has already been handed back to routing. BACKORDER is deliberately NOT
# here — a backordered shipment is cancellable, and is exactly where the replacement
# of a cancelled shipment usually lands.
UNCANCELLABLE_STATUSES = TERMINAL_STATUSES | {"REASSIGNED"}

# Statuses excluded from the cancel-target search on top of UNCANCELLABLE_STATUSES.
DEFAULT_CANCEL_TARGET_EXCLUDED_STATUSES = ""

ON_MULTIPLE_SKIP = "skip"
ON_MULTIPLE_NEWEST = "newest"
DEFAULT_ON_MULTIPLE_CANCEL_TARGETS = ON_MULTIPLE_SKIP

# The shipment to cancel is the one the order fell back to once the warehouse cancelled:
# BACKORDER when routing found nowhere to send it, CUSTOMER_CARE when it was parked for
# the care team.
DEFAULT_CANCEL_TARGET_STATUSES = "BACKORDER,CUSTOMER_CARE"

# Optional extra narrowing of the cancel target by fulfilment location. Empty by
# default: a backordered shipment sits in no warehouse, so there is nothing to match.
DEFAULT_CANCEL_TARGET_LOCATION_PATTERN = ""

# Where the fulfilment location sits on a Kibo shipment.
FULFILLMENT_LOCATION_KEYS = (
    "fulfillmentLocationCode", "fulfillmentLocation", "locationCode", "locationName",
)

# GET /api/commerce/shipments?filter=externalOrderId=={id}
# The Shipment API has its own filter syntax: `field==value`, clauses joined by ';'.
# Only the externalOrderId clause is sent and the statuses are filtered in Python, so
# the query stays one simple, well documented expression.
SHIPMENTS_FILTER_TEMPLATE = "externalOrderId=={value}"
DEFAULT_ORDER_LOOKUP_PAGE_SIZE = 200

EXTERNAL_ORDER_ID_KEYS = ("externalOrderId", "external_order_id")

# The externalOrderId is the shipmentOrderNumber with the generation letter at position
# 6 put back to a hyphen: "DJ-USAST0000124550" -> "DJ-US-ST0000124550".
EXTERNAL_ORDER_ID_CHARACTER = "-"

# Candidate column names, resolved case-insensitively against the real schema.
SHIPMENT_NUMBER_CANDIDATES = ("shipmentOrderNumber", "shipment_order_number")
OMS_NUMBER_CANDIDATES = (
    "oms_shipment_number", "omsShipmentNumber", "shipment_number", "shipmentNumber",
)
PAYLOAD_CANDIDATES = ("requestPayload", "request_payload")
EVENT_TYPE_CANDIDATES = ("eventType", "event_type")
OMS_STATUS_CANDIDATES = ("shipmentStatus", "shipment_status", "status")
TIMESTAMP_CANDIDATES = (
    "eventTimestamp", "event_timestamp", "insertTimestamp", "insert_timestamp",
    "ingestionTimestamp", "ingestion_timestamp", "createdAt", "created_at",
    "creationDate", "creation_date", "loadTimestamp", "load_timestamp",
)

# orders_cancellation_table. The first three are required; a run without reason_code or
# more_info falls back to --default-reason-code / --default-more-info.
CANCELLATION_NUMBER_CANDIDATES = (
    "shipment_order_number", "shipmentOrderNumber", "shipment_number", "shipmentNumber",
)
CANCELLATION_ITEMS_CANDIDATES = ("shipment_items", "shipmentItems", "items")
CANCELLATION_FLAG_CANDIDATES = ("cancelled_on_kibo", "canceled_on_kibo", "cancelledOnKibo")
CANCELLATION_REASON_CANDIDATES = ("reason_code", "reasonCode", "reason")
CANCELLATION_MORE_INFO_CANDIDATES = ("more_info", "moreInfo", "info", "reason_message")

# Values read as "already cancelled on Kibo", whatever the column's type: the flag is
# compared as text so a BOOL, a STRING and an INT64 column all work.
TRUTHY_FLAG_VALUES = ("TRUE", "T", "YES", "Y", "1")

# Reasons used when the table carries no reason_code / more_info column at all.
DEFAULT_REASON_CODE = "No Inventory"
DEFAULT_MORE_INFO = "Cancelled by the warehouse"
# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def log(message: str) -> None:
    print(message, flush=True)


def warn(message: str) -> None:
    print(message, file=sys.stderr, flush=True)


def fail(message: str, exit_code: int = 1) -> None:
    print(f"ERROR: {message}", file=sys.stderr, flush=True)
    raise SystemExit(exit_code)


def clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = str(value).strip()
    return text[:-2] if re.fullmatch(r"\d+\.0", text) else text


def excerpt_of(response: Any, max_chars: int = 800) -> str:
    if response is None:
        return ""
    text = getattr(response, "text", "") or ""
    return re.sub(r"\s+", " ", text).strip()[:max_chars]


def retry_sleep_seconds(attempt: int, response: Optional[Any] = None) -> float:
    if response is not None:
        retry_after = getattr(response, "headers", {}).get("Retry-After")
        if retry_after:
            try:
                return min(60.0, max(0.0, float(retry_after)))
            except ValueError:
                pass
    return min(30.0, (2 ** (attempt - 1)) + random.random())


def chunked(items: Sequence[str], size: int) -> Iterable[List[str]]:
    for start in range(0, len(items), size):
        yield list(items[start:start + size])


def is_terminal_status(status: str) -> bool:
    return status.upper().replace(" ", "").replace("-", "_") in TERMINAL_STATUSES

def sixth_character_of(shipment_order_number: str) -> str:
    """
    The 6th character of the shipmentOrderNumber, upper-cased.

    Counting includes every character, the hyphen as well: "DJ-USE00041696" gives "E".
    Returns "" when the number is shorter than 6 characters.
    """
    value = clean(shipment_order_number)
    return value[5].upper() if len(value) >= 6 else ""


def sixth_character_allowed(shipment_order_number: str, allowed: str) -> bool:
    """
    Whether the workflow may act on this shipment at all.

    An empty `allowed` disables the rule. A number too short to have a 6th character is
    never allowed: `"" in "ABCD"` is True in Python, so the emptiness is checked first.
    """
    if not allowed:
        return True
    character = sixth_character_of(shipment_order_number)
    return bool(character) and character in allowed


def brand_of(shipment_order_number: str) -> str:
    """
    The brand code at the start of a shipmentOrderNumber.

    "WL-FF-WST191241581" -> "WL", "DG-EUA01688483" -> "DG". The longest configured code
    wins, so EFOT is not read as EF. Returns "" when nothing in BRAND_KIBO_CONFIG
    matches.
    """
    value = clean(shipment_order_number).upper()
    for code in sorted(BRAND_KIBO_CONFIG, key=len, reverse=True):
        if value.startswith(code):
            return code
    return ""


def kibo_config_for_brand(brand: str) -> Tuple[str, str]:
    """(siteId, tenantId) for a brand code, or ("", "") when it is not configured."""
    return BRAND_KIBO_CONFIG.get(clean(brand).upper(), ("", ""))


def order_pattern_of(shipment_order_number: str) -> str:
    """
    The LIKE pattern matching every shipmentOrderNumber of the same order.

    prd_shipmentStatus has no order number column, so the order is identified by the
    shipmentOrderNumber with its 6th character — the generation letter, the A/B/C/D the
    sixth-character rule already keys on — replaced by the SQL wildcard:

        DJ-USAST0000124550  ->  DJ-US%ST0000124550

    which is the hand check

        SELECT * FROM `tlg-wlfs-prd`.til.prd_shipmentStatus
        WHERE shipmentOrderNumber LIKE 'DJ-US%ST0000124550'

    Returns "" when the number is shorter than 6 characters, or when it already carries
    a % or _ of its own, since those would widen the pattern in ways nobody intended.
    """
    value = clean(shipment_order_number)
    if len(value) < ORDER_PATTERN_POSITION or "%" in value or "_" in value:
        return ""
    cut = ORDER_PATTERN_POSITION - 1
    return value[:cut] + ORDER_PATTERN_WILDCARD + value[cut + 1:]


def matches_order_pattern(shipment_order_number: str, pattern: str) -> bool:
    """
    The Python twin of `shipmentOrderNumber LIKE pattern`.

    BigQuery returns the rows of several patterns in one go, so each row is handed back
    to the pattern (or patterns) it belongs to here. % matches any run of characters and
    _ a single one, as in SQL.
    """
    if not pattern:
        return False
    regex = "".join(
        ".*" if part == "%" else "." if part == "_" else re.escape(part)
        for part in pattern
    )
    return re.fullmatch(regex, clean(shipment_order_number)) is not None


def matches_like(value: str, pattern: str) -> bool:
    """
    `value LIKE pattern`, case-insensitively, for patterns such as '%GX%'.

    An empty pattern matches everything, so the check can be switched off by passing
    an empty --cancel-target-location-pattern.
    """
    if not pattern:
        return True
    regex = "".join(
        ".*" if part == "%" else "." if part == "_" else re.escape(part)
        for part in pattern
    )
    return re.fullmatch(regex, clean(value), flags=re.IGNORECASE) is not None


def products_of(payload: Dict[str, Any], product_keys: Sequence[str]) -> frozenset[str]:
    """
    Every product identifier carried by a payload, anywhere in the nesting.

    All the configured keys are collected into one set rather than stopping at the first
    one that exists, so two payloads that name their products differently still produce
    comparable sets. Values are upper-cased and stripped.
    """
    products: set[str] = set()
    for key in product_keys:
        for value in iter_values(payload, key):
            text = clean(value).upper()
            if text:
                products.add(text)
    return frozenset(products)


def products_match(left: frozenset[str], right: frozenset[str], mode: str) -> bool:
    """Whether two CANCELLED events concern the same products."""
    if mode == PRODUCT_MATCH_ANY:
        return True
    if not left or not right:
        return False
    if mode == PRODUCT_MATCH_OVERLAP:
        return bool(left & right)
    return left == right


def format_products(products: Iterable[str], limit: int = 8) -> str:
    items = sorted(products)
    if len(items) > limit:
        return ", ".join(items[:limit]) + f", +{len(items) - limit} more"
    return ", ".join(items)

def is_cancellable_status(status: str, excluded: Sequence[str] = ()) -> bool:
    """
    Whether Kibo would accept a cancellation for a shipment in this status.

    An empty status is treated as cancellable: Kibo is the one that decides, and a
    shipment whose status could not be read should not be silently dropped.
    """
    normalized = status.upper().replace(" ", "").replace("-", "_")
    if not normalized:
        return True
    if normalized in UNCANCELLABLE_STATUSES:
        return False
    return normalized not in {clean(s).upper().replace(" ", "").replace("-", "_") for s in excluded}


def normalize_status(status: str) -> str:
    return clean(status).upper().replace(" ", "").replace("-", "_")


def is_wanted_status(status: str, wanted: Sequence[str]) -> bool:
    """
    Whether a shipment carries one of the statuses the cancel target must have.

    Default BACKORDER / CUSTOMER_CARE: once the warehouse cancels, Kibo reassigns, and
    the shipment lands in backorder when routing has nowhere left to send it, or in
    customer care when it was parked there. An empty `wanted` accepts any cancellable
    status.
    """
    if not wanted:
        return True
    normalized = normalize_status(status)
    return normalized in {normalize_status(item) for item in wanted}


def truthy_flag(value: Any) -> bool:
    """
    Read cancelled_on_kibo whatever its column type.

    BOOL arrives as True/False, a STRING column as "true"/"false"/"TRUE", an INT64 one
    as 1/0. Everything is compared as upper-cased text, so NULL and an empty string
    both read as false, which is what "not cancelled on Kibo yet" means.
    """
    if isinstance(value, bool):
        return value
    return clean(value).upper() in TRUTHY_FLAG_VALUES
def shipment_fulfillment_location(shipment: Dict[str, Any]) -> str:
    """
    The shipment's fulfilment location code, at the top level or nested.

    Kibo names it fulfillmentLocationCode on the shipment, but the same value turns up
    as locationCode on items and in some payload shapes, so every known spelling is
    searched, the top level first.
    """
    for key in FULFILLMENT_LOCATION_KEYS:
        value = clean(shipment.get(key))
        if value:
            return value
    for key in FULFILLMENT_LOCATION_KEYS:
        for value in iter_values(shipment, key):
            text = clean(value)
            if text:
                return text
    return ""


def external_order_id_from_shipment_order_number(shipment_order_number: str) -> str:
    """
    The externalOrderId behind a shipmentOrderNumber.

    The 6th character, counting every character including the hyphens, is the generation
    letter (the A/B/C/D the sixth-character rule keys on) and the externalOrderId carries
    a hyphen there instead:

        DJ-USAST0000124550  ->  DJ-US-ST0000124550

    which is the inverse of release_held_exchange_shipments.shipment_order_number_from.
    Returns "" when the number is too short to have a 6th character.
    """
    value = clean(shipment_order_number)
    if len(value) < ORDER_PATTERN_POSITION:
        return ""
    index = ORDER_PATTERN_POSITION - 1
    return value[:index] + EXTERNAL_ORDER_ID_CHARACTER + value[index + 1:]


def shipment_items_of(shipment: Dict[str, Any]) -> List[Any]:
    for key in ("items", "shipmentItems", "lineItems"):
        value = shipment.get(key)
        if isinstance(value, list) and value:
            return value
    return []

def item_products(item: Any, product_keys: Sequence[str]) -> frozenset[str]:
    """Every product identifier carried by one shipment item, nesting included."""
    if not isinstance(item, (dict, list)):
        return frozenset()
    products: set[str] = set()
    for key in product_keys:
        for value in iter_values(item, key):
            text = clean(value).upper()
            if text:
                products.add(text)
    return frozenset(products)


def shipment_matches_products(
    shipment: Dict[str, Any],
    event_products: frozenset[str],
    product_keys: Sequence[str],
    mode: str,
) -> Tuple[bool, frozenset[str]]:
    """
    Whether a Kibo shipment carries the same products as the CANCELLED event.

    Identifiers are compared per item rather than as one flat set, because a Kibo item
    names the same product several ways at once (productCode, variationProductCode, upc)
    while the event payload usually names it once. Flat set equality would therefore
    never hold. "The exact same products" is read as:

        every product of the event is on some item of the shipment
        and every item of the shipment carries a product of the event

    so a shipment that also contains something the event did not cancel, or that is
    missing one of the cancelled products, does not match.

    Returns (matches, the shipment's product identifiers).
    """
    items = shipment_items_of(shipment)
    per_item = [item_products(item, product_keys) for item in items]
    all_products = frozenset().union(*per_item) if per_item else frozenset()

    if mode == PRODUCT_MATCH_ANY:
        return True, all_products
    if not event_products or not per_item:
        return False, all_products

    covered = all_products & event_products
    if mode == PRODUCT_MATCH_OVERLAP:
        return bool(covered), all_products

    every_event_product_shipped = covered == event_products
    every_item_cancelled = all(products & event_products for products in per_item)
    return bool(every_event_product_shipped and every_item_cancelled), all_products


def shipment_created_at(shipment: Dict[str, Any]) -> str:
    """The shipment's creation timestamp as a sortable string, or ""."""
    audit = shipment.get("auditInfo")
    if isinstance(audit, dict):
        for key in ("createDate", "updateDate"):
            value = clean(audit.get(key))
            if value:
                return value
    for key in ("createDate", "orderSubmitDate", "shipmentCreateDate"):
        value = clean(shipment.get(key))
        if value:
            return value
    return ""
# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

def load_json_env(name: str) -> Dict[str, Any]:
    raw = os.environ.get(name, "").strip()
    if not raw:
        fail(f"Environment variable {name} is empty. Check the repository secret.")
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        fail(f"Environment variable {name} does not contain valid JSON: {exc}")
    if not isinstance(data, dict):
        fail(f"Environment variable {name} must contain a JSON object.")
    return data


def bigquery_client(env_name: str, expected_project: str) -> bigquery.Client:
    info = load_json_env(env_name)
    for key in ("type", "project_id", "private_key", "client_email"):
        if not info.get(key):
            fail(f"{env_name} is missing the '{key}' field.")
    project = str(info.get("project_id") or expected_project)
    if project != expected_project:
        warn(f"{env_name}: service account project is '{project}', expected '{expected_project}'. Using '{project}'.")
    credentials = service_account.Credentials.from_service_account_info(info, scopes=BQ_SCOPES)
    return bigquery.Client(project=project, credentials=credentials)


# ---------------------------------------------------------------------------
# BigQuery schema helpers
# ---------------------------------------------------------------------------

def table_columns(client: bigquery.Client, project: str, dataset: str, table: str) -> Dict[str, str]:
    """Return {lowercase column name: real column name}."""
    sql = (
        f"SELECT column_name FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` "
        "WHERE table_name = @table_name"
    )
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("table_name", "STRING", table)]
        ),
    )
    columns = {str(row["column_name"]).lower(): str(row["column_name"]) for row in job.result()}
    if not columns:
        fail(f"Table `{project}.{dataset}.{table}` was not found or has no columns visible to this service account.")
    return columns


def resolve_column(columns: Dict[str, str], candidates: Sequence[str], where: str, required: bool = True) -> str:
    for candidate in candidates:
        real = columns.get(candidate.lower())
        if real:
            return real
    if required:
        fail(f"None of the columns {list(candidates)} exist in {where}. Found: {sorted(columns.values())}")
    return ""


# ---------------------------------------------------------------------------
# Payload parsing
# ---------------------------------------------------------------------------

def parse_payload(raw: Any) -> Optional[Dict[str, Any]]:
    if isinstance(raw, dict):
        return raw
    text = clean(raw)
    if not text:
        return None
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        return None
    if isinstance(parsed, str):  # double-encoded JSON
        try:
            parsed = json.loads(parsed)
        except json.JSONDecodeError:
            return None
    return parsed if isinstance(parsed, dict) else None


def iter_values(node: Any, key: str) -> Iterable[Any]:
    """Yield every value stored under `key` anywhere in a nested structure."""
    if isinstance(node, dict):
        for name, value in node.items():
            if name == key:
                yield value
            yield from iter_values(value, key)
    elif isinstance(node, list):
        for item in node:
            yield from iter_values(item, key)

def logistic_order_number_of(payload: Dict[str, Any]) -> str:
    for value in iter_values(payload, "logisticOrderNumber"):
        text = clean(value)
        if text:
            return text
    return ""



def table_column_types(client: bigquery.Client, project: str, dataset: str, table: str) -> Dict[str, str]:
    """Return {lowercase column name: BigQuery data type}, for the write-back literal."""
    sql = (
        f"SELECT column_name, data_type FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` "
        "WHERE table_name = @table_name"
    )
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("table_name", "STRING", table)]
        ),
    )
    return {str(row["column_name"]).lower(): str(row["data_type"]) for row in job.result()}


def plain(value: Any) -> Any:
    """
    A BigQuery value as plain Python.

    A STRUCT comes back as a Row, which is a Sequence and not a dict, so `iter_values`
    would walk its values and lose the field names. Rows expose `keys()`, so they are
    turned into dicts; lists are converted element by element.
    """
    if isinstance(value, Mapping):
        return {key: plain(item) for key, item in value.items()}
    if hasattr(value, "keys") and callable(getattr(value, "keys")):
        try:
            return {key: plain(value[key]) for key in value.keys()}
        except Exception:  # not a Row after all
            return value
    if isinstance(value, (list, tuple)):
        return [plain(item) for item in value]
    return value


def table_items_of(value: Any, product_keys: Sequence[str]) -> frozenset[str]:
    """
    The product identifiers held by the shipment_items column, whatever its shape.

    The column has been seen as a repeated field, as a repeated STRUCT, as a JSON
    string and as a plain comma separated list, so all four are read:

        ["1234567890123", "1234567890124"]
        [{"sku": "ABC", "quantity": 1}, ...]
        '[{"upc": "1234567890123"}]'
        '1234567890123, 1234567890124'

    Identifiers are upper-cased and stripped, exactly as `products_of` does for an
    event payload, so the two sets are comparable.
    """
    node = plain(value)
    products: set[str] = set()

    def walk(item: Any) -> None:
        if item is None:
            return
        if isinstance(item, dict):
            found = item_products(item, product_keys)
            if found:
                products.update(found)
            else:
                # A mapping naming its product some other way: keep the leaf values.
                for nested in item.values():
                    walk(nested)
            return
        if isinstance(item, (list, tuple)):
            for nested in item:
                walk(nested)
            return
        text = clean(item)
        if not text:
            return
        if text[0] in "[{":
            parsed = None
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = None
            if parsed is not None:
                walk(parsed)
                return
        for token in re.split(r"[,\s;|]+", text):
            token = token.strip().strip("\"'").upper()
            if token:
                products.add(token)

    walk(node)
    return frozenset(products)


def shipment_order_number_of(shipment: Dict[str, Any]) -> str:
    """
    The shipmentOrderNumber a Kibo shipment carries, which lives under `data`.

        data.shipmentOrderNumber   "DG-EUA01688483"

    Used to look the shipment up in the cancellation table, whose
    shipment_order_number column holds either this or the Kibo shipmentNumber.
    """
    for key in ("shipmentOrderNumber", "shipment_order_number"):
        value = clean(shipment.get(key))
        if value:
            return value
    data = shipment.get("data")
    if isinstance(data, dict):
        for key in ("shipmentOrderNumber", "shipment_order_number", "xpoOrderId"):
            value = clean(data.get(key))
            if value:
                return value
    return ""


def cancellation_key(value: Any) -> str:
    """
    The shape a shipment identifier is compared in: trimmed and upper-cased.

    The same expression is used on the SQL side of the write-back
    (`UPPER(TRIM(CAST(... AS STRING)))`), so a key that matches here matches there.
    """
    return clean(value).upper()


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Candidate:
    shipment_order_number: str
    tenant_id: str
    site_id: str
    oms_shipment_status: str = ""
    oms_shipment_number: str = ""
    brand: str = ""
    oms_tenant_id: str = ""
    oms_site_id: str = ""
    tenant_source: str = ""
    sixth_character: str = ""
    order_pattern: str = ""
    latest_event_type: str = ""
    latest_event_shipment_order_number: str = ""
    latest_event_timestamp: str = ""
    has_cancelled_event: bool = False
    cancelled_products: str = ""
    cancelled_order_generations: str = ""
    logistic_order_number: str = ""
    external_order_id: str = ""
    order_shipments: str = ""
    cancel_target_candidates: str = ""
    cancel_target_source: str = ""
    cancel_target_status: str = ""
    cancel_target_location: str = ""
    kibo_shipment_number: str = ""
    target_shipment_order_number: str = ""
    table_shipment_order_number: str = ""
    table_matched_on: str = ""
    table_items: str = ""
    reason_code: str = ""
    more_info: str = ""
    kibo_status_before: str = ""
    kibo_status_after: str = ""
    kibo_reason_after: str = ""
    table_updated: str = ""
    action: str = "skipped"
    success: bool = False
    error: str = ""
    response_excerpt: str = ""
    correlation_id: str = ""


@dataclass
class OrderEvents:
    """
    The warehouse events of one ORDER, across every generation.

    The active shipment of an order usually carries NO events of its own: the warehouse
    cancels generation F, Kibo immediately reassigns into a brand new generation G, and
    G is what sits READY in oms_active_shipments while every CANCELLED event still names
    A to F. Keying the lookup on the active row's own shipmentOrderNumber therefore
    finds nothing and the order is skipped forever, which is why the events are read per
    order instead.

    Every event type is read, not just the cancellations: the rule is that the LAST
    event of the order is a CANCELLED one, so a later PICKED or SHIPPED has to be
    visible for it to win.
    """
    has_cancelled: bool = False
    # The newest event of the order, whatever its type, and whether the rows really
    # came back newest first — which only holds when the table has a timestamp column.
    latest_event_type: str = ""
    latest_event_son: str = ""
    latest_event_timestamp: str = ""
    latest_event_payload: Optional[Dict[str, Any]] = None
    ordered: bool = False
    # The items of the newest CANCELLED event: what the cancel target has to carry.
    latest_cancelled_products: frozenset = frozenset()
    # Every item named by any CANCELLED event of the order, as a fallback.
    cancelled_products: frozenset = frozenset()
    cancelled_sons: List[str] = field(default_factory=list)
    cancelled_logistic_numbers: List[str] = field(default_factory=list)


@dataclass
class CancellationRow:
    """One orders_cancellation_table row: a shipment cleared for cancellation."""
    shipment_order_number: str = ""
    items: frozenset = frozenset()
    raw_items: str = ""
    reason_code: str = ""
    more_info: str = ""
    cancelled_on_kibo: bool = False


# ---------------------------------------------------------------------------
# BigQuery queries
# ---------------------------------------------------------------------------

def fetch_active_shipments(
    client: bigquery.Client,
    only_numbers: Sequence[str],
    limit: int,
    oms_status: str,
) -> List[Candidate]:
    columns = table_columns(client, BI_PROJECT, BI_DATASET, BI_TABLE)
    where = f"`{BI_PROJECT}.{BI_DATASET}.{BI_TABLE}`"
    number_col = resolve_column(columns, SHIPMENT_NUMBER_CANDIDATES, where)
    tenant_col = resolve_column(columns, ("tenant_id", "tenantId"), where)
    site_col = resolve_column(columns, ("site_id", "siteId"), where)
    status_col = resolve_column(columns, OMS_STATUS_CANDIDATES, where, required=bool(oms_status))
    oms_number_col = resolve_column(columns, OMS_NUMBER_CANDIDATES, where, required=False)
    if not oms_number_col:
        warn(
            f"{BI_TABLE} has no shipment number column ({list(OMS_NUMBER_CANDIDATES)}); "
            "the order will be looked up from the shipmentOrderNumber alone."
        )

    filters = [f"`{number_col}` IS NOT NULL", f"TRIM(CAST(`{number_col}` AS STRING)) != ''"]
    params: List[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter] = []
    if oms_status:
        filters.append(f"UPPER(TRIM(CAST(`{status_col}` AS STRING))) = UPPER(@oms_status)")
        params.append(bigquery.ScalarQueryParameter("oms_status", "STRING", oms_status))
    if only_numbers:
        filters.append(f"CAST(`{number_col}` AS STRING) IN UNNEST(@only_numbers)")
        params.append(bigquery.ArrayQueryParameter("only_numbers", "STRING", list(only_numbers)))

    status_select = f"CAST(`{status_col}` AS STRING)" if status_col else "CAST(NULL AS STRING)"
    oms_number_select = f"CAST(`{oms_number_col}` AS STRING)" if oms_number_col else "CAST(NULL AS STRING)"
    sql = f"""
        SELECT DISTINCT
            CAST(`{number_col}` AS STRING) AS shipment_order_number,
            CAST(`{tenant_col}` AS STRING) AS tenant_id,
            CAST(`{site_col}`   AS STRING) AS site_id,
            {status_select} AS oms_shipment_status,
            {oms_number_select} AS oms_shipment_number
        FROM `{BI_PROJECT}.{BI_DATASET}.{BI_TABLE}`
        WHERE {' AND '.join(filters)}
        ORDER BY shipment_order_number
    """
    if limit > 0:
        sql += f"\n        LIMIT {int(limit)}"

    job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
    candidates: List[Candidate] = []
    seen: set[Tuple[str, str, str]] = set()
    for row in job.result():
        number = clean(row["shipment_order_number"])
        tenant = clean(row["tenant_id"])
        site = clean(row["site_id"])
        if not number:
            continue
        if not tenant or not site:
            warn(f"{number}: missing tenant_id or site_id in {BI_TABLE}, skipping.")
            continue
        key = (number, tenant, site)
        if key in seen:
            continue
        seen.add(key)
        candidates.append(Candidate(
            shipment_order_number=number,
            tenant_id=tenant,
            site_id=site,
            oms_shipment_status=clean(row["oms_shipment_status"]),
            oms_shipment_number=clean(row["oms_shipment_number"]),
        ))
    return candidates


def fetch_order_events(
    client: bigquery.Client,
    order_patterns: Sequence[str],
    product_keys: Sequence[str],
    pattern_batch_size: int,
) -> Dict[str, OrderEvents]:
    """
    Return {orderPattern: OrderEvents}, reading prd_shipmentStatus once per order.

        SELECT * FROM `tlg-wlfs-prd`.til.prd_shipmentStatus
        WHERE shipmentOrderNumber LIKE 'FE-EU%00136449'

    One query carries many patterns; each row is handed back to the pattern or patterns
    it belongs to in Python. EVERY event type is read, because the rule is about the
    last event of the order rather than about the cancellations alone: a PICKED or a
    SHIPPED arriving after the cancellation has to be able to win.

    Rows come back newest first when the table has a usable timestamp column, so the
    first row of an order is its latest event. Without such a column `ordered` is False
    and the caller falls back to "a CANCELLED event exists", with a warning.
    """
    where = f"`{WLFS_PROJECT}.{WLFS_DATASET}.{SHIPMENT_STATUS_TABLE}`"
    columns = table_columns(client, WLFS_PROJECT, WLFS_DATASET, SHIPMENT_STATUS_TABLE)
    number_col = resolve_column(columns, SHIPMENT_NUMBER_CANDIDATES, where)
    payload_col = resolve_column(columns, PAYLOAD_CANDIDATES, where)
    event_col = resolve_column(columns, EVENT_TYPE_CANDIDATES, where)
    ts_col = resolve_column(columns, TIMESTAMP_CANDIDATES, where, required=False)

    order_by = f"ORDER BY `{ts_col}` DESC" if ts_col else ""
    ts_select = f"CAST(`{ts_col}` AS STRING)" if ts_col else "CAST(NULL AS STRING)"
    sql = f"""
        SELECT
            CAST(`{number_col}` AS STRING) AS shipment_order_number,
            UPPER(CAST(`{event_col}` AS STRING)) AS event_type,
            {ts_select} AS event_timestamp,
            CAST(`{payload_col}` AS STRING) AS payload
        FROM {where}
        WHERE EXISTS (
              SELECT 1 FROM UNNEST(@order_patterns) AS order_pattern
              WHERE CAST(`{number_col}` AS STRING) LIKE order_pattern
          )
        {order_by}
    """

    found: Dict[str, OrderEvents] = {pattern: OrderEvents() for pattern in order_patterns}
    for batch in chunked(list(order_patterns), max(1, pattern_batch_size)):
        job = client.query(
            sql,
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ArrayQueryParameter("order_patterns", "STRING", batch),
            ]),
        )
        for row in job.result():
            number = clean(row["shipment_order_number"])
            if not number:
                continue
            event_type = clean(row["event_type"]).upper()
            timestamp = clean(row["event_timestamp"])
            payload = parse_payload(row["payload"])
            products = products_of(payload, product_keys) if payload else frozenset()
            logistic = logistic_order_number_of(payload) if payload else ""

            for pattern in batch:
                if not matches_order_pattern(number, pattern):
                    continue
                events = found.setdefault(pattern, OrderEvents())
                events.ordered = bool(ts_col)
                # Rows arrive newest first, so the first one seen for an order is its
                # latest warehouse event, whatever its type.
                if not events.latest_event_type:
                    events.latest_event_type = event_type
                    events.latest_event_son = number
                    events.latest_event_timestamp = timestamp
                    events.latest_event_payload = payload
                    if event_type in CANCELLED_EVENT_TYPES:
                        events.latest_cancelled_products = products
                if event_type in CANCELLED_EVENT_TYPES:
                    events.has_cancelled = True
                    events.cancelled_products |= products
                    if number not in events.cancelled_sons:
                        events.cancelled_sons.append(number)
                    if logistic and logistic not in events.cancelled_logistic_numbers:
                        events.cancelled_logistic_numbers.append(logistic)
    return found


def fetch_cancellation_rows(
    client: bigquery.Client, product_keys: Sequence[str],
) -> Tuple[Dict[str, CancellationRow], int, bool, bool]:
    """
    The cancellation list, keyed by shipment identifier.

        SELECT shipment_order_number, shipment_items, reason_code, more_info
        FROM `tlg-business-intelligence-prd`.til.orders_cancellation_table
        WHERE cancelled_on_kibo is not true

    The whole open list is read in one go rather than queried per shipment: it is the
    gate every cancellation has to pass, and one scan is cheaper than one query per
    candidate order.

    The flag is compared as text so the column can be a BOOL, a STRING or an INT64, and
    NULL counts as "not cancelled yet".

    Returns ({key: CancellationRow}, rows read, reason_code column present,
    more_info column present). Keys are the shipment_order_number values as written in
    the table, normalised with `cancellation_key`.
    """
    where = f"`{BI_PROJECT}.{BI_DATASET}.{CANCELLATION_TABLE}`"
    columns = table_columns(client, BI_PROJECT, BI_DATASET, CANCELLATION_TABLE)
    number_col = resolve_column(columns, CANCELLATION_NUMBER_CANDIDATES, where)
    items_col = resolve_column(columns, CANCELLATION_ITEMS_CANDIDATES, where)
    flag_col = resolve_column(columns, CANCELLATION_FLAG_CANDIDATES, where)
    reason_col = resolve_column(columns, CANCELLATION_REASON_CANDIDATES, where, required=False)
    more_info_col = resolve_column(columns, CANCELLATION_MORE_INFO_CANDIDATES, where, required=False)
    if not reason_col:
        warn(
            f"{CANCELLATION_TABLE} has no reason code column "
            f"({list(CANCELLATION_REASON_CANDIDATES)}); --default-reason-code is used instead."
        )
    if not more_info_col:
        warn(
            f"{CANCELLATION_TABLE} has no moreInfo column "
            f"({list(CANCELLATION_MORE_INFO_CANDIDATES)}); --default-more-info is used instead."
        )

    reason_select = f"`{reason_col}`" if reason_col else "CAST(NULL AS STRING)"
    more_info_select = f"`{more_info_col}`" if more_info_col else "CAST(NULL AS STRING)"
    sql = f"""
        SELECT
            CAST(`{number_col}` AS STRING) AS shipment_order_number,
            `{items_col}` AS shipment_items,
            CAST({reason_select} AS STRING) AS reason_code,
            CAST({more_info_select} AS STRING) AS more_info,
            `{flag_col}` AS cancelled_on_kibo
        FROM {where}
        WHERE IFNULL(UPPER(TRIM(CAST(`{flag_col}` AS STRING))), '') NOT IN UNNEST(@truthy)
          AND `{number_col}` IS NOT NULL
          AND TRIM(CAST(`{number_col}` AS STRING)) != ''
    """

    rows: Dict[str, CancellationRow] = {}
    read = 0
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(query_parameters=[
            bigquery.ArrayQueryParameter("truthy", "STRING", list(TRUTHY_FLAG_VALUES)),
        ]),
    )
    for row in job.result():
        read += 1
        number = clean(row["shipment_order_number"])
        if not number:
            continue
        items = table_items_of(row["shipment_items"], product_keys)
        entry = CancellationRow(
            shipment_order_number=number,
            items=items,
            raw_items=format_products(items),
            reason_code=clean(row["reason_code"]),
            more_info=clean(row["more_info"]),
            cancelled_on_kibo=truthy_flag(row["cancelled_on_kibo"]),
        )
        key = cancellation_key(number)
        existing = rows.get(key)
        if existing is None:
            rows[key] = entry
        else:
            # The same shipment listed twice: the items of both rows are what the
            # table says about it, so they are merged rather than one silently winning.
            existing.items = existing.items | entry.items
            existing.raw_items = format_products(existing.items)
            existing.reason_code = existing.reason_code or entry.reason_code
            existing.more_info = existing.more_info or entry.more_info
    return rows, read, bool(reason_col), bool(more_info_col)


def mark_cancelled_on_kibo(
    client: bigquery.Client, shipment_order_numbers: Sequence[str],
) -> Tuple[int, str]:
    """
    Flag the cancelled rows in orders_cancellation_table, in one statement.

        UPDATE `tlg-business-intelligence-prd`.til.orders_cancellation_table
        SET cancelled_on_kibo = TRUE
        WHERE shipment_order_number IN UNNEST(@numbers)

    The literal matches the column's own type, so a BOOL column gets TRUE, a STRING one
    'true' and an INT64 one 1. Returns (rows updated, error).
    """
    if not shipment_order_numbers:
        return 0, ""
    where = f"`{BI_PROJECT}.{BI_DATASET}.{CANCELLATION_TABLE}`"
    columns = table_columns(client, BI_PROJECT, BI_DATASET, CANCELLATION_TABLE)
    number_col = resolve_column(columns, CANCELLATION_NUMBER_CANDIDATES, where)
    flag_col = resolve_column(columns, CANCELLATION_FLAG_CANDIDATES, where)
    types = table_column_types(client, BI_PROJECT, BI_DATASET, CANCELLATION_TABLE)
    flag_type = types.get(flag_col.lower(), "BOOL").upper()
    if flag_type.startswith("STRING"):
        literal = "'true'"
    elif flag_type.startswith(("INT", "NUMERIC", "BIGNUMERIC", "FLOAT")):
        literal = "1"
    else:
        literal = "TRUE"

    sql = f"""
        UPDATE {where}
        SET `{flag_col}` = {literal}
        WHERE UPPER(TRIM(CAST(`{number_col}` AS STRING))) IN UNNEST(@numbers)
          AND IFNULL(UPPER(TRIM(CAST(`{flag_col}` AS STRING))), '') NOT IN UNNEST(@truthy)
    """
    try:
        job = client.query(
            sql,
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ArrayQueryParameter(
                    "numbers", "STRING",
                    sorted({cancellation_key(n) for n in shipment_order_numbers if clean(n)}),
                ),
                bigquery.ArrayQueryParameter("truthy", "STRING", list(TRUTHY_FLAG_VALUES)),
            ]),
        )
        job.result()
    except Exception as exc:  # the cancellations themselves already went through
        return 0, str(exc)
    return int(job.num_dml_affected_rows or 0), ""


# ---------------------------------------------------------------------------
# Choosing the shipment to cancel
# ---------------------------------------------------------------------------

@dataclass
class CancelTarget:
    """The shipment of the order that should actually be cancelled."""
    shipment: Optional[Dict[str, Any]] = None
    shipment_number: str = ""
    shipment_order_number: str = ""
    status: str = ""
    location: str = ""
    source: str = ""
    considered: str = ""
    matches: List[str] = field(default_factory=list)
    row: Optional[CancellationRow] = None
    table_key: str = ""
    table_matched_on: str = ""
    error: str = ""
    ambiguous: bool = False


def describe_shipments(shipments: Sequence[Dict[str, Any]], limit: int = 12) -> str:
    parts = [
        f"{shipment_number_of(s) or '?'}:{shipment_status(s) or '?'}"
        for s in shipments
    ]
    if len(parts) > limit:
        return ", ".join(parts[:limit]) + f", +{len(parts) - limit} more"
    return ", ".join(parts)


def cancellation_row_for(
    shipment: Dict[str, Any], rows: Mapping[str, CancellationRow],
) -> Tuple[Optional[CancellationRow], str, str]:
    """
    The orders_cancellation_table row listing this shipment, if there is one.

    The column is called shipment_order_number, but the number written in it has been
    seen as both the shipmentOrderNumber ("DJ-USAST0000124550") and the Kibo
    shipmentNumber ("82902"), so both identifiers of the shipment are looked up.

    Returns (row, the key it matched on, which identifier that was).
    """
    for label, value in (
        ("shipmentOrderNumber", shipment_order_number_of(shipment)),
        ("shipmentNumber", shipment_number_of(shipment)),
    ):
        key = cancellation_key(value)
        if not key:
            continue
        row = rows.get(key)
        if row is not None:
            return row, key, label
    return None, "", ""


def resolve_cancel_target(
    shipments: Sequence[Dict[str, Any]],
    event_products: frozenset,
    product_keys: Sequence[str],
    product_match: str,
    excluded_statuses: Sequence[str],
    on_multiple: str,
    wanted_statuses: Sequence[str],
    location_pattern: str,
    cancellation_rows: Mapping[str, CancellationRow],
) -> CancelTarget:
    """
    Pick, out of the shipments of one order, the one to cancel.

    The CANCELLED event names the shipment the warehouse was working on, and Kibo has
    already reassigned it by the time this job runs: that shipmentNumber is REASSIGNED
    and will not accept a cancellation, while whatever the order fell back to will.
    The target is the shipment of the same order that

      - has shipmentStatus BACKORDER or CUSTOMER_CARE (`wanted_statuses`)
      - is in a cancellable status: not already CANCELED or otherwise terminal, and not
        REASSIGNED, which Kibo refuses too (plus any status named in the excluded list)
      - carries the exact same items as the CANCELLED event
      - has a row in orders_cancellation_table with cancelled_on_kibo = false, whose
        shipment_items are those same items

    The table is the gate: a shipment that matches on status and items but is not
    listed is left alone, because the table is what says a cancellation was agreed.

    Returns a CancelTarget whose `shipment_number` is empty when nothing matched;
    `error` then says how far down the chain the order got.
    """
    target = CancelTarget(considered=describe_shipments(shipments))
    by_status = [
        shipment for shipment in shipments
        if is_cancellable_status(shipment_status(shipment), excluded_statuses)
        and is_wanted_status(shipment_status(shipment), wanted_statuses)
        and matches_like(shipment_fulfillment_location(shipment), location_pattern)
    ]
    by_products = [
        shipment for shipment in by_status
        if shipment_matches_products(shipment, event_products, product_keys, product_match)[0]
    ]
    target.matches = [shipment_number_of(s) for s in by_products if shipment_number_of(s)]

    listed: List[Tuple[Dict[str, Any], CancellationRow, str, str]] = []
    items_mismatch: List[str] = []
    not_listed: List[str] = []
    for shipment in by_products:
        row, key, matched_on = cancellation_row_for(shipment, cancellation_rows)
        if row is None:
            not_listed.append(shipment_number_of(shipment) or "?")
            continue
        if not products_match(event_products, row.items, product_match):
            items_mismatch.append(
                f"{shipment_number_of(shipment) or '?'} (table: "
                f"{row.raw_items or 'no items'})"
            )
            continue
        listed.append((shipment, row, key, matched_on))

    if not listed:
        if items_mismatch:
            target.error = (
                f"{CANCELLATION_TABLE} lists {', '.join(items_mismatch)}, but its "
                f"shipment_items are not the items of the CANCELLED event "
                f"({format_products(event_products) or 'none'})."
            )
        elif not_listed:
            target.error = (
                f"No row in {CANCELLATION_TABLE} with cancelled_on_kibo = false for "
                f"{', '.join(not_listed)}."
            )
        elif by_status:
            target.error = (
                f"No {', '.join(wanted_statuses) or 'cancellable'} shipment of the order "
                f"carries the items of the CANCELLED event "
                f"({format_products(event_products) or 'none'}); considered "
                f"{describe_shipments(by_status)}."
            )
        return target

    if len(listed) > 1:
        numbers = [shipment_number_of(s) or "?" for s, _, _, _ in listed]
        if on_multiple != ON_MULTIPLE_NEWEST:
            target.ambiguous = True
            target.error = (
                f"{len(listed)} shipments of the order are cancellable, carry the same "
                f"items and are listed in {CANCELLATION_TABLE} ({', '.join(numbers)}); "
                "--on-multiple-cancel-targets newest would take the most recent one."
            )
            return target
        listed.sort(
            key=lambda entry: (shipment_created_at(entry[0]), shipment_number_of(entry[0])),
            reverse=True,
        )

    shipment, row, key, matched_on = listed[0]
    target.shipment = shipment
    target.shipment_number = shipment_number_of(shipment)
    target.shipment_order_number = shipment_order_number_of(shipment)
    target.status = shipment_status(shipment)
    target.location = shipment_fulfillment_location(shipment)
    target.row = row
    target.table_key = key
    target.table_matched_on = matched_on
    target.source = f"order lookup, listed in {CANCELLATION_TABLE} by {matched_on}"
    if not target.shipment_number:
        target.shipment = None
        target.source = ""
        target.error = "The matching shipment of the order carries no shipmentNumber."
    return target


# ---------------------------------------------------------------------------
# Kibo client
# ---------------------------------------------------------------------------

class KiboClient:
    def __init__(self, credentials: Dict[str, str], auth_url: str, host: str, timeout: float) -> None:
        self.credentials = credentials
        self.auth_url = auth_url
        self.host = host
        self.timeout = timeout
        self.session = requests.Session()
        self._tokens: Dict[str, Tuple[str, float]] = {}

    def token_for_tenant(self, tenant_id: str, force_refresh: bool = False) -> str:
        tenant_id = clean(tenant_id)
        cached = self._tokens.get(tenant_id)
        if cached and not force_refresh and time.time() < cached[1]:
            return cached[0]

        response = self.session.post(
            self.auth_url,
            headers={
                "Accept": "application/json",
                "Content-Type": "application/json",
                "Cache-Control": "no-cache",
                "x-vol-tenant": tenant_id,
            },
            json={
                "grant_type": "client_credentials",
                "client_id": self.credentials["client_id"],
                "client_secret": self.credentials["client_secret"],
            },
            timeout=self.timeout,
        )
        if not (200 <= response.status_code < 300):
            fail(f"Kibo authentication failed for tenant {tenant_id} (HTTP {response.status_code}): {excerpt_of(response)}")
        try:
            data = response.json()
        except ValueError:
            fail("Kibo authentication response was not JSON.")
            return ""

        token = clean(data.get("access_token") or data.get("accessToken") or data.get("token"))
        if not token:
            fail(f"No access token in the Kibo authentication response for tenant {tenant_id}.")

        try:
            expires_in = float(data.get("expires_in", data.get("expiresIn", 600)))
        except (TypeError, ValueError):
            expires_in = 600.0
        if expires_in > 86400:  # milliseconds
            expires_in /= 1000.0
        expires_in = max(60.0, expires_in)
        margin = min(60.0, max(5.0, expires_in * 0.10))
        self._tokens[tenant_id] = (token, time.time() + expires_in - margin)
        return token

    def headers(self, tenant_id: str, site_id: str, correlation_id: str = "") -> Dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.token_for_tenant(tenant_id)}",
            "x-vol-tenant": tenant_id,
        }
        if site_id:
            headers["x-vol-site"] = site_id
        if correlation_id:
            headers["x-vol-correlation"] = correlation_id
        return headers

    def request(
        self,
        method: str,
        url: str,
        tenant_id: str,
        site_id: str,
        json_body: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
        correlation_id: str = "",
        params: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Optional[Any], str]:
        last_error = ""
        last_response = None
        for attempt in range(1, max_retries + 1):
            try:
                response = self.session.request(
                    method,
                    url,
                    headers=self.headers(tenant_id, site_id, correlation_id),
                    json=json_body,
                    params=params,
                    timeout=self.timeout,
                )
                last_response = response
            except requests.RequestException as exc:
                last_error = str(exc)
                if attempt < max_retries:
                    time.sleep(retry_sleep_seconds(attempt))
                    continue
                return None, last_error

            if response.status_code == 401 and attempt < max_retries:
                self.token_for_tenant(tenant_id, force_refresh=True)
                time.sleep(retry_sleep_seconds(attempt, response))
                continue
            if response.status_code in RETRYABLE_STATUS_CODES and attempt < max_retries:
                time.sleep(retry_sleep_seconds(attempt, response))
                continue
            return response, ""
        return last_response, last_error or "Unknown error"

    def base_url(self, tenant_id: str, site_id: str, shipment_number: str) -> str:
        return (
            f"https://t{tenant_id}.{self.host}"
            f"/api/commerce/shipments/{quote(shipment_number, safe='')}"
        )

    def get_shipment(
        self, tenant_id: str, site_id: str, shipment_number: str, max_retries: int
    ) -> Tuple[Optional[Dict[str, Any]], str]:
        """
        One shipment by its number.

        NO x-vol-site header is sent, for the same reason as the order lookup: the site
        scopes the request, and a shipment that lives on another site of the same tenant
        comes back as HTTP 404 "Could not find shipment N" — indistinguishable from a
        number that does not exist. The shipmentNumber is unique within the tenant, so
        the tenant alone is enough to read it, and the shipment then reports its own
        siteId, which is what the later write calls use.
        """
        response, error = self.request(
            "GET", self.base_url(tenant_id, site_id, shipment_number),
            tenant_id, "", max_retries=max_retries,
        )
        if response is None:
            return None, error
        if not (200 <= response.status_code < 300):
            return None, f"HTTP {response.status_code}: {excerpt_of(response, 300)}"
        try:
            return response.json(), ""
        except ValueError:
            return None, "Shipment response was not JSON."

    def shipments_url(self, tenant_id: str) -> str:
        return f"https://t{tenant_id}.{self.host}/api/commerce/shipments"

    def get_shipments_by_external_order_id(
        self, tenant_id: str, site_id: str, external_order_id: str,
        max_retries: int, page_size: int,
    ) -> Tuple[List[Dict[str, Any]], str]:
        """
        Every shipment of one order.

            GET /api/commerce/shipments?filter=externalOrderId=={externalOrderId}

        The Shipment API has its own filter syntax — `field==value`, clauses joined by
        ';' — unlike the `field eq value` of the other Kibo APIs. Only the
        externalOrderId clause is sent; the statuses are filtered in Python so that one
        response shows every sibling shipment, whatever its status, in the CSV.

        NO x-vol-site header is sent. The site scopes the query, so a site that does not
        hold the order's shipments turns the answer into an empty page with a 200 rather
        than an error, which is indistinguishable from an order that really has no
        shipments. The tenant alone is what identifies the order here.
        """
        shipments: List[Dict[str, Any]] = []
        page = 0
        while True:
            response, error = self.request(
                "GET", self.shipments_url(tenant_id), tenant_id, "",
                max_retries=max_retries,
                params={
                    "filter": SHIPMENTS_FILTER_TEMPLATE.format(value=external_order_id),
                    "pageSize": int(page_size),
                    "page": page,
                },
            )
            if response is None:
                return shipments, error
            if not (200 <= response.status_code < 300):
                return shipments, f"HTTP {response.status_code}: {excerpt_of(response, 300)}"
            try:
                payload = response.json()
            except ValueError:
                return shipments, "The shipments response was not JSON."

            batch = shipments_in_response(payload)
            shipments.extend(batch)

            # {"_embedded": {...}, "page": {"size", "totalElements", "totalPages",
            #  "number"}}. Stop on the last page, and stop too if a page comes back
            # empty, so a server that ignores `page` cannot loop forever.
            page_info = payload.get("page") if isinstance(payload, dict) else None
            total_pages = 0
            if isinstance(page_info, dict):
                try:
                    total_pages = int(page_info.get("totalPages") or 0)
                except (TypeError, ValueError):
                    total_pages = 0
            page += 1
            if not batch or total_pages <= page:
                break
        return shipments, ""
    def mark_canceled(
        self, tenant_id: str, site_id: str, shipment_number: str,
        reason_code: str, more_info: str, max_retries: int, correlation_id: str,
    ) -> Tuple[bool, str, str]:
        """
        Cancel one shipment, with the reason the cancellation table carries.

        The CancelShipment schema nests the reason under `canceledReason`. A flat
        {"reasonCode": ..., "moreInfo": ...} body is accepted with a 200 but silently
        ignored, and Kibo records its own default reason ("Canceled") instead.
        """
        url = f"{self.base_url(tenant_id, site_id, shipment_number)}/canceled"
        response, error = self.request(
            "PUT", url, tenant_id, site_id,
            json_body={"canceledReason": {
                "reasonCode": reason_code,
                "moreInfo": more_info or reason_code,
            }},
            max_retries=max_retries, correlation_id=correlation_id,
        )
        if response is None:
            return False, error, ""
        ok = 200 <= response.status_code < 300
        return ok, "" if ok else f"HTTP {response.status_code}", excerpt_of(response)


# ---------------------------------------------------------------------------
# Reading a Kibo shipment
# ---------------------------------------------------------------------------

def shipments_in_response(payload: Any) -> List[Dict[str, Any]]:
    """
    The shipments carried by a getShipments response, whatever shape it arrives in.

    The Shipment API answers with HAL:

        {"_embedded": {"shipments": [...]}, "_links": {...}, "page": {...}}

    so the list sits under _embedded, NOT at the top level. Reading only `items` or
    `shipments` from the top level returns nothing for every order, which looks exactly
    like an order with no shipments. The flatter shapes are kept as fallbacks in case a
    tenant or a future version answers differently.
    """
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    embedded = payload.get("_embedded")
    if isinstance(embedded, dict):
        for key in ("shipments", "items"):
            value = embedded.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    for key in ("shipments", "items"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def shipment_site_id(shipment: Dict[str, Any]) -> str:
    """
    The siteId Kibo reports on the shipment.

    This is the authority for the site, over both oms_active_shipments and the brand
    config: a shipment of tenant 29993 can sit on site 53604 while the brand table says
    53602, and every call that carries the wrong site either 404s or comes back empty.
    """
    for key in ("siteId", "site_id"):
        value = clean(shipment.get(key))
        if value:
            return value
    return ""


def shipment_external_order_id(shipment: Dict[str, Any]) -> str:
    """The shipment's externalOrderId, at the top level or inside its `data` object."""
    for key in EXTERNAL_ORDER_ID_KEYS:
        value = clean(shipment.get(key))
        if value:
            return value
    data = shipment.get("data")
    if isinstance(data, dict):
        for key in EXTERNAL_ORDER_ID_KEYS:
            value = clean(data.get(key))
            if value:
                return value
    return ""


def shipment_number_of(shipment: Dict[str, Any]) -> str:
    for key in ("shipmentNumber", "shipmentId", "number"):
        value = clean(shipment.get(key))
        if value:
            return value
    return ""


def shipment_status_reason(shipment: Dict[str, Any]) -> str:
    """The reasonCode Kibo actually recorded, from shipmentStatusReason."""
    node = shipment.get("shipmentStatusReason")
    if isinstance(node, dict):
        return clean(node.get("reasonCode"))
    return ""


def shipment_status(shipment: Dict[str, Any]) -> str:
    for key in ("shipmentStatus", "status"):
        value = clean(shipment.get(key))
        if value:
            return value
    for key in ("shipmentState", "workflowState"):
        node = shipment.get(key)
        if isinstance(node, dict):
            value = clean(node.get("status"))
            if value:
                return value
    return ""



# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

CSV_FIELDS = [
    "shipment_order_number", "tenant_id", "site_id", "brand",
    "oms_tenant_id", "oms_site_id", "tenant_source", "oms_shipment_status",
    "oms_shipment_number", "sixth_character", "order_pattern",
    "latest_event_type", "latest_event_shipment_order_number", "latest_event_timestamp",
    "has_cancelled_event", "cancelled_products", "cancelled_order_generations",
    "logistic_order_number", "external_order_id", "order_shipments",
    "cancel_target_candidates", "cancel_target_source", "cancel_target_status",
    "cancel_target_location", "kibo_shipment_number", "target_shipment_order_number",
    "table_shipment_order_number", "table_matched_on", "table_items",
    "reason_code", "more_info",
    "kibo_status_before", "kibo_status_after", "kibo_reason_after", "table_updated",
    "action", "success", "error", "response_excerpt", "correlation_id",
]


def write_csv(path: Path, candidates: List[Candidate]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for candidate in candidates:
            writer.writerow({key: asdict(candidate)[key] for key in CSV_FIELDS})


def write_job_summary(candidates: List[Candidate], dry_run: bool) -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    acted = [c for c in candidates if c.kibo_shipment_number]
    cancelled = [c for c in candidates if c.action in ("canceled", "would_cancel") and c.success]
    already = [c for c in candidates if c.action == "already_terminal"]
    excluded = [c for c in candidates if c.action == "sixth_character_excluded"]
    unknown_brand = [c for c in candidates if c.action == "unknown_brand"]
    not_cancelled_last = [c for c in candidates if c.action == "latest_event_not_cancelled"]
    no_events = [c for c in candidates if c.action == "no_cancelled_event"]
    no_target = [c for c in candidates if c.action == "no_cancel_target"]
    ambiguous = [c for c in candidates if c.action == "ambiguous_cancel_target"]
    lookup_failed = [c for c in candidates if c.action == "order_lookup_failed"]
    retenanted = [c for c in candidates if c.oms_tenant_id and (
        c.oms_tenant_id != c.tenant_id or c.oms_site_id != c.site_id)]
    failed = [c for c in candidates if c.error]
    flagged = [c for c in candidates if c.table_updated == "yes"]
    lines = [
        "## Cancel cancelled shipments",
        "",
        f"- Mode: **{'dry run' if dry_run else 'live'}**",
        f"- Rows (one per active shipment): **{len(candidates)}**",
        f"- Reached the cancel step: **{len(acted)}**",
        f"- Cancelled in Kibo: **{len(cancelled)}**",
        f"- Flagged cancelled_on_kibo in {CANCELLATION_TABLE}: **{len(flagged)}**",
        f"- Left alone, the order's last warehouse event is not a cancellation: "
        f"**{len(not_cancelled_last)}**",
        f"- Left alone, no CANCELLED event at all: **{len(no_events)}**",
        f"- Left alone, no BACKORDER/CUSTOMER_CARE shipment of the order listed in "
        f"{CANCELLATION_TABLE} with the same items: **{len(no_target)}**",
        f"- Left alone, several shipments of the order match: **{len(ambiguous)}**",
        f"- Already in a terminal status: **{len(already)}**",
        f"- Order lookup failed: **{len(lookup_failed)}**",
        f"- Tenant/site taken from the brand config rather than {BI_TABLE}: **{len(retenanted)}**",
        f"- Left alone, brand not in the Kibo config: **{len(unknown_brand)}**",
        f"- Left alone, 6th character not allowed: **{len(excluded)}**",
        f"- With an error: **{len(failed)}**",
        "",
    ]
    if acted:
        lines += [
            "| shipmentOrderNumber | tenant | site | shipmentNumber | target status | "
            "orderNumber | last event | items | table row | reasonCode | moreInfo | "
            "recorded reason | after | flagged | action | error |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
        for c in acted[:200]:
            lines.append(
                f"| {c.shipment_order_number} | {c.tenant_id} | {c.site_id} | "
                f"{c.kibo_shipment_number} | {c.cancel_target_status or '-'} | "
                f"{c.order_pattern or '-'} | {c.latest_event_type or '-'} | "
                f"{(c.cancelled_products or '-')[:60]} | "
                f"{c.table_shipment_order_number or '-'} | {c.reason_code or '-'} | "
                f"{(c.more_info or '-')[:40]} | {c.kibo_reason_after or '-'} | "
                f"{c.kibo_status_after or '-'} | {c.table_updated or '-'} | "
                f"{c.action} | {c.error[:120]} |"
            )
        if len(acted) > 200:
            lines.append(
                "| ... | | | | | | | | | | | | | | | "
                f"{len(acted) - 200} more rows in the CSV artifact |"
            )
    with open(summary_path, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true",
                        help="Run every check and read every shipment, but call neither the "
                             "canceled endpoint nor the cancellation table's UPDATE.")
    parser.add_argument("--oms-status", default="READY",
                        help="Keep only rows with this shipmentStatus in oms_active_shipments. "
                             "Pass an empty string to disable the filter.")
    parser.add_argument("--cancel-target-statuses", default=DEFAULT_CANCEL_TARGET_STATUSES,
                        help="Comma separated shipmentStatus values a shipment of the order must "
                             "carry to be cancelled. Default 'BACKORDER,CUSTOMER_CARE': where the "
                             "order lands once the warehouse has cancelled. Pass an empty string "
                             "to accept any cancellable status.")
    parser.add_argument("--product-match", default=DEFAULT_PRODUCT_MATCH,
                        choices=(PRODUCT_MATCH_EXACT, PRODUCT_MATCH_OVERLAP, PRODUCT_MATCH_ANY),
                        help="How 'the same items' is decided between the CANCELLED event, the "
                             "Kibo shipment and the table's shipment_items: 'exact' needs "
                             "identical sets, 'overlap' at least one item in common, 'any' "
                             "ignores items entirely.")
    parser.add_argument("--cancel-target-excluded-statuses",
                        default=DEFAULT_CANCEL_TARGET_EXCLUDED_STATUSES,
                        help="Extra shipment statuses never chosen as the cancel target, on top "
                             "of CANCELED, the other terminal ones and REASSIGNED, which Kibo "
                             "refuses anyway. Comma separated, empty by default.")
    parser.add_argument("--cancel-target-location-pattern",
                        default=DEFAULT_CANCEL_TARGET_LOCATION_PATTERN,
                        help="Optional: the cancel target's fulfillmentLocationCode must match "
                             "this SQL-style LIKE pattern, case-insensitively (e.g. '%%GX%%'). "
                             "Empty by default, since a backordered shipment sits in no "
                             "warehouse.")
    parser.add_argument("--on-multiple-cancel-targets", default=DEFAULT_ON_MULTIPLE_CANCEL_TARGETS,
                        choices=(ON_MULTIPLE_SKIP, ON_MULTIPLE_NEWEST),
                        help="What to do when several shipments of the order qualify: 'skip' "
                             "reports the row as ambiguous_cancel_target and touches nothing, "
                             "'newest' cancels the most recently created one.")
    parser.add_argument("--require-latest-cancelled", dest="require_latest_cancelled",
                        action="store_true",
                        help="Require the order's LAST warehouse event to be a CANCELLED one "
                             "(the default). With --no-require-latest-cancelled any order "
                             "carrying a CANCELLED event somewhere in its history qualifies.")
    parser.add_argument("--no-require-latest-cancelled", dest="require_latest_cancelled",
                        action="store_false",
                        help="Accept an order whose history contains a CANCELLED event even when "
                             "a later event has superseded it.")
    parser.set_defaults(require_latest_cancelled=True)
    parser.add_argument("--no-mark-cancelled", dest="mark_cancelled", action="store_false",
                        help=f"Do not set cancelled_on_kibo = true in {CANCELLATION_TABLE} after "
                             "a successful cancellation. By default the rows cancelled in this "
                             "run are flagged in one UPDATE at the end.")
    parser.set_defaults(mark_cancelled=True)
    parser.add_argument("--default-reason-code", default=DEFAULT_REASON_CODE,
                        help="reasonCode used when the table's reason_code is empty or the column "
                             "does not exist.")
    parser.add_argument("--default-more-info", default=DEFAULT_MORE_INFO,
                        help="moreInfo used when the table's more_info is empty or the column "
                             "does not exist.")
    parser.add_argument("--allow-unknown-brand", action="store_true",
                        help="Process a shipment whose brand is not in BRAND_KIBO_CONFIG, "
                             "using the tenant_id and site_id from oms_active_shipments. "
                             "Off by default: an unconfirmed tenant means a filtered Kibo "
                             "query can come back empty instead of failing.")
    parser.add_argument("--order-lookup-page-size", type=int, default=DEFAULT_ORDER_LOOKUP_PAGE_SIZE,
                        help="pageSize sent with GET /api/commerce/shipments when reading the "
                             "shipments of an order.")
    parser.add_argument("--product-keys", default=",".join(DEFAULT_PRODUCT_KEYS),
                        help="Comma separated keys read as product identifiers, looked for "
                             "anywhere in the CANCELLED requestPayload, on a Kibo shipment item "
                             "and in the table's shipment_items.")
    parser.add_argument("--pattern-batch-size", type=int, default=50,
                        help="Order LIKE patterns per BigQuery query when reading the warehouse "
                             "events. Smaller than --batch-size because LIKE cannot use an index "
                             "and every pattern widens the scan.")
    parser.add_argument("--allowed-sixth-characters", default=DEFAULT_ALLOWED_SIXTH_CHARACTERS,
                        help="Optional filter: only act on a shipment whose shipmentOrderNumber has "
                             "one of these as its 6th character, the generation letter (the 'E' of "
                             "DJ-USE00041696). Case-insensitive. Empty by default, so every "
                             "generation is in scope.")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Kibo host suffix.")
    parser.add_argument("--auth-url", default=DEFAULT_AUTH_URL, help="Kibo OAuth URL.")
    parser.add_argument("--limit", type=int, default=0, help="Limit the number of active shipments (0 = no limit).")
    parser.add_argument("--shipment-order-numbers", default="",
                        help="Optional comma or whitespace separated list to restrict the run.")
    parser.add_argument("--batch-size", type=int, default=500, help="Shipment order numbers per BigQuery query.")
    parser.add_argument("--timeout", type=float, default=60.0, help="HTTP timeout in seconds.")
    parser.add_argument("--max-retries", type=int, default=3, help="Maximum attempts per Kibo request.")
    parser.add_argument("--sleep-between-requests", type=float, default=0.2,
                        help="Pause between Kibo write requests, in seconds.")
    parser.add_argument("--output", default="", help="CSV output path.")
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("--batch-size must be at least 1.")
    if args.max_retries < 1:
        parser.error("--max-retries must be at least 1.")
    if args.pattern_batch_size < 1:
        parser.error("--pattern-batch-size must be at least 1.")
    if args.order_lookup_page_size < 1:
        parser.error("--order-lookup-page-size must be at least 1.")
    args.oms_status = args.oms_status.strip()
    args.cancel_target_location_pattern = args.cancel_target_location_pattern.strip()
    args.default_reason_code = args.default_reason_code.strip()
    args.default_more_info = args.default_more_info.strip()
    args.cancel_target_statuses = tuple(
        status.upper()
        for status in re.split(r"[,\s;]+", args.cancel_target_statuses.strip())
        if status
    )
    args.cancel_target_excluded_statuses = tuple(
        status.upper()
        for status in re.split(r"[,\s;]+", args.cancel_target_excluded_statuses.strip())
        if status
    )
    args.product_keys = tuple(
        key for key in re.split(r"[,\s;]+", args.product_keys.strip()) if key
    )
    if not args.product_keys and args.product_match != PRODUCT_MATCH_ANY:
        parser.error("--product-keys cannot be empty unless --product-match is 'any'.")
    # Accept "ABCD", "A,B,C,D" and "a b c d" alike.
    args.allowed_sixth_characters = "".join(
        sorted({c for c in args.allowed_sixth_characters.upper() if c.isalnum()})
    )
    args.host = re.sub(r"^https?://", "", args.host.strip()).split("/", 1)[0].rstrip(".")
    return args


def split_numbers(raw: str) -> List[str]:
    return [item for item in re.split(r"[,\s;]+", raw.strip()) if item]


def main() -> int:
    args = parse_args()
    started = datetime.now(timezone.utc)
    output_path = Path(args.output or f"cancel_cancelled_shipments_{started.strftime('%Y%m%d_%H%M%S')}.csv")

    log(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    log(f"oms_active_shipments status filter: {args.oms_status or '(none)'}")
    log(
        "The order's LAST warehouse event must be a cancellation."
        if args.require_latest_cancelled else
        "Any order with a CANCELLED event somewhere in its history is in scope."
    )
    log(
        "Cancel target: a shipment of the same order, found with GET "
        "/api/commerce/shipments?filter=externalOrderId==..., with shipmentStatus "
        + (", ".join(args.cancel_target_statuses) or "any cancellable one")
        + (
            f", fulfillmentLocationCode LIKE '{args.cancel_target_location_pattern}'"
            if args.cancel_target_location_pattern else ""
        )
        + f", the same items as the CANCELLED event (--product-match {args.product_match}), "
        f"on several matches: {args.on_multiple_cancel_targets}."
    )
    log(
        f"Gate: the target must have a row in {CANCELLATION_TABLE} with "
        "cancelled_on_kibo = false whose shipment_items are those same items. "
        "reasonCode and moreInfo are taken from that row."
    )
    log(
        f"After a cancellation the row is flagged cancelled_on_kibo = true in "
        f"{CANCELLATION_TABLE}."
        if args.mark_cancelled else
        f"{CANCELLATION_TABLE} is never written to (--no-mark-cancelled)."
    )
    if args.allowed_sixth_characters:
        log(
            "6th character of shipmentOrderNumber must be one of "
            f"{args.allowed_sixth_characters}"
        )
    log(f"Kibo host: {args.host}")

    kibo_credentials = load_json_env("KIBO_API_CREDS")
    client_id = clean(kibo_credentials.get("client_id"))
    client_secret = clean(kibo_credentials.get("client_secret"))
    if not client_id or not client_secret:
        fail("KIBO_API_CREDS must contain non-empty 'client_id' and 'client_secret'.")

    bi_client = bigquery_client("BI_BIGQUERY_CREDS", BI_PROJECT)
    wlfs_client = bigquery_client("TIL_BIGQUERY_CREDS", WLFS_PROJECT)

    only_numbers = split_numbers(args.shipment_order_numbers)
    if only_numbers:
        log(f"Restricting the run to {len(only_numbers)} shipment order number(s).")

    candidates = fetch_active_shipments(bi_client, only_numbers, args.limit, args.oms_status)
    log(
        f"Active shipments read from {BI_TABLE}"
        + (f" with shipmentStatus = {args.oms_status}" if args.oms_status else "")
        + f": {len(candidates)}"
    )
    if not candidates:
        log("Nothing to do.")
        write_csv(output_path, candidates)
        write_job_summary(candidates, args.dry_run)
        return 0

    # The cancellation list. Read once: it is the gate every cancellation has to pass,
    # and one scan is cheaper than a query per order.
    cancellation_rows, rows_read, has_reason_col, has_more_info_col = fetch_cancellation_rows(
        bi_client, args.product_keys
    )
    log(
        f"Open rows in {CANCELLATION_TABLE} (cancelled_on_kibo = false): {rows_read}"
        f", distinct shipment numbers: {len(cancellation_rows)}"
    )
    if not cancellation_rows:
        log(
            f"{CANCELLATION_TABLE} has no open row, so nothing can be cancelled in "
            "this run."
        )

    # Excluded shipments are not queried either: the 6th-character rule decides before
    # anything else, so there is no reason to read their events.
    numbers = [
        c.shipment_order_number for c in candidates
        if sixth_character_allowed(c.shipment_order_number, args.allowed_sixth_characters)
    ]
    excluded_count = len(candidates) - len(numbers)
    if excluded_count:
        log(f"Left alone, 6th character not in {args.allowed_sixth_characters}: {excluded_count}")

    # One read per ORDER, covering every generation. The active shipment of an order
    # rarely carries events of its own, so this is what makes an order visible at all.
    all_patterns = sorted({
        pattern for pattern in (order_pattern_of(number) for number in numbers) if pattern
    })
    without_pattern = sum(1 for number in numbers if not order_pattern_of(number))
    if without_pattern:
        warn(
            f"{without_pattern} shipment order number(s) have no usable order pattern "
            "and cannot be matched to their order's events."
        )
    log(
        f"Orders to read from {SHIPMENT_STATUS_TABLE}: {len(all_patterns)} "
        f"(LIKE patterns, {args.pattern_batch_size} per query"
        + (f", e.g. {all_patterns[0]}" if all_patterns else "")
        + ")"
    )
    order_events = fetch_order_events(
        wlfs_client, all_patterns, args.product_keys, args.pattern_batch_size
    )
    with_cancelled = sum(1 for e in order_events.values() if e.has_cancelled)
    latest_cancelled = sum(
        1 for e in order_events.values() if e.latest_event_type in CANCELLED_EVENT_TYPES
    )
    log(
        f"Orders with a CANCELLED event on some generation: {with_cancelled}, "
        f"of which the LAST event is that cancellation: {latest_cancelled}"
    )
    unordered = [p for p, e in order_events.items() if e.has_cancelled and not e.ordered]
    if unordered:
        warn(
            f"{SHIPMENT_STATUS_TABLE} has no usable timestamp column, so 'the last event "
            "of the order' cannot be decided; falling back to 'a CANCELLED event exists'."
        )

    kibo = KiboClient({"client_id": client_id, "client_secret": client_secret},
                      args.auth_url, args.host, args.timeout)

    rows: List[Candidate] = []
    eligible: List[Candidate] = []
    # The items of each order's latest CANCELLED event, kept aside because `Candidate`
    # only carries their printable form.
    products_by_number: Dict[str, frozenset] = {}
    for candidate in candidates:
        # The brand decides the Kibo tenant. oms_active_shipments carries its own pair,
        # but they can disagree with the Shipment API's, and a wrong site silently
        # empties a filtered query instead of failing, so the configured tenant wins and
        # any disagreement is reported.
        candidate.oms_tenant_id = candidate.tenant_id
        candidate.oms_site_id = candidate.site_id
        candidate.brand = brand_of(candidate.shipment_order_number)
        config_site, config_tenant = kibo_config_for_brand(candidate.brand)
        if config_tenant:
            # Only the TENANT is taken from the config. The configured site has been
            # seen to disagree with the shipment's real one (CA is configured 53602 but
            # its shipments sit on 53604), and a wrong site 404s the shipment read, so
            # the site is left as oms_active_shipments has it and then replaced by what
            # Kibo reports once the shipment has been read.
            if config_tenant != candidate.tenant_id:
                warn(
                    f"{candidate.shipment_order_number}: {BI_TABLE} says tenant "
                    f"{candidate.tenant_id}, brand {candidate.brand} is configured as "
                    f"tenant {config_tenant}. Using {config_tenant}."
                )
            candidate.tenant_id = config_tenant
            candidate.tenant_source = f"brand {candidate.brand}"
            if config_site and config_site != candidate.site_id:
                warn(
                    f"{candidate.shipment_order_number}: configured site {config_site} "
                    f"differs from {BI_TABLE}'s {candidate.site_id}; the site Kibo "
                    "reports on the shipment is used for any write."
                )
        else:
            candidate.tenant_source = BI_TABLE
            if not args.allow_unknown_brand:
                candidate.action = "unknown_brand"
                candidate.error = (
                    f"No brand in BRAND_KIBO_CONFIG matches shipmentOrderNumber "
                    f"'{candidate.shipment_order_number}', so the Kibo tenant and site "
                    "cannot be confirmed. --allow-unknown-brand uses the "
                    f"{BI_TABLE} values instead."
                )
                rows.append(candidate)
                continue

        candidate.sixth_character = sixth_character_of(candidate.shipment_order_number)
        if not sixth_character_allowed(candidate.shipment_order_number, args.allowed_sixth_characters):
            candidate.action = "sixth_character_excluded"
            candidate.error = (
                f"6th character '{candidate.sixth_character}' is not one of "
                f"{args.allowed_sixth_characters}."
                if candidate.sixth_character else
                f"shipmentOrderNumber '{candidate.shipment_order_number}' is shorter than "
                "6 characters, so the 6th character cannot be checked."
            )
            rows.append(candidate)
            continue

        # The events are read for the whole ORDER, not for this generation alone. The
        # active shipment is usually a generation the warehouse has never seen: GXO
        # cancels F, Kibo reassigns into a new G, and G is what sits READY here while
        # every CANCELLED event still names A to F.
        candidate.order_pattern = order_pattern_of(candidate.shipment_order_number)
        events = order_events.get(candidate.order_pattern, OrderEvents())
        candidate.has_cancelled_event = events.has_cancelled
        candidate.latest_event_type = events.latest_event_type
        candidate.latest_event_shipment_order_number = events.latest_event_son
        candidate.latest_event_timestamp = events.latest_event_timestamp
        candidate.cancelled_order_generations = ", ".join(sorted(events.cancelled_sons))
        if events.cancelled_logistic_numbers:
            candidate.logistic_order_number = events.cancelled_logistic_numbers[0]

        if not events.has_cancelled:
            candidate.action = "no_cancelled_event"
            candidate.error = (
                "No CANCELLED event in prd_shipmentStatus for any generation of order "
                f"{candidate.order_pattern or candidate.shipment_order_number}."
            )
            rows.append(candidate)
            continue

        # The warehouse has the last word: only a cancellation that nothing has
        # superseded counts. A later PICKED or SHIPPED means the goods were found and
        # the shipment is being worked, so the older cancellations are history.
        latest_is_cancelled = events.latest_event_type in CANCELLED_EVENT_TYPES
        if args.require_latest_cancelled and events.ordered and not latest_is_cancelled:
            candidate.action = "latest_event_not_cancelled"
            candidate.error = (
                f"The last warehouse event of order "
                f"{candidate.order_pattern or candidate.shipment_order_number} is "
                f"{events.latest_event_type or 'unknown'} on "
                f"{events.latest_event_son or 'an unknown shipment'}"
                + (f" at {events.latest_event_timestamp}" if events.latest_event_timestamp else "")
                + ", not a cancellation."
            )
            log(f"{candidate.shipment_order_number}: {candidate.error}")
            rows.append(candidate)
            continue

        # The items of the LATEST cancellation are what the target has to carry; the
        # union of every cancellation of the order stands in when the latest event
        # carried no readable items.
        event_products = events.latest_cancelled_products or events.cancelled_products
        products_by_number[candidate.shipment_order_number] = event_products
        candidate.cancelled_products = format_products(event_products)
        if not event_products and args.product_match != PRODUCT_MATCH_ANY:
            candidate.action = "no_cancelled_items"
            candidate.error = (
                "The CANCELLED requestPayload carries no product identifier "
                f"({list(args.product_keys)}), so the shipment to cancel cannot be "
                "matched by items."
            )
            rows.append(candidate)
            continue

        candidate.kibo_shipment_number = clean(candidate.oms_shipment_number)
        eligible.append(candidate)
        rows.append(candidate)

    log(f"Orders whose last warehouse event is a cancellation: {len(eligible)}")

    cancelled_table_keys: List[str] = []
    for index, candidate in enumerate(eligible, start=1):
        candidate.correlation_id = uuid.uuid4().hex
        log(
            f"[{index}/{len(eligible)}] {candidate.shipment_order_number} "
            f"(tenant={candidate.tenant_id}, site={candidate.site_id}, "
            f"items={candidate.cancelled_products or 'none'})"
        )

        # The active shipment is read first, when oms_active_shipments gave a number:
        # it is the one that carries the order's real externalOrderId, and Kibo is the
        # authority on which site the order sits on.
        shipment: Optional[Dict[str, Any]] = None
        if candidate.kibo_shipment_number:
            shipment, read_error = kibo.get_shipment(
                candidate.tenant_id, candidate.site_id, candidate.kibo_shipment_number,
                args.max_retries,
            )
            if shipment is None:
                warn(
                    f"    could not read shipment {candidate.kibo_shipment_number}: "
                    f"{read_error}; falling back to the externalOrderId derived from "
                    "the shipmentOrderNumber."
                )
            else:
                candidate.kibo_status_before = shipment_status(shipment)
                kibo_site = shipment_site_id(shipment)
                if kibo_site and kibo_site != candidate.site_id:
                    log(
                        f"    site {candidate.site_id} ({candidate.tenant_source}) -> "
                        f"{kibo_site} as reported by the shipment"
                    )
                    candidate.site_id = kibo_site
                    candidate.tenant_source = f"{candidate.tenant_source}, site from kibo"

        candidate.external_order_id = (
            (shipment_external_order_id(shipment) if shipment else "")
            or external_order_id_from_shipment_order_number(candidate.shipment_order_number)
        )
        if not candidate.external_order_id:
            candidate.action = "no_external_order_id"
            candidate.error = (
                f"No externalOrderId could be read or derived for "
                f"{candidate.shipment_order_number}, so the order's shipments cannot "
                "be listed."
            )
            warn(f"    {candidate.error}")
            continue

        siblings, lookup_error = kibo.get_shipments_by_external_order_id(
            candidate.tenant_id, candidate.site_id, candidate.external_order_id,
            args.max_retries, args.order_lookup_page_size,
        )
        if lookup_error:
            candidate.action = "order_lookup_failed"
            candidate.error = (
                f"Could not read the shipments of order {candidate.external_order_id}: "
                f"{lookup_error}"
            )
            warn(f"    {candidate.error}")
            continue

        target = resolve_cancel_target(
            siblings,
            products_by_number.get(candidate.shipment_order_number, frozenset()),
            args.product_keys,
            args.product_match,
            args.cancel_target_excluded_statuses,
            args.on_multiple_cancel_targets,
            args.cancel_target_statuses,
            args.cancel_target_location_pattern,
            cancellation_rows,
        )
        candidate.order_shipments = target.considered
        candidate.cancel_target_candidates = ", ".join(target.matches)

        if target.ambiguous:
            candidate.action = "ambiguous_cancel_target"
            candidate.error = target.error
            candidate.kibo_shipment_number = ""
            warn(f"    {candidate.error}")
            continue

        if not target.shipment_number or target.row is None:
            candidate.action = "no_cancel_target"
            candidate.kibo_shipment_number = ""
            candidate.error = target.error or (
                f"No shipment of order {candidate.external_order_id} is "
                + (", ".join(args.cancel_target_statuses) or "cancellable")
                + f" with the items of the CANCELLED event and a row in "
                f"{CANCELLATION_TABLE} ({target.considered or 'none returned'})."
            )
            warn(f"    {candidate.error}")
            continue

        candidate.kibo_shipment_number = target.shipment_number
        candidate.target_shipment_order_number = target.shipment_order_number
        candidate.cancel_target_source = target.source
        candidate.cancel_target_status = target.status
        candidate.cancel_target_location = target.location
        candidate.kibo_status_before = target.status
        candidate.kibo_status_after = target.status
        candidate.table_shipment_order_number = target.row.shipment_order_number
        candidate.table_matched_on = target.table_matched_on
        candidate.table_items = target.row.raw_items
        candidate.reason_code = target.row.reason_code or args.default_reason_code
        candidate.more_info = target.row.more_info or args.default_more_info
        shipment = target.shipment or shipment

        # Kibo is the authority on the site, and the target can sit on another one than
        # the active shipment that was read first.
        target_site = shipment_site_id(target.shipment or {})
        if target_site and target_site != candidate.site_id:
            candidate.site_id = target_site
            candidate.tenant_source = f"{candidate.tenant_source}, site from cancel target"

        log(
            f"    cancelling {candidate.kibo_shipment_number} "
            f"({candidate.cancel_target_status or 'unknown'}), listed in "
            f"{CANCELLATION_TABLE} as {candidate.table_shipment_order_number} "
            f"({candidate.table_matched_on}), reasonCode={candidate.reason_code}, "
            f"moreInfo={candidate.more_info}"
        )

        if is_terminal_status(candidate.kibo_status_before):
            candidate.action = "already_terminal"
            candidate.success = True
            log(f"    already {candidate.kibo_status_before}, nothing to do.")
            continue

        if args.dry_run:
            candidate.action = "would_cancel"
            candidate.success = True
            candidate.table_updated = "no (dry run)" if args.mark_cancelled else "no"
            candidate.response_excerpt = "DRY RUN - the canceled endpoint was not called"
            continue

        ok, error, excerpt = kibo.mark_canceled(
            candidate.tenant_id, candidate.site_id, candidate.kibo_shipment_number,
            candidate.reason_code, candidate.more_info, args.max_retries,
            candidate.correlation_id,
        )
        candidate.action = "canceled" if ok else "cancel_failed"
        candidate.error = "" if ok else f"PUT /canceled failed: {error}"
        candidate.success = ok
        candidate.response_excerpt = excerpt

        if ok:
            cancelled_table_keys.append(target.row.shipment_order_number)
            refreshed, _ = kibo.get_shipment(
                candidate.tenant_id, candidate.site_id, candidate.kibo_shipment_number,
                args.max_retries,
            )
            if isinstance(refreshed, dict):
                candidate.kibo_status_after = shipment_status(refreshed) or candidate.kibo_status_after
                candidate.kibo_reason_after = shipment_status_reason(refreshed)
            log(
                f"    OK status={candidate.kibo_status_after or 'unknown'} "
                f"reason={candidate.kibo_reason_after or 'unknown'}"
            )
            # Kibo answers 200 even when it ignores the reason in the body, so check
            # what it actually stored rather than trusting the status code.
            if candidate.kibo_reason_after and candidate.kibo_reason_after != candidate.reason_code:
                warn(
                    f"    Kibo recorded reasonCode '{candidate.kibo_reason_after}' "
                    f"instead of '{candidate.reason_code}'."
                )
        else:
            warn(f"    {candidate.error}")
            if excerpt:
                warn(f"    response: {excerpt[:400]}")

        if args.sleep_between_requests > 0:
            time.sleep(args.sleep_between_requests)

    # The rows cancelled in this run are flagged in one statement, so a later run skips
    # them. Should this update fail, the cancellations stand: the next run finds those
    # shipments CANCELED in Kibo rather than BACKORDER, so they are no longer targets.
    cancelled = [c for c in eligible if c.action == "canceled" and c.success]
    if args.mark_cancelled and cancelled_table_keys and not args.dry_run:
        updated, update_error = mark_cancelled_on_kibo(bi_client, cancelled_table_keys)
        if update_error:
            warn(
                f"Could not set cancelled_on_kibo in {CANCELLATION_TABLE}: {update_error}. "
                f"{len(cancelled_table_keys)} row(s) are still flagged false; the "
                "shipments themselves are cancelled."
            )
            for candidate in cancelled:
                candidate.table_updated = "failed"
        else:
            log(
                f"{CANCELLATION_TABLE}: cancelled_on_kibo set to true on {updated} row(s)."
            )
            for candidate in cancelled:
                candidate.table_updated = "yes"
    elif cancelled_table_keys:
        for candidate in cancelled:
            candidate.table_updated = "no"

    write_csv(output_path, rows)
    write_job_summary(rows, args.dry_run)

    failures = [c for c in eligible if not c.success]
    log(f"Results written to {output_path}")
    log(
        f"Summary: {len(candidates)} active shipment order numbers, "
        f"{len(eligible)} orders whose last warehouse event is a cancellation, "
        f"{len(cancelled)} cancelled in Kibo, "
        f"{sum(1 for c in rows if c.action == 'no_cancel_target')} with no listed "
        f"{', '.join(args.cancel_target_statuses) or 'cancellable'} shipment on the order, "
        f"{sum(1 for c in rows if c.action == 'ambiguous_cancel_target')} ambiguous, "
        f"{sum(1 for c in rows if c.action == 'latest_event_not_cancelled')} superseded by a "
        f"later warehouse event, "
        f"{sum(1 for c in rows if c.action == 'sixth_character_excluded')} left alone on the "
        f"6th character filter, {len(failures)} failed."
    )
    return 0 if not failures else 2


if __name__ == "__main__":
    raise SystemExit(main())
