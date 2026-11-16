#!/usr/bin/env python3
"""
Cancel in Kibo the shipments that the warehouse has already cancelled.

Pipeline
--------
0. Every generation letter is in scope. The 6th character of the shipmentOrderNumber,
   counting every character with the hyphen included, is the generation letter that
   identifies the order ("DJ-USA00041696", "DJ-USE00041696" and "DJ-USI00041696" are
   three generations of one order) and it is read for that purpose only: it does not
   gate the workflow. --allowed-sixth-characters still exists and is EMPTY by default,
   so nothing is excluded; pass a set of letters to narrow a run by hand.

1. Read the active shipments from BigQuery (BI project):
       `tlg-business-intelligence-prd`.til.oms_active_shipments
   keeping only the rows whose shipmentStatus is READY, and taking from them
   shipment_order_number, the shipment number, tenant_id and site_id.

2. For those shipment order numbers, read the events in the WLFS project:
       `tlg-wlfs-prd`.til.prd_shipmentStatus
   - a row with eventType = 'CANCELLED' must exist, otherwise the shipment is skipped
   - the matching row with eventType = 'PICKED' decides the cancellation reason:

         total quantity in the PICKED requestPayload != 0  ->  "Customer Changed Their Mind"
         total quantity in the PICKED requestPayload == 0  ->  "No Inventory"

     When a shipment order number carries several PICKED events, the most recent one
     is used. The quantity is the sum of every `quantity` value found anywhere in the
     payload, so nested item lists are handled.

3. Read data.logisticOrderNumber from the CANCELLED / PICKED requestPayload
   (e.g. "BB82902"), drop the first 2 characters and use the rest as the Kibo
   shipmentNumber (82902), exactly as fulfill_gxo_shipments.py does.

   The two keys then have to line up on the same oms_active_shipments row:

       event shipmentOrderNumber  ==  oms_active_shipments.shipment_order_number
       shipmentNumber from logisticOrderNumber  ==  oms_active_shipments shipment number

   A shipment order number carrying several logisticOrderNumber values keeps only the
   one whose shipmentNumber matches; when none matches, nothing is sent to Kibo and
   the row is reported. The check is skipped, with a warning, when
   oms_active_shipments has no shipment number column.

4. In Kibo:
       GET https://t{tenantId}.{host}/api/commerce/shipments/{shipmentNumber}

   The GET is a safety net: a shipment that Kibo already reports as cancelled, closed
   or otherwise terminal is reported as `already_terminal` and left alone. It also
   supplies the shipment's `channel` and `externalOrderId`.

4b. Whenever the outcome is a cancellation, the shipment that actually gets cancelled is
   resolved against the order rather than assumed to be the one the event names.

   Kibo auto reassigns a shipment as soon as the warehouse cancels it: the shipment the
   CANCELLED event points at is already REASSIGNED by the time this job runs, and a
   REASSIGNED shipment cannot be cancelled. The replacement shipment, under a new
   shipmentNumber, is the one to cancel.

       GET https://t{tenantId}.{host}/api/commerce/shipments?filter=externalOrderId=={id}

   The externalOrderId is the shipment's own field, falling back to the
   shipmentOrderNumber with its 6th character — the generation letter — put back to a
   hyphen ("DJ-USAST0000124550" -> "DJ-US-ST0000124550"), the inverse of what
   release_held_exchange_shipments.py does.

   Out of the shipments of that order, the target is the one that

     - has shipmentStatus READY (--cancel-target-status, empty to accept any
       cancellable status instead)
     - sits in a GXO location: fulfillmentLocationCode LIKE '%GX%'
       (--cancel-target-location-pattern, empty to drop the check)
     - is in a cancellable status: not already CANCELED, nor any other terminal status
       (CLOSED, SHIPPED, ...), nor REASSIGNED, which Kibo refuses as well. Statuses can
       be excluded on top of those with --cancel-target-excluded-statuses.
     - carries the exact same products as the CANCELLED event: every product of the
       event is on the shipment and every item of the shipment is in the event.
       --product-match overlap only asks for one product in common, any skips the check

   When several match, the row is reported as `ambiguous_cancel_target` and left alone;
   --on-multiple-cancel-targets newest takes the most recent one instead. When nothing
   matches, the row is reported as `no_cancel_target` and left alone, because the point
   of the lookup is to find the shipment the warehouse is actually holding;
   --on-no-cancel-target event restores the old fallback to the shipment the event
   names. The whole step is skipped with --no-resolve-cancel-target.

4c. A cancellation is sent to the warehouse FIRST, and Kibo is only cancelled once GXO
   has accepted it, so the two systems cannot drift apart.

       POST https://prdedi.gxo.com:4084/sync/mft/tlg_it/shipmentStatusUpdate

       {"eventType": "CANCEL", "eventClass": "SHIPMENT", "eventId": "<uuid>",
        "eventTimestamp": "...Z",
        "data": {"shipmentOrderNumber": ..., "logisticOrderNumber": ...,
                 "locationCode": ..., "brandCode": ..., "brand": ...,
                 "products": [], "updateReason": "external",
                 "reasonMessage": "Kibo notification cancellation"}}

   with the api_key header from GXO_API_CREDS, over the legacy-renegotiation TLS
   context the endpoint requires, exactly as cancellation_api_sender_to_GXO.py does.

   The payload fields come from the WLFS project:

       SELECT o.logisticOrderNumber, o.shipmentOrderNumber, o.locationName,
              o.brand, bc.brandCode
       FROM `tlg-wlfs-prd`.til.prd_orders AS o
       LEFT JOIN `tlg-wlfs-prd`.til.prd_brandConfig AS bc ON o.brand = bc.brand
       WHERE o.locationName LIKE '%GX%'

   read for the whole order (the LIKE pattern of step 5c) and matched to the cancel
   target on the same key as everywhere else: logisticOrderNumber minus its first 2
   characters == the Kibo shipmentNumber. The row describes the shipment GXO is holding,
   so the shipmentOrderNumber and logisticOrderNumber sent to GXO are the target's own,
   not those of the CANCELLED event that triggered the run.

   No row for the target -> `no_warehouse_order_row`, and nothing is cancelled anywhere.
   GXO refusing the POST -> `warehouse_cancel_failed`, and Kibo is left untouched.
   --no-warehouse-cancel skips the whole step and cancels in Kibo only, as before.

4d. Kibo is cancelled as soon as GXO has accepted the CANCEL, in the same run. The
   shipment cancelled is the READY GXO target of step 4b, whichever generation the
   latest CANCELLED event happened to name: the CANCEL just sent to the warehouse is
   what makes that shipment dead, so there is nothing to wait for.

   --require-event-target restores the cautious variant: Kibo is then cancelled only
   when the latest CANCELLED event names the cancel target itself, and otherwise the
   row is reported as `canceled_warehouse_only`, leaving Kibo to a later run once GXO
   has answered with its own CANCELLED event for that shipment. The CANCELLED events
   are read newest first, so the first logisticOrderNumber carried by a CANCELLED
   payload is the latest cancellation.

5a. Channel is NOT ecommerce  ->  cancel the shipment:

       PUT https://t{tenantId}.{host}/api/commerce/shipments/{shipmentNumber}/canceled

       {"canceledReason": {"reasonCode": "<reason>", "moreInfo": "<reason>"}}

   The reason has to be nested under canceledReason: Kibo answers 200 to a flat
   {"reasonCode": ..., "moreInfo": ...} body but ignores it and records its own
   default reason. After a successful cancellation the shipment is read back and the
   stored shipmentStatusReason.reasonCode is reported.

5b. Channel IS ecommerce  ->  auto reassign the shipment instead, so that it goes
    back through order routing and lands in backorder when no location can fulfil it:

       PUT https://t{tenantId}.{host}/api/commerce/shipments/{shipmentNumber}/reassigned

       {"blockAssignment": false,
        "isUserAction": true,
        "reassignedReason": {"reasonCode": "<reason>", "moreInfo": "<reason>"}}

    Kibo's ReassignShipment schema is {blockAssignment, fulfillmentLocationCode,
    holdBlockAssignment, isUserAction, items, reassignedReason, runId}. Leaving
    fulfillmentLocationCode OUT is what makes this an AUTO reassign: Kibo's Order Admin
    offers "Manual" and "Auto Reassign" against this same endpoint, where manual names a
    target location and auto "sends the shipment back through the order routing process".
    blockAssignment stays false, so routing may pick the same location again; pass
    --block-assignment to exclude the location that just cancelled the shipment.

    The channel is read from oms_active_shipments when the table has a channel column,
    and otherwise from the `channel` field of the Kibo shipment itself. Comparison is
    case-insensitive and the value is configurable with --ecommerce-channel.

    A shipment Kibo already reports as REASSIGNED or BACKORDER is left alone.

5c. Channel IS ecommerce, but the same products of the same order have already been
    cancelled too many times  ->  cancel instead of reassigning.

    CURRENTLY DISABLED. --cancel-ecommerce is off by default, so an ecommerce shipment
    is never cancelled: it is always auto reassigned, whatever the count below says, and
    no CANCEL is sent to the warehouse for it. The rest of this step describes what
    happens once --cancel-ecommerce is passed.

    Reassigning only helps while some other location might still fulfil the order. Once
    the same products have come back CANCELLED under more than 3 distinct
    shipmentOrderNumber values of the same order number, routing has run out of places
    to try and another reassign would only bounce the shipment again, so the active
    READY shipment is cancelled with the reason of step 2 instead.

    - prd_shipmentStatus carries no order number, so the shipments of one order are
      found by wildcarding the 6th character of the shipmentOrderNumber, the generation
      letter that the rule in step 0 already keys on:

          SELECT * FROM `tlg-wlfs-prd`.til.prd_shipmentStatus
          WHERE shipmentOrderNumber LIKE 'DJ-US%ST0000124550'

      so DJ-USAST0000124550, DJ-USBST0000124550 and DJ-USCST0000124550 are one order.

    - the count is over distinct shipmentOrderNumber values of that order that
      carry a CANCELLED event in prd_shipmentStatus, the shipment being processed
      included, and is NOT limited to the shipments still in oms_active_shipments
    - "the same products" is decided on the product codes found anywhere in the
      CANCELLED requestPayload (upc, sku, productCode, ...). --product-match exact,
      the default, counts only the shipmentOrderNumbers whose product set is identical;
      overlap counts those sharing at least one product; any ignores products entirely
    - the threshold is --max-cancelled-order-shipments (default 3, so the 4th fires),
      and --no-repeat-cancel turns the whole rule off

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
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote

import ssl

import requests
import urllib3
from requests.adapters import HTTPAdapter
from urllib3.util.ssl_ import create_urllib3_context
from google.cloud import bigquery
from google.oauth2 import service_account

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BI_PROJECT = "tlg-business-intelligence-prd"
BI_DATASET = "til"
BI_TABLE = "oms_active_shipments"

WLFS_PROJECT = "tlg-wlfs-prd"
WLFS_DATASET = "til"
SHIPMENT_STATUS_TABLE = "prd_shipmentStatus"
ORDERS_TABLE = "prd_orders"
BRAND_CONFIG_TABLE = "prd_brandConfig"

# The warehouse endpoint the cancellation is sent to before Kibo is touched, and the
# fields of the CANCEL event, as in cancellation_api_sender_to_GXO.py.
DEFAULT_GXO_URL = "https://prdedi.gxo.com:4084/sync/mft/tlg_it/shipmentStatusUpdate"
GXO_EVENT_TYPE = "CANCEL"
GXO_EVENT_CLASS = "SHIPMENT"
GXO_UPDATE_REASON = "external"
GXO_REASON_MESSAGE = "Kibo notification cancellation"
# OP_LEGACY_SERVER_CONNECT: the endpoint negotiates in a way OpenSSL 3 refuses by
# default. Numeric so it works on Python < 3.12 as well.
SSL_OP_LEGACY_SERVER_CONNECT = 0x4

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
PICKED_EVENT_TYPE = "PICKED"

REASON_CUSTOMER = "Customer Changed Their Mind"
REASON_NO_INVENTORY = "No Inventory"

# Shipments on this channel are auto reassigned instead of cancelled.
DEFAULT_ECOMMERCE_CHANNEL = "ecommerce"

# An ecommerce shipment is cancelled rather than reassigned once the same products of
# the same order carry a CANCELLED event under more than this many distinct
# shipmentOrderNumber values (the shipment being processed included).
DEFAULT_MAX_CANCELLED_ORDER_SHIPMENTS = 3

# How the product sets of two CANCELLED events are compared.
PRODUCT_MATCH_EXACT = "exact"
PRODUCT_MATCH_OVERLAP = "overlap"
PRODUCT_MATCH_ANY = "any"
DEFAULT_PRODUCT_MATCH = PRODUCT_MATCH_EXACT

# Product identifiers looked for anywhere in a CANCELLED requestPayload.
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
# order, but it no longer restricts the run: every letter is in scope, so that the
# repeat-cancellation rule of step 5c can still see an order that has been recycled
# past D. Narrow a single run with --allowed-sixth-characters if ever needed.
DEFAULT_ALLOWED_SIXTH_CHARACTERS = ""

RESOLUTION_CANCEL = "cancel"
RESOLUTION_REASSIGN = "reassign"

# Kibo statuses that mean "nothing left to cancel".
TERMINAL_STATUSES = {
    "CANCELED", "CANCELLED", "CLOSED", "COMPLETE", "COMPLETED",
    "FULFILLED", "SHIPPED", "PICKEDUP", "PICKED_UP",
}

# Statuses that mean the shipment has already left the fulfilment location, so there is
# nothing to auto reassign: it is already back with routing or already in backorder.
ALREADY_REASSIGNED_STATUSES = {"REASSIGNED", "BACKORDER", "BACK_ORDER", "BACKORDERED"}

# Statuses Kibo will not accept a cancellation for. REASSIGNED is the one this whole
# step exists for: the shipment the CANCELLED event names has already been handed back
# to routing, so it is the replacement shipment that has to be cancelled. BACKORDER is
# deliberately NOT here — a backordered shipment is cancellable, and is very often
# exactly where the replacement lands.
UNCANCELLABLE_STATUSES = TERMINAL_STATUSES | {"REASSIGNED"}

# Statuses excluded from the cancel-target search on top of UNCANCELLABLE_STATUSES.
# Empty by default: the rule is "not already CANCELED", which UNCANCELLABLE_STATUSES
# already covers along with the other terminal statuses and REASSIGNED, so a READY
# replacement is a perfectly good target.
DEFAULT_CANCEL_TARGET_EXCLUDED_STATUSES = ""

ON_MULTIPLE_SKIP = "skip"
ON_MULTIPLE_NEWEST = "newest"
DEFAULT_ON_MULTIPLE_CANCEL_TARGETS = ON_MULTIPLE_SKIP

# The cancel target is the shipment the warehouse is actually holding: READY, in a GXO
# location, with the products of the CANCELLED event.
DEFAULT_CANCEL_TARGET_STATUS = "READY"
DEFAULT_CANCEL_TARGET_LOCATION_PATTERN = "%GX%"

# What to do when no shipment of the order matches. 'skip' leaves the row alone, which
# is the safe reading now that a cancellation also goes to the warehouse: cancelling the
# shipment the event names would tell GXO about a shipment it no longer holds.
ON_NO_TARGET_SKIP = "skip"
ON_NO_TARGET_EVENT = "event"
DEFAULT_ON_NO_CANCEL_TARGET = ON_NO_TARGET_SKIP

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

# Per-item product identifiers on a Kibo shipment item, matched against the product
# identifiers of the CANCELLED requestPayload.
SHIPMENT_ITEM_PRODUCT_KEYS = (
    "upc", "sku", "productCode", "variationProductCode", "itemCode", "partNumber",
    "productId", "barcode", "ean", "styleCode",
)

# Candidate column names, resolved case-insensitively against the real schema.
SHIPMENT_NUMBER_CANDIDATES = ("shipmentOrderNumber", "shipment_order_number")
OMS_NUMBER_CANDIDATES = (
    "oms_shipment_number", "omsShipmentNumber", "shipment_number", "shipmentNumber",
)
PAYLOAD_CANDIDATES = ("requestPayload", "request_payload")
EVENT_TYPE_CANDIDATES = ("eventType", "event_type")
OMS_STATUS_CANDIDATES = ("shipmentStatus", "shipment_status", "status")
CHANNEL_CANDIDATES = (
    "channel", "channel_name", "channelName", "sales_channel", "salesChannel",
    "order_channel", "orderChannel",
)
QUANTITY_KEYS = ("quantity", "quantityPicked", "pickedQuantity", "qty")
TIMESTAMP_CANDIDATES = (
    "eventTimestamp", "event_timestamp", "insertTimestamp", "insert_timestamp",
    "ingestionTimestamp", "ingestion_timestamp", "createdAt", "created_at",
    "creationDate", "creation_date", "loadTimestamp", "load_timestamp",
)


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


def is_already_reassigned_status(status: str) -> bool:
    """True when the shipment is already back with routing or already in backorder."""
    return status.upper().replace(" ", "").replace("-", "_") in ALREADY_REASSIGNED_STATUSES


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


def is_wanted_status(status: str, wanted: str) -> bool:
    """Whether a shipment carries the status the cancel target must have."""
    if not wanted:
        return True
    return normalize_status(status) == normalize_status(wanted)


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


def reassigned_or_cancelled_items_of(shipment: Dict[str, Any]) -> List[Any]:
    """
    The items that have left the shipment: reassigned, cancelled, rejected, transferred.

    Only used to read the channel, never for the product match: these lines are no
    longer part of what the shipment would ship, so counting them as products would make
    a shipment match an event it no longer covers.
    """
    items: List[Any] = []
    for key in ("reassignedItems", "canceledItems", "cancelledItems",
                "rejectedItems", "transferredItems"):
        value = shipment.get(key)
        if isinstance(value, list):
            items.extend(value)
    return items


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


def same_channel(value: str, wanted: str) -> bool:
    """Channel comparison: case-insensitive, and blind to spaces, hyphens and underscores."""
    def normalize(text: str) -> str:
        return re.sub(r"[\s_\-]+", "", clean(text)).upper()
    return bool(wanted) and normalize(value) == normalize(wanted)


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


def as_number(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = clean(value)
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def picked_quantity_of(payload: Dict[str, Any]) -> Tuple[float, bool]:
    """
    Total quantity carried by a PICKED payload.

    Returns (total, found). `found` is False when the payload holds no quantity field
    at all, which is not the same as a payload that explicitly says zero.
    """
    total = 0.0
    found = False
    for key in QUANTITY_KEYS:
        for value in iter_values(payload, key):
            number = as_number(value)
            if number is None:
                continue
            found = True
            total += number
        if found:
            # Stop at the first key that exists, so quantity and quantityPicked
            # on the same items are never added together.
            break
    return total, found


def logistic_order_number_of(payload: Dict[str, Any]) -> str:
    for value in iter_values(payload, "logisticOrderNumber"):
        text = clean(value)
        if text:
            return text
    return ""


def kibo_number_from_logistic(logistic_order_number: str) -> Tuple[str, str]:
    """'BB82902' -> ('82902', ''). Returns (shipmentNumber, error)."""
    value = clean(logistic_order_number)
    if len(value) <= 2:
        return "", f"logisticOrderNumber '{value}' is too short to strip a 2-character prefix."
    stripped = value[2:]
    if not stripped.isdigit():
        return "", (
            f"logisticOrderNumber '{value}' does not become a numeric Kibo shipmentNumber "
            f"after removing the first 2 characters (got '{stripped}')."
        )
    return str(int(stripped)), ""


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
    oms_channel: str = ""
    kibo_channel: str = ""
    channel: str = ""
    channel_source: str = ""
    order_pattern: str = ""
    cancelled_products: str = ""
    cancelled_order_shipments: str = ""
    cancelled_order_shipment_numbers: str = ""
    cancelled_order_generations: str = ""
    latest_event_type: str = ""
    latest_event_shipment_order_number: str = ""
    picked_from: str = ""
    repeat_cancel: str = ""
    resolution: str = ""
    resolution_source: str = ""
    has_cancelled_event: bool = False
    has_picked_event: bool = False
    picked_quantity: str = ""
    logistic_order_number: str = ""
    kibo_shipment_number: str = ""
    number_matches_oms: str = ""
    external_order_id: str = ""
    event_shipment_number: str = ""
    latest_cancelled_shipment_number: str = ""
    event_is_cancel_target: str = ""
    order_shipments: str = ""
    cancel_target_candidates: str = ""
    cancel_target_source: str = ""
    cancel_target_location: str = ""
    warehouse_shipment_order_number: str = ""
    warehouse_logistic_order_number: str = ""
    warehouse_location_code: str = ""
    warehouse_brand_code: str = ""
    warehouse_event_id: str = ""
    warehouse_source: str = ""
    warehouse_action: str = ""
    warehouse_response: str = ""
    reason_code: str = ""
    kibo_status_before: str = ""
    kibo_status_after: str = ""
    kibo_reason_after: str = ""
    action: str = "skipped"
    success: bool = False
    error: str = ""
    response_excerpt: str = ""
    correlation_id: str = ""


@dataclass
class StatusEvents:
    """The CANCELLED / PICKED events of one shipment order number."""
    has_cancelled: bool = False
    has_picked: bool = False
    picked_payload: Optional[Dict[str, Any]] = None
    # Every distinct logisticOrderNumber carried by the events, newest first.
    logistic_numbers: List[str] = field(default_factory=list)
    # The same, but only from the CANCELLED events, newest first. The first entry is
    # therefore the shipment the LATEST cancellation names, which is what decides
    # whether Kibo is cancelled as well as the warehouse.
    cancelled_logistic_numbers: List[str] = field(default_factory=list)
    # The products the CANCELLED events name, for the repeat-cancellation rule of 5c.
    cancelled_products: frozenset = frozenset()


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
            "the logisticOrderNumber cannot be checked against it."
        )
    channel_col = resolve_column(columns, CHANNEL_CANDIDATES, where, required=False)
    if not channel_col:
        warn(
            f"{BI_TABLE} has no channel column ({list(CHANNEL_CANDIDATES)}); "
            "the channel will be read from the Kibo shipment instead."
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
    channel_select = f"CAST(`{channel_col}` AS STRING)" if channel_col else "CAST(NULL AS STRING)"
    sql = f"""
        SELECT DISTINCT
            CAST(`{number_col}` AS STRING) AS shipment_order_number,
            CAST(`{tenant_col}` AS STRING) AS tenant_id,
            CAST(`{site_col}`   AS STRING) AS site_id,
            {status_select} AS oms_shipment_status,
            {oms_number_select} AS oms_shipment_number,
            {channel_select} AS oms_channel
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
            oms_channel=clean(row["oms_channel"]),
        ))
    return candidates


def fetch_status_events(
    client: bigquery.Client, numbers: Sequence[str], batch_size: int,
    product_keys: Sequence[str],
) -> Dict[str, StatusEvents]:
    """
    Return {shipmentOrderNumber: StatusEvents} for the CANCELLED and PICKED events.

    Same join as fulfill_gxo_shipments.fetch_shipped_events: prd_shipmentStatus is read
    by shipmentOrderNumber, and the Kibo shipment is identified by the
    logisticOrderNumber carried in the requestPayload. A shipment order number can
    carry several events and therefore several logisticOrderNumber values, so every
    distinct one is kept, newest event first.

    Rows come back newest first when the table has a usable timestamp column, so the
    first PICKED payload seen per shipment order number is the most recent one.
    """
    where = f"`{WLFS_PROJECT}.{WLFS_DATASET}.{SHIPMENT_STATUS_TABLE}`"
    columns = table_columns(client, WLFS_PROJECT, WLFS_DATASET, SHIPMENT_STATUS_TABLE)
    number_col = resolve_column(columns, SHIPMENT_NUMBER_CANDIDATES, where)
    payload_col = resolve_column(columns, PAYLOAD_CANDIDATES, where)
    event_col = resolve_column(columns, EVENT_TYPE_CANDIDATES, where)
    ts_col = resolve_column(columns, TIMESTAMP_CANDIDATES, where, required=False)

    order_by = f"ORDER BY `{ts_col}` DESC" if ts_col else ""
    wanted = list(CANCELLED_EVENT_TYPES) + [PICKED_EVENT_TYPE]
    sql = f"""
        SELECT
            CAST(`{number_col}` AS STRING) AS shipment_order_number,
            UPPER(CAST(`{event_col}` AS STRING)) AS event_type,
            CAST(`{payload_col}` AS STRING) AS payload
        FROM {where}
        WHERE CAST(`{number_col}` AS STRING) IN UNNEST(@numbers)
          AND UPPER(CAST(`{event_col}` AS STRING)) IN UNNEST(@event_types)
        {order_by}
    """

    found: Dict[str, StatusEvents] = {}
    for batch in chunked(numbers, batch_size):
        job = client.query(
            sql,
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ArrayQueryParameter("numbers", "STRING", batch),
                bigquery.ArrayQueryParameter("event_types", "STRING", wanted),
            ]),
        )
        for row in job.result():
            number = clean(row["shipment_order_number"])
            if not number:
                continue
            event_type = clean(row["event_type"]).upper()
            events = found.setdefault(number, StatusEvents())
            if event_type in CANCELLED_EVENT_TYPES:
                events.has_cancelled = True
            elif event_type == PICKED_EVENT_TYPE:
                events.has_picked = True

            payload = parse_payload(row["payload"])
            if payload is None:
                continue
            if event_type == PICKED_EVENT_TYPE and events.picked_payload is None:
                events.picked_payload = payload
            if event_type in CANCELLED_EVENT_TYPES:
                # Several CANCELLED events can describe the same shipment in pieces, so
                # the products of all of them make up one set.
                events.cancelled_products |= products_of(payload, product_keys)
            logistic = logistic_order_number_of(payload)
            if logistic and logistic not in events.logistic_numbers:
                events.logistic_numbers.append(logistic)
            if (event_type in CANCELLED_EVENT_TYPES and logistic
                    and logistic not in events.cancelled_logistic_numbers):
                events.cancelled_logistic_numbers.append(logistic)
    return found


def fetch_cancelled_order_shipments(
    client: bigquery.Client, order_patterns: Sequence[str], batch_size: int,
    product_keys: Sequence[str], pattern_batch_size: int,
) -> Dict[str, Dict[str, frozenset]]:
    """
    Return {orderPattern: {shipmentOrderNumber: products}} for the CANCELLED events.

    prd_shipmentStatus has no order number column, so the shipments of one order are
    found the way the hand check finds them:

        SELECT * FROM `tlg-wlfs-prd`.til.prd_shipmentStatus
        WHERE shipmentOrderNumber LIKE 'DJ-US%ST0000124550'

    One query carries many patterns at once, each row matching at least one of them, and
    the rows are handed back to their pattern in Python. Unlike fetch_status_events this
    is not restricted to the shipment order numbers of oms_active_shipments, so it also
    sees the earlier generations of the order that are long gone from the active table:
    those are exactly the ones the repeat-cancellation rule has to count.

    LIKE with a leading-literal pattern cannot use a BigQuery index, so every row of the
    scanned partitions is tested; patterns are therefore sent in smaller batches than the
    plain IN UNNEST queries, and the whole step is skipped with --no-repeat-cancel.
    """
    where = f"`{WLFS_PROJECT}.{WLFS_DATASET}.{SHIPMENT_STATUS_TABLE}`"
    columns = table_columns(client, WLFS_PROJECT, WLFS_DATASET, SHIPMENT_STATUS_TABLE)
    number_col = resolve_column(columns, SHIPMENT_NUMBER_CANDIDATES, where)
    payload_col = resolve_column(columns, PAYLOAD_CANDIDATES, where)
    event_col = resolve_column(columns, EVENT_TYPE_CANDIDATES, where)

    sql = f"""
        SELECT
            CAST(`{number_col}` AS STRING) AS shipment_order_number,
            CAST(`{payload_col}` AS STRING) AS payload
        FROM {where}
        WHERE UPPER(CAST(`{event_col}` AS STRING)) IN UNNEST(@event_types)
          AND EXISTS (
              SELECT 1 FROM UNNEST(@order_patterns) AS order_pattern
              WHERE CAST(`{number_col}` AS STRING) LIKE order_pattern
          )
    """

    found: Dict[str, Dict[str, frozenset]] = {pattern: {} for pattern in order_patterns}
    for batch in chunked(order_patterns, max(1, pattern_batch_size)):
        job = client.query(
            sql,
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ArrayQueryParameter("order_patterns", "STRING", batch),
                bigquery.ArrayQueryParameter(
                    "event_types", "STRING", list(CANCELLED_EVENT_TYPES)
                ),
            ]),
        )
        for row in job.result():
            number = clean(row["shipment_order_number"])
            if not number:
                continue
            payload = parse_payload(row["payload"])
            products = products_of(payload, product_keys) if payload else frozenset()
            # A row can satisfy more than one pattern of the batch; it counts for each.
            for pattern in batch:
                if matches_order_pattern(number, pattern):
                    per_order = found.setdefault(pattern, {})
                    per_order[number] = per_order.get(number, frozenset()) | products
    return found


@dataclass
class WarehouseOrder:
    """One prd_orders row for a shipment GXO is holding."""
    shipment_order_number: str = ""
    logistic_order_number: str = ""
    location_name: str = ""
    brand: str = ""
    brand_code: str = ""


@dataclass
class OrderEvents:
    """
    The CANCELLED / PICKED events of one ORDER, across every generation.

    The active shipment of an order usually carries NO events of its own: the warehouse
    cancels generation F, Kibo immediately reassigns into a brand new generation G, and
    G is what sits READY in oms_active_shipments while every CANCELLED event still names
    A to F. Keying the lookup on the active row's own shipmentOrderNumber therefore
    finds nothing and the order is skipped forever, which is why the events are read per
    order instead.
    """
    has_cancelled: bool = False
    has_picked: bool = False
    picked_payload: Optional[Dict[str, Any]] = None
    picked_shipment_order_number: str = ""
    # The newest warehouse event of the order, whatever its type. When it is a PICKED
    # that actually picked something, the warehouse has moved on and nothing is
    # cancelled; see `warehouse_moved_on_of`. `ordered` says whether the rows really
    # came back newest first, which only holds when the table has a timestamp column.
    latest_event_type: str = ""
    latest_event_son: str = ""
    latest_event_payload: Optional[Dict[str, Any]] = None
    ordered: bool = False
    # Per generation: its own latest event, and its own latest PICKED payload. The
    # moved-on rule and the reason code are decided from the generation that is
    # actually READY, not from whichever generation of the order moved last.
    latest_by_son: Dict[str, Tuple[str, Optional[Dict[str, Any]]]] = field(default_factory=dict)
    picked_by_son: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    logistic_numbers: List[str] = field(default_factory=list)
    cancelled_logistic_numbers: List[str] = field(default_factory=list)
    # {shipmentOrderNumber: products} for the generations carrying a CANCELLED event.
    cancelled_sons: Dict[str, frozenset] = field(default_factory=dict)
    cancelled_products: frozenset = frozenset()


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
    it belongs to in Python. Rows come back newest first when the table has a usable
    timestamp column, so the first PICKED payload of an order is its most recent one and
    the first CANCELLED logisticOrderNumber names the latest cancellation.

    This replaces both the old per-shipmentOrderNumber event read and the separate
    repeat-cancellation query: the cancelled generations counted by that rule are the
    same rows, so one pass serves both.
    """
    where = f"`{WLFS_PROJECT}.{WLFS_DATASET}.{SHIPMENT_STATUS_TABLE}`"
    columns = table_columns(client, WLFS_PROJECT, WLFS_DATASET, SHIPMENT_STATUS_TABLE)
    number_col = resolve_column(columns, SHIPMENT_NUMBER_CANDIDATES, where)
    payload_col = resolve_column(columns, PAYLOAD_CANDIDATES, where)
    event_col = resolve_column(columns, EVENT_TYPE_CANDIDATES, where)
    ts_col = resolve_column(columns, TIMESTAMP_CANDIDATES, where, required=False)

    order_by = f"ORDER BY `{ts_col}` DESC" if ts_col else ""
    wanted = list(CANCELLED_EVENT_TYPES) + [PICKED_EVENT_TYPE]
    sql = f"""
        SELECT
            CAST(`{number_col}` AS STRING) AS shipment_order_number,
            UPPER(CAST(`{event_col}` AS STRING)) AS event_type,
            CAST(`{payload_col}` AS STRING) AS payload
        FROM {where}
        WHERE UPPER(CAST(`{event_col}` AS STRING)) IN UNNEST(@event_types)
          AND EXISTS (
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
                bigquery.ArrayQueryParameter("event_types", "STRING", wanted),
            ]),
        )
        for row in job.result():
            number = clean(row["shipment_order_number"])
            if not number:
                continue
            event_type = clean(row["event_type"]).upper()
            payload = parse_payload(row["payload"])
            products = products_of(payload, product_keys) if payload else frozenset()
            logistic = logistic_order_number_of(payload) if payload else ""

            for pattern in batch:
                if not matches_order_pattern(number, pattern):
                    continue
                events = found.setdefault(pattern, OrderEvents())
                # Rows arrive newest first, so the first one seen for an order is its
                # latest warehouse event.
                events.ordered = bool(ts_col)
                if not events.latest_event_type:
                    events.latest_event_type = event_type
                    events.latest_event_son = number
                    events.latest_event_payload = payload
                if number not in events.latest_by_son:
                    events.latest_by_son[number] = (event_type, payload)
                if event_type == PICKED_EVENT_TYPE and number not in events.picked_by_son:
                    if payload is not None:
                        events.picked_by_son[number] = payload
                if event_type in CANCELLED_EVENT_TYPES:
                    events.has_cancelled = True
                    events.cancelled_products |= products
                    events.cancelled_sons[number] = (
                        events.cancelled_sons.get(number, frozenset()) | products
                    )
                    if logistic and logistic not in events.cancelled_logistic_numbers:
                        events.cancelled_logistic_numbers.append(logistic)
                elif event_type == PICKED_EVENT_TYPE:
                    events.has_picked = True
                    if payload is not None and events.picked_payload is None:
                        events.picked_payload = payload
                        events.picked_shipment_order_number = number
                if logistic and logistic not in events.logistic_numbers:
                    events.logistic_numbers.append(logistic)
    return found


def warehouse_moved_on_of(events: OrderEvents, son: str) -> Tuple[bool, str]:
    """
    Whether the warehouse has moved on from the cancellations and is picking again.

    An order's generations are cancelled one after another while no location can fulfil
    it, but a later generation can be picked successfully: GXO then holds a shipment it
    is actively working, and the earlier CANCELLED events are history. Cancelling at
    that point would kill a shipment that is on its way out of the door.

        WF-EUA00019586  CANCELLED        12 Nov 10:33   qty 1
        WF-EUB00019586  PICKED           16 Nov 17:40   qty 1   <- latest, picked

    The rule is read on ONE generation — the one that is READY, passed as `son` — and
    not on the order as a whole, because a cancellation and a successful pick usually
    belong to two different shipments:

        WF-EUA00019586  CANCELLED  12 Nov   qty 1    <- an older, dead generation
        WF-EUB00019586  PICKED     16 Nov   qty 1    <- READY, the warehouse is picking it

    Here the non-zero PICKED belongs to the READY shipment, so nothing is cancelled.
    Had the non-zero PICKED belonged to some other generation while the READY one
    carried the cancellation, the READY shipment would still be cancelled: another
    shipment being picked says nothing about this one.

    When the READY generation carries BOTH — a PICKED of non-zero quantity and then a
    CANCELLED — the cancellation is the later word and the shipment is cancelled, with
    the reason "Customer Changed Their Mind". That is why only the generation's LATEST
    event counts, not merely the presence of a pick.

    A generation with no events of its own (the usual case: Kibo has just reassigned
    into a fresh one) does not fire the rule, so the workflow continues.

    Returns (moved_on, why). It never fires when the rows could not be ordered by time,
    since "latest" would then be meaningless.
    """
    if not events.ordered or not son:
        return False, ""
    latest = events.latest_by_son.get(son)
    if latest is None:
        return False, ""
    event_type, payload = latest
    if event_type != PICKED_EVENT_TYPE or payload is None:
        return False, ""
    total, found = picked_quantity_of(payload)
    if not found or total == 0:
        return False, ""
    return True, (
        f"The latest warehouse event of {son}, the READY shipment, is a PICKED of "
        f"quantity {format_quantity(total)}: the warehouse is picking this shipment, "
        "so nothing was cancelled."
    )


def warehouse_order_from_shipment(shipment: Dict[str, Any]) -> WarehouseOrder:
    """
    The CANCEL event's fields read off the Kibo shipment itself.

    The shipment carries everything GXO needs, which is more reliable than looking the
    row up in prd_orders by a reverse-engineered key:

        data.shipmentOrderNumber   "DG-EUA01688483"  (the generation GXO is holding)
        data.brandCode             "15"
        fulfillmentLocationCode    "TLITGX0015"

    Only the logisticOrderNumber is absent, so it is rebuilt the way the rest of this
    script reads it: the 2-character brand prefix of the shipmentOrderNumber followed by
    the Kibo shipmentNumber, the inverse of kibo_number_from_logistic. prd_orders stays
    the preferred source and this is the fallback.
    """
    data = shipment.get("data") if isinstance(shipment.get("data"), dict) else {}
    order_number = clean(data.get("shipmentOrderNumber")) or clean(data.get("xpoOrderId"))
    shipment_number = shipment_number_of(shipment)
    prefix = order_number[:2].upper() if len(order_number) >= 2 else ""
    return WarehouseOrder(
        shipment_order_number=order_number,
        logistic_order_number=f"{prefix}{shipment_number}" if prefix and shipment_number else "",
        location_name=shipment_fulfillment_location(shipment),
        brand=brand_of(order_number),
        brand_code=clean(data.get("brandCode")),
    )


def fetch_warehouse_orders(
    client: bigquery.Client,
    order_patterns: Sequence[str],
    location_pattern: str,
    pattern_batch_size: int,
) -> Dict[str, WarehouseOrder]:
    """
    Return {kiboShipmentNumber: WarehouseOrder} for the GXO shipments of these orders.

    Same query as cancellation_api_sender_to_GXO.fetch_order_data, with two changes:
    the shipmentOrderNumbers come from the order LIKE patterns rather than an Excel
    column, and they are parameterised instead of interpolated into the SQL.

        SELECT o.logisticOrderNumber, o.shipmentOrderNumber, o.locationName,
               o.brand, bc.brandCode
        FROM `tlg-wlfs-prd`.til.prd_orders AS o
        LEFT JOIN `tlg-wlfs-prd`.til.prd_brandConfig AS bc ON o.brand = bc.brand
        WHERE o.locationName LIKE '%GX%'

    The rows are keyed by the Kibo shipmentNumber, i.e. logisticOrderNumber minus its
    first 2 characters, which is the same mapping the rest of this script and
    fulfill_gxo_shipments.py use. That is what lets a cancel target resolved against
    Kibo be matched to the warehouse row describing the very same shipment, instead of
    assuming it is the one the CANCELLED event named.
    """
    orders_table = f"`{WLFS_PROJECT}.{WLFS_DATASET}.{ORDERS_TABLE}`"
    brand_table = f"`{WLFS_PROJECT}.{WLFS_DATASET}.{BRAND_CONFIG_TABLE}`"
    sql = f"""
        SELECT
            CAST(o.logisticOrderNumber AS STRING) AS logistic_order_number,
            CAST(o.shipmentOrderNumber AS STRING) AS shipment_order_number,
            CAST(o.locationName        AS STRING) AS location_name,
            CAST(o.brand               AS STRING) AS brand,
            CAST(bc.brandCode          AS STRING) AS brand_code
        FROM {orders_table} AS o
        LEFT JOIN {brand_table} AS bc
            ON o.brand = bc.brand
        WHERE (@location_pattern = '' OR o.locationName LIKE @location_pattern)
          AND EXISTS (
              SELECT 1 FROM UNNEST(@order_patterns) AS order_pattern
              WHERE CAST(o.shipmentOrderNumber AS STRING) LIKE order_pattern
          )
    """

    found: Dict[str, WarehouseOrder] = {}
    for batch in chunked(list(order_patterns), max(1, pattern_batch_size)):
        job = client.query(
            sql,
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ArrayQueryParameter("order_patterns", "STRING", batch),
                bigquery.ScalarQueryParameter(
                    "location_pattern", "STRING", location_pattern or ""
                ),
            ]),
        )
        for row in job.result():
            logistic = clean(row["logistic_order_number"])
            number, error = kibo_number_from_logistic(logistic)
            if error:
                warn(f"{ORDERS_TABLE}: {error}")
                continue
            # A shipment can appear more than once; the rows carry the same identifiers,
            # so the first one wins and the rest are ignored.
            found.setdefault(number, WarehouseOrder(
                shipment_order_number=clean(row["shipment_order_number"]),
                logistic_order_number=logistic,
                location_name=clean(row["location_name"]),
                brand=clean(row["brand"]),
                brand_code=clean(row["brand_code"]),
            ))
    return found


def repeat_cancellation(
    candidate: Candidate,
    own_products: frozenset,
    cancelled_by_order: Dict[str, Dict[str, frozenset]],
    product_match: str,
) -> Tuple[int, List[str]]:
    """
    How many shipmentOrderNumbers of this order were CANCELLED with the same products.

    The shipment being processed counts itself, since it too carries a CANCELLED event.
    Returns (count, the shipment order numbers counted).
    """
    siblings = cancelled_by_order.get(candidate.order_pattern, {})
    matched = [
        number for number, products in siblings.items()
        if number == candidate.shipment_order_number
        or products_match(own_products, products, product_match)
    ]
    # The shipment being processed counts only if it actually carries a CANCELLED event
    # of its own. Usually it does not: it is the fresh generation Kibo reassigned into
    # after the warehouse cancelled the previous one, and adding it would report one
    # cancellation more than the warehouse has really sent.
    return len(matched), sorted(matched)


@dataclass
class CancelTarget:
    """The shipment of the order that should actually be cancelled."""
    shipment: Optional[Dict[str, Any]] = None
    shipment_number: str = ""
    status: str = ""
    location: str = ""
    source: str = ""
    considered: str = ""
    matches: List[str] = field(default_factory=list)
    error: str = ""
    ambiguous: bool = False


def describe_shipments(shipments: Sequence[Dict[str, Any]], limit: int = 12) -> str:
    parts = [
        f"{shipment_number_of(s) or '?'}:{shipment_status(s) or '?'}"
        f"@{shipment_fulfillment_location(s) or '?'}"
        for s in shipments
    ]
    if len(parts) > limit:
        return ", ".join(parts[:limit]) + f", +{len(parts) - limit} more"
    return ", ".join(parts)


def resolve_cancel_target(
    shipments: Sequence[Dict[str, Any]],
    event_products: frozenset,
    product_keys: Sequence[str],
    product_match: str,
    excluded_statuses: Sequence[str],
    on_multiple: str,
    wanted_status: str = DEFAULT_CANCEL_TARGET_STATUS,
    location_pattern: str = DEFAULT_CANCEL_TARGET_LOCATION_PATTERN,
) -> CancelTarget:
    """
    Pick, out of the shipments of one order, the one to cancel.

    The CANCELLED event names the shipment the warehouse was working on, and Kibo has
    already auto reassigned it by the time this job runs: that shipmentNumber is
    REASSIGNED and will not accept a cancellation, while its replacement, under a new
    shipmentNumber, will. The replacement is the shipment of the same order that

      - has shipmentStatus READY (`wanted_status`, empty to accept any cancellable one)
      - sits in a GXO location: fulfillmentLocationCode LIKE '%GX%'
        (`location_pattern`, empty to drop the check)
      - is in a cancellable status: not already CANCELED or otherwise terminal, and not
        REASSIGNED, which Kibo refuses too (plus any status named in the excluded list)
      - carries the exact same products as the CANCELLED event

    READY and the GX location are what make the target the shipment the warehouse is
    actually holding, which is the one the CANCEL event is sent to GXO for. Without
    them a BACKORDER replacement, or a READY one that routing has already sent to a
    different 3PL, would be cancelled and GXO told about a shipment it never had.

    Returns a CancelTarget: `shipment_number` empty with `ambiguous` False means nothing
    matched, and what the caller does then is --on-no-cancel-target.
    """
    target = CancelTarget(considered=describe_shipments(shipments))
    cancellable = [
        shipment for shipment in shipments
        if is_cancellable_status(shipment_status(shipment), excluded_statuses)
        and is_wanted_status(shipment_status(shipment), wanted_status)
        and matches_like(shipment_fulfillment_location(shipment), location_pattern)
    ]
    matched = [
        shipment for shipment in cancellable
        if shipment_matches_products(shipment, event_products, product_keys, product_match)[0]
    ]
    target.matches = [shipment_number_of(s) for s in matched if shipment_number_of(s)]

    if not matched:
        return target
    if len(matched) > 1:
        if on_multiple != ON_MULTIPLE_NEWEST:
            target.ambiguous = True
            target.error = (
                f"{len(matched)} shipments of the order are cancellable and carry the "
                f"same products ({', '.join(target.matches)}); "
                "--on-multiple-cancel-targets newest would take the most recent one."
            )
            return target
        matched.sort(key=lambda s: (shipment_created_at(s), shipment_number_of(s)), reverse=True)

    target.shipment = matched[0]
    target.shipment_number = shipment_number_of(matched[0])
    target.status = shipment_status(matched[0])
    target.location = shipment_fulfillment_location(matched[0])
    target.source = "order lookup"
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
        self, tenant_id: str, site_id: str, shipment_number: str, reason: str,
        max_retries: int, correlation_id: str,
    ) -> Tuple[bool, str, str]:
        url = f"{self.base_url(tenant_id, site_id, shipment_number)}/canceled"
        # The CancelShipment schema nests the reason under `canceledReason`. A flat
        # {"reasonCode": ..., "moreInfo": ...} body is accepted with a 200 but silently
        # ignored, and Kibo records its own default reason ("Canceled") instead.
        response, error = self.request(
            "PUT", url, tenant_id, site_id,
            json_body={"canceledReason": {"reasonCode": reason, "moreInfo": reason}},
            max_retries=max_retries, correlation_id=correlation_id,
        )
        if response is None:
            return False, error, ""
        ok = 200 <= response.status_code < 300
        return ok, "" if ok else f"HTTP {response.status_code}", excerpt_of(response)

    def auto_reassign(
        self, tenant_id: str, site_id: str, shipment_number: str, reason: str,
        block_assignment: bool, max_retries: int, correlation_id: str,
    ) -> Tuple[bool, str, str]:
        """
        Send the shipment back through order routing (Kibo's "Auto Reassign").

        The ReassignShipment schema accepts blockAssignment, fulfillmentLocationCode,
        holdBlockAssignment, isUserAction, items, reassignedReason and runId. Omitting
        fulfillmentLocationCode is what separates an auto reassign from a manual one:
        with no target location, Kibo re-runs order routing, and the shipment drops into
        backorder when no location can fulfil it. blockAssignment keeps routing from
        choosing the location that just cancelled, which would otherwise loop.
        """
        url = f"{self.base_url(tenant_id, site_id, shipment_number)}/reassigned"
        body: Dict[str, Any] = {
            "blockAssignment": block_assignment,
            "isUserAction": True,
            "reassignedReason": {"reasonCode": reason, "moreInfo": reason},
        }
        response, error = self.request(
            "PUT", url, tenant_id, site_id,
            json_body=body, max_retries=max_retries, correlation_id=correlation_id,
        )
        if response is None:
            return False, error, ""
        ok = 200 <= response.status_code < 300
        return ok, "" if ok else f"HTTP {response.status_code}", excerpt_of(response)


class LegacyTLSAdapter(HTTPAdapter):
    """
    The GXO endpoint needs legacy SSL renegotiation, which OpenSSL 3 refuses by default.

    Same adapter as cancellation_api_sender_to_GXO.send_cancel_event, lifted out of the
    request function so the context and the pool are built once per run instead of once
    per shipment.
    """

    def init_poolmanager(self, *args: Any, **kwargs: Any) -> Any:
        context = create_urllib3_context()
        context.options |= SSL_OP_LEGACY_SERVER_CONNECT
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        kwargs["ssl_context"] = context
        return super().init_poolmanager(*args, **kwargs)


class GxoClient:
    """Sends the CANCEL event that tells the warehouse to stop working a shipment."""

    def __init__(self, api_key: str, url: str, timeout: float) -> None:
        self.api_key = api_key
        self.url = url
        self.timeout = timeout
        self.session = requests.Session()
        self.session.mount("https://", LegacyTLSAdapter())
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    @staticmethod
    def build_payload(order: WarehouseOrder, event_id: str) -> Dict[str, Any]:
        """
        The CANCEL event, field for field as cancellation_api_sender_to_GXO builds it.

        `products` stays empty: the event cancels the whole shipment, and GXO reads the
        lines from the order it already holds. locationName is sent as locationCode.
        """
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
        return {
            "eventType": GXO_EVENT_TYPE,
            "eventClass": GXO_EVENT_CLASS,
            "eventId": event_id,
            "eventTimestamp": timestamp,
            "data": {
                "shipmentOrderNumber": order.shipment_order_number,
                "logisticOrderNumber": order.logistic_order_number,
                "locationCode": order.location_name,
                "brandCode": order.brand_code,
                "brand": order.brand,
                "products": [],
                "updateReason": GXO_UPDATE_REASON,
                "reasonMessage": GXO_REASON_MESSAGE,
            },
        }

    def send_cancel(
        self, order: WarehouseOrder, event_id: str, max_retries: int,
    ) -> Tuple[bool, str, str]:
        """Returns (ok, error, response excerpt)."""
        payload = self.build_payload(order, event_id)
        headers = {"Content-Type": "application/json", "api_key": self.api_key}
        last_error = ""
        for attempt in range(1, max_retries + 1):
            try:
                response = self.session.post(
                    self.url, headers=headers, json=payload,
                    timeout=self.timeout, verify=False,
                )
            except requests.RequestException as exc:
                last_error = str(exc)
                if attempt < max_retries:
                    time.sleep(retry_sleep_seconds(attempt))
                    continue
                return False, last_error, ""
            if (response.status_code in RETRYABLE_STATUS_CODES
                    and attempt < max_retries):
                time.sleep(retry_sleep_seconds(attempt, response))
                continue
            ok = 200 <= response.status_code < 300
            return (
                ok,
                "" if ok else f"HTTP {response.status_code}",
                excerpt_of(response),
            )
        return False, last_error or "Unknown error", ""


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


def shipment_channel(shipment: Dict[str, Any]) -> str:
    """
    The shipment's channel as Kibo reports it.

    The channel is read from the ITEMS, in each item's `data` map:

        items[].data.channel   "ecommerce", "Farfetch", ...

    That is the sales channel the order came through, and it is what decides between
    cancelling and auto reassigning. The shipment's own `data.channel` is NOT it: it
    names how the order reached Kibo ("Order Connector", with "channelType":
    "marketplace") and would read as a channel of its own, so the items win over it.

    Items that are no longer on the shipment are consulted too, because a shipment whose
    lines have all been reassigned or cancelled keeps them under reassignedItems or
    canceledItems and would otherwise report no channel at all. The shipment-level data
    and a deep search remain as last resorts, for a shipment with no items left anywhere.
    """
    for item in shipment_items_of(shipment) + reassigned_or_cancelled_items_of(shipment):
        data = item.get("data") if isinstance(item, dict) else None
        if isinstance(data, dict):
            for key in ("channel", "channelName", "salesChannel"):
                value = clean(data.get(key))
                if value:
                    return value
    for key in ("channel", "channelName", "salesChannel"):
        value = clean(shipment.get(key))
        if value:
            return value
    data = shipment.get("data")
    if isinstance(data, dict):
        for key in ("channel", "channelName", "salesChannel"):
            value = clean(data.get(key))
            if value:
                return value
    for key in ("channel", "channelName", "salesChannel"):
        for value in iter_values(shipment, key):
            text = clean(value)
            if text:
                return text
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
    "oms_shipment_number", "sixth_character", "oms_channel", "kibo_channel", "channel", "channel_source",
    "order_pattern", "cancelled_products", "cancelled_order_shipments",
    "cancelled_order_shipment_numbers", "cancelled_order_generations",
    "latest_event_type", "latest_event_shipment_order_number", "picked_from",
    "repeat_cancel",
    "resolution", "resolution_source", "has_cancelled_event", "has_picked_event", "picked_quantity",
    "logistic_order_number", "kibo_shipment_number", "number_matches_oms",
    "external_order_id", "event_shipment_number", "latest_cancelled_shipment_number",
    "event_is_cancel_target", "order_shipments",
    "cancel_target_candidates", "cancel_target_source", "cancel_target_location",
    "warehouse_shipment_order_number", "warehouse_logistic_order_number",
    "warehouse_location_code", "warehouse_brand_code", "warehouse_event_id",
    "warehouse_source",
    "warehouse_action", "warehouse_response", "reason_code",
    "kibo_status_before", "kibo_status_after", "kibo_reason_after",
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
    eligible = [c for c in candidates if c.kibo_shipment_number]
    cancelled = [c for c in candidates if c.action == "canceled" and c.success]
    reassigned = [c for c in candidates if c.action == "reassigned" and c.success]
    already = [c for c in candidates if c.action in ("already_terminal", "already_reassigned")]
    excluded = [c for c in candidates if c.action == "sixth_character_excluded"]
    unknown_brand = [c for c in candidates if c.action == "unknown_brand"]
    warehouse_picked = [c for c in candidates if c.action == "warehouse_picked"]
    ecommerce_blocked = [c for c in candidates if c.repeat_cancel == "blocked"]
    ecommerce_untouched = [c for c in candidates if c.action == "ecommerce_disabled"]
    retenanted = [c for c in candidates if c.oms_tenant_id and (
        c.oms_tenant_id != c.tenant_id or c.oms_site_id != c.site_id)]
    failed = [c for c in candidates if c.error]
    customer = [c for c in eligible if c.reason_code == REASON_CUSTOMER]
    no_inventory = [c for c in eligible if c.reason_code == REASON_NO_INVENTORY]
    backordered = [c for c in reassigned if is_already_reassigned_status(c.kibo_status_after)]
    repeat = [c for c in eligible if c.repeat_cancel == "yes"]
    retargeted = [
        c for c in eligible
        if c.event_shipment_number and c.kibo_shipment_number != c.event_shipment_number
    ]
    ambiguous = [c for c in candidates if c.action == "ambiguous_cancel_target"]
    no_target = [c for c in candidates if c.action == "no_cancel_target"]
    warehouse_sent = [c for c in candidates if c.warehouse_action == "sent"]
    warehouse_only = [c for c in candidates if c.action == "canceled_warehouse_only"]
    warehouse_failed = [c for c in candidates if c.action == "warehouse_cancel_failed"]
    no_warehouse_row = [c for c in candidates if c.action == "no_warehouse_order_row"]
    lines = [
        "## Cancel cancelled shipments",
        "",
        f"- Mode: **{'dry run' if dry_run else 'live'}**",
        f"- Rows (one per shipment): **{len(candidates)}**",
        f"- Eligible (CANCELLED event + shipmentNumber): **{len(eligible)}**",
        f"- Reason *{REASON_CUSTOMER}*: **{len(customer)}**",
        f"- Reason *{REASON_NO_INVENTORY}*: **{len(no_inventory)}**",
        f"- Cancelled: **{len(cancelled)}**",
        f"- Auto reassigned (ecommerce): **{len(reassigned)}**"
        + (f", of which now backorder/reassigned: **{len(backordered)}**" if reassigned else ""),
        f"- Ecommerce cancelled instead of reassigned (same products cancelled too "
        f"often on the order): **{len(repeat)}**",
        f"- Cancelled on the order's replacement shipment rather than the one the event "
        f"names: **{len(retargeted)}**",
        f"- CANCEL events accepted by the warehouse: **{len(warehouse_sent)}**",
        f"- Warehouse only, the latest CANCELLED event is not the READY GXO shipment "
        f"(Kibo waits for GXO to confirm): **{len(warehouse_only)}**",
        f"- Left alone, the warehouse refused the CANCEL: **{len(warehouse_failed)}**",
        f"- Left alone, no {ORDERS_TABLE} row for the cancel target: **{len(no_warehouse_row)}**",
        f"- Left alone, no READY shipment of the order in a GXO location: **{len(no_target)}**",
        f"- Left alone, several cancellable shipments of the order match: **{len(ambiguous)}**",
        f"- Already in a terminal or reassigned status: **{len(already)}**",
        f"- Tenant/site taken from the brand config rather than {BI_TABLE}: "
        f"**{len(retenanted)}**",
        f"- Ecommerce left untouched, neither cancelled nor reassigned: "
        f"**{len(ecommerce_untouched)}**",
        f"- Ecommerce reassigned rather than cancelled, cancellation disabled: "
        f"**{len(ecommerce_blocked) - len(ecommerce_untouched)}**",
        f"- Left alone, the warehouse's latest event is a successful PICKED: "
        f"**{len(warehouse_picked)}**",
        f"- Left alone, brand not in the Kibo config: **{len(unknown_brand)}**",
        f"- Left alone, 6th character not allowed: **{len(excluded)}**",
        f"- With an error: **{len(failed)}**",
        "",
    ]
    if eligible:
        lines += [
            "| shipmentOrderNumber | tenant | site | shipmentNumber | location | channel | orderNumber | cancelled on order | resolution | picked qty | reason | recorded reason | warehouse | before | after | action | error |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
        for c in eligible[:200]:
            lines.append(
                f"| {c.shipment_order_number} | {c.tenant_id} | {c.site_id} | "
                f"{c.kibo_shipment_number}"
                + (
                    f" (was {c.event_shipment_number})"
                    if c.event_shipment_number and c.event_shipment_number != c.kibo_shipment_number
                    else ""
                )
                + " | "
                f"{c.cancel_target_location or c.warehouse_location_code or '-'} | "
                f"{c.channel or '-'} | {c.order_pattern or '-'} | "
                f"{c.cancelled_order_shipments or '-'} | "
                f"{c.resolution or '-'}{' (repeat)' if c.repeat_cancel == 'yes' else ''} | "
                f"{c.picked_quantity or '-'} | {c.reason_code or '-'} | {c.kibo_reason_after or '-'} | "
                f"{c.warehouse_action or '-'} | "
                f"{c.kibo_status_before or '-'} | "
                f"{c.kibo_status_after or '-'} | {c.action} | {c.error[:120]} |"
            )
        if len(eligible) > 200:
            lines.append(f"| ... | | | | | | | | | | | | | | | | {len(eligible) - 200} more rows in the CSV artifact |")
    with open(summary_path, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true",
                        help="Run every check and read each Kibo shipment, but do not call the canceled endpoint.")
    parser.add_argument("--oms-status", default="READY",
                        help="Keep only rows with this shipmentStatus in oms_active_shipments. "
                             "Pass an empty string to disable the filter.")
    parser.add_argument("--no-picked-reason", default="skip", choices=("skip", "no-inventory", "customer"),
                        help="What to do when a CANCELLED shipment has no PICKED event at all.")
    parser.add_argument("--ecommerce-channel", default=DEFAULT_ECOMMERCE_CHANNEL,
                        help="Shipments on this channel are auto reassigned instead of cancelled, so they "
                             "go back through order routing and into backorder. Compared case-insensitively. "
                             "Pass an empty string to cancel every shipment regardless of channel.")
    parser.add_argument("--block-assignment", dest="block_assignment", action="store_true",
                        help="Auto reassign with blockAssignment = true, so routing cannot hand the "
                             "shipment straight back to the location that cancelled it. "
                             "Off by default: blockAssignment = false.")
    parser.set_defaults(block_assignment=False)
    parser.add_argument("--max-cancelled-order-shipments", type=int,
                        default=DEFAULT_MAX_CANCELLED_ORDER_SHIPMENTS,
                        help="For ecommerce shipments only: when the same products of the same order "
                             "number already carry a CANCELLED event under more than this many distinct "
                             "shipmentOrderNumber values (this shipment included), cancel the READY "
                             "shipment instead of auto reassigning it. Default 3, so the 4th fires.")
    parser.add_argument("--product-match", default=DEFAULT_PRODUCT_MATCH,
                        choices=(PRODUCT_MATCH_EXACT, PRODUCT_MATCH_OVERLAP, PRODUCT_MATCH_ANY),
                        help="How 'the same products' is decided when counting the cancelled "
                             "shipmentOrderNumbers of an order: 'exact' needs identical product sets, "
                             "'overlap' at least one product in common, 'any' ignores products.")
    parser.add_argument("--no-resolve-cancel-target", dest="resolve_cancel_target",
                        action="store_false",
                        help="Cancel the shipmentNumber the CANCELLED event names instead of "
                             "looking the order up with getShipments by externalOrderId. Kibo "
                             "auto reassigns a cancelled shipment, so that number is usually "
                             "REASSIGNED and the cancellation is refused.")
    parser.set_defaults(resolve_cancel_target=True)
    parser.add_argument("--cancel-target-excluded-statuses",
                        default=DEFAULT_CANCEL_TARGET_EXCLUDED_STATUSES,
                        help="Extra shipment statuses never chosen as the cancel target, on top "
                             "of CANCELED and the other terminal ones and REASSIGNED, which Kibo "
                             "refuses anyway. Comma separated, empty by default: every "
                             "cancellable status is a valid target.")
    parser.add_argument("--cancel-target-status", default=DEFAULT_CANCEL_TARGET_STATUS,
                        help="The shipment of the order chosen as the cancel target must "
                             "carry this shipmentStatus. Default READY: that is the "
                             "shipment the warehouse is holding and the one the CANCEL "
                             "event is sent to GXO for. Pass an empty string to accept "
                             "any cancellable status.")
    parser.add_argument("--cancel-target-location-pattern",
                        default=DEFAULT_CANCEL_TARGET_LOCATION_PATTERN,
                        help="The cancel target's fulfillmentLocationCode must match this "
                             "SQL-style LIKE pattern, case-insensitively. Default '%%GX%%', "
                             "so only shipments sitting in a GXO location are cancelled. "
                             "Pass an empty string to drop the check.")
    parser.add_argument("--on-no-cancel-target", default=DEFAULT_ON_NO_CANCEL_TARGET,
                        choices=(ON_NO_TARGET_SKIP, ON_NO_TARGET_EVENT),
                        help="What to do when no shipment of the order is READY in a GXO "
                             "location with the same products: 'skip' reports the row as "
                             "no_cancel_target and touches nothing, 'event' falls back to "
                             "the shipmentNumber the CANCELLED event names, as before.")
    parser.add_argument("--allow-unknown-brand", action="store_true",
                        help="Process a shipment whose brand is not in BRAND_KIBO_CONFIG, "
                             "using the tenant_id and site_id from oms_active_shipments. "
                             "Off by default: an unconfirmed tenant means a filtered Kibo "
                             "query can come back empty instead of failing.")
    parser.add_argument("--require-event-target", dest="require_event_target",
                        action="store_true",
                        help="Cancel in Kibo only when the latest CANCELLED event names the "
                             "READY GXO shipment itself, and otherwise send the CANCEL to "
                             "GXO and leave Kibo for a later run, once GXO has confirmed "
                             "with its own CANCELLED event. OFF BY DEFAULT: the READY GXO "
                             "shipment is cancelled in Kibo as soon as GXO accepts the "
                             "CANCEL, whichever generation the cancellation named.")
    parser.set_defaults(require_event_target=False)
    parser.add_argument("--no-warehouse-cancel", dest="warehouse_cancel",
                        action="store_false",
                        help="Do not send the CANCEL event to GXO; cancel in Kibo only. "
                             "By default the warehouse is told first and Kibo is only "
                             "cancelled once GXO has accepted.")
    parser.set_defaults(warehouse_cancel=True)
    parser.add_argument("--gxo-url", default=DEFAULT_GXO_URL,
                        help="Warehouse endpoint the CANCEL event is POSTed to.")
    parser.add_argument("--on-multiple-cancel-targets", default=DEFAULT_ON_MULTIPLE_CANCEL_TARGETS,
                        choices=(ON_MULTIPLE_SKIP, ON_MULTIPLE_NEWEST),
                        help="What to do when several shipments of the order are cancellable and "
                             "carry the same products: 'skip' reports the row as "
                             "ambiguous_cancel_target and touches nothing, 'newest' cancels the "
                             "most recently created one.")
    parser.add_argument("--order-lookup-page-size", type=int, default=DEFAULT_ORDER_LOOKUP_PAGE_SIZE,
                        help="pageSize sent with GET /api/commerce/shipments when reading the "
                             "shipments of an order.")
    parser.add_argument("--product-keys", default=",".join(DEFAULT_PRODUCT_KEYS),
                        help="Comma separated payload keys read as product identifiers, looked for "
                             "anywhere in the CANCELLED requestPayload.")
    parser.add_argument("--pattern-batch-size", type=int, default=50,
                        help="Order LIKE patterns per BigQuery query when counting the cancelled "
                             "shipmentOrderNumbers of an order. Smaller than --batch-size because "
                             "LIKE cannot use an index and every pattern widens the scan.")
    parser.add_argument("--cancel-ecommerce", dest="cancel_ecommerce", action="store_true",
                        help="Allow an ecommerce shipment to be cancelled when the "
                             "repeat-cancellation rule fires. OFF BY DEFAULT: for now an "
                             "ecommerce shipment is never cancelled and is always auto "
                             "reassigned instead, however often the order has come back "
                             "cancelled. Non-ecommerce shipments are unaffected.")
    parser.set_defaults(cancel_ecommerce=False)
    parser.add_argument("--reassign-ecommerce", dest="reassign_ecommerce",
                        action="store_true",
                        help="Allow an ecommerce shipment to be auto reassigned. OFF BY "
                             "DEFAULT: together with --cancel-ecommerce being off, this "
                             "leaves ecommerce shipments entirely untouched for now — "
                             "neither cancelled nor reassigned, and nothing sent to the "
                             "warehouse. Non-ecommerce shipments are unaffected.")
    parser.set_defaults(reassign_ecommerce=False)
    parser.add_argument("--no-repeat-cancel", dest="repeat_cancel", action="store_false",
                        help="Turn the repeat-cancellation rule off: ecommerce shipments are always "
                             "auto reassigned, however often the order has been cancelled before.")
    parser.set_defaults(repeat_cancel=True)
    parser.add_argument("--allowed-sixth-characters", default=DEFAULT_ALLOWED_SIXTH_CHARACTERS,
                        help="Optional filter: only act on a shipment whose shipmentOrderNumber has "
                             "one of these as its 6th character, the generation letter (the 'E' of "
                             "DJ-USE00041696). Case-insensitive. EMPTY BY DEFAULT, so every "
                             "generation is in scope; an order recycled past D is still handled, "
                             "and the repeat-cancellation rule decides when to stop reassigning it.")
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
    if args.max_cancelled_order_shipments < 1:
        parser.error("--max-cancelled-order-shipments must be at least 1.")
    if args.order_lookup_page_size < 1:
        parser.error("--order-lookup-page-size must be at least 1.")
    args.oms_status = args.oms_status.strip()
    args.ecommerce_channel = args.ecommerce_channel.strip()
    args.cancel_target_status = args.cancel_target_status.strip()
    args.cancel_target_location_pattern = args.cancel_target_location_pattern.strip()
    args.gxo_url = args.gxo_url.strip()
    args.cancel_target_excluded_statuses = tuple(
        status.upper()
        for status in re.split(r"[,\s;]+", args.cancel_target_excluded_statuses.strip())
        if status
    )
    args.product_keys = tuple(
        key for key in re.split(r"[,\s;]+", args.product_keys.strip()) if key
    )
    if args.repeat_cancel and not args.product_keys and args.product_match != PRODUCT_MATCH_ANY:
        parser.error("--product-keys cannot be empty unless --product-match is 'any'.")
    # Accept "ABCD", "A,B,C,D" and "a b c d" alike.
    args.allowed_sixth_characters = "".join(
        sorted({c for c in args.allowed_sixth_characters.upper() if c.isalnum()})
    )
    args.host = re.sub(r"^https?://", "", args.host.strip()).split("/", 1)[0].rstrip(".")
    return args


def split_numbers(raw: str) -> List[str]:
    return [item for item in re.split(r"[,\s;]+", raw.strip()) if item]


def format_quantity(total: float) -> str:
    return str(int(total)) if float(total).is_integer() else str(total)


def main() -> int:
    args = parse_args()
    started = datetime.now(timezone.utc)
    output_path = Path(args.output or f"cancel_cancelled_shipments_{started.strftime('%Y%m%d_%H%M%S')}.csv")

    log(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    log(f"oms_active_shipments status filter: {args.oms_status or '(none)'}")
    log(f"Shipments with a CANCELLED event but no PICKED event: {args.no_picked_reason}")
    log(
        f"6th character of shipmentOrderNumber must be one of "
        f"{args.allowed_sixth_characters}"
        if args.allowed_sixth_characters else
        "No 6th character rule: every shipmentOrderNumber is in scope."
    )
    if args.ecommerce_channel:
        log(
            f"Channel '{args.ecommerce_channel}': auto reassign instead of cancel "
            f"(blockAssignment={'true' if args.block_assignment else 'false'})"
        )
        if not args.cancel_ecommerce and not args.reassign_ecommerce:
            log(
                "  ECOMMERCE DISABLED ENTIRELY: an ecommerce shipment is neither "
                "cancelled nor reassigned, and nothing is sent to the warehouse for "
                "one; it is left exactly as it is. Pass --cancel-ecommerce and/or "
                "--reassign-ecommerce to put each half back."
            )
        elif not args.cancel_ecommerce:
            log(
                "  ECOMMERCE CANCELLATION DISABLED: an ecommerce shipment is never "
                "cancelled, however often the order has come back cancelled, and no "
                "CANCEL is sent to the warehouse for one; it is auto reassigned "
                "instead. Pass --cancel-ecommerce to re-enable the "
                "repeat-cancellation rule."
            )
        elif args.repeat_cancel:
            log(
                f"  except when more than {args.max_cancelled_order_shipments} "
                f"shipmentOrderNumber values of the same order carry a CANCELLED "
                f"event for the same products (--product-match {args.product_match}): "
                "those are cancelled instead."
            )
        else:
            log("  repeat-cancellation rule off: ecommerce is always auto reassigned.")
    else:
        log("No ecommerce channel set: every shipment is cancelled.")
    if args.resolve_cancel_target:
        log(
            "Cancellations target the shipment of the order found with GET "
            "/api/commerce/shipments?filter=externalOrderId==...: "
            + (f"shipmentStatus {args.cancel_target_status}" if args.cancel_target_status
               else "any cancellable status")
            + (
                f", fulfillmentLocationCode LIKE '{args.cancel_target_location_pattern}'"
                if args.cancel_target_location_pattern else ", any location"
            )
            + (
                f", never {', '.join(args.cancel_target_excluded_statuses)}"
                if args.cancel_target_excluded_statuses else ""
            )
            + f", same products (--product-match {args.product_match}), "
            f"on several matches: {args.on_multiple_cancel_targets}, "
            f"on no match: {args.on_no_cancel_target}."
        )
    else:
        log("Cancellations target the shipmentNumber the CANCELLED event names.")
    if args.warehouse_cancel:
        log(
            f"A CANCEL event is sent to the warehouse first ({args.gxo_url}); "
            "Kibo is only cancelled once GXO accepts it."
        )
    else:
        log("Warehouse cancellation disabled: Kibo is cancelled without telling GXO.")
    if args.require_event_target:
        log(
            "Kibo is cancelled only when the latest CANCELLED event names the READY GXO "
            "shipment; otherwise the warehouse is told and Kibo waits for GXO's own "
            "CANCELLED event."
        )
    else:
        log(
            "The READY GXO shipment is cancelled in Kibo as soon as GXO accepts the "
            "CANCEL, whichever generation the latest CANCELLED event named."
        )
    log(f"Kibo host: {args.host}")

    kibo_credentials = load_json_env("KIBO_API_CREDS")
    client_id = clean(kibo_credentials.get("client_id"))
    client_secret = clean(kibo_credentials.get("client_secret"))
    if not client_id or not client_secret:
        fail("KIBO_API_CREDS must contain non-empty 'client_id' and 'client_secret'.")

    gxo_api_key = ""
    if args.warehouse_cancel:
        gxo_api_key = clean(load_json_env("GXO_API_CREDS").get("api_key"))
        if not gxo_api_key:
            fail("GXO_API_CREDS must contain a non-empty 'api_key'.")

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

    # Excluded shipments are not queried either: the 6th-character rule decides before
    # anything else, so there is no reason to read their events.
    numbers = [
        c.shipment_order_number for c in candidates
        if sixth_character_allowed(c.shipment_order_number, args.allowed_sixth_characters)
    ]
    excluded_count = len(candidates) - len(numbers)
    if excluded_count:
        log(
            f"Left alone, 6th character not in {args.allowed_sixth_characters}: {excluded_count}"
        )
    # One read per ORDER, covering every generation. The active shipment of an order
    # rarely carries events of its own, so this is what makes an order visible at all,
    # and the same rows feed the repeat-cancellation rule.
    all_patterns = sorted({
        pattern for pattern in (order_pattern_of(number) for number in numbers) if pattern
    })
    without_pattern = len(numbers) - sum(
        1 for number in numbers if order_pattern_of(number)
    )
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
    log(f"Orders with a CANCELLED event on some generation: {with_cancelled}")

    kibo = KiboClient({"client_id": client_id, "client_secret": client_secret},
                      args.auth_url, args.host, args.timeout)
    gxo = GxoClient(gxo_api_key, args.gxo_url, args.timeout) if args.warehouse_cancel else None

    rows: List[Candidate] = []
    eligible: List[Candidate] = []
    # The products each shipment order number was CANCELLED with, kept aside because
    # `Candidate` only carries their printable form.
    products_by_number: Dict[str, frozenset] = {}
    for candidate in candidates:
        # The 6th character of the shipmentOrderNumber gates the whole workflow: a
        # shipment outside the allowed set is left alone, neither cancelled nor
        # reassigned. Checked first so no Kibo call is made for it at all.
        # The brand decides the Kibo tenant and site. oms_active_shipments carries its
        # own pair, but they can disagree with the Shipment API's, and a wrong site
        # silently empties a filtered query instead of failing, so the configured values
        # win and any disagreement is reported.
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
        # every CANCELLED event still names A to F. Looking only at G's own
        # shipmentOrderNumber finds nothing and the order is never acted on.
        candidate.order_pattern = order_pattern_of(candidate.shipment_order_number)
        events = order_events.get(candidate.order_pattern, OrderEvents())
        candidate.has_cancelled_event = events.has_cancelled
        candidate.has_picked_event = events.has_picked
        candidate.cancelled_products = format_products(events.cancelled_products)
        # The products of the order's cancellations stand in for this generation's own,
        # which are usually absent: they are what the cancel target has to carry.
        products_by_number[candidate.shipment_order_number] = events.cancelled_products
        candidate.cancelled_order_generations = ", ".join(sorted(events.cancelled_sons))
        candidate.latest_event_type = events.latest_event_type
        candidate.latest_event_shipment_order_number = events.latest_event_son

        # The warehouse has the last word. If its most recent event on this order is a
        # PICKED that actually picked something, the goods were found and the shipment
        # is being worked; the older cancellations are history. Checked before the
        # CANCELLED test so no Kibo call and no CANCEL to GXO is ever made for it.
        moved_on, why = warehouse_moved_on_of(events, candidate.shipment_order_number)
        if moved_on:
            candidate.action = "warehouse_picked"
            candidate.success = True
            candidate.error = why
            log(f"{candidate.shipment_order_number}: {why}")
            rows.append(candidate)
            continue

        if not events.has_cancelled:
            candidate.action = "skipped"
            candidate.error = (
                "No CANCELLED event in prd_shipmentStatus for any generation of order "
                f"{candidate.order_pattern or candidate.shipment_order_number}."
            )
            rows.append(candidate)
            continue

        # The PICKED quantity decides the reason sent to Kibo. The READY generation's
        # own PICKED wins when it has one, so a shipment that was picked and then
        # cancelled is reported as "Customer Changed Their Mind" rather than taking the
        # quantity from whichever other generation happened to be picked last.
        picked_payload = (
            events.picked_by_son.get(candidate.shipment_order_number)
            or events.picked_payload
        )
        candidate.picked_from = (
            candidate.shipment_order_number
            if candidate.shipment_order_number in events.picked_by_son
            else events.picked_shipment_order_number
        )
        if events.has_picked and picked_payload is not None:
            total, found = picked_quantity_of(picked_payload)
            if not found:
                candidate.action = "skipped"
                candidate.error = "The PICKED requestPayload carries no quantity field."
                rows.append(candidate)
                continue
            candidate.picked_quantity = format_quantity(total)
            candidate.reason_code = REASON_CUSTOMER if total != 0 else REASON_NO_INVENTORY
        elif events.has_picked:
            candidate.action = "skipped"
            candidate.error = "The PICKED requestPayload could not be parsed as JSON."
            rows.append(candidate)
            continue
        elif args.no_picked_reason == "skip":
            candidate.action = "skipped"
            candidate.error = (
                "No PICKED event in prd_shipmentStatus for any generation of order "
                f"{candidate.order_pattern or candidate.shipment_order_number}."
            )
            rows.append(candidate)
            continue
        else:
            candidate.reason_code = (
                REASON_NO_INVENTORY if args.no_picked_reason == "no-inventory" else REASON_CUSTOMER
            )

        # The shipment the LATEST cancellation names. prd_shipmentStatus is read newest
        # first, so the first CANCELLED logisticOrderNumber is the most recent one.
        # Kibo is only cancelled when this is the shipment that is READY in a GXO
        # location, i.e. when the warehouse cancelled the shipment it is still holding
        # rather than one Kibo has already reassigned away from it.
        if events.cancelled_logistic_numbers:
            candidate.logistic_order_number = events.cancelled_logistic_numbers[0]
            latest_number, latest_error = kibo_number_from_logistic(
                candidate.logistic_order_number
            )
            candidate.latest_cancelled_shipment_number = latest_number
            if latest_error:
                warn(f"{candidate.shipment_order_number}: {latest_error}")

        # The starting shipmentNumber is the ACTIVE one of this oms_active_shipments
        # row, not one derived from a logisticOrderNumber: the events name generations
        # Kibo has already reassigned away, so their numbers are stale. Step 4b then
        # resolves the real cancel target against the order anyway. Only when the OMS
        # table carries no shipment number does the latest cancellation's number stand
        # in, which is the old behaviour.
        expected = clean(candidate.oms_shipment_number)
        if expected:
            candidate.kibo_shipment_number = expected
            candidate.number_matches_oms = (
                "yes" if expected == candidate.latest_cancelled_shipment_number else "no"
            )
        elif candidate.latest_cancelled_shipment_number:
            candidate.kibo_shipment_number = candidate.latest_cancelled_shipment_number
            candidate.number_matches_oms = "unchecked"
        else:
            candidate.action = "skipped"
            candidate.error = (
                f"{BI_TABLE} carries no shipment number and no logisticOrderNumber was "
                "found in the order's CANCELLED payloads, so there is no Kibo shipment "
                "to act on."
            )
            rows.append(candidate)
            continue

        eligible.append(candidate)
        rows.append(candidate)

    log(f"Eligible shipments to cancel: {len(eligible)}")
    log(
        f"  reason '{REASON_CUSTOMER}': {sum(1 for c in eligible if c.reason_code == REASON_CUSTOMER)}, "
        f"reason '{REASON_NO_INVENTORY}': {sum(1 for c in eligible if c.reason_code == REASON_NO_INVENTORY)}"
    )

    # The repeat-cancellation rule counts the cancelled generations of each order. Those
    # are already in hand: fetch_order_events read every generation of every order, not
    # just the ones still in oms_active_shipments, so no second query is needed.
    cancelled_by_order: Dict[str, Dict[str, frozenset]] = {
        pattern: events.cancelled_sons for pattern, events in order_events.items()
    }
    if args.repeat_cancel and args.ecommerce_channel:
        missing = sum(1 for c in eligible if not c.order_pattern)
        if missing:
            warn(
                f"{missing} eligible shipment(s) have no usable order pattern, so the "
                "repeat-cancellation rule cannot be applied to them."
            )

    # The CANCEL event sent to GXO needs the shipment's logisticOrderNumber, location
    # and brand. They come from prd_orders, read once for every order in the run and
    # keyed by Kibo shipmentNumber so the cancel target resolved in step 4b can be
    # matched to the warehouse row describing that same shipment.
    warehouse_orders: Dict[str, WarehouseOrder] = {}
    if args.warehouse_cancel and eligible:
        order_patterns = sorted({c.order_pattern for c in eligible if c.order_pattern})
        if order_patterns:
            warehouse_orders = fetch_warehouse_orders(
                wlfs_client, order_patterns, args.cancel_target_location_pattern,
                args.pattern_batch_size,
            )
            log(
                f"Warehouse rows read from {ORDERS_TABLE}"
                + (
                    f" with locationName LIKE '{args.cancel_target_location_pattern}'"
                    if args.cancel_target_location_pattern else ""
                )
                + f": {len(warehouse_orders)}"
            )

    for index, candidate in enumerate(eligible, start=1):
        candidate.correlation_id = uuid.uuid4().hex
        log(
            f"[{index}/{len(eligible)}] {candidate.shipment_order_number} -> "
            f"shipmentNumber={candidate.kibo_shipment_number} "
            f"(tenant={candidate.tenant_id}, site={candidate.site_id}, "
            f"pickedQty={candidate.picked_quantity or 'n/a'}, reason={candidate.reason_code})"
        )

        shipment, read_error = kibo.get_shipment(
            candidate.tenant_id, candidate.site_id, candidate.kibo_shipment_number, args.max_retries
        )
        if shipment is None:
            candidate.action = "read_failed"
            candidate.error = f"Could not read the shipment: {read_error}"
            warn(f"    {candidate.error}")
            continue

        candidate.kibo_status_before = shipment_status(shipment)
        candidate.kibo_status_after = candidate.kibo_status_before
        candidate.kibo_channel = shipment_channel(shipment)

        # Kibo has now told us which site the shipment really sits on. It is the
        # authority over both oms_active_shipments and the brand config, which have both
        # been seen to be off by a site, and the cancel and reassign PUTs use it.
        kibo_site = shipment_site_id(shipment)
        if kibo_site and kibo_site != candidate.site_id:
            log(
                f"    site {candidate.site_id} ({candidate.tenant_source}) -> "
                f"{kibo_site} as reported by the shipment"
            )
            candidate.site_id = kibo_site
            candidate.tenant_source = f"{candidate.tenant_source}, site from kibo"

        # oms_active_shipments wins when it carries a channel; otherwise the Kibo
        # shipment itself is the source. Both are kept in the CSV.
        if candidate.oms_channel:
            candidate.channel = candidate.oms_channel
            candidate.channel_source = BI_TABLE
        elif candidate.kibo_channel:
            candidate.channel = candidate.kibo_channel
            candidate.channel_source = "kibo"
        else:
            candidate.channel = ""
            candidate.channel_source = "unknown"

        is_ecommerce = same_channel(candidate.channel, args.ecommerce_channel)
        candidate.resolution = RESOLUTION_REASSIGN if is_ecommerce else RESOLUTION_CANCEL
        candidate.resolution_source = "channel"

        # An ecommerce shipment is reassigned only while routing still has somewhere to
        # send it. Once the same products of this order have come back CANCELLED under
        # more than --max-cancelled-order-shipments shipmentOrderNumbers, reassigning
        # again would just bounce it once more, so the READY shipment is cancelled.
        # A kill switch over everything below: while it is on, an ecommerce shipment is
        # NEVER cancelled, whatever the repeat-cancellation rule would say. It is always
        # auto reassigned instead, so routing keeps the order alive and the warehouse is
        # not told to cancel. Turn it off with --cancel-ecommerce to restore the
        # repeat-cancellation behaviour.
        if is_ecommerce and not args.cancel_ecommerce:
            candidate.repeat_cancel = "blocked"
            candidate.resolution = RESOLUTION_REASSIGN
            candidate.resolution_source = "channel (ecommerce cancellation disabled)"
        elif is_ecommerce and args.repeat_cancel:
            if not candidate.order_pattern:
                candidate.repeat_cancel = "unknown"
                candidate.resolution_source = (
                    "channel (no order pattern: shipmentOrderNumber too short)"
                )
            else:
                own_products = products_by_number.get(
                    candidate.shipment_order_number, frozenset()
                )
                if not own_products and args.product_match != PRODUCT_MATCH_ANY:
                    candidate.repeat_cancel = "unknown"
                    candidate.resolution_source = "channel (no products in the CANCELLED payload)"
                else:
                    count, numbers_counted = repeat_cancellation(
                        candidate, own_products, cancelled_by_order, args.product_match
                    )
                    candidate.cancelled_order_shipments = str(count)
                    candidate.cancelled_order_shipment_numbers = ", ".join(numbers_counted)
                    if count > args.max_cancelled_order_shipments:
                        candidate.repeat_cancel = "yes"
                        candidate.resolution = RESOLUTION_CANCEL
                        candidate.resolution_source = (
                            f"{count} cancelled shipmentOrderNumbers on order "
                            f"{candidate.order_pattern}"
                        )
                        log(
                            f"    order {candidate.order_pattern}: the same products were "
                            f"cancelled on {count} shipmentOrderNumbers "
                            f"({', '.join(numbers_counted)}) -> cancel instead of reassign"
                        )
                    else:
                        candidate.repeat_cancel = "no"

        log(f"    channel={candidate.channel or 'unknown'} ({candidate.channel_source}) "
            f"-> {candidate.resolution} ({candidate.resolution_source})")

        # The second half of the ecommerce kill switch: with cancelling disabled the
        # only thing left for an ecommerce shipment is the auto reassign, and that is
        # off too for now, so the shipment is left exactly as it is. Kibo is not
        # touched, nothing goes to the warehouse, and routing is left to do whatever it
        # does on its own. --reassign-ecommerce puts the reassign back.
        if (is_ecommerce and candidate.resolution == RESOLUTION_REASSIGN
                and not args.reassign_ecommerce):
            candidate.action = "ecommerce_disabled"
            candidate.success = True
            candidate.error = (
                f"Channel '{candidate.channel}': ecommerce shipments are neither "
                "cancelled nor reassigned at the moment, so nothing was done."
            )
            log("    ecommerce reassignment disabled: left untouched.")
            continue

        # The shipment the CANCELLED event names has already been auto reassigned by
        # Kibo, so it is REASSIGNED and will refuse a cancellation; the replacement
        # shipment of the same order is the one to cancel. It is found by asking Kibo
        # for the shipments of the order and keeping the cancellable one that carries
        # the same products as the event.
        candidate.event_shipment_number = candidate.kibo_shipment_number
        candidate.external_order_id = (
            shipment_external_order_id(shipment)
            or external_order_id_from_shipment_order_number(candidate.shipment_order_number)
        )
        if candidate.resolution == RESOLUTION_CANCEL and args.resolve_cancel_target:
            if not candidate.external_order_id:
                candidate.cancel_target_source = "event (no externalOrderId)"
                warn(
                    f"    no externalOrderId for {candidate.shipment_order_number}: "
                    f"cancelling {candidate.kibo_shipment_number} as named by the event."
                )
            else:
                siblings, lookup_error = kibo.get_shipments_by_external_order_id(
                    candidate.tenant_id, candidate.site_id, candidate.external_order_id,
                    args.max_retries, args.order_lookup_page_size,
                )
                if lookup_error:
                    candidate.action = "order_lookup_failed"
                    candidate.error = (
                        f"Could not read the shipments of order "
                        f"{candidate.external_order_id}: {lookup_error}"
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
                    args.cancel_target_status,
                    args.cancel_target_location_pattern,
                )
                candidate.order_shipments = target.considered
                candidate.cancel_target_candidates = ", ".join(target.matches)

                if target.ambiguous:
                    candidate.action = "ambiguous_cancel_target"
                    candidate.error = target.error
                    warn(f"    {candidate.error}")
                    continue

                if target.shipment_number:
                    candidate.cancel_target_source = target.source
                    candidate.cancel_target_location = target.location
                    if target.shipment_number != candidate.kibo_shipment_number:
                        log(
                            f"    order {candidate.external_order_id}: cancelling "
                            f"{target.shipment_number} ({target.status or 'unknown'} "
                            f"@ {target.location or 'unknown'}) "
                            f"instead of {candidate.kibo_shipment_number} "
                            f"({candidate.kibo_status_before or 'unknown'})"
                        )
                    candidate.kibo_shipment_number = target.shipment_number
                    candidate.kibo_status_before = target.status
                    candidate.kibo_status_after = target.status
                    shipment = target.shipment or shipment
                elif args.on_no_cancel_target == ON_NO_TARGET_SKIP:
                    # Nothing of this order is READY in a GXO location with these
                    # products, so there is no shipment for the warehouse to cancel.
                    # Cancelling the one the event names would tell GXO about a
                    # shipment it no longer holds.
                    candidate.action = "no_cancel_target"
                    candidate.error = target.error or (
                        f"No shipment of order {candidate.external_order_id} is "
                        + (f"{args.cancel_target_status} " if args.cancel_target_status else "cancellable ")
                        + (
                            f"in a location matching '{args.cancel_target_location_pattern}' "
                            if args.cancel_target_location_pattern else ""
                        )
                        + f"with the same products ({target.considered or 'none returned'})."
                    )
                    warn(f"    {candidate.error}")
                    continue
                else:
                    candidate.cancel_target_source = "event (no match on the order)"
                    if target.error:
                        warn(f"    {target.error}")
                    log(
                        f"    order {candidate.external_order_id}: no cancellable "
                        f"shipment with the same products ({target.considered or 'none returned'}); "
                        f"keeping {candidate.kibo_shipment_number}."
                    )

        if is_terminal_status(candidate.kibo_status_before):
            candidate.action = "already_terminal"
            candidate.success = True
            log(f"    already {candidate.kibo_status_before}, nothing to do.")
            continue

        if (candidate.resolution == RESOLUTION_REASSIGN
                and is_already_reassigned_status(candidate.kibo_status_before)):
            candidate.action = "already_reassigned"
            candidate.success = True
            log(f"    already {candidate.kibo_status_before}, nothing to reassign.")
            continue

        # A cancellation goes to the warehouse before Kibo, so the row describing the
        # cancel target in prd_orders has to exist. It is looked up by the target's own
        # shipmentNumber, not by the one the CANCELLED event named: after step 4b those
        # are usually two different shipments, and GXO is holding the target.
        # Kibo is cancelled only when the shipment named by the LATEST CANCELLED event
        # is itself the cancel target, i.e. the one that is READY in a GXO location.
        #
        # When it is not, the warehouse cancelled a shipment Kibo has already reassigned
        # elsewhere, and the READY GXO shipment is a replacement GXO has not cancelled
        # yet. Cancelling it in Kibo would throw away a shipment the floor still
        # considers live, so only the CANCEL event goes out; GXO answers with its own
        # CANCELLED event for that shipment, and the next run — where the latest
        # cancellation does name the READY GXO shipment — cancels it in Kibo.
        cancel_in_kibo = True
        if candidate.resolution == RESOLUTION_CANCEL and args.require_event_target:
            latest = candidate.latest_cancelled_shipment_number or candidate.event_shipment_number
            cancel_in_kibo = bool(latest) and latest == candidate.kibo_shipment_number
            candidate.event_is_cancel_target = "yes" if cancel_in_kibo else "no"
            if not cancel_in_kibo:
                log(
                    f"    the latest CANCELLED event names {latest or 'nothing'}, not the "
                    f"READY GXO shipment {candidate.kibo_shipment_number}: warehouse only, "
                    "Kibo is left alone."
                )
                if not args.warehouse_cancel:
                    candidate.action = "not_cancel_target"
                    candidate.error = (
                        f"The latest CANCELLED event names {latest or 'nothing'}, not the "
                        f"READY GXO shipment {candidate.kibo_shipment_number}, and the "
                        "warehouse step is disabled, so there was nothing to do."
                    )
                    continue

        warehouse_order: Optional[WarehouseOrder] = None
        if candidate.resolution == RESOLUTION_CANCEL and args.warehouse_cancel:
            warehouse_order = warehouse_orders.get(candidate.kibo_shipment_number)
            if warehouse_order is None:
                # prd_orders has no row for the target. The Kibo shipment carries the
                # same fields, so the CANCEL event can still be built from it.
                from_kibo = warehouse_order_from_shipment(shipment)
                if from_kibo.shipment_order_number and from_kibo.logistic_order_number:
                    warehouse_order = from_kibo
                    candidate.warehouse_source = "kibo shipment"
                    warn(
                        f"    no {ORDERS_TABLE} row for shipmentNumber "
                        f"{candidate.kibo_shipment_number}; building the CANCEL event "
                        f"from the Kibo shipment ({from_kibo.shipment_order_number} / "
                        f"{from_kibo.logistic_order_number})."
                    )
            else:
                candidate.warehouse_source = ORDERS_TABLE
            if warehouse_order is None:
                candidate.action = "no_warehouse_order_row"
                candidate.error = (
                    f"No {ORDERS_TABLE} row for shipmentNumber "
                    f"{candidate.kibo_shipment_number}"
                    + (
                        f" with locationName LIKE '{args.cancel_target_location_pattern}'"
                        if args.cancel_target_location_pattern else ""
                    )
                    + ": the CANCEL event cannot be built, so nothing was cancelled."
                )
                warn(f"    {candidate.error}")
                continue
            candidate.warehouse_shipment_order_number = warehouse_order.shipment_order_number
            candidate.warehouse_logistic_order_number = warehouse_order.logistic_order_number
            candidate.warehouse_location_code = warehouse_order.location_name
            candidate.warehouse_brand_code = warehouse_order.brand_code

        if args.dry_run:
            if candidate.resolution == RESOLUTION_REASSIGN:
                candidate.action = "would_reassign"
            elif cancel_in_kibo:
                candidate.action = "would_cancel"
            else:
                candidate.action = "would_cancel_warehouse_only"
            candidate.success = True
            candidate.response_excerpt = (
                "DRY RUN - the reassigned endpoint was not called"
                if candidate.resolution == RESOLUTION_REASSIGN
                else "DRY RUN - neither GXO nor the canceled endpoint was called"
            )
            if warehouse_order is not None:
                candidate.warehouse_action = "would_send"
                log(
                    f"    DRY RUN would send CANCEL to GXO for "
                    f"{warehouse_order.shipment_order_number} / "
                    f"{warehouse_order.logistic_order_number} @ "
                    f"{warehouse_order.location_name}"
                )
            log(f"    DRY RUN status={candidate.kibo_status_before or 'unknown'} reason={candidate.reason_code}")
            continue

        if candidate.resolution == RESOLUTION_REASSIGN:
            ok, error, excerpt = kibo.auto_reassign(
                candidate.tenant_id, candidate.site_id, candidate.kibo_shipment_number,
                candidate.reason_code, args.block_assignment, args.max_retries,
                candidate.correlation_id,
            )
            candidate.action = "reassigned" if ok else "reassign_failed"
            candidate.error = "" if ok else f"PUT /reassigned failed: {error}"
        else:
            # The warehouse is told first: Kibo is only cancelled once GXO has accepted
            # the CANCEL event, so the two systems cannot end up disagreeing about a
            # shipment that is still being worked on the floor.
            if warehouse_order is not None and gxo is not None:
                candidate.warehouse_event_id = str(uuid.uuid4()).upper()
                sent, gxo_error, gxo_excerpt = gxo.send_cancel(
                    warehouse_order, candidate.warehouse_event_id, args.max_retries,
                )
                candidate.warehouse_response = gxo_excerpt
                candidate.warehouse_action = "sent" if sent else "failed"
                if not sent:
                    candidate.action = "warehouse_cancel_failed"
                    candidate.error = (
                        f"POST to GXO failed for "
                        f"{warehouse_order.shipment_order_number} / "
                        f"{warehouse_order.logistic_order_number}: {gxo_error}. "
                        "Kibo was left untouched."
                    )
                    candidate.success = False
                    warn(f"    {candidate.error}")
                    if gxo_excerpt:
                        warn(f"    response: {gxo_excerpt[:400]}")
                    if args.sleep_between_requests > 0:
                        time.sleep(args.sleep_between_requests)
                    continue
                log(
                    f"    CANCEL sent to GXO for "
                    f"{warehouse_order.shipment_order_number} / "
                    f"{warehouse_order.logistic_order_number} @ "
                    f"{warehouse_order.location_name}"
                )

            if not cancel_in_kibo:
                # The warehouse has been told; Kibo keeps the shipment until GXO sends
                # back its own CANCELLED event for it, which a later run acts on.
                candidate.action = "canceled_warehouse_only"
                candidate.success = True
                candidate.response_excerpt = candidate.warehouse_response
                log(
                    "    warehouse only: Kibo keeps "
                    f"{candidate.kibo_shipment_number} until GXO confirms the "
                    "cancellation with its own CANCELLED event."
                )
                if args.sleep_between_requests > 0:
                    time.sleep(args.sleep_between_requests)
                continue

            ok, error, excerpt = kibo.mark_canceled(
                candidate.tenant_id, candidate.site_id, candidate.kibo_shipment_number,
                candidate.reason_code, args.max_retries, candidate.correlation_id,
            )
            candidate.action = "canceled" if ok else "cancel_failed"
            candidate.error = "" if ok else (
                f"PUT /canceled failed: {error}"
                + (
                    " — the warehouse CANCEL was already sent, so GXO and Kibo now "
                    "disagree and the shipment has to be cancelled in Kibo by hand."
                    if candidate.warehouse_action == "sent" else ""
                )
            )
        candidate.success = ok
        candidate.response_excerpt = excerpt

        if ok:
            refreshed, _ = kibo.get_shipment(
                candidate.tenant_id, candidate.site_id, candidate.kibo_shipment_number, args.max_retries
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
            # An auto reassign that found no location leaves the shipment in BACKORDER,
            # which is the intended landing spot; anything else is worth seeing.
            if candidate.resolution == RESOLUTION_REASSIGN and candidate.kibo_status_after:
                log(f"    reassigned, shipment is now {candidate.kibo_status_after}")
        else:
            warn(f"    {candidate.error}")
            if excerpt:
                warn(f"    response: {excerpt[:400]}")

        if args.sleep_between_requests > 0:
            time.sleep(args.sleep_between_requests)

    write_csv(output_path, rows)
    write_job_summary(rows, args.dry_run)

    failures = [c for c in eligible if not c.success]
    log(f"Results written to {output_path}")
    log(
        f"Summary: {len(candidates)} active shipment order numbers, {len(eligible)} eligible shipments, "
        f"{sum(1 for c in eligible if c.action == 'canceled' and c.success)} cancelled "
        f"(of which {sum(1 for c in eligible if c.repeat_cancel == 'yes')} ecommerce on the "
        f"repeat-cancellation rule), "
        f"{sum(1 for c in eligible if c.action == 'reassigned' and c.success)} auto reassigned, "
        f"{sum(1 for c in eligible if c.warehouse_action == 'sent')} CANCEL events sent to "
        f"the warehouse (of which "
        f"{sum(1 for c in eligible if c.action == 'canceled_warehouse_only')} warehouse only, "
        f"Kibo left for a later run), "
        f"{sum(1 for c in eligible if c.action == 'no_cancel_target')} with no READY GXO "
        f"shipment on the order, "
        f"{sum(1 for c in rows if c.action == 'sixth_character_excluded')} left alone on the "
        f"6th character filter, {len(failures)} failed."
    )
    return 0 if not failures else 2


if __name__ == "__main__":
    raise SystemExit(main())
