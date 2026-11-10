#!/usr/bin/env python3
"""
Cancel in Kibo the shipments that the warehouse has already cancelled.

Pipeline
--------
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
       PUT https://t{tenantId}.{host}/api/commerce/shipments/{shipmentNumber}/canceled

       {"reasonCode": "<reason>", "moreInfo": "<reason>"}

   The GET is a safety net: a shipment that Kibo already reports as cancelled, closed
   or otherwise terminal is reported as `already_terminal` and left alone.

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

import requests
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

DEFAULT_HOST = "tp3.mozu.com"
DEFAULT_AUTH_URL = "https://home.mozu.com/api/platform/applications/authtickets/oauth"

BQ_SCOPES = ["https://www.googleapis.com/auth/bigquery"]
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Both spellings exist in the data.
CANCELLED_EVENT_TYPES = ("CANCELLED", "CANCELED")
PICKED_EVENT_TYPE = "PICKED"

REASON_CUSTOMER = "Customer Changed Their Mind"
REASON_NO_INVENTORY = "No Inventory"

# Kibo statuses that mean "nothing left to cancel".
TERMINAL_STATUSES = {
    "CANCELED", "CANCELLED", "CLOSED", "COMPLETE", "COMPLETED",
    "FULFILLED", "SHIPPED", "PICKEDUP", "PICKED_UP",
}

# Candidate column names, resolved case-insensitively against the real schema.
SHIPMENT_NUMBER_CANDIDATES = ("shipmentOrderNumber", "shipment_order_number")
OMS_NUMBER_CANDIDATES = (
    "oms_shipment_number", "omsShipmentNumber", "shipment_number", "shipmentNumber",
)
PAYLOAD_CANDIDATES = ("requestPayload", "request_payload")
EVENT_TYPE_CANDIDATES = ("eventType", "event_type")
OMS_STATUS_CANDIDATES = ("shipmentStatus", "shipment_status", "status")
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
    has_cancelled_event: bool = False
    has_picked_event: bool = False
    picked_quantity: str = ""
    logistic_order_number: str = ""
    kibo_shipment_number: str = ""
    number_matches_oms: str = ""
    reason_code: str = ""
    kibo_status_before: str = ""
    kibo_status_after: str = ""
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


def fetch_status_events(
    client: bigquery.Client, numbers: Sequence[str], batch_size: int
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
            logistic = logistic_order_number_of(payload)
            if logistic and logistic not in events.logistic_numbers:
                events.logistic_numbers.append(logistic)
    return found


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
        response, error = self.request(
            "GET", self.base_url(tenant_id, site_id, shipment_number),
            tenant_id, site_id, max_retries=max_retries,
        )
        if response is None:
            return None, error
        if not (200 <= response.status_code < 300):
            return None, f"HTTP {response.status_code}: {excerpt_of(response, 300)}"
        try:
            return response.json(), ""
        except ValueError:
            return None, "Shipment response was not JSON."

    def mark_canceled(
        self, tenant_id: str, site_id: str, shipment_number: str, reason: str,
        max_retries: int, correlation_id: str,
    ) -> Tuple[bool, str, str]:
        url = f"{self.base_url(tenant_id, site_id, shipment_number)}/canceled"
        response, error = self.request(
            "PUT", url, tenant_id, site_id,
            json_body={"reasonCode": reason, "moreInfo": reason},
            max_retries=max_retries, correlation_id=correlation_id,
        )
        if response is None:
            return False, error, ""
        ok = 200 <= response.status_code < 300
        return ok, "" if ok else f"HTTP {response.status_code}", excerpt_of(response)


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
    "shipment_order_number", "tenant_id", "site_id", "oms_shipment_status",
    "oms_shipment_number", "has_cancelled_event", "has_picked_event", "picked_quantity",
    "logistic_order_number", "kibo_shipment_number", "number_matches_oms", "reason_code",
    "kibo_status_before", "kibo_status_after",
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
    already = [c for c in candidates if c.action == "already_terminal"]
    failed = [c for c in candidates if c.error]
    customer = [c for c in eligible if c.reason_code == REASON_CUSTOMER]
    no_inventory = [c for c in eligible if c.reason_code == REASON_NO_INVENTORY]
    lines = [
        "## Cancel cancelled shipments",
        "",
        f"- Mode: **{'dry run' if dry_run else 'live'}**",
        f"- Rows (one per shipment): **{len(candidates)}**",
        f"- Eligible (CANCELLED event + shipmentNumber): **{len(eligible)}**",
        f"- Reason *{REASON_CUSTOMER}*: **{len(customer)}**",
        f"- Reason *{REASON_NO_INVENTORY}*: **{len(no_inventory)}**",
        f"- Cancelled: **{len(cancelled)}**",
        f"- Already in a terminal status: **{len(already)}**",
        f"- With an error: **{len(failed)}**",
        "",
    ]
    if eligible:
        lines += [
            "| shipmentOrderNumber | tenant | site | shipmentNumber | picked qty | reason | before | after | action | error |",
            "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
        for c in eligible[:200]:
            lines.append(
                f"| {c.shipment_order_number} | {c.tenant_id} | {c.site_id} | {c.kibo_shipment_number} | "
                f"{c.picked_quantity or '-'} | {c.reason_code or '-'} | {c.kibo_status_before or '-'} | "
                f"{c.kibo_status_after or '-'} | {c.action} | {c.error[:120]} |"
            )
        if len(eligible) > 200:
            lines.append(f"| ... | | | | | | | | | {len(eligible) - 200} more rows in the CSV artifact |")
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
    args.oms_status = args.oms_status.strip()
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

    numbers = [c.shipment_order_number for c in candidates]
    events_by_number = fetch_status_events(wlfs_client, numbers, args.batch_size)
    with_cancelled = sum(1 for e in events_by_number.values() if e.has_cancelled)
    log(f"Shipment order numbers with a CANCELLED event: {with_cancelled}")

    kibo = KiboClient({"client_id": client_id, "client_secret": client_secret},
                      args.auth_url, args.host, args.timeout)

    rows: List[Candidate] = []
    eligible: List[Candidate] = []
    for candidate in candidates:
        events = events_by_number.get(candidate.shipment_order_number, StatusEvents())
        candidate.has_cancelled_event = events.has_cancelled
        candidate.has_picked_event = events.has_picked

        if not events.has_cancelled:
            candidate.action = "skipped"
            candidate.error = "No CANCELLED event in prd_shipmentStatus."
            rows.append(candidate)
            continue

        # The PICKED quantity decides the reason sent to Kibo.
        if events.has_picked and events.picked_payload is not None:
            total, found = picked_quantity_of(events.picked_payload)
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
            candidate.error = "No PICKED event in prd_shipmentStatus."
            rows.append(candidate)
            continue
        else:
            candidate.reason_code = (
                REASON_NO_INVENTORY if args.no_picked_reason == "no-inventory" else REASON_CUSTOMER
            )

        if not events.logistic_numbers:
            candidate.action = "skipped"
            candidate.error = "No logisticOrderNumber in the CANCELLED or PICKED requestPayload."
            rows.append(candidate)
            continue

        # The PICKED / CANCELLED events were matched on shipmentOrderNumber; the Kibo
        # shipmentNumber taken from logisticOrderNumber has to match the shipment
        # number of the same oms_active_shipments row. One shipment order number can
        # carry several logisticOrderNumber values, and only the matching one is the
        # Kibo shipment of this row.
        expected = clean(candidate.oms_shipment_number)
        if len(events.logistic_numbers) > 1:
            log(
                f"{candidate.shipment_order_number}: {len(events.logistic_numbers)} "
                f"logisticOrderNumber values ({', '.join(events.logistic_numbers)})"
            )

        derived: List[Tuple[str, str]] = []  # (logisticOrderNumber, kiboShipmentNumber)
        conversion_errors: List[str] = []
        seen_shipment_numbers: set[str] = set()
        for logistic in events.logistic_numbers:
            number, error = kibo_number_from_logistic(logistic)
            if error:
                conversion_errors.append(error)
                continue
            if number in seen_shipment_numbers:
                continue
            seen_shipment_numbers.add(number)
            derived.append((logistic, number))

        if expected:
            matching = [(l, n) for l, n in derived if n == expected]
        else:
            # No shipment number in oms_active_shipments: nothing to check against.
            matching = derived

        if not matching:
            candidate.action = "skipped"
            if expected and derived:
                candidate.number_matches_oms = "no"
                candidate.logistic_order_number = derived[0][0]
                candidate.error = (
                    f"No logisticOrderNumber matches oms shipment number {expected} "
                    f"(payload gives {', '.join(n for _, n in derived)})."
                )
            elif conversion_errors:
                candidate.error = conversion_errors[0]
            else:
                candidate.error = "No logisticOrderNumber in the CANCELLED or PICKED requestPayload."
            rows.append(candidate)
            continue

        for logistic, number in matching:
            row = replace(candidate)
            row.logistic_order_number = logistic
            row.kibo_shipment_number = number
            row.number_matches_oms = "yes" if expected else "unchecked"
            eligible.append(row)
            rows.append(row)

    log(f"Eligible shipments to cancel: {len(eligible)}")
    log(
        f"  reason '{REASON_CUSTOMER}': {sum(1 for c in eligible if c.reason_code == REASON_CUSTOMER)}, "
        f"reason '{REASON_NO_INVENTORY}': {sum(1 for c in eligible if c.reason_code == REASON_NO_INVENTORY)}"
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

        if is_terminal_status(candidate.kibo_status_before):
            candidate.action = "already_terminal"
            candidate.success = True
            log(f"    already {candidate.kibo_status_before}, nothing to do.")
            continue

        if args.dry_run:
            candidate.action = "would_cancel"
            candidate.success = True
            candidate.response_excerpt = "DRY RUN - the canceled endpoint was not called"
            log(f"    DRY RUN status={candidate.kibo_status_before or 'unknown'} reason={candidate.reason_code}")
            continue

        ok, error, excerpt = kibo.mark_canceled(
            candidate.tenant_id, candidate.site_id, candidate.kibo_shipment_number,
            candidate.reason_code, args.max_retries, candidate.correlation_id,
        )
        candidate.action = "canceled" if ok else "cancel_failed"
        candidate.success = ok
        candidate.error = "" if ok else f"PUT /canceled failed: {error}"
        candidate.response_excerpt = excerpt

        if ok:
            refreshed, _ = kibo.get_shipment(
                candidate.tenant_id, candidate.site_id, candidate.kibo_shipment_number, args.max_retries
            )
            if isinstance(refreshed, dict):
                candidate.kibo_status_after = shipment_status(refreshed) or candidate.kibo_status_after
            log(f"    OK status={candidate.kibo_status_after or 'unknown'}")
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
        f"{sum(1 for c in eligible if c.action == 'canceled' and c.success)} cancelled, "
        f"{len(failures)} failed."
    )
    return 0 if not failures else 2


if __name__ == "__main__":
    raise SystemExit(main())
