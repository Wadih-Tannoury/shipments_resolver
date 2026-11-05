#!/usr/bin/env python3
"""
Fulfill Kibo shipments that are shipped by GXO and already invoiced.

Pipeline
--------
1. Read the active shipments from BigQuery (BI project):
       `tlg-business-intelligence-prd`.til.oms_active_shipments
   keeping shipment_order_number, tenant_id, site_id.

2. For those shipment order numbers, check in the WLFS project:
   - `tlg-wlfs-prd`.til.prd_shipmentStatus
         eventType = 'SHIPPED' and warehouse = 'GXO'
   - `tlg-wlfs-prd`.til.prd_invoiceUpdate
         INVOICE_RESULT whose requestPayload contains at least one
         documentLink starting with "http"

3. For the shipments that satisfy both conditions, read
   data.logisticOrderNumber from the SHIPPED requestPayload
   (e.g. "BB82902"), drop the first 2 characters and use the rest as the
   Kibo shipmentNumber (82902).

4. In Kibo:
       GET https://t{tenantId}-s{siteId}.{host}/api/commerce/shipments/{shipmentNumber}
       PUT https://t{tenantId}-s{siteId}.{host}/api/commerce/shipments/{shipmentNumber}/fulfilled

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
INVOICE_UPDATE_TABLE = "prd_invoiceUpdate"

DEFAULT_HOST = "tp3.mozu.com"
DEFAULT_AUTH_URL = "https://home.mozu.com/api/platform/applications/authtickets/oauth"

BQ_SCOPES = ["https://www.googleapis.com/auth/bigquery"]
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Kibo statuses that mean "nothing left to do".
TERMINAL_STATUSES = {
    "FULFILLED", "COMPLETE", "COMPLETED", "CANCELED", "CANCELLED",
    "CLOSED", "SHIPPED", "PICKEDUP", "PICKED_UP",
}

# Candidate column names, resolved case-insensitively against the real schema.
SHIPMENT_NUMBER_CANDIDATES = ("shipmentOrderNumber", "shipment_order_number")
PAYLOAD_CANDIDATES = ("requestPayload", "request_payload")
EVENT_TYPE_CANDIDATES = ("eventType", "event_type")
WAREHOUSE_CANDIDATES = ("warehouse", "warehouseCode", "warehouse_code")
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


def first_http_document_link(payload: Dict[str, Any]) -> str:
    for value in iter_values(payload, "documentLink"):
        text = clean(value)
        if text.lower().startswith("http"):
            return text
    return ""


def logistic_order_number_of(payload: Dict[str, Any]) -> str:
    for value in iter_values(payload, "logisticOrderNumber"):
        text = clean(value)
        if text:
            return text
    return ""


def kibo_shipment_number(logistic_order_number: str) -> Tuple[str, str]:
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
    has_shipped_gxo: bool = False
    has_invoice_document: bool = False
    document_link: str = ""
    logistic_order_number: str = ""
    kibo_shipment_number: str = ""
    kibo_status_before: str = ""
    kibo_status_after: str = ""
    action: str = "skipped"
    success: bool = False
    error: str = ""
    response_excerpt: str = ""
    correlation_id: str = ""


# ---------------------------------------------------------------------------
# BigQuery queries
# ---------------------------------------------------------------------------

def fetch_active_shipments(
    client: bigquery.Client,
    only_numbers: Sequence[str],
    limit: int,
) -> List[Candidate]:
    columns = table_columns(client, BI_PROJECT, BI_DATASET, BI_TABLE)
    where = f"`{BI_PROJECT}.{BI_DATASET}.{BI_TABLE}`"
    number_col = resolve_column(columns, SHIPMENT_NUMBER_CANDIDATES, where)
    tenant_col = resolve_column(columns, ("tenant_id", "tenantId"), where)
    site_col = resolve_column(columns, ("site_id", "siteId"), where)

    filters = [f"`{number_col}` IS NOT NULL", f"TRIM(CAST(`{number_col}` AS STRING)) != ''"]
    params: List[bigquery.ScalarQueryParameter | bigquery.ArrayQueryParameter] = []
    if only_numbers:
        filters.append(f"CAST(`{number_col}` AS STRING) IN UNNEST(@only_numbers)")
        params.append(bigquery.ArrayQueryParameter("only_numbers", "STRING", list(only_numbers)))

    sql = f"""
        SELECT DISTINCT
            CAST(`{number_col}` AS STRING) AS shipment_order_number,
            CAST(`{tenant_col}` AS STRING) AS tenant_id,
            CAST(`{site_col}`   AS STRING) AS site_id
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
        candidates.append(Candidate(shipment_order_number=number, tenant_id=tenant, site_id=site))
    return candidates


def fetch_shipped_events(
    client: bigquery.Client, numbers: Sequence[str], warehouse: str, batch_size: int
) -> Dict[str, Dict[str, Any]]:
    """Return {shipmentOrderNumber: latest SHIPPED payload} for the given warehouse."""
    where = f"`{WLFS_PROJECT}.{WLFS_DATASET}.{SHIPMENT_STATUS_TABLE}`"
    columns = table_columns(client, WLFS_PROJECT, WLFS_DATASET, SHIPMENT_STATUS_TABLE)
    number_col = resolve_column(columns, SHIPMENT_NUMBER_CANDIDATES, where)
    payload_col = resolve_column(columns, PAYLOAD_CANDIDATES, where)
    event_col = resolve_column(columns, EVENT_TYPE_CANDIDATES, where)
    warehouse_col = resolve_column(columns, WAREHOUSE_CANDIDATES, where)
    ts_col = resolve_column(columns, TIMESTAMP_CANDIDATES, where, required=False)

    order_by = f"ORDER BY `{ts_col}` DESC" if ts_col else ""
    sql = f"""
        SELECT
            CAST(`{number_col}` AS STRING) AS shipment_order_number,
            CAST(`{payload_col}` AS STRING) AS payload
        FROM {where}
        WHERE CAST(`{number_col}` AS STRING) IN UNNEST(@numbers)
          AND UPPER(CAST(`{event_col}` AS STRING)) = 'SHIPPED'
          AND UPPER(CAST(`{warehouse_col}` AS STRING)) = UPPER(@warehouse)
        {order_by}
    """

    found: Dict[str, Dict[str, Any]] = {}
    for batch in chunked(numbers, batch_size):
        job = client.query(
            sql,
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ArrayQueryParameter("numbers", "STRING", batch),
                bigquery.ScalarQueryParameter("warehouse", "STRING", warehouse),
            ]),
        )
        for row in job.result():
            number = clean(row["shipment_order_number"])
            if number in found:  # rows are newest first
                continue
            payload = parse_payload(row["payload"])
            if payload is not None:
                found[number] = payload
            else:
                found.setdefault(number, {})
    return found


def fetch_invoice_documents(
    client: bigquery.Client, numbers: Sequence[str], batch_size: int
) -> Dict[str, str]:
    """Return {shipmentOrderNumber: documentLink} for INVOICE_RESULT payloads with an http link."""
    where = f"`{WLFS_PROJECT}.{WLFS_DATASET}.{INVOICE_UPDATE_TABLE}`"
    columns = table_columns(client, WLFS_PROJECT, WLFS_DATASET, INVOICE_UPDATE_TABLE)
    number_col = resolve_column(columns, SHIPMENT_NUMBER_CANDIDATES, where)
    payload_col = resolve_column(columns, PAYLOAD_CANDIDATES, where)
    event_col = resolve_column(columns, EVENT_TYPE_CANDIDATES, where, required=False)
    ts_col = resolve_column(columns, TIMESTAMP_CANDIDATES, where, required=False)

    event_filter = (
        f"AND UPPER(CAST(`{event_col}` AS STRING)) = 'INVOICE_RESULT'" if event_col else ""
    )
    order_by = f"ORDER BY `{ts_col}` DESC" if ts_col else ""
    sql = f"""
        SELECT
            CAST(`{number_col}` AS STRING) AS shipment_order_number,
            CAST(`{payload_col}` AS STRING) AS payload
        FROM {where}
        WHERE CAST(`{number_col}` AS STRING) IN UNNEST(@numbers)
        {event_filter}
        {order_by}
    """

    found: Dict[str, str] = {}
    for batch in chunked(numbers, batch_size):
        job = client.query(
            sql,
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ArrayQueryParameter("numbers", "STRING", batch),
            ]),
        )
        for row in job.result():
            number = clean(row["shipment_order_number"])
            if number in found:
                continue
            payload = parse_payload(row["payload"])
            if payload is None:
                continue
            # When eventType is not a column, make sure this really is an INVOICE_RESULT.
            if not event_col and clean(payload.get("eventType")).upper() not in ("", "INVOICE_RESULT"):
                continue
            link = first_http_document_link(payload)
            if link:
                found[number] = link
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
            f"https://t{tenant_id}-s{site_id}.{self.host}"
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

    def mark_fulfilled(
        self, tenant_id: str, site_id: str, shipment_number: str, max_retries: int, correlation_id: str
    ) -> Tuple[bool, str, str]:
        url = f"{self.base_url(tenant_id, site_id, shipment_number)}/fulfilled"
        response, error = self.request(
            "PUT", url, tenant_id, site_id, max_retries=max_retries, correlation_id=correlation_id
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
    "shipment_order_number", "tenant_id", "site_id",
    "has_shipped_gxo", "has_invoice_document", "document_link",
    "logistic_order_number", "kibo_shipment_number",
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
    fulfilled = [c for c in candidates if c.action == "fulfilled" and c.success]
    already = [c for c in candidates if c.action == "already_terminal"]
    failed = [c for c in candidates if c.error]
    lines = [
        "## Fulfil GXO shipments",
        "",
        f"- Mode: **{'dry run' if dry_run else 'live'}**",
        f"- Active shipments read: **{len(candidates)}**",
        f"- Eligible (SHIPPED@GXO + invoice document): **{len(eligible)}**",
        f"- Fulfilled: **{len(fulfilled)}**",
        f"- Already in a terminal status: **{len(already)}**",
        f"- With an error: **{len(failed)}**",
        "",
    ]
    if eligible:
        lines += [
            "| shipmentOrderNumber | tenant | site | shipmentNumber | before | after | action | error |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
        for c in eligible[:200]:
            lines.append(
                f"| {c.shipment_order_number} | {c.tenant_id} | {c.site_id} | {c.kibo_shipment_number} | "
                f"{c.kibo_status_before or '-'} | {c.kibo_status_after or '-'} | {c.action} | {c.error[:120]} |"
            )
        if len(eligible) > 200:
            lines.append(f"| ... | | | | | | | {len(eligible) - 200} more rows in the CSV artifact |")
    with open(summary_path, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true",
                        help="Run every check and read each Kibo shipment, but do not call the fulfilled endpoint.")
    parser.add_argument("--warehouse", default="GXO", help="Warehouse filter on the SHIPPED event.")
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
    args.host = re.sub(r"^https?://", "", args.host.strip()).split("/", 1)[0].rstrip(".")
    return args


def split_numbers(raw: str) -> List[str]:
    return [item for item in re.split(r"[,\s;]+", raw.strip()) if item]


def main() -> int:
    args = parse_args()
    started = datetime.now(timezone.utc)
    output_path = Path(args.output or f"fulfill_gxo_shipments_{started.strftime('%Y%m%d_%H%M%S')}.csv")

    log(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    log(f"Warehouse filter: {args.warehouse}")
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

    candidates = fetch_active_shipments(bi_client, only_numbers, args.limit)
    log(f"Active shipments read from {BI_TABLE}: {len(candidates)}")
    if not candidates:
        log("Nothing to do.")
        write_csv(output_path, candidates)
        write_job_summary(candidates, args.dry_run)
        return 0

    numbers = [c.shipment_order_number for c in candidates]
    shipped = fetch_shipped_events(wlfs_client, numbers, args.warehouse, args.batch_size)
    log(f"SHIPPED events at {args.warehouse}: {len(shipped)}")
    invoiced = fetch_invoice_documents(wlfs_client, numbers, args.batch_size)
    log(f"INVOICE_RESULT events with an http documentLink: {len(invoiced)}")

    kibo = KiboClient({"client_id": client_id, "client_secret": client_secret},
                      args.auth_url, args.host, args.timeout)

    eligible: List[Candidate] = []
    for candidate in candidates:
        payload = shipped.get(candidate.shipment_order_number)
        candidate.has_shipped_gxo = payload is not None
        candidate.document_link = invoiced.get(candidate.shipment_order_number, "")
        candidate.has_invoice_document = bool(candidate.document_link)

        if not candidate.has_shipped_gxo or not candidate.has_invoice_document:
            missing = []
            if not candidate.has_shipped_gxo:
                missing.append(f"SHIPPED@{args.warehouse}")
            if not candidate.has_invoice_document:
                missing.append("INVOICE_RESULT document")
            candidate.action = "skipped"
            candidate.error = "Missing " + " and ".join(missing)
            continue

        candidate.logistic_order_number = logistic_order_number_of(payload or {})
        if not candidate.logistic_order_number:
            candidate.action = "skipped"
            candidate.error = "No logisticOrderNumber in the SHIPPED requestPayload."
            continue

        number, error = kibo_shipment_number(candidate.logistic_order_number)
        if error:
            candidate.action = "skipped"
            candidate.error = error
            continue
        candidate.kibo_shipment_number = number
        eligible.append(candidate)

    log(f"Eligible for fulfillment: {len(eligible)}")

    for index, candidate in enumerate(eligible, start=1):
        candidate.correlation_id = uuid.uuid4().hex
        log(
            f"[{index}/{len(eligible)}] {candidate.shipment_order_number} -> "
            f"shipmentNumber={candidate.kibo_shipment_number} "
            f"(tenant={candidate.tenant_id}, site={candidate.site_id})"
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
            candidate.action = "would_fulfill"
            candidate.success = True
            candidate.response_excerpt = "DRY RUN - the fulfilled endpoint was not called"
            log(f"    DRY RUN status={candidate.kibo_status_before or 'unknown'}")
            continue

        ok, error, excerpt = kibo.mark_fulfilled(
            candidate.tenant_id, candidate.site_id, candidate.kibo_shipment_number,
            args.max_retries, candidate.correlation_id,
        )
        candidate.action = "fulfilled" if ok else "fulfill_failed"
        candidate.success = ok
        candidate.error = "" if ok else f"PUT /fulfilled failed: {error}"
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

    write_csv(output_path, candidates)
    write_job_summary(candidates, args.dry_run)

    failures = [c for c in eligible if not c.success]
    log(f"Results written to {output_path}")
    log(
        f"Summary: {len(candidates)} active, {len(eligible)} eligible, "
        f"{sum(1 for c in eligible if c.action == 'fulfilled' and c.success)} fulfilled, "
        f"{len(failures)} failed."
    )
    return 0 if not failures else 2


if __name__ == "__main__":
    raise SystemExit(main())
