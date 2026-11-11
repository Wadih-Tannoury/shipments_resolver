#!/usr/bin/env python3
"""
Reassign blocked Kibo shipments to the location they are already assigned to.

Pipeline
--------
1. Read from BigQuery (BI project):

       SELECT * FROM `tlg-business-intelligence-prd`.til.prd_fulfillment_monitoring_table
       WHERE fulfillment_status = 'Accept Shipment'
         AND shipment_order_number IS NULL
         AND location_name IS NOT NULL
         AND location_class <> 'preorder'
         AND (blocking_reason = 'OMS Shipment Update - Assign shipmentOrderNumber action not successful'
              OR blocking_reason = 'exchange order should be unblocked')
       ORDER BY order_submit_date DESC

2. Split the rows on their hold flags:

   | hold_exchange_process | hold_process   | What happens                              |
   | --------------------- | -------------- | ----------------------------------------- |
   | false / null          | false / null   | reassign                                  |
   | true                  | false / null   | release the exchange hold, then reassign  |
   | any                   | true           | skipped, reported as `hold_process_true`  |

3. Release the exchange hold, exactly as release_held_exchange_shipments.py does:

       PUT https://t{tenantId}.{host}/api/commerce/shipments/{shipmentNumber}/data?merge=true
       x-vol-tenant: {tenantId}
       x-vol-site:   {siteId}

       {"holdExchangeProcess": false}

4. Reassign the shipment to the location it is already assigned to:

       PUT https://t{tenantId}-s{siteId}.{host}/api/commerce/shipments/{shipmentNumber}/reassigned
       x-vol-tenant: {tenantId}
       x-vol-site:   {siteId}

       {"blockAssignment": false, "fulfillmentLocationCode": "<location>", "isUserAction": true}

Nothing is written back to BigQuery.

Credentials come from environment variables (GitHub repository secrets):
    BI_BIGQUERY_CREDS   service account JSON for tlg-business-intelligence-prd
    KIBO_API_CREDS      {"client_id": "...", "client_secret": "..."}

The service account only reads, so BigQuery Job User on the project and Data Viewer on
the `til` dataset is enough.
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
from dataclasses import asdict, dataclass
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
BI_TABLE = "prd_fulfillment_monitoring_table"

FULFILLMENT_STATUS = "Accept Shipment"
EXCLUDED_LOCATION_CLASS = "preorder"
BLOCKING_REASONS = (
    "OMS Shipment Update - Assign shipmentOrderNumber action not successful",
    "exchange order should be unblocked",
)

DEFAULT_HOST = "tp3.mozu.com"
DEFAULT_AUTH_URL = "https://home.mozu.com/api/platform/applications/authtickets/oauth"

# Step 3: the same body release_held_exchange_shipments.py sends.
RELEASE_HOLD_BODY: Dict[str, Any] = {"holdExchangeProcess": False}

BQ_SCOPES = ["https://www.googleapis.com/auth/bigquery"]
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Candidate column names, resolved case-insensitively against the real schema.
# prd_fulfillment_monitoring_table calls the Kibo shipment number `oms_shipment_number`,
# the same name oms_active_shipments uses; the rest are fallbacks for other tables.
SHIPMENT_NUMBER_CANDIDATES = (
    "oms_shipment_number", "omsShipmentNumber", "shipment_number", "shipmentNumber",
    "kibo_shipment_number", "kiboShipmentNumber", "shipment_id", "shipmentId",
)
TENANT_CANDIDATES = ("tenant_id", "tenantId")
SITE_CANDIDATES = ("site_id", "siteId")
# The reassign call needs the location CODE. prd_fulfillment_monitoring_table has no
# code column, only location_name, so that is what the run falls back to; on these
# tenants the name and the code are usually the same string. Override with
# --location-column if that turns out not to hold.
LOCATION_CODE_CANDIDATES = (
    "location_code", "locationCode", "fulfillment_location_code", "fulfillmentLocationCode",
    "location", "location_name", "locationName",
)
# Resolving to one of these means the run is sending a name where Kibo wants a code.
LOCATION_NAME_FALLBACKS = {"location_name", "locationname"}
FULFILLMENT_STATUS_CANDIDATES = ("fulfillment_status", "fulfillmentStatus")
SHIPMENT_ORDER_NUMBER_CANDIDATES = ("shipment_order_number", "shipmentOrderNumber")
LOCATION_NAME_CANDIDATES = ("location_name", "locationName")
LOCATION_CLASS_CANDIDATES = ("location_class", "locationClass")
BLOCKING_REASON_CANDIDATES = ("blocking_reason", "blockingReason")
ORDER_SUBMIT_DATE_CANDIDATES = ("order_submit_date", "orderSubmitDate")
HOLD_EXCHANGE_CANDIDATES = ("hold_exchange_process", "holdExchangeProcess")
HOLD_PROCESS_CANDIDATES = ("hold_process", "holdProcess")
ORDER_NUMBER_CANDIDATES = ("order_number", "orderNumber", "external_order_id", "externalOrderId")

TRUE_STRINGS = {"TRUE", "T", "1", "Y", "YES"}


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
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, float) and value.is_integer():
        return str(int(value))
    text = str(value).strip()
    return text[:-2] if re.fullmatch(r"\d+\.0", text) else text


def is_true(value: Any) -> bool:
    """A hold flag is true only when it says so; null and anything empty are false."""
    return clean(value).upper() in TRUE_STRINGS


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


def resolve_column(
    columns: Dict[str, str], candidates: Sequence[str], where: str, required: bool = True
) -> str:
    for candidate in candidates:
        real = columns.get(candidate.lower())
        if real:
            return real
    if required:
        fail(f"None of the columns {list(candidates)} exist in {where}. Found: {sorted(columns.values())}")
    return ""


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Shipment:
    """One row of the monitoring table."""
    shipment_number: str = ""
    order_number: str = ""
    tenant_id: str = ""
    site_id: str = ""
    location_code: str = ""
    location_name: str = ""
    blocking_reason: str = ""
    order_submit_date: str = ""
    hold_exchange_process: str = ""
    hold_process: str = ""
    needs_release: bool = False
    released: bool = False
    release_error: str = ""
    action: str = "skipped"
    success: bool = False
    status_code: Optional[int] = None
    error: str = ""
    response_excerpt: str = ""
    correlation_id: str = ""


# ---------------------------------------------------------------------------
# BigQuery: the blocked shipments
# ---------------------------------------------------------------------------

def fetch_blocked_shipments(
    client: bigquery.Client,
    only_shipment_numbers: Sequence[str],
    limit: int,
    location_column: str,
) -> Tuple[List[Shipment], int]:
    """
    Run the monitoring-table query.

    Returns (shipments, number of BigQuery rows read). Rows that cannot be acted on, because
    a tenant, site, shipment number or location is missing, come back carrying their error so
    they show up in the report instead of disappearing.
    """
    where = f"`{BI_PROJECT}.{BI_DATASET}.{BI_TABLE}`"
    columns = table_columns(client, BI_PROJECT, BI_DATASET, BI_TABLE)

    number_col = resolve_column(columns, SHIPMENT_NUMBER_CANDIDATES, where)
    tenant_col = resolve_column(columns, TENANT_CANDIDATES, where)
    site_col = resolve_column(columns, SITE_CANDIDATES, where)
    status_col = resolve_column(columns, FULFILLMENT_STATUS_CANDIDATES, where)
    order_number_col = resolve_column(columns, SHIPMENT_ORDER_NUMBER_CANDIDATES, where)
    location_name_col = resolve_column(columns, LOCATION_NAME_CANDIDATES, where)
    location_class_col = resolve_column(columns, LOCATION_CLASS_CANDIDATES, where)
    blocking_col = resolve_column(columns, BLOCKING_REASON_CANDIDATES, where)
    submit_col = resolve_column(columns, ORDER_SUBMIT_DATE_CANDIDATES, where, required=False)
    hold_exchange_col = resolve_column(columns, HOLD_EXCHANGE_CANDIDATES, where)
    hold_process_col = resolve_column(columns, HOLD_PROCESS_CANDIDATES, where)
    external_order_col = resolve_column(columns, ORDER_NUMBER_CANDIDATES, where, required=False)

    if location_column:
        location_col = columns.get(location_column.lower())
        if not location_col:
            fail(f"Column '{location_column}' does not exist in {where}. Found: {sorted(columns.values())}")
    else:
        location_col = resolve_column(columns, LOCATION_CODE_CANDIDATES, where)
    log(f"Location code read from column `{location_col}`.")
    if not location_column and location_col.lower() in LOCATION_NAME_FALLBACKS:
        warn(
            f"{BI_TABLE} has no location code column, so `{location_col}` is being sent as "
            "fulfillmentLocationCode. If Kibo rejects the location, pass --location-column "
            "with the column that holds the real code."
        )

    filters = [
        f"`{status_col}` = @fulfillment_status",
        f"`{order_number_col}` IS NULL",
        f"`{location_name_col}` IS NOT NULL",
        f"`{location_class_col}` <> @excluded_location_class",
        f"`{blocking_col}` IN UNNEST(@blocking_reasons)",
    ]
    params: List[Any] = [
        bigquery.ScalarQueryParameter("fulfillment_status", "STRING", FULFILLMENT_STATUS),
        bigquery.ScalarQueryParameter("excluded_location_class", "STRING", EXCLUDED_LOCATION_CLASS),
        bigquery.ArrayQueryParameter("blocking_reasons", "STRING", list(BLOCKING_REASONS)),
    ]
    if only_shipment_numbers:
        filters.append(f"CAST(`{number_col}` AS STRING) IN UNNEST(@only_shipment_numbers)")
        params.append(
            bigquery.ArrayQueryParameter("only_shipment_numbers", "STRING", list(only_shipment_numbers))
        )

    submit_select = f"CAST(`{submit_col}` AS STRING)" if submit_col else "CAST(NULL AS STRING)"
    external_select = f"CAST(`{external_order_col}` AS STRING)" if external_order_col else "CAST(NULL AS STRING)"
    order_by = f"ORDER BY `{submit_col}` DESC" if submit_col else ""

    sql = f"""
        SELECT
            CAST(`{number_col}` AS STRING)         AS shipment_number,
            {external_select}                      AS order_number,
            CAST(`{tenant_col}` AS STRING)         AS tenant_id,
            CAST(`{site_col}` AS STRING)           AS site_id,
            CAST(`{location_col}` AS STRING)       AS location_code,
            CAST(`{location_name_col}` AS STRING)  AS location_name,
            CAST(`{blocking_col}` AS STRING)       AS blocking_reason,
            {submit_select}                        AS order_submit_date,
            CAST(`{hold_exchange_col}` AS STRING)  AS hold_exchange_process,
            CAST(`{hold_process_col}` AS STRING)   AS hold_process
        FROM {where}
        WHERE {' AND '.join(filters)}
        {order_by}
    """
    if limit > 0:
        sql += f"\n        LIMIT {int(limit)}"

    job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))

    shipments: List[Shipment] = []
    seen: set[Tuple[str, str, str]] = set()
    read = 0
    for record in job.result():
        read += 1
        shipment = Shipment(
            shipment_number=clean(record["shipment_number"]),
            order_number=clean(record["order_number"]),
            tenant_id=clean(record["tenant_id"]),
            site_id=clean(record["site_id"]),
            location_code=clean(record["location_code"]),
            location_name=clean(record["location_name"]),
            blocking_reason=clean(record["blocking_reason"]),
            order_submit_date=clean(record["order_submit_date"]),
            hold_exchange_process=clean(record["hold_exchange_process"]),
            hold_process=clean(record["hold_process"]),
        )

        # The monitoring table has one row per shipment line on some tenants, so the same
        # shipment can come back several times. Reassigning it once is enough.
        key = (shipment.shipment_number, shipment.tenant_id, shipment.site_id)
        if shipment.shipment_number and key in seen:
            continue
        seen.add(key)

        missing = []
        if not shipment.shipment_number:
            missing.append(number_col)
        if not shipment.tenant_id:
            missing.append(tenant_col)
        if not shipment.site_id:
            missing.append(site_col)
        if not shipment.location_code:
            missing.append(location_col)
        if missing:
            shipment.action = "incomplete_row"
            shipment.error = "Missing in the monitoring table: " + ", ".join(missing)
            shipments.append(shipment)
            continue

        # hold_process = true means something else is deliberately holding the shipment, so
        # it is left alone whatever the exchange flag says.
        if is_true(shipment.hold_process):
            shipment.action = "hold_process_true"
            shipment.error = "hold_process is true, the shipment was left alone."
            shipments.append(shipment)
            continue

        shipment.needs_release = is_true(shipment.hold_exchange_process)
        shipment.action = "pending"
        shipments.append(shipment)

    return shipments, read


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

    def shipment_data_url(self, tenant_id: str, shipment_number: str) -> str:
        return (
            f"https://t{tenant_id}.{self.host}"
            f"/api/commerce/shipments/{quote(shipment_number, safe='')}/data?merge=true"
        )

    def reassign_url(self, tenant_id: str, site_id: str, shipment_number: str) -> str:
        return (
            f"https://t{tenant_id}-s{site_id}.{self.host}"
            f"/api/commerce/shipments/{quote(shipment_number, safe='')}/reassigned"
        )

    def release_exchange_hold(
        self, shipment: Shipment, max_retries: int, correlation_id: str
    ) -> Tuple[bool, str, str]:
        url = self.shipment_data_url(shipment.tenant_id, shipment.shipment_number)
        response, error = self.request(
            "PUT", url, shipment.tenant_id, shipment.site_id,
            json_body=RELEASE_HOLD_BODY, max_retries=max_retries, correlation_id=correlation_id,
        )
        if response is None:
            return False, error, ""
        ok = 200 <= response.status_code < 300
        return ok, "" if ok else f"HTTP {response.status_code}", excerpt_of(response)

    def reassign(
        self, shipment: Shipment, max_retries: int, correlation_id: str
    ) -> Tuple[bool, Optional[int], str, str]:
        url = self.reassign_url(shipment.tenant_id, shipment.site_id, shipment.shipment_number)
        body = {
            "blockAssignment": False,
            "fulfillmentLocationCode": shipment.location_code,
            "isUserAction": True,
        }
        response, error = self.request(
            "PUT", url, shipment.tenant_id, shipment.site_id,
            json_body=body, max_retries=max_retries, correlation_id=correlation_id,
        )
        if response is None:
            return False, None, error, ""
        ok = 200 <= response.status_code < 300
        return ok, response.status_code, "" if ok else f"HTTP {response.status_code}", excerpt_of(response)


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

CSV_FIELDS = [
    "shipment_number", "order_number", "tenant_id", "site_id", "location_code", "location_name",
    "blocking_reason", "order_submit_date", "hold_exchange_process", "hold_process",
    "needs_release", "released", "release_error",
    "action", "success", "status_code", "error", "response_excerpt", "correlation_id",
]


def write_csv(path: Path, shipments: Sequence[Shipment]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for shipment in shipments:
            record = asdict(shipment)
            writer.writerow({key: record[key] for key in CSV_FIELDS})


def write_job_summary(shipments: Sequence[Shipment], rows_read: int, dry_run: bool) -> None:
    """Condensed report for the Actions job summary."""
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    eligible = [s for s in shipments if s.action not in ("hold_process_true", "incomplete_row")]
    reassigned = [s for s in eligible if s.success]
    released = [s for s in eligible if s.released]
    held = [s for s in shipments if s.action == "hold_process_true"]
    incomplete = [s for s in shipments if s.action == "incomplete_row"]
    failed = [s for s in eligible if not s.success]
    lines = [
        "## Reassign blocked shipments",
        "",
        f"- Mode: **{'dry run' if dry_run else 'live'}**",
        f"- Rows read from {BI_TABLE}: **{rows_read}**",
        f"- Shipments to reassign: **{len(eligible)}**",
        f"- Exchange hold released first: **{len(released)}**",
        f"- Reassigned: **{len(reassigned)}**",
        f"- With an error: **{len(failed)}**",
        f"- Left alone (hold_process true): **{len(held)}**",
        f"- Incomplete rows: **{len(incomplete)}**",
        "",
    ]
    if eligible:
        lines += [
            "| shipmentNumber | orderNumber | tenant | site | location | released | action | error |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
        for s in eligible[:200]:
            lines.append(
                f"| {s.shipment_number} | {s.order_number or '-'} | {s.tenant_id} | {s.site_id} | "
                f"{s.location_code} | {'yes' if s.released else '-'} | {s.action} | {s.error[:120]} |"
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
                        help="Read and report everything, but call neither Kibo endpoint.")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Kibo host suffix.")
    parser.add_argument("--auth-url", default=DEFAULT_AUTH_URL, help="Kibo OAuth URL.")
    parser.add_argument("--limit", type=int, default=0, help="Limit the number of rows read (0 = no limit).")
    parser.add_argument("--shipment-numbers", default="",
                        help="Optional comma or whitespace separated shipmentNumbers to restrict the run.")
    parser.add_argument("--location-column", default="",
                        help=f"Monitoring table column holding the fulfillment location CODE. "
                             f"Default: the first of {list(LOCATION_CODE_CANDIDATES)} that exists.")
    parser.add_argument("--timeout", type=float, default=60.0, help="HTTP timeout in seconds.")
    parser.add_argument("--max-retries", type=int, default=3, help="Maximum attempts per Kibo request.")
    parser.add_argument("--sleep-between-requests", type=float, default=0.2,
                        help="Pause between Kibo write requests, in seconds.")
    parser.add_argument("--output", default="", help="CSV output path.")
    args = parser.parse_args()
    if args.max_retries < 1:
        parser.error("--max-retries must be at least 1.")
    if args.timeout <= 0:
        parser.error("--timeout must be greater than 0.")
    if args.sleep_between_requests < 0:
        parser.error("--sleep-between-requests cannot be negative.")
    args.host = re.sub(r"^https?://", "", args.host.strip(), flags=re.IGNORECASE)
    args.host = args.host.split("/", 1)[0]
    args.host = re.sub(r"^t\d+(?:-s\d+)?\.", "", args.host, flags=re.IGNORECASE).rstrip(".")
    return args


def split_values(raw: str) -> List[str]:
    return [item for item in re.split(r"[,\s;]+", raw.strip()) if item]


def main() -> int:
    args = parse_args()
    started = datetime.now(timezone.utc)
    output_path = Path(args.output or f"reassign_blocked_shipments_{started.strftime('%Y%m%d_%H%M%S')}.csv")

    log(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    log(f"Kibo host: {args.host}")

    kibo_credentials = load_json_env("KIBO_API_CREDS")
    client_id = clean(kibo_credentials.get("client_id"))
    client_secret = clean(kibo_credentials.get("client_secret"))
    if not client_id or not client_secret:
        fail("KIBO_API_CREDS must contain non-empty 'client_id' and 'client_secret'.")

    bi_client = bigquery_client("BI_BIGQUERY_CREDS", BI_PROJECT)

    only_shipment_numbers = split_values(args.shipment_numbers)
    if only_shipment_numbers:
        log(f"Restricting the run to {len(only_shipment_numbers)} shipmentNumber(s).")

    shipments, rows_read = fetch_blocked_shipments(
        bi_client, only_shipment_numbers, args.limit, args.location_column
    )
    eligible = [s for s in shipments if s.action == "pending"]
    held = [s for s in shipments if s.action == "hold_process_true"]
    incomplete = [s for s in shipments if s.action == "incomplete_row"]
    to_release = [s for s in eligible if s.needs_release]

    log(f"Rows read from {BI_TABLE}: {rows_read}")
    log(f"Shipments to reassign: {len(eligible)} (exchange hold to release first: {len(to_release)})")
    if held:
        log(f"Left alone because hold_process is true: {len(held)}")
    for s in incomplete:
        warn(f"{s.shipment_number or '(no shipment number)'}: {s.error}")

    if not eligible:
        log("Nothing to reassign.")
        write_csv(output_path, shipments)
        write_job_summary(shipments, rows_read, args.dry_run)
        log(f"Results written to {output_path}")
        return 0

    kibo = KiboClient({"client_id": client_id, "client_secret": client_secret},
                      args.auth_url, args.host, args.timeout)

    for index, shipment in enumerate(eligible, start=1):
        shipment.correlation_id = uuid.uuid4().hex
        log(
            f"[{index}/{len(eligible)}] shipmentNumber={shipment.shipment_number} "
            f"order={shipment.order_number or '-'} location={shipment.location_code} "
            f"(tenant={shipment.tenant_id}, site={shipment.site_id})"
            + (" [hold_exchange_process=true]" if shipment.needs_release else "")
        )

        if args.dry_run:
            if shipment.needs_release:
                log(f"    DRY RUN PUT {kibo.shipment_data_url(shipment.tenant_id, shipment.shipment_number)}")
                log(f"              {json.dumps(RELEASE_HOLD_BODY)}")
            url = kibo.reassign_url(shipment.tenant_id, shipment.site_id, shipment.shipment_number)
            log(f"    DRY RUN PUT {url}")
            log("              " + json.dumps({
                "blockAssignment": False,
                "fulfillmentLocationCode": shipment.location_code,
                "isUserAction": True,
            }))
            shipment.action = "would_reassign"
            shipment.success = True
            shipment.response_excerpt = "DRY RUN - no Kibo endpoint was called"
            continue

        # Step 3: clear the exchange hold first, the same way release_held_exchange_shipments.py does.
        if shipment.needs_release:
            ok, error, excerpt = kibo.release_exchange_hold(
                shipment, args.max_retries, shipment.correlation_id
            )
            shipment.released = ok
            if not ok:
                shipment.action = "release_failed"
                shipment.release_error = f"PUT /data?merge=true failed: {error}"
                shipment.error = shipment.release_error
                shipment.response_excerpt = excerpt
                warn(f"    {shipment.error}")
                if excerpt:
                    warn(f"    response: {excerpt[:400]}")
                if args.sleep_between_requests > 0:
                    time.sleep(args.sleep_between_requests)
                continue
            log("    OK holdExchangeProcess=false")
            if args.sleep_between_requests > 0:
                time.sleep(args.sleep_between_requests)

        # Step 4: reassign to the location the shipment is already on.
        ok, status_code, error, excerpt = kibo.reassign(
            shipment, args.max_retries, shipment.correlation_id
        )
        shipment.action = "reassigned" if ok else "reassign_failed"
        shipment.success = ok
        shipment.status_code = status_code
        shipment.error = "" if ok else f"PUT /reassigned failed: {error}"
        shipment.response_excerpt = excerpt

        if ok:
            log(f"    OK reassigned to {shipment.location_code}")
        else:
            warn(f"    {shipment.error}")
            if excerpt:
                warn(f"    response: {excerpt[:400]}")

        if args.sleep_between_requests > 0:
            time.sleep(args.sleep_between_requests)

    write_csv(output_path, shipments)
    write_job_summary(shipments, rows_read, args.dry_run)

    failures = [s for s in eligible if not s.success]
    log(f"Results written to {output_path}")
    log(
        f"Summary: {rows_read} rows read, {len(eligible)} shipments, "
        f"{sum(1 for s in eligible if s.released)} exchange holds released, "
        f"{sum(1 for s in eligible if s.success)} reassigned, "
        f"{len(failures)} with an error, {len(held)} left alone, "
        f"{len(incomplete)} incomplete rows."
    )
    return 0 if not failures else 2


if __name__ == "__main__":
    raise SystemExit(main())
