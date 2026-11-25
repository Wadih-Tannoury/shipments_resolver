"""
Simulate the GXO PICKED event for GLAFAYETTE shipments stuck on "Validate Items In Stock".

Pipeline
--------
1. Read the candidates from BigQuery (BI project):
       `tlg-business-intelligence-prd`.til.oms_active_shipments
   keeping shipment_order_number LIKE '%GL%' (those are GLAFAYETTE), with the
   tenant_id and site_id of each row.
2. Drop the ones already PICKED in `tlg-wlfs-prd.til.prd_shipmentStatus`.
3. Kibo gate - GET /api/commerce/shipments?filter=externalOrderId=={externalOrderId}
   and keep the shipment only when BOTH of these hold:
       * activeTaskName           == Validate Items In Stock
       * fulfillmentLocationCode  LIKE %GX%
4. Call the GETFULLORDER API for each surviving shipment.
5. Insert synthetic PICKED rows into prd_shipmentStatus.

Credentials are read from the local files below, or from the environment variables
BI_BIGQUERY_CREDS / TIL_BIGQUERY_CREDS / GETFULLORDER_API_CREDS / KIBO_API_CREDS when
those are set (so the same file runs unchanged in GitHub Actions).

Usage
-----
    python PICKED_event_simulator_glafayette.py --dry-run --limit 5
    python PICKED_event_simulator_glafayette.py --dry-run
    python PICKED_event_simulator_glafayette.py
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import string
import sys
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests
from google.cloud import bigquery
from google.oauth2 import service_account


# ---------------------------------------------------------------------------
# Configuration - adjust paths as needed
# ---------------------------------------------------------------------------

BI_PROJECT = "tlg-business-intelligence-prd"
BI_DATASET = "til"
BI_TABLE = "oms_active_shipments"

WLFS_PROJECT = "tlg-wlfs-prd"
WLFS_DATASET = "til"
SHIPMENT_STATUS_TABLE = "prd_shipmentStatus"

BI_CREDS_FILE = (
    r"C:\Users\WadihTannoury\OneDrive - The Level Group"
    r"\API keys and credentials\tlg-business-intelligence-prd"
    r"\service account API credentials.txt"
)

TIL_CREDS_FILE = (
    r"C:\Users\WadihTannoury\OneDrive - The Level Group"
    r"\API keys and credentials\tlg-wlfs-prd"
    r"\service account API credentials.txt"
)

API_CREDS_FILE = (
    r"C:\Users\WadihTannoury\OneDrive - The Level Group"
    r"\API keys and credentials\getFullOrder"
    r"\getFullOrder_API_credentials.json"
)

# Kibo OAuth credentials: {"client_id": "...", "client_secret": "..."}
KIBO_CREDS_FILE = (
    r"C:\Users\WadihTannoury\OneDrive - The Level Group"
    r"\API keys and credentials\Kibo"
    r"\kibo_API_credentials.json"
)

# Kibo
DEFAULT_HOST = "tp3.mozu.com"
DEFAULT_AUTH_URL = "https://home.mozu.com/api/platform/applications/authtickets/oauth"
SHIPMENTS_FILTER_TEMPLATE = "externalOrderId=={value}"
DEFAULT_PAGE_SIZE = 200

# The filters
DEFAULT_SHIPMENT_PATTERN = "%GL%"             # GLAFAYETTE shipment order numbers
DEFAULT_ACTIVE_TASK = "Validate Items In Stock"
DEFAULT_LOCATION_PATTERN = "%GX%"             # GXO fulfilment locations
DEFAULT_OMS_STATUS = ""                       # no shipmentStatus filter by default

BQ_SCOPES = ["https://www.googleapis.com/auth/bigquery"]
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Where the fulfilment location can sit on a Kibo shipment.
FULFILLMENT_LOCATION_KEYS = (
    "fulfillmentLocationCode", "assignedLocationCode", "fulfillmentLocation",
    "locationCode", "locationName",
)

# Candidate column names, resolved case-insensitively against the real schema.
SHIPMENT_NUMBER_CANDIDATES = ("shipmentOrderNumber", "shipment_order_number")
TENANT_CANDIDATES = ("tenant_id", "tenantId")
SITE_CANDIDATES = ("site_id", "siteId")
OMS_STATUS_CANDIDATES = ("shipmentStatus", "shipment_status", "status")

# shipmentOrderNumber -> externalOrderId: the 6th character becomes a hyphen,
# except for the exception tags, which are already in their external form.
EXTERNAL_ORDER_ID_POSITION = 6
EXTERNAL_ORDER_ID_CHARACTER = "-"
ORDER_ID_EXCEPTION_TAGS = ("EUF", "USF", "FF-")


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
    return str(value).strip()


def excerpt_of(response: Any, max_chars: int = 300) -> str:
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


def iter_values(node: Any, key: str) -> Iterable[Any]:
    """Every value stored under `key` anywhere in a nested structure."""
    if isinstance(node, dict):
        for name, value in node.items():
            if name == key:
                yield value
            yield from iter_values(value, key)
    elif isinstance(node, list):
        for item in node:
            yield from iter_values(item, key)


def like_to_regex(pattern: str) -> re.Pattern[str]:
    """Turn a SQL LIKE pattern ('%GX%') into a case-insensitive regex."""
    parts = [re.escape(piece).replace("_", ".") for piece in pattern.split("%")]
    return re.compile("^" + ".*".join(parts) + "$", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Credentials
# ---------------------------------------------------------------------------

def load_json_credentials(env_name: str, file_path: str, what: str) -> Dict[str, Any]:
    """Credentials from the environment variable when it is set, otherwise from disk."""
    raw = os.environ.get(env_name, "").strip()
    source = f"environment variable {env_name}"
    if not raw:
        path = Path(file_path)
        if not path.is_file():
            fail(f"{what}: neither {env_name} nor the file {file_path} is available.")
        raw = path.read_text(encoding="utf-8")
        source = file_path
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        fail(f"{what}: {source} does not contain valid JSON: {exc}")
    if not isinstance(data, dict):
        fail(f"{what}: {source} must contain a JSON object.")
    return data


def get_bq_client(creds: Dict[str, Any], expected_project: str) -> bigquery.Client:
    for key in ("type", "project_id", "private_key", "client_email"):
        if not creds.get(key):
            fail(f"The service account for {expected_project} is missing the '{key}' field.")
    project = clean(creds.get("project_id")) or expected_project
    if project != expected_project:
        warn(f"Service account project is '{project}', expected '{expected_project}'. Using '{project}'.")
    credentials = service_account.Credentials.from_service_account_info(creds, scopes=BQ_SCOPES)
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
    columns = {clean(row["column_name"]).lower(): clean(row["column_name"]) for row in job.result()}
    if not columns:
        fail(f"Table `{project}.{dataset}.{table}` was not found or has no columns "
             f"visible to this service account.")
    return columns


def resolve_column(
    columns: Dict[str, str], candidates: Sequence[str], where: str, required: bool = True
) -> str:
    for candidate in candidates:
        real = columns.get(candidate.lower())
        if real:
            return real
    if required:
        fail(f"None of the columns {list(candidates)} exist in {where}. "
             f"Found: {sorted(columns.values())}")
    return ""


# ---------------------------------------------------------------------------
# Synthetic event values
# ---------------------------------------------------------------------------

def random_guid() -> str:
    return str(uuid.uuid4())


def random_product_line_id() -> str:
    return str(uuid.uuid4()).upper()


def random_carton_id() -> str:
    return "777" + "".join(random.choices(string.digits, k=15))


def random_event_id() -> str:
    date_part = datetime.now(timezone.utc).strftime("%Y%m%d")
    seq = f"{random.randint(0, 999999999999):012d}"
    return f"{date_part}-1GXO-2TLG-CCCC-{seq}"


def current_bq_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.000")


def current_iso_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# shipmentOrderNumber -> order id
# ---------------------------------------------------------------------------

def external_order_id_of(shipment_order_number: str) -> str:
    """
    'DJ-GLA1300463359004' -> 'DJ-GL-1300463359004'.

    The 6th character is the generation letter and the externalOrderId carries a
    hyphen there instead. Numbers tagged EUF / USF / FF- are already external.
    """
    value = clean(shipment_order_number)
    if any(tag in value for tag in ORDER_ID_EXCEPTION_TAGS):
        return value
    if len(value) < EXTERNAL_ORDER_ID_POSITION:
        return value
    index = EXTERNAL_ORDER_ID_POSITION - 1
    return value[:index] + EXTERNAL_ORDER_ID_CHARACTER + value[index + 1:]


def build_order_url(shipment_order_number: str) -> str:
    """The GETFULLORDER URL, unchanged from the original script."""
    brand_code = shipment_order_number[:2]
    order_id = external_order_id_of(shipment_order_number)
    return (
        f"https://zelda.thelevelgroup.com/return/api/v1"
        f"/brands/{brand_code}/orders/{order_id}"
    )


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Candidate:
    shipment_order_number: str
    tenant_id: str
    site_id: str
    oms_shipment_status: str = ""


# ---------------------------------------------------------------------------
# BigQuery queries
# ---------------------------------------------------------------------------

def fetch_glafayette_candidates(
    client: bigquery.Client, pattern: str, oms_status: str, limit: int,
    only_numbers: Sequence[str],
) -> List[Candidate]:
    """
    The active GLAFAYETTE shipments.

    A GLAFAYETTE shipment order number carries 'GL' ('DJ-GLA1300463359004'), so the
    LIKE pattern is enough to single the channel out and no channel column is needed.
    """
    where = f"`{BI_PROJECT}.{BI_DATASET}.{BI_TABLE}`"
    columns = table_columns(client, BI_PROJECT, BI_DATASET, BI_TABLE)
    number_col = resolve_column(columns, SHIPMENT_NUMBER_CANDIDATES, where)
    tenant_col = resolve_column(columns, TENANT_CANDIDATES, where)
    site_col = resolve_column(columns, SITE_CANDIDATES, where)
    status_col = resolve_column(columns, OMS_STATUS_CANDIDATES, where, required=bool(oms_status))

    filters = [
        f"`{number_col}` IS NOT NULL",
        f"TRIM(CAST(`{number_col}` AS STRING)) != ''",
        f"UPPER(CAST(`{number_col}` AS STRING)) LIKE UPPER(@pattern)",
    ]
    params: List[Any] = [bigquery.ScalarQueryParameter("pattern", "STRING", pattern)]
    if oms_status:
        filters.append(f"UPPER(TRIM(CAST(`{status_col}` AS STRING))) = UPPER(@oms_status)")
        params.append(bigquery.ScalarQueryParameter("oms_status", "STRING", oms_status))
    if only_numbers:
        filters.append(f"CAST(`{number_col}` AS STRING) IN UNNEST(@only_numbers)")
        params.append(bigquery.ArrayQueryParameter("only_numbers", "STRING", list(only_numbers)))

    status_select = f"CAST(`{status_col}` AS STRING)" if status_col else "CAST(NULL AS STRING)"
    sql = f"""
        SELECT DISTINCT
            CAST(`{number_col}` AS STRING) AS shipment_order_number,
            CAST(`{tenant_col}` AS STRING) AS tenant_id,
            CAST(`{site_col}`   AS STRING) AS site_id,
            {status_select} AS oms_shipment_status
        FROM {where}
        WHERE {' AND '.join(filters)}
        ORDER BY shipment_order_number
    """
    if limit > 0:
        sql += f"\n        LIMIT {int(limit)}"

    job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
    candidates: List[Candidate] = []
    seen: set[str] = set()
    for row in job.result():
        number = clean(row["shipment_order_number"])
        tenant = clean(row["tenant_id"])
        if not number or number in seen:
            continue
        if not tenant:
            warn(f"  {number}: no tenant_id in {BI_TABLE}, skipping.")
            continue
        seen.add(number)
        candidates.append(Candidate(
            shipment_order_number=number,
            tenant_id=tenant,
            site_id=clean(row["site_id"]),
            oms_shipment_status=clean(row["oms_shipment_status"]),
        ))
    return candidates


def fetch_already_picked(
    client: bigquery.Client, numbers: Sequence[str], batch_size: int
) -> set[str]:
    """The shipment order numbers that already carry a PICKED event."""
    sql = f"""
        SELECT DISTINCT shipmentOrderNumber
        FROM `{WLFS_PROJECT}.{WLFS_DATASET}.{SHIPMENT_STATUS_TABLE}`
        WHERE eventType = 'PICKED'
          AND shipmentOrderNumber IN UNNEST(@sons)
    """
    found: set[str] = set()
    for batch in chunked(numbers, batch_size):
        job = client.query(
            sql,
            job_config=bigquery.QueryJobConfig(query_parameters=[
                bigquery.ArrayQueryParameter("sons", "STRING", batch),
            ]),
        )
        for row in job.result():
            number = clean(row["shipmentOrderNumber"])
            if number:
                found.add(number)
    return found


# ---------------------------------------------------------------------------
# Kibo
# ---------------------------------------------------------------------------

class KiboClient:
    """Just enough of the Kibo Shipment API to read an order's shipments."""

    def __init__(self, credentials: Dict[str, str], auth_url: str, host: str, timeout: float) -> None:
        self.credentials = credentials
        self.auth_url = auth_url
        self.host = host
        self.timeout = timeout
        self.session = requests.Session()
        self._tokens: Dict[str, Tuple[str, float]] = {}

    def token_for_tenant(self, tenant_id: str, force_refresh: bool = False) -> str:
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
            fail(f"Kibo authentication failed for tenant {tenant_id} "
                 f"(HTTP {response.status_code}): {excerpt_of(response)}")
        try:
            data = response.json()
        except ValueError:
            fail("The Kibo authentication response was not JSON.")
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

    def get_shipments_by_external_order_id(
        self, tenant_id: str, external_order_id: str, max_retries: int, page_size: int,
    ) -> Tuple[List[Dict[str, Any]], str]:
        """
        Every shipment of one order.

            GET /api/commerce/shipments?filter=externalOrderId=={externalOrderId}

        The Shipment API has its own filter syntax (`field==value`, clauses joined by
        ';'). No x-vol-site header is sent: the site scopes the query, so a site that
        does not hold the order's shipments answers 200 with an empty page, which cannot
        be told apart from an order with no shipments.
        """
        url = f"https://t{tenant_id}.{self.host}/api/commerce/shipments"
        shipments: List[Dict[str, Any]] = []
        page = 0
        while True:
            response, error = self._request(
                "GET", url, tenant_id, max_retries,
                params={
                    "filter": SHIPMENTS_FILTER_TEMPLATE.format(value=external_order_id),
                    "pageSize": int(page_size),
                    "page": page,
                },
            )
            if response is None:
                return shipments, error
            if not (200 <= response.status_code < 300):
                return shipments, f"HTTP {response.status_code}: {excerpt_of(response)}"
            try:
                payload = response.json()
            except ValueError:
                return shipments, "The shipments response was not JSON."

            batch = shipments_in_response(payload)
            shipments.extend(batch)

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

    def _request(
        self, method: str, url: str, tenant_id: str, max_retries: int,
        params: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Optional[Any], str]:
        last_error = ""
        for attempt in range(1, max_retries + 1):
            try:
                response = self.session.request(
                    method, url,
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {self.token_for_tenant(tenant_id)}",
                        "x-vol-tenant": tenant_id,
                    },
                    params=params,
                    timeout=self.timeout,
                )
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
        return None, last_error or "Unknown error"


def shipments_in_response(payload: Any) -> List[Dict[str, Any]]:
    """
    The shipments of a getShipments response.

    The Shipment API answers with HAL, so the list sits under _embedded.shipments and
    NOT at the top level. The flatter shapes are kept as fallbacks.
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


# ---------------------------------------------------------------------------
# The Kibo conditions
# ---------------------------------------------------------------------------

def shipment_order_number_of(shipment: Dict[str, Any]) -> str:
    """The shipmentOrderNumber a Kibo shipment carries, top level or under data."""
    for value in iter_values(shipment, "shipmentOrderNumber"):
        text = clean(value)
        if text:
            return text
    return ""


def shipment_active_task(shipment: Dict[str, Any]) -> str:
    """workflowState.activeTaskName, or the active entry of the task list."""
    for value in iter_values(shipment, "activeTaskName"):
        text = clean(value)
        if text:
            return text
    for task_list in iter_values(shipment, "taskList"):
        if not isinstance(task_list, list):
            continue
        for task in task_list:
            if isinstance(task, dict) and task.get("active") and not task.get("completed"):
                text = clean(task.get("name"))
                if text:
                    return text
    return ""


def shipment_fulfillment_location(shipment: Dict[str, Any]) -> str:
    """The fulfilment location code, the top level first."""
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


def shipment_matches(
    shipment: Dict[str, Any], active_task: str, location_regex: re.Pattern[str],
) -> Tuple[bool, str, Dict[str, str]]:
    """Whether one Kibo shipment is on the right task in the right warehouse, and why not."""
    task = shipment_active_task(shipment)
    location = shipment_fulfillment_location(shipment)
    seen = {"active_task": task, "fulfillment_location_code": location}

    if active_task and task.upper() != active_task.upper():
        return False, f"active task is '{task or '-'}', not '{active_task}'", seen
    if not location_regex.match(location):
        return False, f"fulfillmentLocationCode '{location or '-'}' does not match the pattern", seen
    return True, "", seen


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def split_numbers(raw: str) -> List[str]:
    return [item for item in re.split(r"[,\s;]+", raw.strip()) if item]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="Run every check and build the rows, but do not insert into BigQuery.")
    parser.add_argument("--shipment-pattern", default=DEFAULT_SHIPMENT_PATTERN,
                        help="SQL LIKE pattern the shipment_order_number must match in "
                             f"{BI_TABLE} (default '{DEFAULT_SHIPMENT_PATTERN}', GLAFAYETTE).")
    parser.add_argument("--active-task", default=DEFAULT_ACTIVE_TASK,
                        help="Required activeTaskName in Kibo. Pass an empty string to disable.")
    parser.add_argument("--location-pattern", default=DEFAULT_LOCATION_PATTERN,
                        help="SQL LIKE pattern the fulfillmentLocationCode must match.")
    parser.add_argument("--oms-status", default=DEFAULT_OMS_STATUS,
                        help=f"Keep only rows with this shipmentStatus in {BI_TABLE} "
                             "(empty = no filter).")
    parser.add_argument("--shipment-order-numbers", default="",
                        help="Optional comma or whitespace separated list to restrict the run.")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Kibo host suffix.")
    parser.add_argument("--auth-url", default=DEFAULT_AUTH_URL, help="Kibo OAuth URL.")
    parser.add_argument("--page-size", type=int, default=DEFAULT_PAGE_SIZE,
                        help="pageSize sent with GET /api/commerce/shipments.")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="Shipment order numbers per BigQuery query.")
    parser.add_argument("--limit", type=int, default=0,
                        help="Maximum number of candidates to read (0 = no limit).")
    parser.add_argument("--timeout", type=float, default=60.0, help="HTTP timeout in seconds.")
    parser.add_argument("--max-retries", type=int, default=3,
                        help="Maximum attempts per Kibo request.")
    args = parser.parse_args()
    if args.batch_size < 1:
        parser.error("--batch-size must be at least 1.")
    if args.max_retries < 1:
        parser.error("--max-retries must be at least 1.")
    args.oms_status = args.oms_status.strip()
    args.host = re.sub(r"^https?://", "", args.host.strip()).split("/", 1)[0].rstrip(".")
    return args


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main() -> int:
    args = parse_args()
    now_ts = current_bq_timestamp()
    now_iso = current_iso_timestamp()
    location_regex = like_to_regex(args.location_pattern)

    log(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    log(f"Candidates: {BI_TABLE} where shipment_order_number LIKE '{args.shipment_pattern}'"
        + (f", shipmentStatus = {args.oms_status}" if args.oms_status else ""))
    log(f"Kibo gate: active task = '{args.active_task or '(any)'}', "
        f"fulfillmentLocationCode LIKE '{args.location_pattern}'")

    # ------------------------------------------------------------------
    # STEP 1 - GLAFAYETTE candidates from oms_active_shipments
    # ------------------------------------------------------------------
    log("\n> Step 1: Reading GLAFAYETTE shipments from oms_active_shipments ...")
    bi_creds = load_json_credentials("BI_BIGQUERY_CREDS", BI_CREDS_FILE, "BI BigQuery credentials")
    bi_client = get_bq_client(bi_creds, BI_PROJECT)

    candidates = fetch_glafayette_candidates(
        bi_client, args.shipment_pattern, args.oms_status, args.limit,
        split_numbers(args.shipment_order_numbers),
    )
    log(f"  Found {len(candidates)} active GLAFAYETTE shipments.")
    if not candidates:
        log("  Nothing to process. Exiting.")
        return 0

    # ------------------------------------------------------------------
    # STEP 2 - Exclude shipments already PICKED in prd_shipmentStatus
    # ------------------------------------------------------------------
    log("\n> Step 2: Checking prd_shipmentStatus for already-PICKED ...")
    til_creds = load_json_credentials("TIL_BIGQUERY_CREDS", TIL_CREDS_FILE, "WLFS BigQuery credentials")
    til_client = get_bq_client(til_creds, WLFS_PROJECT)

    already_picked = fetch_already_picked(
        til_client, [c.shipment_order_number for c in candidates], args.batch_size
    )
    remaining = [c for c in candidates if c.shipment_order_number not in already_picked]
    log(f"  Excluded {len(already_picked)} already-PICKED shipments. {len(remaining)} remaining.")
    if not remaining:
        log("  Nothing left to process. Exiting.")
        return 0

    # ------------------------------------------------------------------
    # STEP 3 - Kibo gate: active task and fulfilment location
    # ------------------------------------------------------------------
    log(f"\n> Step 3: Checking Kibo getShipments for {len(remaining)} shipments ...")
    kibo_creds = load_json_credentials("KIBO_API_CREDS", KIBO_CREDS_FILE, "Kibo credentials")
    if not clean(kibo_creds.get("client_id")) or not clean(kibo_creds.get("client_secret")):
        fail("The Kibo credentials must contain non-empty 'client_id' and 'client_secret'.")
    kibo = KiboClient(kibo_creds, args.auth_url, args.host, args.timeout)

    eligible: List[Candidate] = []
    for candidate in remaining:
        son = candidate.shipment_order_number
        external_order_id = external_order_id_of(son)
        shipments, error = kibo.get_shipments_by_external_order_id(
            candidate.tenant_id, external_order_id, args.max_retries, args.page_size
        )
        if error:
            warn(f"  {son}: Kibo lookup failed ({error}). Skipping.")
            continue
        if not shipments:
            log(f"  {son}: no shipment for externalOrderId {external_order_id}. Skipping.")
            continue

        # The order can hold several shipments; keep the one carrying this number.
        matched = [s for s in shipments if shipment_order_number_of(s) == son]
        if not matched:
            log(f"  {son}: none of the {len(shipments)} shipments of {external_order_id} "
                f"carries this shipmentOrderNumber. Skipping.")
            continue

        for shipment in matched:
            ok, reason, seen = shipment_matches(shipment, args.active_task, location_regex)
            if ok:
                log(f"  {son}: OK (task '{seen['active_task']}', "
                    f"location {seen['fulfillment_location_code']})")
                eligible.append(candidate)
                break
            log(f"  {son}: skipped - {reason}")

    log(f"  {len(eligible)} of {len(remaining)} shipments satisfy the Kibo conditions.")
    if not eligible:
        log("  Nothing left to process. Exiting.")
        return 0

    # ------------------------------------------------------------------
    # STEP 4 - Call GETFULLORDER for each eligible shipment
    # ------------------------------------------------------------------
    log(f"\n> Step 4: Calling GETFULLORDER for {len(eligible)} shipments ...")
    api_creds = load_json_credentials(
        "GETFULLORDER_API_CREDS", API_CREDS_FILE, "GETFULLORDER credentials"
    )
    log(f"  Loaded API credentials (client_id: {clean(api_creds['client_id'])[:6]}...)")

    session = requests.Session()
    session.headers.update({
        "client_id": api_creds["client_id"],
        "client_secret": api_creds["client_secret"],
    })

    rows_to_insert: List[Dict[str, Any]] = []

    for candidate in eligible:
        son = candidate.shipment_order_number
        url = build_order_url(son)
        log(f"  GET {url}")

        try:
            resp = session.get(url, timeout=args.timeout)
        except requests.RequestException as exc:
            warn(f"    Network error: {exc}")
            continue

        if resp.status_code == 404:
            log("    -> Not found (404). Skipping.")
            continue
        if not resp.ok:
            warn(f"    HTTP {resp.status_code}. Skipping.")
            continue

        try:
            order_data = resp.json()
        except ValueError:
            warn("    The GETFULLORDER response was not JSON. Skipping.")
            continue

        # Match shipment in API response
        matched_shipment = None
        for s in order_data.get("shipments", []):
            if clean(s.get("shipmentOrderNumber")) == son:
                matched_shipment = s
                break

        if matched_shipment is None:
            log("    -> No matching shipment found in the API response. Skipping.")
            continue

        items = matched_shipment.get("items", [])
        if not items:
            log("    -> No items in the matched shipment. Skipping.")
            continue

        order_info = order_data.get("order", {})
        brand_code = clean(order_info.get("brandCode")) or son[:2]
        oms_shipment_number = clean(matched_shipment.get("shipmentId"))
        brand = clean(order_info.get("brand")) or brand_code

        carton_id = random_carton_id()
        products = []
        for item in items:
            products.append({
                "data": {
                    "externalSerialNumber": "",
                    "serialNumber": "",
                    "productLineId": random_product_line_id(),
                    "cartonId": carton_id,
                },
                "productId": clean(item.get("sku")),
                "quantity": item.get("quantity", 1),
            })

        request_payload = json.dumps({
            "products": products,
            "eventClass": "SHIPMENT",
            "eventType": "PICKED",
            "eventId": random_event_id(),
            "eventTimestamp": now_iso,
            "data": {
                "logisticOrderNumber": f"{brand}{oms_shipment_number}",
                "shipmentOrderNumber": son,
            },
        })

        request_user_properties = json.dumps({
            "originWMS": "GXO",
            "eventType": "PICKED",
            "contentType": "application/json; charset=UTF-8",
            "MULE_ENCODING": "UTF-8",
        })

        rows_to_insert.append({
            "transactionId": random_guid(),
            "requestPayload": request_payload,
            "requestUserProperties": request_user_properties,
            "errorMessage": "null",
            "queueName": "wlfs-shipment-status-picking-queue-v1",
            "lastModifiedDate": now_ts,
            "toBeReprocessed": True,
            "numberOfRetries": 0,
            "eventClass": "SHIPMENT",
            "warehouse": "GXO",
            "eventType": "PICKED",
            "fileName": "",
            "eventTimeStamp": now_ts,
            "brand": brand,
            "brandCode": brand_code,
            "shipmentOrderNumber": son,
        })
        log(f"    Prepared row for {son}")

    # ------------------------------------------------------------------
    # STEP 5 - Insert rows into prd_shipmentStatus
    # ------------------------------------------------------------------
    if not rows_to_insert:
        log("\n> No rows to insert. Done.")
        return 0

    if args.dry_run:
        log(f"\n> DRY RUN: {len(rows_to_insert)} rows would be inserted into prd_shipmentStatus:")
        for row in rows_to_insert:
            log(f"  {row['shipmentOrderNumber']}  {row['requestPayload']}")
        return 0

    log(f"\n> Step 5: Inserting {len(rows_to_insert)} rows into prd_shipmentStatus ...")
    table_ref = til_client.dataset(WLFS_DATASET).table(SHIPMENT_STATUS_TABLE)
    errors = til_client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        fail(f"BigQuery insert errors: {errors}")
    log(f"  Successfully inserted {len(rows_to_insert)} rows.")
    log("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
