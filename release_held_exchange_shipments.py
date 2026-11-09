#!/usr/bin/env python3
"""
Release Kibo shipments that are stuck with "Shipment held with exchange".

Pipeline
--------
1. Read from BigQuery (WLFS project):

       SELECT * FROM `tlg-wlfs-prd`.til.prd_tilEvents til1
       WHERE til1.eventType = 'shipment.itemscanceled'
         AND til1.warningMessage LIKE '%Shipment held with exchange%'
         AND NOT EXISTS (
             SELECT 1 FROM `tlg-wlfs-prd`.til.prd_tilEvents til2
             WHERE til2.eventType = 'shipment.itemscanceled'
               AND (til2.warningMessage IS NULL OR til2.warningMessage = '')
               AND til2.orderNumber = til1.orderNumber
         )
       ORDER BY eventTimestamp DESC

2. From each row, extract out of the `requestPayload` column:
   - externalOrderId  <- the `orderNumber` field
   - shipmentNumbers  <- the `entityId` field (one row can carry several)
   - tenantId         <- the `x-vol-tenant` field
   - siteId           <- the `x-vol-site` field

3. In Kibo, per shipment number:

       PUT https://t{tenantId}.{host}/api/commerce/shipments/{shipmentNumber}/data?merge=true
       x-vol-tenant: {tenantId}
       x-vol-site:   {siteId}

       {"holdExchangeProcess": false}

4. Back in BigQuery, set `toBeReprocessed = TRUE` on the rows returned by step 1,
   identified by their `transactionId`.

Credentials come from environment variables (GitHub repository secrets):
    TIL_BIGQUERY_CREDS  service account JSON for tlg-wlfs-prd
    KIBO_API_CREDS      {"client_id": "...", "client_secret": "..."}

The service account needs BigQuery Job User on the project and Data Editor on the
`til` dataset, because step 4 is a DML statement.
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

WLFS_PROJECT = "tlg-wlfs-prd"
WLFS_DATASET = "til"
TIL_EVENTS_TABLE = "prd_tilEvents"

EVENT_TYPE = "shipment.itemscanceled"
WARNING_MESSAGE_LIKE = "%Shipment held with exchange%"

DEFAULT_HOST = "tp3.mozu.com"
DEFAULT_AUTH_URL = "https://home.mozu.com/api/platform/applications/authtickets/oauth"

SHIPMENT_DATA_BODY: Dict[str, Any] = {"holdExchangeProcess": False}

# Retry limit: a transactionId that already appears in more than this many
# shipment.itemscanceled rows has been retried enough and is left alone.
DEFAULT_MAX_ROWS_PER_TRANSACTION = 2

BQ_SCOPES = ["https://www.googleapis.com/auth/bigquery"]
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Candidate column names, resolved case-insensitively against the real schema.
ORDER_NUMBER_CANDIDATES = ("orderNumber", "order_number")
PAYLOAD_CANDIDATES = ("requestPayload", "request_payload")
EVENT_TYPE_CANDIDATES = ("eventType", "event_type")
WARNING_CANDIDATES = ("warningMessage", "warning_message")
REPROCESS_CANDIDATES = ("toBeReprocessed", "to_be_reprocessed", "tobereprocessed")
TIMESTAMP_CANDIDATES = (
    "eventTimestamp", "event_timestamp", "insertTimestamp", "insert_timestamp",
    "ingestionTimestamp", "ingestion_timestamp", "createdAt", "created_at",
    "creationDate", "creation_date", "loadTimestamp", "load_timestamp",
)
# Identifies the rows to flag in step 4.
TRANSACTION_ID_CANDIDATES = ("transactionId", "transaction_id")

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


def chunked(items: Sequence[Any], size: int) -> Iterable[List[Any]]:
    for start in range(0, len(items), size):
        yield list(items[start:start + size])


def dedupe(items: Iterable[str]) -> List[str]:
    seen: set[str] = set()
    result: List[str] = []
    for item in items:
        if item and item not in seen:
            seen.add(item)
            result.append(item)
    return result


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

def table_schema(client: bigquery.Client, project: str, dataset: str, table: str) -> Dict[str, Tuple[str, str]]:
    """Return {lowercase column name: (real column name, data type)}."""
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
    schema = {
        str(row["column_name"]).lower(): (str(row["column_name"]), str(row["data_type"]).upper())
        for row in job.result()
    }
    if not schema:
        fail(f"Table `{project}.{dataset}.{table}` was not found or has no columns visible to this service account.")
    return schema


def resolve_column(
    schema: Dict[str, Tuple[str, str]], candidates: Sequence[str], where: str, required: bool = True
) -> str:
    for candidate in candidates:
        entry = schema.get(candidate.lower())
        if entry:
            return entry[0]
    if required:
        fail(f"None of the columns {list(candidates)} exist in {where}. Found: {sorted(name for name, _ in schema.values())}")
    return ""


def column_data_type(schema: Dict[str, Tuple[str, str]], column: str) -> str:
    entry = schema.get(column.lower())
    return entry[1] if entry else ""


# ---------------------------------------------------------------------------
# Payload parsing
# ---------------------------------------------------------------------------

def parse_payload(raw: Any) -> Optional[Any]:
    if isinstance(raw, (dict, list)):
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
    return parsed if isinstance(parsed, (dict, list)) else None


def iter_values(node: Any, key: str) -> Iterable[Any]:
    """
    Yield every value stored under `key` anywhere in a nested structure.

    The comparison is case-insensitive, so `x-vol-tenant` is found whether the payload
    spells it `x-vol-tenant`, `X-Vol-Tenant` or `x-vol-Tenant`, at any nesting depth
    (top level, inside `headers`, inside a list of header objects...).
    """
    wanted = key.lower()
    if isinstance(node, dict):
        for name, value in node.items():
            if str(name).lower() == wanted:
                yield value
            yield from iter_values(value, key)
    elif isinstance(node, list):
        for item in node:
            yield from iter_values(item, key)


def flatten_scalars(value: Any) -> Iterable[str]:
    """
    Turn a field into the list of scalar strings it holds.

    Handles a plain string, a list, and the comma/semicolon/whitespace separated
    strings that sometimes carry several ids in one field.
    """
    if isinstance(value, list):
        for item in value:
            yield from flatten_scalars(item)
        return
    if isinstance(value, dict):
        for item in value.values():
            yield from flatten_scalars(item)
        return
    text = clean(value)
    if not text:
        return
    for piece in re.split(r"[,;\s]+", text):
        piece = piece.strip().strip('"').strip("'")
        if piece:
            yield piece


def first_scalar(payload: Any, key: str) -> str:
    for value in iter_values(payload, key):
        for text in flatten_scalars(value):
            return text
    return ""


def external_order_id_of(payload: Any) -> str:
    """
    The externalOrderId carried by the `orderNumber` field.

    Usually `orderNumber` is the value itself. If it happens to be an object, the
    nested externalOrderId is taken instead; as a last resort any externalOrderId
    found in the payload is used.
    """
    for value in iter_values(payload, "orderNumber"):
        if isinstance(value, dict):
            nested = first_scalar(value, "externalOrderId")
            if nested:
                return nested
        for text in flatten_scalars(value):
            return text
    return first_scalar(payload, "externalOrderId")


def shipment_numbers_of(payload: Any) -> List[str]:
    """Every shipmentNumber carried by the `entityId` field, in payload order."""
    numbers: List[str] = []
    for value in iter_values(payload, "entityId"):
        numbers.extend(flatten_scalars(value))
    return dedupe(numbers)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class Row:
    """One (BigQuery row, shipmentNumber) pair."""
    transaction_id: str = ""
    order_number: str = ""
    external_order_id: str = ""
    tenant_id: str = ""
    site_id: str = ""
    shipment_number: str = ""
    event_timestamp: str = ""
    transaction_row_count: int = 0
    action: str = "skipped"
    success: bool = False
    error: str = ""
    response_excerpt: str = ""
    correlation_id: str = ""
    marked_to_be_reprocessed: bool = False


# ---------------------------------------------------------------------------
# BigQuery: the held shipments
# ---------------------------------------------------------------------------

def fetch_held_events(
    client: bigquery.Client,
    only_order_numbers: Sequence[str],
    limit: int,
    include_already_marked: bool,
    max_rows_per_transaction: int,
) -> Tuple[List[Row], Dict[str, str], int]:
    """
    Run the held-with-exchange query.

    Returns (rows, the column names the update step needs, number of BigQuery rows read).
    One BigQuery row produces one Row per shipmentNumber found in its requestPayload,
    and one Row carrying the parsing error when nothing usable is in there.

    Each row also carries how many `shipment.itemscanceled` rows share its transactionId,
    which is the number of attempts made so far. Past max_rows_per_transaction the row is
    reported as `retry_limit_reached` and neither released nor flagged.
    """
    where = f"`{WLFS_PROJECT}.{WLFS_DATASET}.{TIL_EVENTS_TABLE}`"
    schema = table_schema(client, WLFS_PROJECT, WLFS_DATASET, TIL_EVENTS_TABLE)
    order_col = resolve_column(schema, ORDER_NUMBER_CANDIDATES, where)
    payload_col = resolve_column(schema, PAYLOAD_CANDIDATES, where)
    event_col = resolve_column(schema, EVENT_TYPE_CANDIDATES, where)
    warning_col = resolve_column(schema, WARNING_CANDIDATES, where)
    ts_col = resolve_column(schema, TIMESTAMP_CANDIDATES, where, required=False)
    transaction_col = resolve_column(schema, TRANSACTION_ID_CANDIDATES, where)
    reprocess_col = resolve_column(schema, REPROCESS_CANDIDATES, where, required=False)

    filters = [
        f"til1.`{event_col}` = @event_type",
        f"til1.`{warning_col}` LIKE @warning_like",
        f"""NOT EXISTS (
            SELECT 1 FROM {where} til2
            WHERE til2.`{event_col}` = @event_type
              AND (til2.`{warning_col}` IS NULL OR til2.`{warning_col}` = '')
              AND til2.`{order_col}` = til1.`{order_col}`
        )""",
    ]
    params: List[Any] = [
        bigquery.ScalarQueryParameter("event_type", "STRING", EVENT_TYPE),
        bigquery.ScalarQueryParameter("warning_like", "STRING", WARNING_MESSAGE_LIKE),
    ]
    if only_order_numbers:
        filters.append(f"CAST(til1.`{order_col}` AS STRING) IN UNNEST(@only_order_numbers)")
        params.append(bigquery.ArrayQueryParameter("only_order_numbers", "STRING", list(only_order_numbers)))
    if reprocess_col and not include_already_marked:
        filters.append(not_marked_predicate(f"til1.`{reprocess_col}`", column_data_type(schema, reprocess_col)))

    transaction_select = f"CAST(til1.`{transaction_col}` AS STRING)"
    ts_select = f"CAST(til1.`{ts_col}` AS STRING)" if ts_col else "CAST(NULL AS STRING)"
    order_by = f"ORDER BY til1.`{ts_col}` DESC" if ts_col else ""
    # Counted over the whole event type, not over the filtered set: an earlier attempt is
    # usually already flagged toBeReprocessed and would otherwise be missed. Other event
    # types sharing the transactionId are excluded so they cannot inflate the count.
    sql = f"""
        SELECT
            {transaction_select} AS transaction_id,
            CAST(til1.`{order_col}` AS STRING) AS order_number,
            CAST(til1.`{payload_col}` AS STRING) AS payload,
            {ts_select} AS event_timestamp,
            (
                SELECT COUNT(*) FROM {where} til3
                WHERE til3.`{event_col}` = @event_type
                  AND til3.`{transaction_col}` = til1.`{transaction_col}`
            ) AS transaction_row_count
        FROM {where} til1
        WHERE {' AND '.join(filters)}
        {order_by}
    """
    if limit > 0:
        sql += f"\n        LIMIT {int(limit)}"

    job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))

    rows: List[Row] = []
    read = 0
    for record in job.result():
        read += 1
        base = Row(
            transaction_id=clean(record["transaction_id"]),
            order_number=clean(record["order_number"]),
            event_timestamp=clean(record["event_timestamp"]),
            transaction_row_count=int(record["transaction_row_count"] or 0),
        )
        if 0 < max_rows_per_transaction < base.transaction_row_count:
            base.action = "retry_limit_reached"
            base.error = (
                f"transactionId appears in {base.transaction_row_count} "
                f"{EVENT_TYPE} rows, over the limit of {max_rows_per_transaction}."
            )
            rows.append(base)
            continue

        payload = parse_payload(record["payload"])
        if payload is None:
            base.error = f"{payload_col} is empty or not valid JSON."
            rows.append(base)
            continue

        base.external_order_id = external_order_id_of(payload)
        base.tenant_id = first_scalar(payload, "x-vol-tenant")
        base.site_id = first_scalar(payload, "x-vol-site")
        shipment_numbers = shipment_numbers_of(payload)

        missing = []
        if not shipment_numbers:
            missing.append("entityId (shipmentNumber)")
        if not base.tenant_id:
            missing.append("x-vol-tenant")
        if not base.site_id:
            missing.append("x-vol-site")
        if missing:
            base.error = "Missing in the payload: " + ", ".join(missing)
            rows.append(base)
            continue

        for number in shipment_numbers:
            row = Row(**asdict(base))
            row.shipment_number = number
            rows.append(row)

    columns = {
        "transaction": transaction_col,
        "event": event_col,
        "warning": warning_col,
        "reprocess": reprocess_col,
        "reprocess_type": column_data_type(schema, reprocess_col) if reprocess_col else "",
    }
    return rows, columns, read


def not_marked_predicate(expression: str, data_type: str) -> str:
    """SQL that keeps the rows whose toBeReprocessed flag is not already true."""
    if data_type.startswith("BOOL"):
        return f"NOT IFNULL({expression}, FALSE)"
    if data_type.startswith(("INT", "NUMERIC", "BIGNUMERIC", "FLOAT")):
        return f"IFNULL({expression}, 0) = 0"
    literals = ", ".join(f"'{value}'" for value in sorted(TRUE_STRINGS))
    return f"UPPER(IFNULL(CAST({expression} AS STRING), '')) NOT IN ({literals})"


def reprocess_true_literal(data_type: str) -> str:
    if data_type.startswith("BOOL"):
        return "TRUE"
    if data_type.startswith(("INT", "NUMERIC", "BIGNUMERIC", "FLOAT")):
        return "1"
    return "'true'"


# ---------------------------------------------------------------------------
# BigQuery: the update
# ---------------------------------------------------------------------------

def mark_to_be_reprocessed(
    client: bigquery.Client,
    columns: Dict[str, str],
    rows: Sequence[Row],
    batch_size: int,
) -> Tuple[int, str]:
    """
    Set toBeReprocessed = TRUE on the given BigQuery rows.

    The rows are identified by the transactionId values the SELECT returned, so exactly
    those rows are touched and nothing else that shares an order number.

    Returns (rows updated, error).
    """
    table = f"`{WLFS_PROJECT}.{WLFS_DATASET}.{TIL_EVENTS_TABLE}`"
    reprocess_col = columns["reprocess"]
    if not reprocess_col:
        return 0, (
            f"No column named {list(REPROCESS_CANDIDATES)} exists in {TIL_EVENTS_TABLE}, "
            "so the rows cannot be flagged."
        )
    value = reprocess_true_literal(columns["reprocess_type"])
    transaction_col = columns["transaction"]

    transaction_ids = dedupe(row.transaction_id for row in rows)
    if not transaction_ids:
        return 0, f"No {transaction_col} was returned by the query, so no row can be identified."

    # The transactionId is shared by every row of the same transaction, including other
    # event types, retries and request/response pairs. The two predicates below keep the
    # update inside the set of rows the SELECT returned, so those siblings stay untouched.
    sql = f"""
        UPDATE {table}
        SET `{reprocess_col}` = {value}
        WHERE CAST(`{transaction_col}` AS STRING) IN UNNEST(@transaction_ids)
          AND `{columns['event']}` = @event_type
          AND `{columns['warning']}` LIKE @warning_like
    """

    updated = 0
    for batch in chunked(transaction_ids, batch_size):
        params: List[Any] = [
            bigquery.ArrayQueryParameter("transaction_ids", "STRING", batch),
            bigquery.ScalarQueryParameter("event_type", "STRING", EVENT_TYPE),
            bigquery.ScalarQueryParameter("warning_like", "STRING", WARNING_MESSAGE_LIKE),
        ]
        try:
            job = client.query(sql, job_config=bigquery.QueryJobConfig(query_parameters=params))
            job.result()
        except Exception as exc:  # noqa: BLE001 - the message goes to the report
            return updated, re.sub(r"\s+", " ", str(exc))[:500]
        updated += int(job.num_dml_affected_rows or 0)
    return updated, ""


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

    def release_exchange_hold(
        self, tenant_id: str, site_id: str, shipment_number: str, max_retries: int, correlation_id: str
    ) -> Tuple[bool, str, str]:
        url = self.shipment_data_url(tenant_id, shipment_number)
        response, error = self.request(
            "PUT", url, tenant_id, site_id,
            json_body=SHIPMENT_DATA_BODY, max_retries=max_retries, correlation_id=correlation_id,
        )
        if response is None:
            return False, error, ""
        ok = 200 <= response.status_code < 300
        return ok, "" if ok else f"HTTP {response.status_code}", excerpt_of(response)


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

CSV_FIELDS = [
    "transaction_id", "order_number", "external_order_id", "tenant_id", "site_id",
    "shipment_number", "event_timestamp", "transaction_row_count",
    "action", "success", "error", "response_excerpt", "correlation_id",
    "marked_to_be_reprocessed",
]


def write_csv(path: Path, rows: Sequence[Row]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for row in rows:
            record = asdict(row)
            writer.writerow({key: record[key] for key in CSV_FIELDS})


def write_job_summary(
    rows: Sequence[Row], events_read: int, updated: int, update_error: str, dry_run: bool
) -> None:
    """Condensed report for the Actions job summary."""
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    eligible = [r for r in rows if r.shipment_number]
    released = [r for r in eligible if r.action == "released" and r.success]
    over_limit = [r for r in rows if r.action == "retry_limit_reached"]
    failed = [r for r in rows if r.error and r.action != "retry_limit_reached"]
    lines = [
        "## Release shipments held with exchange",
        "",
        f"- Mode: **{'dry run' if dry_run else 'live'}**",
        f"- Events read from {TIL_EVENTS_TABLE}: **{events_read}**",
        f"- Over the retry limit, left alone: **{len(over_limit)}**",
        f"- Shipments to release: **{len(eligible)}**",
        f"- Released: **{len(released)}**",
        f"- With an error: **{len(failed)}**",
        f"- Rows flagged toBeReprocessed: **{updated}**" + (f" (error: {update_error})" if update_error else ""),
        "",
    ]
    if eligible:
        lines += [
            "| externalOrderId | orderNumber | tenant | site | shipmentNumber | action | error |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
        for row in eligible[:200]:
            lines.append(
                f"| {row.external_order_id or '-'} | {row.order_number or '-'} | {row.tenant_id} | "
                f"{row.site_id} | {row.shipment_number} | {row.action} | {row.error[:120]} |"
            )
        if len(eligible) > 200:
            lines.append(f"| ... | | | | | | {len(eligible) - 200} more rows in the CSV artifact |")
    with open(summary_path, "a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true",
                        help="Read and report everything, but call neither Kibo nor the BigQuery update.")
    parser.add_argument("--host", default=DEFAULT_HOST, help="Kibo host suffix.")
    parser.add_argument("--auth-url", default=DEFAULT_AUTH_URL, help="Kibo OAuth URL.")
    parser.add_argument("--limit", type=int, default=0, help="Limit the number of events read (0 = no limit).")
    parser.add_argument("--order-numbers", default="",
                        help="Optional comma or whitespace separated orderNumbers to restrict the run.")
    parser.add_argument("--max-rows-per-transaction", type=int, default=DEFAULT_MAX_ROWS_PER_TRANSACTION,
                        help="Retry limit: skip a transactionId that already appears in more than this "
                             f"many {EVENT_TYPE} rows (0 = no limit).")
    parser.add_argument("--include-already-marked", action="store_true",
                        help="Also process events whose toBeReprocessed flag is already true.")
    parser.add_argument("--mark-scope", choices=("released", "all", "none"), default="released",
                        help="Which events get toBeReprocessed = TRUE: only those whose shipments were all "
                             "released (default), every event returned by the query, or none.")
    parser.add_argument("--batch-size", type=int, default=500,
                        help="transactionId values per BigQuery update statement.")
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
    if args.max_rows_per_transaction < 0:
        parser.error("--max-rows-per-transaction cannot be negative (0 = no limit).")
    args.host = re.sub(r"^https?://", "", args.host.strip()).split("/", 1)[0].rstrip(".")
    return args


def split_values(raw: str) -> List[str]:
    return [item for item in re.split(r"[,\s;]+", raw.strip()) if item]


def rows_to_mark(rows: Sequence[Row], scope: str) -> List[Row]:
    """
    Pick the events to flag.

    'released' keeps an event only when every shipment it carries was released, so a
    partially failed event stays untouched and is picked up by the next run.
    """
    if scope == "none":
        return []
    if scope == "all":
        return list(rows)
    by_event: Dict[str, List[Row]] = {}
    for row in rows:
        by_event.setdefault(row.transaction_id, []).append(row)
    marked: List[Row] = []
    for group in by_event.values():
        if all(row.shipment_number and row.success for row in group):
            marked.extend(group)
    return marked


def main() -> int:
    args = parse_args()
    started = datetime.now(timezone.utc)
    output_path = Path(args.output or f"release_held_exchange_shipments_{started.strftime('%Y%m%d_%H%M%S')}.csv")

    log(f"Mode: {'DRY RUN' if args.dry_run else 'LIVE'}")
    log(f"Kibo host: {args.host}")
    log(f"Mark scope: {args.mark_scope}")
    log(
        "Retry limit: "
        + (f"at most {args.max_rows_per_transaction} {EVENT_TYPE} row(s) per transactionId"
           if args.max_rows_per_transaction > 0 else "none")
    )

    kibo_credentials = load_json_env("KIBO_API_CREDS")
    client_id = clean(kibo_credentials.get("client_id"))
    client_secret = clean(kibo_credentials.get("client_secret"))
    if not client_id or not client_secret:
        fail("KIBO_API_CREDS must contain non-empty 'client_id' and 'client_secret'.")

    wlfs_client = bigquery_client("TIL_BIGQUERY_CREDS", WLFS_PROJECT)

    only_order_numbers = split_values(args.order_numbers)
    if only_order_numbers:
        log(f"Restricting the run to {len(only_order_numbers)} orderNumber(s).")

    rows, columns, events_read = fetch_held_events(
        wlfs_client, only_order_numbers, args.limit, args.include_already_marked,
        args.max_rows_per_transaction,
    )
    eligible = [row for row in rows if row.shipment_number]
    over_limit = [row for row in rows if row.action == "retry_limit_reached"]
    log(f"Events held with exchange: {events_read}")
    if over_limit:
        log(f"Over the retry limit, left alone: {len(over_limit)}")
    log(f"Shipments to release: {len(eligible)}")
    for row in rows:
        if not row.shipment_number:
            warn(f"{row.transaction_id or row.order_number or '(unknown event)'}: {row.error}")

    if not eligible:
        log("Nothing to release.")
        write_csv(output_path, rows)
        write_job_summary(rows, events_read, 0, "", args.dry_run)
        log(f"Results written to {output_path}")
        return 0

    kibo = KiboClient({"client_id": client_id, "client_secret": client_secret},
                      args.auth_url, args.host, args.timeout)

    for index, row in enumerate(eligible, start=1):
        row.correlation_id = uuid.uuid4().hex
        log(
            f"[{index}/{len(eligible)}] order={row.order_number or '-'} "
            f"externalOrderId={row.external_order_id or '-'} "
            f"shipmentNumber={row.shipment_number} (tenant={row.tenant_id}, site={row.site_id})"
        )

        if args.dry_run:
            row.action = "would_release"
            row.success = True
            row.response_excerpt = "DRY RUN - the shipment data endpoint was not called"
            log(f"    DRY RUN PUT {kibo.shipment_data_url(row.tenant_id, row.shipment_number)}")
            continue

        ok, error, excerpt = kibo.release_exchange_hold(
            row.tenant_id, row.site_id, row.shipment_number, args.max_retries, row.correlation_id
        )
        row.action = "released" if ok else "release_failed"
        row.success = ok
        row.error = "" if ok else f"PUT /data?merge=true failed: {error}"
        row.response_excerpt = excerpt

        if ok:
            log("    OK holdExchangeProcess=false")
        else:
            warn(f"    {row.error}")
            if excerpt:
                warn(f"    response: {excerpt[:400]}")

        if args.sleep_between_requests > 0:
            time.sleep(args.sleep_between_requests)

    # Step 4: flag the events so they are reprocessed.
    updated = 0
    update_error = ""
    to_mark = rows_to_mark(rows, args.mark_scope)
    if args.dry_run:
        log(f"DRY RUN: {len(to_mark)} event row(s) would be flagged toBeReprocessed = TRUE.")
    elif to_mark:
        expected = len(dedupe(row.transaction_id for row in to_mark))
        updated, update_error = mark_to_be_reprocessed(wlfs_client, columns, to_mark, args.batch_size)
        if update_error:
            warn(f"Could not flag the events: {update_error}")
        else:
            log(f"Flagged toBeReprocessed = TRUE on {updated} BigQuery row(s) "
                f"for {expected} transactionId(s).")
            if updated != expected:
                warn(
                    f"Flagged {updated} row(s) for {expected} transactionId(s): "
                    + (
                        "some transactions carry several held events."
                        if updated > expected
                        else "some rows were already flagged or are still in the streaming buffer."
                    )
                )
            for row in to_mark:
                row.marked_to_be_reprocessed = True
    else:
        log("No event qualifies for toBeReprocessed = TRUE.")

    write_csv(output_path, rows)
    write_job_summary(rows, events_read, updated, update_error, args.dry_run)

    failures = [row for row in rows if row.error and row.action != "retry_limit_reached"]
    log(f"Results written to {output_path}")
    log(
        f"Summary: {events_read} events, {len(eligible)} shipments, "
        f"{sum(1 for r in eligible if r.action == 'released' and r.success)} released, "
        f"{len(failures)} with an error, {len(over_limit)} over the retry limit, "
        f"{updated} rows flagged."
    )
    if update_error:
        return 2
    return 0 if not failures else 2


if __name__ == "__main__":
    raise SystemExit(main())
