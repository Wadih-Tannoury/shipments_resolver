# Fulfill GXO shipments in Kibo

Manually triggered GitHub Actions workflow that fulfills Kibo shipments which GXO has
already shipped and for which an invoice document exists.

## Repository layout

```
README-fulfill-gxo-shipments.md
README-release-held-exchange-shipments.md
fulfill_gxo_shipments.py
release_held_exchange_shipments.py
requirements.txt
.github/workflows/shipments-resolver.yml
```

Everything sits at the repository root. The workflow file has to live under
`.github/workflows/` — GitHub only picks up workflows from that path.
`shipments-resolver.yml` triggers every script in the repository; this one is its
`fulfill-gxo` job.

## Repository secrets

| Secret | Content |
| --- | --- |
| `BI_BIGQUERY_CREDS` | Service account JSON for `tlg-business-intelligence-prd` |
| `TIL_BIGQUERY_CREDS` | Service account JSON for `tlg-wlfs-prd` |
| `KIBO_API_CREDS` | `{"client_id": "...", "client_secret": "..."}` |

The two BigQuery secrets are the full service account JSON files, pasted as-is. Each
service account needs BigQuery Job User on its own project and Data Viewer on the
`til` dataset.

## What the run does

1. Reads `shipment_order_number`, `tenant_id`, `site_id` from
   `tlg-business-intelligence-prd.til.oms_active_shipments`, keeping only the rows whose
   `shipmentStatus` is `READY`. Anything already fulfilled, cancelled or otherwise not
   pending never reaches Kibo.
2. Checks `tlg-wlfs-prd.til.prd_shipmentStatus` for a `SHIPPED` event with
   `warehouse = 'GXO'`.
3. Checks `tlg-wlfs-prd.til.prd_invoiceUpdate` for an `INVOICE_RESULT` whose
   `requestPayload` contains at least one `documentLink` starting with `http`
   (the payload is parsed in Python, so the link is found at any nesting depth).
4. For the shipments that pass both checks, takes `data.logisticOrderNumber` from the
   SHIPPED payload (`BB82902`), drops the first two characters and uses the rest as the
   Kibo `shipmentNumber` (`82902`). A value that is not numeric after the prefix is
   removed is reported as an error rather than sent to Kibo.

   A shipment order number can be shipped in several parcels and therefore have several
   SHIPPED events, each with its own `logisticOrderNumber` and its own Kibo shipment.
   Every distinct one is fulfilled, and the CSV gets one row per Kibo shipment. The
   reverse does not happen: a Kibo shipment belongs to exactly one shipment order number.
5. Calls, per shipment:
   - `GET  https://t{tenantId}-s{siteId}.tp3.mozu.com/api/commerce/shipments/{shipmentNumber}`
   - `PUT  https://t{tenantId}-s{siteId}.tp3.mozu.com/api/commerce/shipments/{shipmentNumber}/fulfilled`

   The `GET` is a last safety net: a shipment that Kibo already reports in a terminal
   status (FULFILLED, CANCELED, CLOSED…) is reported as `already_terminal` and left alone,
   which should be rare now that the status filter happens in BigQuery.

Kibo tokens are obtained from `https://home.mozu.com/api/platform/applications/authtickets/oauth`
and cached per `tenantId`, with a refresh on 401 and backoff on 429/5xx.

## Running it

Actions → **Shipments resolver** → Run workflow, with `scripts` set to `fulfill-gxo`
(or `all`). This script is not on the daily schedule; only `release-held-exchange` is.

| Input | Default | Notes |
| --- | --- | --- |
| `scripts` | `release-held-exchange` | Set to `fulfill-gxo` or `all` to run this script |
| `dry_run` | `false` | Runs every check and reads each shipment, but never calls `/fulfilled` |
| `warehouse` | `GXO` | Warehouse filter on the SHIPPED event |
| `oms_status` | `READY` | `shipmentStatus` kept from `oms_active_shipments`; leave empty to disable the filter |
| `numbers` | empty | Comma separated `shipmentOrderNumber`s to restrict the run |
| `limit` | `0` | Cap on the number of active shipments (0 = no limit) |
| `kibo_host` | `tp3.mozu.com` | Kibo host suffix |

The `mark_scope` and `include_already_marked` inputs belong to the release job and are
ignored here.

Start with `dry_run` on, review the artifact, then re-run with it off.

## Running it locally

```bash
pip install -r requirements.txt
export BI_BIGQUERY_CREDS="$(cat bi-service-account.json)"
export TIL_BIGQUERY_CREDS="$(cat til-service-account.json)"
export KIBO_API_CREDS="$(cat kibo_credentials.json)"
python fulfill_gxo_shipments.py --dry-run
```

## Output

Every run uploads `fulfill-gxo-shipments-results` containing a CSV with one row per
active shipment: the two BigQuery checks, the document link, the logistic order number,
the OMS status, the derived Kibo shipment number, the Kibo status before and after, the action taken, and any
error. A condensed table is also written to the job summary. The job exits with code 2 if
any eligible shipment failed, so a failed run is visible in the Actions list.

## Schema notes

Column names are resolved case-insensitively against `INFORMATION_SCHEMA.COLUMNS`, so
`shipmentOrderNumber` / `shipment_order_number` and `requestPayload` / `request_payload`
both work. The script also looks for an event timestamp column to pick the most recent
event per shipment; if none of the usual names exist, it takes the first row returned.
If `prd_invoiceUpdate` has no `eventType` column, the event type is read from inside the
payload instead.
