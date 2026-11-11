# Reassign blocked shipments

GitHub Actions job that reassigns blocked Kibo shipments to the location they are already
assigned to, clearing the exchange hold first where one is set. Reassigning a shipment to
its own location is what pushes Kibo to run the assignment again, which is what unblocks
the shipment.

## Repository layout

```
reassign_blocked_shipments.py
requirements.txt
.github/workflows/shipments_resolver.yml
```

`shipments_resolver.yml` is the single workflow for every script in the repository; this
one is the `reassign-blocked` job inside it. It is manual only — add
`github.event_name == 'schedule' ||` to the job's `if:` to put it on the daily schedule.

## Repository secrets

| Secret | Content |
| --- | --- |
| `BI_BIGQUERY_CREDS` | Service account JSON for `tlg-business-intelligence-prd` |
| `KIBO_API_CREDS` | `{"client_id": "...", "client_secret": "..."}` |

Nothing is written back to BigQuery, so BigQuery **Job User** on the project and **Data
Viewer** on the `til` dataset is enough.

## What the run does

1. Runs the blocked-shipments query against
   `tlg-business-intelligence-prd.til.prd_fulfillment_monitoring_table`:

   ```sql
   SELECT ... FROM `tlg-business-intelligence-prd`.til.prd_fulfillment_monitoring_table
   WHERE fulfillment_status = 'Accept Shipment'
     AND shipment_order_number IS NULL
     AND location_name IS NOT NULL
     AND location_class <> 'preorder'
     AND (blocking_reason = 'OMS Shipment Update - Assign shipmentOrderNumber action not successful'
          OR blocking_reason = 'exchange order should be unblocked')
   ORDER BY order_submit_date DESC
   ```

   The same shipment can appear on several rows, so `(shipmentNumber, tenantId, siteId)` is
   deduplicated: each shipment is reassigned once per run.

2. Sorts each row on its two hold flags. Both are read as text, so `TRUE`, `true`, `1` and
   `Y` all count as true, and `NULL` counts as false:

   | `hold_exchange_process` | `hold_process` | What happens |
   | --- | --- | --- |
   | false / null | false / null | reassign |
   | true | false / null | release the exchange hold, then reassign |
   | any | **true** | left alone, reported as `hold_process_true` |

   The last line is the deliberate reading of the two rules together: `hold_process = true`
   means something else is holding the shipment on purpose, so it is reported rather than
   touched.

3. For a row with `hold_exchange_process = true`, clears the hold exactly the way
   `release_held_exchange_shipments.py` does:

   ```
   PUT https://t{tenantId}.{host}/api/commerce/shipments/{shipmentNumber}/data?merge=true
   x-vol-tenant: {tenantId}
   x-vol-site:   {siteId}

   {"holdExchangeProcess": false}
   ```

   If this call fails, the shipment is reported as `release_failed` and **not** reassigned,
   so the next run picks it up again.

4. Reassigns the shipment to the location it is already on:

   ```
   PUT https://t{tenantId}-s{siteId}.{host}/api/commerce/shipments/{shipmentNumber}/reassigned
   x-vol-tenant: {tenantId}
   x-vol-site:   {siteId}

   {"blockAssignment": false, "fulfillmentLocationCode": "<location>", "isUserAction": true}
   ```

## The location column

The reassign call needs the location **code**, while the query filters on
`location_name`. The script takes the first of these columns that exists in the table:

```
location_code, locationCode, fulfillment_location_code, fulfillmentLocationCode,
location, location_name, locationName
```

and logs which one it picked. If the monitoring table names it something else, pass
`--location-column` (the `location_column` workflow input). Every other column is resolved
the same way, case-insensitively, so `tenantId` and `tenant_id` both work; a column that
cannot be found stops the run with the list of columns that do exist.

## Running it

From the Actions tab, pick **Shipments resolver**, set **Which script(s) to run** to
`reassign-blocked`, and tick **Dry run** for the first pass. A dry run reads BigQuery and
prints every request it would send without calling Kibo at all.

Locally:

```bash
export BI_BIGQUERY_CREDS="$(cat bi-service-account.json)"
export KIBO_API_CREDS='{"client_id": "...", "client_secret": "..."}'

python reassign_blocked_shipments.py --dry-run
python reassign_blocked_shipments.py --shipment-numbers "123456,123457"
```

| Option | Default | What it does |
| --- | --- | --- |
| `--dry-run` | off | Read and report everything, call neither Kibo endpoint |
| `--host` | `tp3.mozu.com` | Kibo host suffix |
| `--auth-url` | `https://home.mozu.com/...` | Kibo OAuth URL |
| `--limit` | `0` | Maximum rows read, `0` = no limit |
| `--shipment-numbers` | empty | Restrict the run to these shipmentNumbers |
| `--location-column` | auto | Column holding the fulfillment location code |
| `--timeout` | `60` | HTTP timeout in seconds |
| `--max-retries` | `3` | Attempts per Kibo request |
| `--sleep-between-requests` | `0.2` | Pause between Kibo writes |
| `--output` | timestamped | CSV output path |

## Output

A CSV with one line per shipment, uploaded as the `reassign-blocked-shipments-results`
artifact, plus a table in the job summary. The `action` column says what happened:

| `action` | Meaning |
| --- | --- |
| `reassigned` | Reassigned successfully |
| `would_reassign` | Dry run only |
| `release_failed` | The exchange hold would not clear, so nothing was reassigned |
| `reassign_failed` | The reassign call failed |
| `hold_process_true` | Left alone because `hold_process` is true |
| `incomplete_row` | The monitoring table row has no shipment number, tenant, site or location |

The `released` column says whether the exchange hold was cleared first.

Exit codes: `0` nothing failed, `2` at least one shipment failed, `1` a configuration or
credentials problem stopped the run.
