# Release shipments held with exchange

Daily GitHub Actions workflow that clears the exchange hold on Kibo shipments whose
`shipment.itemscanceled` event came back with `Shipment held with exchange`, then flags
those events in BigQuery so they get reprocessed.

## Repository layout

```
release_held_exchange_shipments.py
requirements.txt
.github/workflows/shipments-resolver.yml
```

`shipments-resolver.yml` is the single workflow for every script in the repository; this
one is the `release-held-exchange` job inside it.

## Repository secrets

| Secret | Content |
| --- | --- |
| `TIL_BIGQUERY_CREDS` | Service account JSON for `tlg-wlfs-prd` |
| `KIBO_API_CREDS` | `{"client_id": "...", "client_secret": "..."}` |

The service account needs BigQuery **Job User** on the project and **Data Editor** on the
`til` dataset. Data Viewer is not enough here: the last step is an `UPDATE` statement.

## What the run does

1. Runs the held-with-exchange query against `tlg-wlfs-prd.til.prd_tilEvents`:

   ```sql
   SELECT ... FROM `tlg-wlfs-prd`.til.prd_tilEvents til1
   WHERE til1.eventType = 'shipment.itemscanceled'
     AND til1.warningMessage LIKE '%Shipment held with exchange%'
     AND NOT EXISTS (
         SELECT 1 FROM `tlg-wlfs-prd`.til.prd_tilEvents til2
         WHERE til2.eventType = 'shipment.itemscanceled'
           AND til2.warningMessage IS NULL
           AND til2.orderNumber = til1.orderNumber
     )
   ORDER BY eventTimestamp DESC
   ```

   By default rows already flagged `toBeReprocessed` are left out, so a daily run does not
   keep re-sending yesterday's shipments. Pass `--include-already-marked` to process them
   anyway.

2. Reads out of the `requestPayload` column of each row:

   | Value | Payload field |
   | --- | --- |
   | `externalOrderId` | `orderNumber` |
   | `shipmentNumber` list | `entityId` |
   | `tenantId` | `x-vol-tenant` |
   | `siteId` | `x-vol-site` |

   Fields are looked up case-insensitively at any nesting depth, so `x-vol-tenant` is
   found whether it sits at the top level, inside `headers`, or inside a list of header
   objects. `entityId` may hold a single value, a list, or several ids in one separated
   string; every distinct one becomes its own Kibo call and its own CSV row. A row whose
   payload has no `entityId`, no tenant or no site is reported as `skipped` with the
   missing fields named, and is never flagged for reprocessing.

3. Per shipment number:

   ```
   PUT https://t{tenantId}.tp3.mozu.com/api/commerce/shipments/{shipmentNumber}/data?merge=true
   x-vol-tenant: {tenantId}
   x-vol-site:   {siteId}

   {"holdExchangeProcess": false}
   ```

   Note the host carries only the tenant; the site travels in the header.

4. Sets `toBeReprocessed = TRUE` on the events from step 1. When `prd_tilEvents` has an
   identifier column (`eventId`, `id`, `messageId`…) the update targets exactly those
   rows; otherwise it replays the step 1 predicate restricted to the order numbers that
   were processed. The column is written as `TRUE`, `1` or `'true'` depending on its
   declared type.

   By default (`--mark-scope released`) an event is flagged only when **all** of its
   shipments were released, so a partial failure is retried on the next run instead of
   being silently closed. `--mark-scope all` flags every event the query returned,
   `--mark-scope none` flags nothing.

Kibo tokens come from `https://home.mozu.com/api/platform/applications/authtickets/oauth`,
cached per `tenantId`, refreshed on 401, with backoff on 429/5xx.

## Running it

It runs on its own every day at **06:00 UTC**, which is the only script the schedule
triggers. For a manual run: Actions → **Shipments resolver** → Run workflow, with
`scripts` set to `release-held-exchange` (or `both`).

| Input | Default | Notes |
| --- | --- | --- |
| `scripts` | `release-held-exchange` | Set to `both` to also run the GXO script |
| `dry_run` | `false` | Reads and reports everything, calls neither Kibo nor the update |
| `numbers` | empty | Comma separated `orderNumber`s to restrict the run |
| `limit` | `0` | Cap on the number of events (0 = no limit) |
| `kibo_host` | `tp3.mozu.com` | Kibo host suffix |
| `mark_scope` | `released` | `released`, `all` or `none` (see step 4) |
| `include_already_marked` | `false` | Also process events already flagged |

The `warehouse` and `oms_status` inputs belong to the GXO job and are ignored here.

Before the first live run, trigger it once with `dry_run` on and check the artifact: it
shows the exact URL, tenant, site and shipment numbers that would be called.

## Running it locally

```bash
pip install -r requirements.txt
export TIL_BIGQUERY_CREDS="$(cat til-service-account.json)"
export KIBO_API_CREDS="$(cat kibo_credentials.json)"
python release_held_exchange_shipments.py --dry-run
```

## Output

Every run uploads `release-held-exchange-shipments-results`, a CSV with one row per
shipment: the event identifier, order number, externalOrderId, tenant, site, shipment
number, event timestamp, the action taken, any error, the Kibo response excerpt, the
correlation id sent as `x-vol-correlation`, and whether the event was flagged. A
condensed table also goes to the job summary. The job exits with code 2 when any shipment
failed or the update could not be applied, so a bad run is visible in the Actions list.

## Notes

- Column names are resolved case-insensitively against `INFORMATION_SCHEMA.COLUMNS`, so
  `requestPayload` / `request_payload` and `orderNumber` / `order_number` both work. If
  no `toBeReprocessed`-style column exists, the releases still happen and the update step
  reports the missing column rather than failing silently.
- BigQuery cannot run DML against rows still in the streaming buffer (roughly the last 30
  minutes of inserts). Such an event simply stays unflagged and is picked up by the next
  daily run.
- `concurrency` is shared by every job in `shipments-resolver.yml`, so a manual run during
  the scheduled one queues instead of double-sending.
