# Cancel cancelled shipments in Kibo

Manually triggered GitHub Actions workflow that cancels in Kibo the shipments the
warehouse has already cancelled, with the reason code derived from the PICKED quantity.

## Repository layout

```
README-cancel-cancelled-shipments.md
README-fulfill-gxo-shipments.md
README-release-held-exchange-shipments.md
cancel_cancelled_shipments.py
fulfill_gxo_shipments.py
release_held_exchange_shipments.py
requirements.txt
.github/workflows/shipments_resolver.yml
```

`shipments_resolver.yml` triggers every script in the repository; this one is its
`cancel-cancelled` job. Pick it in the **Which script(s) to run** dropdown, or pick
`all` to run every script.

## Repository secrets

| Secret | Content |
| --- | --- |
| `BI_BIGQUERY_CREDS` | Service account JSON for `tlg-business-intelligence-prd` |
| `TIL_BIGQUERY_CREDS` | Service account JSON for `tlg-wlfs-prd` |
| `KIBO_API_CREDS` | `{"client_id": "...", "client_secret": "..."}` |

Same secrets as the `fulfill-gxo` job: nothing new to create. Both service accounts
only need read access (Job User on their project, Data Viewer on the `til` dataset).

## What the run does

1. Reads `shipment_order_number`, `tenant_id`, `site_id` from
   `tlg-business-intelligence-prd.til.oms_active_shipments`. Unlike `fulfill-gxo`,
   no `shipmentStatus` filter is applied by default, because a cancelled shipment can
   sit in several statuses; set **cancel_oms_status** to filter.
2. For those shipment order numbers, reads the `CANCELLED` and `PICKED` events from
   `tlg-wlfs-prd.til.prd_shipmentStatus` — the same join as the `fulfill-gxo` job.
   A shipment with no `CANCELLED` event is skipped. Both `CANCELLED` and `CANCELED`
   spellings are matched.
3. Reads the total quantity from the `requestPayload` of the `PICKED` event — the sum
   of every `quantity` value found anywhere in the payload, so nested item lists are
   handled. When several `PICKED` events exist, the most recent one is used.
4. Picks the reason:

   | PICKED quantity | `reasonCode` and `moreInfo` |
   | --- | --- |
   | not zero | `Customer Changed Their Mind` |
   | zero | `No Inventory` |

5. Calls, per shipment:
   - `GET https://t{tenantId}.tp3.mozu.com/api/commerce/shipments/{shipmentNumber}`
   - `PUT https://t{tenantId}.tp3.mozu.com/api/commerce/shipments/{shipmentNumber}/canceled`

     ```json
     {"reasonCode": "No Inventory", "moreInfo": "No Inventory"}
     ```

   Both carry `x-vol-tenant` and `x-vol-site`. The `GET` is a safety net: a shipment
   Kibo already reports as CANCELED, CLOSED, SHIPPED or otherwise terminal is reported
   as `already_terminal` and left alone, so re-running the job is harmless.

## How a row is matched to a Kibo shipment

Both keys of the `oms_active_shipments` row have to line up:

| `oms_active_shipments` | matched against |
| --- | --- |
| `shipment_order_number` | the `shipmentOrderNumber` of the CANCELLED / PICKED event |
| shipment number | the Kibo `shipmentNumber` derived from `data.logisticOrderNumber` |

The derivation is the `fulfill-gxo` rule: take `data.logisticOrderNumber` from the
CANCELLED or PICKED `requestPayload` (`BB82902`), drop the first two characters, and
the rest is the Kibo `shipmentNumber` (`82902`).

A shipment order number can carry several `logisticOrderNumber` values; only the one
whose `shipmentNumber` equals the shipment number of that `oms_active_shipments` row is
cancelled. When none of them matches, nothing is sent to Kibo and the row is reported
with `number_matches_oms = no` and the numbers the payload actually gave, so a mismatch
is visible in the CSV instead of becoming a cancellation on the wrong shipment.

If `oms_active_shipments` turns out to have no shipment number column, the script warns
once, skips the check, and marks those rows `unchecked`.

## Workflow inputs

| Input | Default | Meaning |
| --- | --- | --- |
| `scripts` | `release-held-exchange` | Pick `cancel-cancelled` to run this job |
| `dry_run` | `false` | Read everything, call no `canceled` endpoint |
| `numbers` | empty | Restrict the run to these `shipmentOrderNumber`s |
| `limit` | `0` | Maximum rows read from `oms_active_shipments` (0 = no limit) |
| `kibo_host` | `tp3.mozu.com` | Kibo host suffix |
| `cancel_oms_status` | empty | Keep only this `shipmentStatus` in `oms_active_shipments` |
| `no_picked_reason` | `skip` | What to do with a CANCELLED shipment that has no PICKED event: `skip`, or cancel it as `no-inventory` or `customer` |

A shipment whose PICKED payload carries no `quantity` field at all is skipped and
reported, rather than treated as zero.

## Results

The job uploads `cancel_cancelled_shipments_results.csv` as an artifact (30 days) and
writes a summary table to the run page. One row per Kibo shipment, with
`oms_shipment_number`, `has_cancelled_event`, `has_picked_event`, `picked_quantity`,
`reason_code`, `logistic_order_number`, `kibo_shipment_number`, `number_matches_oms`,
the Kibo status before and after, the
action (`canceled`, `would_cancel`, `already_terminal`, `skipped`, `read_failed`,
`cancel_failed`), and the error when there is one.

Exit code `0` when nothing failed, `2` when at least one eligible shipment failed.

## Running it locally

```bash
pip install -r requirements.txt
export BI_BIGQUERY_CREDS="$(cat bi-service-account.json)"
export TIL_BIGQUERY_CREDS="$(cat til-service-account.json)"
export KIBO_API_CREDS='{"client_id":"...","client_secret":"..."}'

python cancel_cancelled_shipments.py --dry-run --limit 20
```

Start with `--dry-run` on a handful of known shipment order numbers:

```bash
python cancel_cancelled_shipments.py --dry-run --shipment-order-numbers "SO123,SO456"
```
