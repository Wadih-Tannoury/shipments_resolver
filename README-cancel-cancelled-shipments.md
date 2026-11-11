# Cancel cancelled shipments in Kibo

Manually triggered GitHub Actions workflow that resolves in Kibo the shipments the
warehouse has already cancelled, with the reason code derived from the PICKED quantity.
Shipments on the **ecommerce** channel are auto reassigned instead of cancelled, so they
go back through order routing and land in backorder; everything else is cancelled.

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

## Which shipments are in scope

The 6th character of the `shipmentOrderNumber` gates everything. Counting every
character, the hyphen included, it has to be one of **A, B, C or D**; anything else is
left alone — neither cancelled nor reassigned — and reported as
`sixth_character_excluded`. Excluded shipments cost no Kibo call and no event query.

| shipmentOrderNumber | 6th character | In scope |
| --- | --- | --- |
| `DJ-USA00041696` | `A` | yes |
| `DJ-USD00041696` | `D` | yes |
| `DJ-USE00041696` | `E` | no |
| `DJ-USF00041696` | `F` | no |
| `DJ-USI00041696` | `I` | no |

A number shorter than 6 characters is treated as out of scope and reported, rather than
guessed at. Change the set with the `allowed_sixth_characters` input (`ABCD`, `A,B,C,D`
and `abcd` all work); set it empty to turn the rule off.

## What the run does

1. Reads `shipment_order_number`, the shipment number, `tenant_id` and `site_id` from
   `tlg-business-intelligence-prd.til.oms_active_shipments`, keeping only the rows
   whose `shipmentStatus` is `READY`, same as the `fulfill-gxo` job. Anything already
   cancelled, fulfilled or otherwise not pending never reaches Kibo.
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

5. Reads the shipment from Kibo:

   `GET https://t{tenantId}.tp3.mozu.com/api/commerce/shipments/{shipmentNumber}`

   The `GET` is a safety net: a shipment Kibo already reports as CANCELED, CLOSED,
   SHIPPED or otherwise terminal is reported as `already_terminal` and left alone, so
   re-running the job is harmless. It also supplies the shipment's `channel`, which
   decides step 6.

6. **Channel is not ecommerce — cancel:**

   `PUT https://t{tenantId}.tp3.mozu.com/api/commerce/shipments/{shipmentNumber}/canceled`

   ```json
   {"canceledReason": {"reasonCode": "No Inventory", "moreInfo": "No Inventory"}}
   ```

   The reason has to be nested under `canceledReason`, per the `CancelShipment`
   schema. Kibo answers `200 OK` to a flat `{"reasonCode": ..., "moreInfo": ...}`
   body but ignores it and records its own default reason (`Canceled`), so the run
   looks successful while the Reason column in the admin UI is wrong.

   **Channel is ecommerce — auto reassign instead:**

   `PUT https://t{tenantId}.tp3.mozu.com/api/commerce/shipments/{shipmentNumber}/reassigned`

   ```json
   {"blockAssignment": false,
    "isUserAction": true,
    "reassignedReason": {"reasonCode": "No Inventory", "moreInfo": "No Inventory"}}
   ```

   A shipment Kibo already reports as REASSIGNED or BACKORDER is reported as
   `already_reassigned` and left alone.

   After a successful call the shipment is read back and the stored
   `shipmentStatusReason.reasonCode` is written to the `kibo_reason_after` column; if
   it differs from the reason that was sent, the run logs a warning. Every call carries
   `x-vol-tenant` and `x-vol-site`.

## Why that body is an auto reassign

Kibo's `ReassignShipment` schema is
`{blockAssignment, fulfillmentLocationCode, holdBlockAssignment, isUserAction, items, reassignedReason, runId}`
([Reassign Shipments](https://docs.kibocommerce.com/api-reference/shipment/reassign-shipments)).
There is no separate auto-reassign endpoint or flag: the Order Admin offers **Manual**
and **Auto Reassign** against this same call, and the difference is whether a target
location is named. Manual sets `fulfillmentLocationCode`; auto leaves it out, and
[the docs](https://docs.kibocommerce.com/pages/edit-order-shipments) describe that case
as sending the shipment back through the order routing process. Routing then picks a new
location, or drops the shipment into backorder when no location can fulfil it.

`blockAssignment` is `false`, so routing stays free to pick the location that just
cancelled the shipment. Set the `block_assignment` input to `true` (or pass
`--block-assignment`) to exclude that location instead, which prevents a shipment
bouncing back to the warehouse that could not fulfil it.

## Where the channel comes from

The channel is read from `oms_active_shipments` when the table has a `channel` column
(`channel_name`, `sales_channel` and `order_channel` are also accepted). If it has none,
the script warns once and falls back to the `channel` field of the Kibo shipment read in
step 5. Both values and the one actually used are in the CSV as `oms_channel`,
`kibo_channel`, `channel` and `channel_source`.

The comparison ignores case, spaces, hyphens and underscores, so `ecommerce`,
`E-Commerce` and `E_COMMERCE` all match. Change the value with the `ecommerce_channel`
input; set it to empty to cancel every shipment regardless of channel, which is the
behaviour this job had before.

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
| `dry_run` | `false` | Read everything, call neither the `canceled` nor the `reassigned` endpoint |
| `numbers` | empty | Restrict the run to these `shipmentOrderNumber`s |
| `limit` | `0` | Maximum rows read from `oms_active_shipments` (0 = no limit) |
| `kibo_host` | `tp3.mozu.com` | Kibo host suffix |
| `cancel_oms_status` | `READY` | Keep only this `shipmentStatus` in `oms_active_shipments` (empty = no filter) |
| `no_picked_reason` | `skip` | What to do with a CANCELLED shipment that has no PICKED event: `skip`, or resolve it as `no-inventory` or `customer` |
| `ecommerce_channel` | `ecommerce` | Shipments on this channel are auto reassigned instead of cancelled (empty = cancel everything) |
| `block_assignment` | `false` | On auto reassign, block the location that cancelled the shipment |
| `allowed_sixth_characters` | `ABCD` | Only act when the 6th character of the `shipmentOrderNumber` is one of these (empty = no rule) |

A shipment whose PICKED payload carries no `quantity` field at all is skipped and
reported, rather than treated as zero.

## Results

The job uploads `cancel_cancelled_shipments_results.csv` as an artifact (30 days) and
writes a summary table to the run page. One row per Kibo shipment, with
`oms_shipment_number`, `sixth_character`, `oms_channel`, `kibo_channel`, `channel`, `channel_source`,
`resolution` (`cancel` or `reassign`), `has_cancelled_event`, `has_picked_event`,
`picked_quantity`, `reason_code`, `logistic_order_number`, `kibo_shipment_number`,
`number_matches_oms`, the Kibo status before and after, `kibo_reason_after`, the
action (`canceled`, `reassigned`, `would_cancel`, `would_reassign`, `already_terminal`,
`already_reassigned`, `sixth_character_excluded`, `skipped`, `read_failed`,
`cancel_failed`, `reassign_failed`), and the error when there is one.

For an ecommerce shipment, `kibo_status_after` is where routing put it: `BACKORDER` when
no location could take it, otherwise the new state.

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
