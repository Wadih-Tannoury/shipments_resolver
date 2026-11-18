# Cancel cancelled shipments in Kibo

> **This flow is switched off.** `cancel-cancelled` is no longer in the **Which
> script(s) to run** dropdown, `all` does not include it, and its job in
> `.github/workflows/shipments_resolver.yml` is pinned to `if: false`. The script
> itself is complete and ready; the other three flows (`release-held-exchange`,
> `fulfill-gxo`, `reassign-blocked`) are unaffected. See
> [Turning it back on](#turning-it-back-on).

Resolves in Kibo the shipments the warehouse has already cancelled. The cancellation
list is **`tlg-business-intelligence-prd`.til.orders_cancellation_table**: nothing is
cancelled unless that table carries a row for the shipment with
`cancelled_on_kibo = false`, and the `reasonCode` and `moreInfo` sent to Kibo are the
row's own.

## Repository layout

```
README-cancel-cancelled-shipments.md
README-fulfill-gxo-shipments.md
README-reassign-blocked-shipments.md
README-release-held-exchange-shipments.md
cancel_cancelled_shipments.py
fulfill_gxo_shipments.py
reassign_blocked_shipments.py
release_held_exchange_shipments.py
requirements.txt
.github/workflows/shipments_resolver.yml
```

## Repository secrets

| Secret | Content |
| --- | --- |
| `BI_BIGQUERY_CREDS` | Service account JSON for `tlg-business-intelligence-prd` |
| `TIL_BIGQUERY_CREDS` | Service account JSON for `tlg-wlfs-prd` |
| `KIBO_API_CREDS` | `{"client_id": "...", "client_secret": "..."}` |

`GXO_API_CREDS` is no longer used: nothing is sent to the warehouse any more.
`BI_BIGQUERY_CREDS` now needs **write** access as well (BigQuery Data Editor on the
`til` dataset), because the run flags the rows it cancelled; read access is enough with
`--no-mark-cancelled`.

## The flow

### 1. Narrow the list

```sql
SELECT shipment_order_number, shipment_number, tenant_id, site_id
FROM `tlg-business-intelligence-prd`.til.oms_active_shipments
WHERE shipmentStatus = 'READY'
```

The status filter is the `cancel_oms_status` input (`--oms-status`); empty disables it.

### 2. The order's last warehouse event must be a cancellation

```sql
SELECT * FROM `tlg-wlfs-prd`.til.prd_shipmentStatus
WHERE shipmentOrderNumber LIKE 'DJ-US%ST0000124550'
```

`prd_shipmentStatus` has no order number, so the order is the `shipmentOrderNumber`
with its 6th character — the generation letter — wildcarded. The events are read per
**order**, not per `shipmentOrderNumber`, because the active shipment usually carries
none of its own: the warehouse cancels generation F, Kibo reassigns into a fresh G, and
G is what sits `READY` while every event still names A to F.

Every event type is read, and the **most recent** one has to be a `CANCELLED`
(`CANCELED` is accepted too). A later `PICKED`, `SHIPPED` or anything else means the
warehouse moved on, and the row is reported as `latest_event_not_cancelled` and left
alone. `--no-require-latest-cancelled` relaxes this to "a cancellation exists
somewhere in the history".

The items of that last `CANCELLED` event are what the rest of the run matches on.

### 3. Find the shipment to cancel

```
GET https://t{tenantId}.{host}/api/commerce/shipments?filter=externalOrderId=={id}
```

The `externalOrderId` is the shipment's own field, falling back to the
`shipmentOrderNumber` with its 6th character put back to a hyphen
(`DJ-USAST0000124550` → `DJ-US-ST0000124550`).

Out of the shipments of that order, the target is the one that

| Condition | Default | Input |
| --- | --- | --- |
| `shipmentStatus` is one of | `BACKORDER`, `CUSTOMER_CARE` | `cancel_target_statuses` |
| is in a cancellable status | not `CANCELED`/terminal, not `REASSIGNED` | `--cancel-target-excluded-statuses` |
| carries the exact same items as the `CANCELLED` event | exact | `product_match` |
| has a row in `orders_cancellation_table` with `cancelled_on_kibo = false` | required | — |
| whose `shipment_items` are those same items | exact | `product_match` |

The table is looked up on **both** the shipment's `shipmentOrderNumber` and its Kibo
`shipmentNumber`, so the `shipment_order_number` column may hold either. Which one
matched is reported in the CSV as `table_matched_on`.

Several matches → `ambiguous_cancel_target`, nothing is touched
(`--on-multiple-cancel-targets newest` takes the most recent one instead). No match →
`no_cancel_target`, and the CSV's `error` says how far down the chain the order got:
wrong status, items that do not line up, or no open row in the table.

### 4. Cancel in Kibo

```
PUT https://t{tenantId}.{host}/api/commerce/shipments/{shipmentNumber}/canceled

{"canceledReason": {"reasonCode": "<reason_code>", "moreInfo": "<more_info>"}}
```

`reasonCode` comes from the table's `reason_code` column and `moreInfo` from its
`more_info` column; `--default-reason-code` / `--default-more-info` fill in when a
value is empty or the column does not exist. The reason has to be nested under
`canceledReason`: Kibo answers 200 to a flat body but ignores it and records its own
default. The shipment is read back afterwards and the stored
`shipmentStatusReason.reasonCode` is reported, so a silently ignored reason shows up.

### 5. Flag the rows

```sql
UPDATE `tlg-business-intelligence-prd`.til.orders_cancellation_table
SET cancelled_on_kibo = TRUE
WHERE shipment_order_number IN UNNEST(@numbers)
```

One statement at the end of the run, so a later run skips them. Turn it off with the
`mark_cancelled_on_kibo` input (`--no-mark-cancelled`); a dry run never writes. The
literal matches the column's type, so `BOOL`, `STRING` and `INT64` all work, and the
same tolerance applies when reading the flag (`true`, `TRUE`, `1`, `yes`; `NULL` reads
as false).

Should a run die between the cancellation and this update, the cancellations stand and
nothing is cancelled twice: the next run finds those shipments `CANCELED` in Kibo
rather than `BACKORDER`, so they are no longer targets.

## What is no longer done

- **Nothing is sent to GXO.** The warehouse has already cancelled — that is what put
  the shipment in the table — and the target is a backordered or customer-care
  shipment it never held. The `CANCEL` event, `prd_orders`, `prd_brandConfig` and the
  legacy-TLS adapter are gone.
- **Nothing is reassigned.** The ecommerce branch (auto reassign instead of cancel) and
  the repeat-cancellation rule that counted cancelled generations are gone. The table
  decides what gets cancelled, whatever the channel.
- **The reason is no longer derived from the `PICKED` quantity.** It comes from the
  table.

## Inputs

| Input | Default | Meaning |
| --- | --- | --- |
| `dry_run` | false | Run every check and read, write nothing |
| `cancel_oms_status` | `READY` | `shipmentStatus` kept in `oms_active_shipments` |
| `cancel_target_statuses` | `BACKORDER,CUSTOMER_CARE` | Statuses a shipment of the order must have to be cancelled |
| `mark_cancelled_on_kibo` | true | Set `cancelled_on_kibo = true` after a successful cancellation |
| `product_match` | `exact` | `exact`, `overlap` or `any` |
| `allowed_sixth_characters` | *(empty)* | Optional generation-letter filter; empty means every generation |
| `numbers` | *(empty)* | Restrict the run to these `shipmentOrderNumber`s |
| `limit` | 0 | Maximum rows to process |
| `kibo_host` | `tp3.mozu.com` | Kibo host suffix |
| `cancel_extra_args` | *(empty)* | Flags passed straight through, e.g. `--on-multiple-cancel-targets newest` |

## Results

The job uploads `cancel_cancelled_shipments_results.csv` and writes a summary table to
the run page. One row per active shipment; `action` is one of:

| action | Meaning |
| --- | --- |
| `canceled` | Cancelled in Kibo |
| `would_cancel` | Dry run: everything matched, nothing was called |
| `cancel_failed` | Kibo refused the cancellation |
| `latest_event_not_cancelled` | A later warehouse event superseded the cancellation |
| `no_cancelled_event` | No `CANCELLED` event for any generation of the order |
| `no_cancelled_items` | The `CANCELLED` payload carries no product identifier |
| `no_cancel_target` | No listed `BACKORDER`/`CUSTOMER_CARE` shipment with those items |
| `ambiguous_cancel_target` | Several shipments of the order qualify |
| `already_terminal` | Kibo already had it cancelled or closed |
| `order_lookup_failed` | `getShipments` by `externalOrderId` failed |
| `no_external_order_id` | No `externalOrderId` could be read or derived |
| `unknown_brand` | Brand not in `BRAND_KIBO_CONFIG` |
| `sixth_character_excluded` | Outside `allowed_sixth_characters` |

`table_updated` says whether the `orders_cancellation_table` row was flagged
(`yes`, `no`, `failed`, `no (dry run)`).

## Turning it back on

Two edits in `.github/workflows/shipments_resolver.yml`:

1. Put `- cancel-cancelled` back in the options of the `scripts` input.
2. Restore the `cancel-cancelled` job's condition, which is commented out just above
   its current `if: false`:

   ```yaml
   if: >-
     inputs.scripts == 'cancel-cancelled'
     || inputs.scripts == 'all'
   ```

Then run it with **Dry run** ticked first: the CSV shows exactly which shipment each
order would be cancelled on, with the table row and reason behind it, without calling
the `canceled` endpoint or writing to BigQuery.
