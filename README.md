# HEMA Retail Sales — Medallion ETL Pipeline

Event-driven ETL pipeline that ingests the Superstore retail dataset into a
medallion (bronze / silver / gold) architecture on AWS, using **Delta Lake** for
ACID merges, schema evolution, and idempotent reprocessing.

> Scope note: per the assignment, this repository contains **ETL code only** —
> no IaC and no CI/CD definitions. The architecture diagram (see `docs/`)
> illustrates orchestration and CI/CD as required by the design portion.

## Pipeline flow

```
New CSV  →  S3 landing  →  (s3:ObjectCreated)  →  Lambda  →  Step Functions
                                                               │
                          ┌────────────────────────────────────┘
                          ▼
              Bronze (append)  →  Silver (clean + MERGE)  →  Gold (split + MERGE)
                                                              ├── Sales
                                                              └── Customer
```±

A new file landing in S3 emits an `ObjectCreated` event that invokes the
Lambda. The Lambda reads **only the key of the new object** from the event
payload (it never lists the bucket, so old files are never re-read) and starts
the Step Functions state machine, passing that key down the chain.

## Repository layout

```
hema-retail-etl/
├── common/
│   ├── logging_utils.py     # consistent structured logging for all components
│   ├── manifest_utils.py    # symlink-manifest generation for Athena external tables
│   └── spark_session.py     # Spark+Delta session builder, shared paths/config
├── lambda_src/
│   └── trigger_handler.py   # S3-event entrypoint; starts Step Functions
├── glue_jobs/
│   ├── bronze_job.py        # ingest the one new file → bronze Delta (append)
│   ├── silver_job.py        # clean/conform → silver Delta (MERGE by row_id)
│   ├── gold_sales_job.py    # Sales dataset → gold Delta (MERGE by order_id)
│   └── gold_customer_job.py # Customer dataset → gold Delta (MERGE by customer_id)
├── catalog/
│   └── external_tables.sql  # Glue external tables over the symlink manifests
├── docs/                    # architecture diagram
├── requirements.txt
└── README.md
```

## Medallion layers

| Layer | Write mode | Key | Notes |
|-------|-----------|-----|-------|
| Bronze | append | — | Raw, all columns kept as strings + ingest metadata. Partitioned by `year/month/day` from order date. |
| Silver | MERGE | `row_id` | Typed dates, trimmed strings, `Customer Name` split into first/last, deduped. |
| Gold Sales | MERGE | `order_id` | Order-grain. Attributes: Order ID, Order Date, Shipment Date, Shipment Mode, City. Partitioned by date. |
| Gold Customer | MERGE | `customer_id` | Attributes + rolling order counts (see below). |

## Key design decisions

**Why event-driven instead of a scheduled cron?** A file landing is the natural
trigger. The S3 event carries the new key, so the pipeline reacts immediately
and processes exactly one file with no bucket scanning. (A scheduled trigger
remains a valid "daily batch" framing; the upstream contract is one file/day.)

**Why no DynamoDB ledger?** Idempotency is handled in the **data layer**, not a
side service. Silver MERGEs on `row_id`, so re-loading the same file adds no
rows; gold is a pure recompute from silver, so re-runs converge to identical
output. A Lambda retry or accidental re-drop is therefore safe. (This was
verified — see "Validation" below.) At compliance scale you'd reintroduce a
ledger purely for an explicit audit trail; Delta's `DESCRIBE HISTORY` covers
traceability here.

**Why Delta Lake?** Two assignment requirements map directly onto Delta
features: graceful schema evolution at the data layer (`mergeSchema`) and clean
upserts (`MERGE INTO`) — especially for the Customer rolling aggregates, which
plain Parquet can't upsert at row level.

## Exposing the tables to consumers (Glue Catalog + Athena)

The jobs write Delta tables to S3 *by path*. Writing to S3 updates the **Delta
transaction log**, which is the table's source of truth for schema and live
files — but it does **not** by itself update the Glue Data Catalog. Those are
separate metadata stores.

To make the gold tables queryable in Athena we use the **symlink manifest**
pattern:

- After each gold write, the job calls `generate("symlink_format_manifest")`
  and sets `delta.compatibility.symlinkFormatManifest.enabled = true`, so Delta
  keeps a `_symlink_format_manifest/` file list pointing at the current
  version's Parquet files (see `common/manifest_utils.py`).
- Consumers register a Glue **external table** with `SymlinkTextInputFormat`
  pointed at that manifest (DDL in `catalog/external_tables.sql`). Athena then
  reads only the live files, never stale versions.

Two honest limits of this approach, handled explicitly rather than glossed over:

1. **The manifest is file paths, not schema.** A new column added at the Delta
   layer (`mergeSchema`) is *not* automatically visible in the external table —
   it needs an explicit `ALTER TABLE ADD COLUMNS`. Schema evolution (data layer)
   and schema visibility (Catalog) are decoupled.
2. **New partitions need registering** via `MSCK REPAIR TABLE` (or a targeted
   `ALTER TABLE ADD PARTITION`) before Athena sees them.

So "expose new attributes transparently to downstream users" is satisfied at the
Delta layer automatically and at the Catalog layer through a deliberate DDL step.

## Customer rolling-window metrics — important detail

The windows are anchored to the **dataset's fixed latest day (2018-12-30)**, not
`current_date`, exactly as the spec states:

- `orders_last_month` — distinct orders in `(2018-11-30, 2018-12-30]`
- `orders_last_6_months` — distinct orders in `(2018-06-30, 2018-12-30]`
- `orders_all_time` — all distinct orders ever

"Quantity of orders" is counted as **distinct `order_id`**, so multiple line
items belonging to one order count once.

## Logging

Every component uses `common/logging_utils.get_logger`, producing single-line
structured logs (component, run context, message) that CloudWatch ingests
directly. Each Glue job logs row counts in/out and the file key it processed,
so a run can be traced end to end.

## Validation

The business logic was validated against a faithful sample of the dataset:

- gold Sales row count equals distinct order count (order-grain correct)
- customer counts satisfy `last_month ≤ last_6_months ≤ all_time` (monotonic)
- `orders_all_time` equals raw distinct orders per customer
- **idempotency**: processing the same file once vs. twice yields byte-identical
  customer counts

## Running on AWS Glue

Each Glue job needs the job parameter:

```
--datalake-formats delta
```

which makes the Delta libraries available; `common/spark_session.py` then adds
the required Spark SQL extensions. The `--bucket` / `--key` arguments are passed
by Step Functions from the Lambda's event parsing.
