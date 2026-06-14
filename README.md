# HEMA - ETL Pipeline Assignment

Event driven ETL pipeline that ingests the dataset into a
medallion (bronze / silver / gold) architecture on AWS, using **Delta Lake** for
ACID merges, schema evolution, and idempotent reprocessing.

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
├── pipeline_architecture/                    # architecture diagram
└── README.md
```
