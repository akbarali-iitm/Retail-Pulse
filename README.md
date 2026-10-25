# RetailPulse --- End-to-End Retail Data Engineering Pipeline

RetailPulse is a portfolio data engineering project that transforms the
**UCI Online Retail II** dataset into validated, analytics-ready data
and loads it into a PostgreSQL analytical warehouse.

It demonstrates a practical ETL workflow using **Python, PySpark, SQL,
PostgreSQL, and pytest**, with Bronze--Silver--Gold data organization,
data-quality checks, dimensional modeling, reconciliation, query-plan
analysis, and reusable transformation code.

> **Current status:** Core batch processing, PostgreSQL warehouse
> loading, reconciliation checks, transformation logging, and six
> automated transformation tests have been implemented and validated.
> Incremental loading and automated orchestration are not currently
> implemented.

## Contents

-   [Problem statement](#problem-statement)
-   [Technology stack](#technology-stack)
-   [Dataset](#dataset)
-   [Architecture](#architecture)
-   [Implemented pipeline](#implemented-pipeline)
-   [Data-quality and transaction
    rules](#data-quality-and-transaction-rules)
-   [PostgreSQL warehouse](#postgresql-warehouse)
-   [Validation results](#validation-results)
-   [Query-plan benchmarking](#query-plan-benchmarking)
-   [Repository structure](#repository-structure)
-   [Run the tests](#run-the-tests)
-   [Reproduce the notebook workflow](#reproduce-the-notebook-workflow)
-   [Limitations and next steps](#limitations-and-next-steps)

## Problem statement

Retail transaction data can contain missing customer identifiers and
product descriptions, repeated records, cancellations, returns, invalid
values, and inconsistent source formats. Loading it directly into
reporting tables can produce misleading results.

RetailPulse processes the source data through explicit transformation
and validation steps, preserves data-quality information, and loads the
resulting records into a relational warehouse for SQL analysis.

## Technology stack

  -----------------------------------------------------------------------
  Technology                          Role
  ----------------------------------- -----------------------------------
  Python                              Pipeline logic, reusable
                                      transformations, logging, and tests

  PySpark                             Data standardization,
                                      classification, validation, and
                                      aggregation

  PostgreSQL                          Relational analytical warehouse

  SQL                                 Warehouse validation, analytical
                                      queries, and query-plan inspection

  Parquet and CSV                     Intermediate and analytical data
                                      storage

  pytest                              Automated transformation tests

  Git and GitHub                      Version control and project
                                      documentation

  Google Colab                        Development and execution
                                      environment
  -----------------------------------------------------------------------

## Dataset

**Source:** [UCI Online Retail
II](https://archive.ics.uci.edu/dataset/502/online+retail+ii)

The workbook contains two worksheets: `Year 2009-2010` and
`Year 2010-2011`. The pipeline ingested **1,067,371 rows** across both
worksheets. Relevant fields include invoice, stock code, product
description, quantity, invoice date, unit price, customer ID, and
country. The source worksheet is retained as lineage information.

The source dataset and generated data outputs are not committed to this
repository. Download the dataset from UCI when reproducing the full
workflow.

## Architecture

``` text
UCI Online Retail II workbook
             |
             v
      Python ingestion
             |
             v
       Bronze layer
   Source rows / Parquet
             |
             v
       Silver layer
 Standardization, transaction
 classification, quality flags
             |
       +-----+------+
       |            |
       v            v
  Valid records  Rejected records
       |
       v
        Gold layer
  Line values and analytical
  monthly/product/country summaries
       |
       v
   PostgreSQL warehouse
  Fact table + dimensions
       |
       v
    SQL analytics and
   query-plan experiments
```

The architecture follows the Bronze--Silver--Gold pattern. In the
validated run, all records passed the structural validation rules, so
the rejected dataset contained zero rows. Quality issues such as missing
customer IDs and descriptions remain represented as flags rather than
being silently discarded.

## Implemented pipeline

### 1. Ingestion and Bronze

-   Read both worksheets from the Online Retail II workbook.
-   Stream worksheet rows to CSV with a `SourceSheet` field.
-   Convert Bronze data to Parquet for downstream Spark processing.
-   Preserve source-sheet lineage.

### 2. Silver transformations

Reusable functions are implemented in
`src/retailpulse/transformations.py`:

-   `standardize_columns()` standardizes column names, trims text, and
    casts data types.
-   `classify_transactions()` assigns a transaction category and
    attaches data-quality flags.
-   `split_valid_and_rejected()` separates structural rejects from
    accepted records.
-   `transform_bronze_to_silver()` composes the transformation steps and
    logs processing counts and errors.

Malformed quantities are handled with a tolerant cast so invalid values
can be identified and rejected through validation instead of
unexpectedly terminating the transformation.

### 3. Gold outputs

The Gold layer calculates transaction line value as:

`line_value = quantity × unit_price`

It also derives transaction date and calendar fields used for monthly
analysis. Analytical summaries are saved for monthly activity, product
performance, and country-level activity.

**Interpretation note:** line value is a signed transaction measure, not
automatically equivalent to recognized net revenue or profit.
Cancellation, return, and accounting-adjustment treatment must be
considered when interpreting business metrics.

### 4. PostgreSQL warehouse

The PostgreSQL warehouse uses a star-schema design with one fact table
and four dimensions. The warehouse is loaded in batches, and the
fact-table full-refresh operation runs inside a transaction so that
failures can be rolled back.

### 5. Logging and error handling

`src/retailpulse/logging_config.py` configures console and file logging.
The transformation pipeline logs its start, classified/accepted/rejected
counts, completion, and exceptions. The default log file is
`logs/pipeline.log`; generated logs are excluded from Git.

## Data-quality and transaction rules

The current transaction-classification precedence is:

1.  `ACCOUNTING_ADJUSTMENT`: invoice starts with `A`, stock code equals
    `B`, or description contains `adjust bad debt`.
2.  `CANCELLATION_OR_CREDIT`: invoice starts with `C`, unless a
    preceding accounting-adjustment rule matched.
3.  `RETURN_OR_NEGATIVE_QUANTITY`: quantity is negative, unless a
    preceding rule matched.
4.  `STANDARD_TRANSACTION`: all other records.

Structural rejection conditions are missing invoice, missing stock code,
invalid quantity, invalid invoice date, invalid unit price, or missing
country.

Missing customer IDs, missing descriptions, zero unit prices, and
negative unit prices are captured as quality flags; they do not alone
trigger rejection. Repeated business records were investigated, but
records were not automatically deduplicated.

These rules are project-specific classifications, not a guarantee that
every source transaction has been assigned its final accounting meaning.

## PostgreSQL warehouse

### Schema

  Table            Purpose
  ---------------- ----------------------------------------------------
  `fact_sales`     Transaction-level measures and foreign keys
  `dim_product`    Product identifier and description
  `dim_customer`   Customer identifier
  `dim_date`       Calendar date, month, quarter, and year attributes
  `dim_country`    Country identifier and name

Indexes were created for common fact-table lookup and filtering columns,
including date, product, customer, and invoice.

### Loaded row counts

  Table                   Rows
  ---------------- -----------
  `fact_sales`       1,067,371
  `dim_product`          5,304
  `dim_customer`         5,942
  `dim_country`             43
  `dim_date`               604

Customer IDs missing from the source remain unmatched to a customer
dimension row, while the fact record itself is retained with a nullable
customer foreign key.

## Validation results

The completed run was checked for row-count consistency, required-field
nulls, dimensional referential integrity, line-value calculations, and
date-key consistency.

  Check                                             Result
  ------------------------------------------------- -----------
  Classified records                                1,067,371
  Accepted records                                  1,067,371
  Rejected records                                  0
  Fact-table records                                1,067,371
  Distinct invoices                                 53,628
  Fact rows without a known customer                243,007
  Orphaned dimension foreign keys                   0
  Incorrect line-value calculations                 0
  Invoice/date-dimension mismatches                 0
  Silver-to-warehouse signed-value reconciliation   Passed

The signed total of `line_value` reconciled to **19,287,250.5680**
across Silver and the warehouse in the validated run. This is a signed
total across classified source transactions; it should not be presented
as audited net revenue.

### Transaction classifications

  Transaction type                           Rows
  ------------------------------- ---------------
  `STANDARD_TRANSACTION`                1,044,414
  `CANCELLATION_OR_CREDIT`                 19,494
  `RETURN_OR_NEGATIVE_QUANTITY`             3,457
  `ACCOUNTING_ADJUSTMENT`                       6
  **Total**                         **1,067,371**

All ten warehouse validation checks in the final validation cell passed,
including row-count and signed-value reconciliation. A transaction
rollback test also confirmed that a failed insert did not leave partial
rows in the test transaction.

## Query-plan benchmarking

A monthly product aggregation query against `fact_sales` was inspected
with PostgreSQL `EXPLAIN (ANALYZE, BUFFERS)`. The query filters December
2010 records and aggregates results by product.

Six runs were measured for each configuration:

  Metric                     Default planner   Bitmap-preferred diagnostic configuration
  ------------------------ ----------------- -------------------------------------------
  Access method observed          Index Scan                            Bitmap Heap Scan
  Median execution time           158.902 ms                                  142.706 ms
  Minimum execution time           80.027 ms                                  106.615 ms
  Maximum execution time          203.122 ms                                  163.804 ms

The bitmap-preferred configuration had a median approximately **10.2%
lower** in this experiment. However, the default planner produced the
fastest individual run. These are observations from a limited benchmark
in a shared Colab environment, not a general guarantee of improved
performance. The diagnostic planner settings were used for comparison;
PostgreSQL's normal planner remains the default.

## Repository structure

``` text
Retail-Pulse/
├── .gitignore
├── README.md
├── LICENSE
├── src/
│   └── retailpulse/
│       ├── __init__.py
│       ├── logging_config.py
│       └── transformations.py
└── tests/
    └── test_transformations.py
```

The working Colab environment also contains data-layer outputs, logs,
and the notebook. Large raw data, generated Parquet/CSV outputs, logs,
caches, and environment files are excluded from Git by `.gitignore`. Add
the notebook to the repository separately if you want it directly
accessible from GitHub.

## Run the tests

The transformation tests were run successfully in Google Colab with
Python 3.13, pytest 8.4.2, and PySpark 4.0.4.

From the repository root, install compatible versions of PySpark and
pytest in your Python environment, then run:

``` bash
PYTHONPATH=src python -m pytest -v tests/test_transformations.py
```

**Latest verified result:** `6 passed`.

The tests cover column standardization and data types,
transaction-classification precedence, quality flags, invalid-quantity
rejection, row-count reconciliation, and blank-text normalization.

## Reproduce the notebook workflow

The full ingestion, Gold aggregation, and PostgreSQL loading workflow
was developed in Google Colab.

1.  Download Online Retail II from the [official UCI dataset
    page](https://archive.ics.uci.edu/dataset/502/online+retail+ii).
2.  Open the [Google Colab
    notebook](https://colab.research.google.com/drive/1oSmk3IdYQYZrZBkvd0srynXZGVkoeIT0?usp=sharing).
3.  Place the source workbook in the `data/raw/` location expected by
    the notebook.
4.  Execute the notebook cells in order to ingest the worksheets, build
    Bronze/Silver/Gold outputs, and run validation.
5.  Start PostgreSQL and create the warehouse schema using the SQL
    definitions in the notebook.
6.  Load the warehouse and run the reconciliation and analytics queries.
7.  Run the automated transformation tests using the command above.

The current repository does not yet include a complete standalone
pipeline entry point, dependency lockfile, or all SQL scripts as
separate files. The notebook is the reference for reproducing the
end-to-end warehouse workflow.

## Limitations and next steps

The current implementation is a working batch-processing portfolio
project, not yet a scheduled production deployment.

Potential improvements include:

-   Add a documented dependency file and standalone command-line
    pipeline entry point.
-   Add warehouse schema and analytics SQL as version-controlled `.sql`
    files.
-   Add CI to run transformation tests on every pull request.
-   Implement incremental ingestion with a defined watermark or
    change-detection strategy. The current fact loader uses a
    transactional full refresh.
-   Expand automated tests for warehouse loading, failure recovery, and
    data-quality edge cases.
-   Add orchestration and scheduling, for example with Apache Airflow.
-   Add a dashboard for monthly trends, product activity, and quality
    metrics.
-   Explore a managed Spark environment or cloud object storage.

## Relevance to Data Engineer I

  -----------------------------------------------------------------------
  Skill                               Evidence in RetailPulse
  ----------------------------------- -----------------------------------
  Python                              Reusable transformations, logging,
                                      error handling, automated tests

  PySpark                             Standardization, classification,
                                      validation, and aggregations

  SQL                                 Warehouse validation and analytical
                                      queries

  Data warehousing                    Fact/dimension model and
                                      foreign-key relationships

  Data quality                        Structural validation, quality
                                      flags, reconciliation

  PostgreSQL                          Batched loading, transactional full
                                      refresh, indexes

  Performance analysis                Query-plan inspection and repeated
                                      timing measurements

  Git                                 Source control and documented
                                      project structure
  -----------------------------------------------------------------------

------------------------------------------------------------------------

*RetailPulse is a learning and portfolio project built with the public
UCI Online Retail II dataset. The reported counts and timings describe
the validated project run and benchmark; results may vary when the data,
runtime, or environment changes.*