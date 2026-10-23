# RetailPulse — End-to-End Retail Data Engineering Pipeline

**RetailPulse** is an end-to-end data engineering project designed to transform raw retail transaction data into clean, reliable, and analytics-ready datasets.

The project demonstrates how SQL, Python, and PySpark can be used to build an ETL pipeline that handles data ingestion, cleaning, validation, transformation, and loading into a relational data warehouse.

The goal is to simulate a practical data engineering workflow used in real-world businesses, where data from different sources must be processed consistently before it can be used for reporting and decision-making.

## 1. Problem Statement

Retail businesses generate large volumes of transaction data containing information about invoices, products, customers, quantities, prices, and transaction dates.

However, raw data is not always ready for analysis. It may contain:

- Missing customer information or product descriptions.
- Duplicate transaction records.
- Invalid quantities, prices, or timestamps.
- Cancelled orders and returned products.
- Inconsistent data types and formatting.

If this data is loaded directly into a reporting system, it can lead to inaccurate sales metrics and unreliable business decisions.

RetailPulse addresses this problem by building a repeatable data pipeline that processes raw transaction data, applies data quality checks, and stores validated information in a structured analytical warehouse.

## 2. Project Objectives

- Build an end-to-end ETL pipeline using Python and PySpark.
- Process structured retail transaction data.
- Clean, validate, and transform raw records.
- Implement data quality checks and duplicate detection.
- Design a relational data warehouse using dimensional modeling.
- Use SQL to answer business questions and generate analytical insights.
- Implement incremental loading and prevent duplicate processing.
- Evaluate query and transformation performance.
- Maintain readable, testable, and version-controlled code.

## 3. Technology Stack

| Technology | Purpose |
|---|---|
| Python | Pipeline logic, ingestion, validation, and error handling |
| PySpark | Data transformation, filtering, joins, and aggregations |
| SQL | Data analysis, warehouse queries, and data validation |
| PostgreSQL | Relational database and analytical data warehouse |
| CSV / JSON / Parquet | Data ingestion and intermediate storage formats |
| Git and GitHub | Version control and project documentation |
| Pytest | Automated testing of transformations and data quality |
| Docker Compose (optional) | Reproducible local environment |

The project will initially run locally. Cloud platforms such as Databricks and Snowflake can be explored as future extensions after the core pipeline is working.

## 4. Dataset

The project will use the **UCI Online Retail II** dataset.

Dataset: https://archive.ics.uci.edu/dataset/502/online+retail+ii

The dataset contains retail transaction records covering approximately two years of online sales activity.

Important fields include:

- `Invoice`: Invoice or transaction identifier.
- `StockCode`: Product identifier.
- `Description`: Product description.
- `Quantity`: Number of units purchased or returned.
- `InvoiceDate`: Transaction timestamp.
- `Price`: Unit price.
- `Customer ID`: Customer identifier.
- `Country`: Customer's country.

The dataset is suitable for practising data cleaning, transaction processing, SQL analytics, and PySpark transformations on a realistic dataset.

## 5. Application Architecture and Data Flow

RetailPulse follows a simplified **Bronze–Silver–Gold architecture**, also known as the Medallion Architecture.

```text
             UCI Online Retail II Dataset
                         |
                         v
                 1. Data Ingestion
                  Python + PySpark
                         |
                         v
                 2. Bronze Layer
                Raw Data Preservation
                         |
                         v
                3. Silver Layer
              Cleaning and Validation
                         |
             +-----------+-----------+
             |                       |
             v                       v
       Valid Records           Rejected Records
             |                 Quality Reports
             v
                 4. Gold Layer
             Business Transformations
                         |
                         v
                 5. PostgreSQL
                Data Warehouse
                         |
                         v
                 6. SQL Analytics
            Revenue and Product Insights
```

### Step 1: Data Ingestion

The pipeline reads the downloaded retail dataset using Python and PySpark.

During ingestion, it will:

- Load the source file.
- Validate expected column names and data types.
- Record the batch identifier and ingestion timestamp.
- Capture the number of records received.
- Preserve the original input for traceability.

The original dataset will remain unchanged so that later processing can be audited and repeated.

### Step 2: Bronze Layer — Raw Data

The Bronze layer stores the ingested data with minimal modification.

Its purpose is to maintain a recoverable copy of the source records and provide a consistent starting point for subsequent processing.

The raw data can be stored in CSV or converted to Parquet for more efficient downstream processing.

### Step 3: Silver Layer — Cleaning and Validation

PySpark processes the raw data and applies data quality rules.

Examples include:

- Standardizing column names and data types.
- Handling missing values.
- Identifying duplicate records.
- Validating quantities, prices, and timestamps.
- Identifying cancellation transactions and returns.
- Separating invalid records for further investigation.

Records that pass validation continue to the next stage. Rejected records are retained separately with rejection reasons rather than being silently discarded.

Data quality metrics, such as accepted records, rejected records, and duplicate counts, will be recorded for each batch.

### Step 4: Gold Layer — Business-Ready Data

The Gold layer contains cleaned and transformed data prepared for analytical use.

Business transformations may include:

- Calculating transaction line values.
- Aggregating sales by month.
- Summarizing product performance.
- Calculating customer-level spending.
- Analysing country-level sales.
- Tracking cancellation and return activity.

Revenue calculations will distinguish ordinary sales from cancellations and returns so that business metrics remain meaningful.

### Step 5: PostgreSQL Data Warehouse

The processed data will be loaded into PostgreSQL using a dimensional data model.

The initial warehouse will use a simple star schema.

**Fact table: `fact_sales`**

Stores transaction-level measures and references to relevant dimensions.

Potential fields include:

- Invoice identifier.
- Product, customer, and date keys.
- Quantity.
- Unit price.
- Line value.

**Dimension tables:**

- `dim_product` — Product identifiers and descriptions.
- `dim_customer` — Customer identifiers.
- `dim_date` — Calendar attributes for time-based analysis.
- `dim_country` — Geographic information.

Appropriate primary keys, foreign keys, data types, and uniqueness constraints will be implemented where applicable.

This model will make analytical queries easier to understand and maintain.

### Step 6: SQL Analytics

SQL will be used to answer practical retail business questions, including:

- What is the monthly sales trend?
- Which products generate the highest revenue?
- Which countries contribute the most to sales?
- Who are the highest-spending customers?
- Which products have the highest return or cancellation activity?
- How does sales performance change from one month to the next?

The project will demonstrate joins, aggregations, Common Table Expressions (CTEs), window functions, date operations, and query optimization.

### Step 7: Data Quality, Logging, and Reliability

The pipeline will include basic reliability mechanisms so that it behaves like a real data engineering application.

These will include:

- Record-count reconciliation between processing stages.
- Validation of required columns and data types.
- Duplicate detection and uniqueness checks.
- Logging of batch status and processing errors.
- Safe handling of rejected records.
- Idempotent loading so that rerunning a batch does not create duplicate warehouse records.
- Automated tests for transformation logic and data quality rules.

Incremental loading will be added after the initial full-load pipeline works correctly.

## 6. Repository Structure

The project will use a simple structure without excessive modularization.

```text
retailpulse/
│
├── data/
│   ├── raw/
│   ├── processed/
│   └── README.md
│
├── notebooks/
│   └── exploration.ipynb
│
├── src/
│   └── pipeline.py
│
├── sql/
│   ├── schema.sql
│   └── analytics.sql
│
├── tests/
│   └── test_pipeline.py
│
├── logs/
│
├── requirements.txt
├── .env.example
├── .gitignore
├── docker-compose.yml
└── README.md
```

### Directory Overview

- `data/raw/`: Original downloaded dataset.
- `data/processed/`: Intermediate or transformed data files.
- `notebooks/`: Initial data exploration and experimentation.
- `src/pipeline.py`: Main pipeline containing ingestion, cleaning, transformation, and loading logic.
- `sql/schema.sql`: Warehouse tables and constraints.
- `sql/analytics.sql`: Business analysis queries.
- `tests/test_pipeline.py`: Automated tests for core pipeline logic.
- `logs/`: Local pipeline execution logs.
- `requirements.txt`: Python dependencies.
- `.env.example`: Example configuration variables without real credentials.
- `docker-compose.yml`: Optional local service configuration.

As the project grows, `pipeline.py` can be split into smaller modules only when doing so improves readability or testing.

## 7. Step-by-Step Implementation Plan

The project will be developed incrementally, ensuring that every stage works before moving to the next.

### Phase 1: Environment Setup and Data Exploration

1. Create the GitHub repository.
2. Set up a Python virtual environment.
3. Install PySpark, PostgreSQL dependencies, pandas, and Pytest.
4. Download the UCI Online Retail II dataset.
5. Inspect its columns, data types, missing values, and duplicate records.
6. Write initial SQL queries to understand the data.

**Deliverable:** A working development environment and an initial understanding of the source data.

### Phase 2: Build the Initial ETL Pipeline

1. Read the source data using PySpark.
2. Validate the input schema.
3. Preserve the raw data in the Bronze layer.
4. Apply cleaning and validation rules.
5. Separate rejected records from valid records.
6. Generate initial data quality statistics.

**Deliverable:** A working raw-to-cleaned data pipeline.

### Phase 3: Implement Business Transformations

1. Calculate transaction line values.
2. Handle cancellation and return records according to documented rules.
3. Create product-level and customer-level summaries.
4. Generate monthly and geographic sales aggregations.
5. Save the transformed datasets in the Gold layer.

**Deliverable:** Analytics-ready data generated through PySpark.

### Phase 4: Build the SQL Data Warehouse

1. Create PostgreSQL database tables.
2. Implement the fact and dimension tables.
3. Define keys, relationships, and appropriate constraints.
4. Load the processed data into the warehouse.
5. Validate warehouse records against the transformed data.

**Deliverable:** A functioning analytical data warehouse.

### Phase 5: Implement Reliability and Incremental Loading

1. Add batch identifiers and execution logs.
2. Reconcile record counts across pipeline stages.
3. Implement safe reruns and duplicate prevention.
4. Add incremental processing using an appropriate record identifier or change-detection strategy.
5. Test failure scenarios and recovery behaviour.

**Deliverable:** A more reliable and repeatable ETL process.

### Phase 6: SQL Analytics and Performance Optimization

1. Write queries using joins, CTEs, aggregations, and window functions.
2. Analyse monthly revenue and product performance.
3. Examine query plans using PostgreSQL `EXPLAIN ANALYZE`.
4. Test suitable indexes.
5. Benchmark selected PySpark transformations and partitioning strategies.
6. Document actual performance results and trade-offs.

**Deliverable:** A collection of business queries and documented optimization experiments.

### Phase 7: Testing and Documentation

1. Add automated tests for data transformations.
2. Test schema validation and duplicate detection.
3. Validate data quality rules and rejected-record handling.
4. Document the architecture and execution steps.
5. Add sample outputs and benchmark results.
6. Verify that the project can be reproduced from a clean environment.

**Deliverable:** A documented, tested, and interview-ready GitHub repository.

## 8. Setup and Execution

The following commands illustrate the intended local setup. Exact dependencies and database configuration will be finalized during implementation.

### 1. Clone the repository

```bash
git clone <your-repository-url>
cd retailpulse
```

### 2. Create a virtual environment

```bash
python -m venv .venv
```

Activate it on Windows:

```bash
.venv\Scripts\activate
```

Activate it on Linux or macOS:

```bash
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure PostgreSQL

Create the local database and configure the connection using environment variables.

Keep real database credentials out of source control.

### 5. Prepare the dataset

Download Online Retail II from UCI and place the required source file in the location documented under `data/raw/`.

### 6. Create the warehouse schema

```bash
psql -U <username> -d <database> -f sql/schema.sql
```

### 7. Run the pipeline

```bash
python src/pipeline.py
```

### 8. Execute analytics queries

Run the queries in `sql/analytics.sql` against the populated PostgreSQL database.

### 9. Run tests

```bash
pytest -v
```

These commands describe the planned execution workflow; they should be updated if the implementation or database setup changes.

## 9. Expected Outcomes

Once implemented, RetailPulse will demonstrate the ability to:

- Build an ETL pipeline using Python and PySpark.
- Work with structured datasets and handle common data-quality problems.
- Design and populate a SQL-based analytical warehouse.
- Write SQL queries for business reporting.
- Implement basic data validation, reconciliation, and incremental processing.
- Debug pipeline failures and test data transformations.
- Evaluate performance using measurable evidence.
- Maintain a clean GitHub repository with reproducible instructions.

Performance improvements, processing times, and data-quality statistics will be documented after running the pipeline rather than being assumed in advance.

## 10. Future Enhancements

After completing the core implementation, the project may be extended with:

- **Databricks:** Run the PySpark pipeline on a managed Spark platform.
- **Snowflake:** Load Gold-layer data into a cloud data warehouse.
- **Apache Airflow:** Schedule and orchestrate pipeline execution.
- **Cloud storage:** Store raw and processed data in Amazon S3 or an equivalent service.
- **Data visualization:** Build a small dashboard for sales trends and data quality metrics.
- **CI/CD:** Run automated tests on GitHub whenever code changes.

These enhancements are optional. The initial focus remains on building a correct, reliable, and well-tested pipeline using SQL, Python, PySpark, and PostgreSQL.

## 11. Relevance to Data Engineer I

RetailPulse is designed to demonstrate skills directly relevant to an entry-level data engineering position.

| Job requirement | Project implementation |
|---|---|
| SQL proficiency | Analytical queries, joins, CTEs, window functions |
| PySpark | DataFrame transformations, joins, aggregations |
| ETL development | Ingestion, transformation, and warehouse loading |
| Data warehousing | Fact tables, dimension tables, star schema |
| Data quality | Validation, deduplication, reconciliation |
| Troubleshooting | Logging, error handling, recovery |
| Performance tuning | Query plans, indexing, Spark benchmarks |
| Python | Pipeline implementation and automated tests |
| Git | Version control and project documentation |

**Project philosophy:** Start simple, make the pipeline correct, and then improve its reliability and performance. Every added component should solve a specific data engineering problem rather than add unnecessary complexity.

---

*RetailPulse is a learning and portfolio project intended to demonstrate practical data engineering concepts using a public retail dataset.*