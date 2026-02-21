# OpenAQ Data Transformation (dbt)

This directory contains the dbt project responsible for transforming raw air quality data ingested from the OpenAQ API into a structured, analytical-ready format within Google BigQuery.

## Architecture (Medallion Pattern)

This project strictly follows the Medallion Architecture to ensure data quality, scalability, and performance:

* **Bronze (Raw Layer):** Raw NDJSON data ingested via Apache Airflow into BigQuery (`openAQ_raw`). Source of our dbt models.
* **Silver (Staging Layer):** Cleaned, deduplicated, and strongly-typed views (`openAQ_staging`).
  * **Surrogate Keys:** Generated using `dbt_utils` for robust historical tracking.
  * **Idempotency:** Implements `QUALIFY ROW_NUMBER()` logic to silently handle and remove duplicates generated during API extraction or Airflow re-runs.
  * **Data Quality:** Strict YAML-defined tests (`not_null`, `unique`) applied to critical fields.
* **Gold (Marts Layer):** *[Work in Progress]* Dimensional modeling (Star Schema) optimized for downstream BI tools like Looker Studio.

## Project Structure

```text
openaq_transform/
├── dbt_project.yml          # Main dbt configuration file
├── macros/
│   └── generate_schema_name.sql # Custom macro to enforce strict schema naming rules
└── models/
    ├── staging/             # Silver Layer (Data Cleansing & Standardization)
    │   ├── src_openaq.yml   # Source definitions (Bronze layer mapping)
    │   ├── staging.yml      # Data quality tests and documentation
    │   ├── stg_openaq__locations.sql
    │   ├── stg_openaq__sensors.sql
    │   └── stg_openaq__measurements.sql
    ├── intermediate/        # View models for complex joins (Coming Soon)
    └── marts/               # Gold Layer / Business logic (Coming Soon)
```

## Quick Start

### 1. Prerequisites

Ensure you have dbt-bigquery installed.

A valid Google Cloud Service Account with BigQuery Data Editor and Job User roles.

Your ~/.dbt/profiles.yml correctly configured to point to the spicific region where your dataset is located.

### 2. Install Dependencies

This project uses external dbt packages (e.g., dbt_utils). Download them first:

```Bash
dbt deps
```

### 3. Run and Test

To compile the models, materialize the views in BigQuery, and execute all data quality tests in a single run:

```Bash
dbt build
```

To run and test a specific model (example: stg_openaq__measurements model):

```Bash
dbt build --select stg_openaq__measurements
```
