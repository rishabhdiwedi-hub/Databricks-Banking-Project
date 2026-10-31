# Databricks Banking Project

A metadata-driven Lakehouse implementation built on Databricks for a retail banking data platform. The project ingests customer, account, transaction, branch, bureau, and payment gateway data from source systems into a layered Delta Lake architecture and exposes curated analytical datasets for banking analytics and operational reporting.

## Overview

This repository implements a modern data engineering pattern for banking data processing using Databricks SQL, PySpark, Delta Lake, and notebook-based orchestration. It follows the medallion architecture pattern:

- Bronze: raw landing layer for source data
- Silver: cleaned and standardized business data
- Gold: business-ready analytical datasets for reporting and decisioning

The design is driven by a metadata catalog (`banking.metadata`) that stores:

- logical table definitions
- load parameters (MERGE / APPEND / FULL)
- primary keys and watermark columns
- pipeline audit metadata and run status

This makes the ingestion logic reusable and scalable across multiple source tables.

## Business Use Case

The solution models a banking data platform that supports:

- customer 360 and relationship analytics
- branch performance monitoring
- payment and transaction channel analysis
- daily KPI reporting
- risk and credit exposure monitoring
- historical and incremental data loading with auditability

## Technical Stack

- Databricks Lakehouse
- Apache Spark / PySpark
- Delta Lake
- SQL (Databricks SQL)
- Python for notebook automation
- Azure Blob Storage / Databricks Volumes for file-based source ingestion
- SQL Server as a source system
- Notebook-driven orchestration via `dbutils.notebook.run()`

## Architecture

The project follows a metadata-driven Lakehouse pipeline:

```text
┌──────────────────────┐      ┌──────────────────────┐
│ Source Systems       │      │ File-Based Sources   │
│ ─────────────────── │      │ ─────────────────── │
│ SQL Server           │      │ Blob CSV Files       │
│ customers            │      │ credit_bureau_reports│
│ accounts             │      │ payment_gateway_logs│
│ transactions         │      │                      │
│ branches             │      │                      │
└───────────┬──────────┘      └──────────┬───────────┘
            │                             │
            │                             │
            ▼                             ▼
   ┌────────────────────────────────────────────────────┐
   │ Databricks Source Landing / Volume Layer          │
   │ banking.source.volume + secret scope config       │
   └──────────────────────┬─────────────────────────────┘
                          │
                          ▼
   ┌────────────────────────────────────────────────────┐
   │ Metadata Registry                                  │
   │ banking.metadata.tables                            │
   │ banking.metadata.table_parameters                  │
   │ banking.metadata.table_watermark                   │
   │ banking.metadata.pipeline_runs                     │
   └──────────────────────┬─────────────────────────────┘
                          │
                          ▼
   ┌────────────────────────────────────────────────────┐
   │ Bronze Layer (raw ingestion)                       │
   │ banking.bronze.*                                   │
   │ raw source tables without heavy transformation      │
   └──────────────────────┬─────────────────────────────┘
                          │
                          ▼
   ┌────────────────────────────────────────────────────┐
   │ Silver Layer (cleaned and standardized)            │
   │ banking.silver.*                                   │
   │ deduplication, schema consistency, merge logic     │
   └──────────────────────┬─────────────────────────────┘
                          │
                          ▼
   ┌────────────────────────────────────────────────────┐
   │ Gold Layer (curated business entities)            │
   │ banking.gold.*                                     │
   │ customer_360, branch_performance, daily_bank_kpi, │
   │ transaction_channel_summary, risk_customer_summary │
   └──────────────────────┬─────────────────────────────┘
                          │
                          ▼
   ┌────────────────────────────────────────────────────┐
   │ Analytics / Reporting / BI                        │
   │ dashboards, operational KPIs, customer insights   │
   └────────────────────────────────────────────────────┘
```

## Metadata-Driven Design

The project uses `banking.metadata` as the control plane for the pipeline. The key metadata objects are:

- `banking.metadata.tables`
  - logical table registry
  - source system and target layer mapping
  - active/inactive table flags
  - load ordering

- `banking.metadata.table_parameters`
  - `load_type` (`MERGE`, `APPEND`, `FULL`)
  - `primary_key`
  - `watermark_column`

- `banking.metadata.table_watermark`
  - stores last processed high-watermark to enable incremental loading

- `banking.metadata.pipeline_runs`
  - run_id, table_id, layer, status, start/end time, record count, error_message

This audit trail makes the process robust, operational, and easier to troubleshoot.

## Repository Structure

```text
Databricks-Banking-Project/
├── 01_SetUp_Metadata/
│   ├── 01_SetUp_Metadata.ipynb
│   └── 02_Check_Metadata.ipynb
├── 02_Source_to_Silver/
│   ├── 00_SetUp_Secret_Scope.ipynb
│   ├── 01_Read_Tables_List.ipynb
│   ├── 02_Read_Table_Parameters.ipynb
│   ├── 03_Source_to_Bronze.ipynb
│   └── 04_Bronze_to_Silver.ipynb
├── 03_Silver_to_Gold/
│   ├── 01_Silver_to_Gold_Driver.py
│   └── gold_transformation/
│       ├── branch_performance.ipynb
│       ├── customer_360.ipynb
│       ├── daily_bank_kpi.ipynb
│       ├── risk_customer_summary.ipynb
│       └── transaction_channel_summary.ipynb
├── README.md
└── READ.md
```

## Pipeline Stages

### 1. Metadata Setup

The `01_SetUp_Metadata` stage creates the Databricks catalog and schemas used across the implementation:

- `banking`
- `banking.metadata`
- `banking.source`
- `banking.bronze`
- `banking.silver`
- `banking.gold`

This also initializes the metadata registry tables and source volumes.

### 2. Source to Bronze

The source layer includes:

- SQL Server tables:
  - `customers`
  - `accounts`
  - `transactions`
  - `branches`
- Blob-based source files:
  - `credit_bureau_reports`
  - `payment_gateway_logs`

The ingestion workflow reads source metadata and brings raw records into the bronze layer with minimal transformation. This step preserves raw data for reprocessing and lineage tracking.

### 3. Bronze to Silver

The `04_Bronze_to_Silver.ipynb` notebook is the core transformation step. It uses Delta Lake operations to:

- read from bronze tables
- apply incremental filtering based on watermark columns
- append or merge records depending on the table load type
- create Silver tables if they do not exist
- update watermark metadata
- record execution details in `pipeline_runs`

It handles:

- `MERGE` for slowly changing customer/account/entity tables
- `APPEND` for transaction log tables
- `FULL` loads for dimension-style datasets such as branches

### 4. Silver to Gold

The gold layer is implemented by a driver script and a set of transformation notebooks. The driver script loads metadata from the pipeline control tables and then calls the appropriate notebook for each gold table.

`03_Silver_to_Gold/01_Silver_to_Gold_Driver.py` performs:

- reads the target metadata for a specific table
- creates the `banking.gold` schema if needed
- updates `banking.metadata.pipeline_runs`
- executes the transformation notebook in the `gold_transformation` folder
- captures status, record counts, and errors

## Gold Layer Business Tables

The project creates a set of curated analytical tables:

### customer_360

Creates a customer-level 360-degree profile by combining:

- customer master data
- account counts and balances
- transaction counts and amounts
- latest credit bureau record
- branch mapping
- customer segmentation based on balance thresholds

Key fields include:

- `customer_id`
- `customer_name`
- `branch_name`
- `total_accounts`
- `total_balance`
- `total_transactions`
- `total_transaction_amount`
- `credit_score`
- `risk_grade`
- `customer_segment`

### branch_performance

Aggregates branch-level operational and financial performance using product and customer behavior in silver tables.

### transaction_channel_summary

Summarizes payment and transaction behavior by channel, helping measure channel adoption, transaction volumes, and value breakdowns.

### daily_bank_kpi

Builds executive KPI views such as:

- daily transaction volume
- daily transaction value
- account movement metrics
- operational health indicators for the bank

### risk_customer_summary

Produces a risk-focused customer view using credit bureau and account behavior to prioritize customer risk analysis.

## Data Flow Example

A typical pipeline run for a customer table follows this flow:

1. Source metadata is defined in `banking.metadata.tables` and `banking.metadata.table_parameters`
2. Initial metadata catalog and volumes are created in `01_SetUp_Metadata`
3. Source data is ingested into bronze storage
4. Bronze-to-Silver processing reads the latest watermark and applies incremental logic
5. Delta tables are updated in `banking.silver.*`
6. Gold transformation notebooks produce curated analytical tables
7. Pipeline run metadata is logged in `banking.metadata.pipeline_runs` for auditability

## Key Implementation Patterns

### Incremental Loading

The solution uses watermarks to process only new or changed records for append/merge operations. This minimizes reprocessing cost and supports near-real-time or scheduled incremental pipelines.

### Delta Lake Reliability

Using Delta tables ensures:

- ACID transactions
- schema evolution support
- time travel capability
- reliable pipeline execution

### Operational Auditing

Each execution writes run-status metrics to `pipeline_runs`, allowing the team to:

- track pipeline health
- identify failures quickly
- monitor record counts
- debug ingestion and transformation delays

### Reusable Notebook Orchestration

The project relies on a common pipeline pattern where each table has:

- metadata entry
- load configuration
- transformation logic
- run logging

This encourages a modular, scalable data platform design.

## How to Use

### Prerequisites

- Databricks workspace access
- SQL Server connectivity configured for source tables
- Blob storage or Databricks volumes available for file landing
- Secret scope or environment credentials for source access

### Typical Execution Sequence

1. Run `01_SetUp_Metadata.ipynb` to create catalogs, schemas, and metadata tables
2. Run `00_SetUp_Secret_Scope.ipynb` to configure access to external sources
3. Run source ingestion notebooks in `02_Source_to_Silver`
4. Execute the bronze-to-silver notebook for each source table
5. Run the gold driver notebook or orchestrated gold process
6. Validate outputs in `banking.gold.*`

## Benefits of the Architecture

- scalable and modular Lakehouse pattern
- metadata-driven processing for operational reuse
- strong lineage and traceability
- incremental processing reduces compute costs
- gold layer supports both reporting and advanced analytics
- clear separation between raw, curated, and analytical layers

## Summary

This repository demonstrates a complete Databricks banking data platform pattern built around Delta Lake, metadata-driven orchestration, and layered analytics. It is designed to support enterprise-grade data engineering practices while producing high-value business datasets for customer analytics, performance monitoring, and risk analysis.

The project is especially valuable as a reference for:

- Lakehouse architecture implementation
- banking data engineering
- Databricks medallion architecture
- metadata-driven ELT pipelines
- end-to-end data platform automation

