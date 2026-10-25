# System Architecture: Kenya National Retail and Market Intelligence Platform

## 1. High-Level Architecture Overview

The platform bridges macro-economic public market intelligence with granular micro-retail operations using an end-to-end Medallion (Bronze -> Silver -> Gold) data architecture supported by real-time Kafka streaming, Spark-compatible batch processing, PostgreSQL warehousing, and Apache Airflow orchestration.

```mermaid
flowchart TD
    subgraph DataSources ["1. Data Sources"]
        KAMIS["KAMIS (Agricultural Market Info System)<br/>Wholesale & Retail Commodity Prices"]
        KNBS["KNBS (Kenya National Bureau of Statistics)<br/>CPI & Inflation Indices (Base 2019=100)"]
        OP_GEN["Retail Chain Operational Systems<br/>15 Stores | 25 SKUs | 20 Suppliers"]
    end

    subgraph StreamingLayer ["2. Real-Time Streaming (Kafka)"]
        POS_STREAM["retail.pos.transactions (6 Partitions)"]
        INV_STREAM["retail.inventory.movements (4 Partitions)"]
        ALERT_STREAM["market.price.alerts (3 Partitions)"]
        DLQ_STREAM["retail.dlq.errors (Dead Letter Queue)"]
    end

    subgraph BronzeLayer ["3. Medallion Bronze Layer (Immutable Staging)"]
        B_KAMIS["bronze_kamis.parquet"]
        B_KNBS["bronze_knbs.parquet"]
        B_POS["bronze_pos_sales.parquet"]
        B_INV["bronze_inventory.parquet"]
        B_STREAM["stream_consumed.parquet"]
    end

    subgraph SilverLayer ["4. Medallion Silver Layer (Curated & Enriched)"]
        S_CLEAN["- Deduplication & Schema Enforcement<br/>- Unit Normalisation (per 1 KG / 1 L)<br/>- Currency Validation (KES only)"]
        S_DEFLATE["- KNBS Real Revenue Deflation (Base 2019)<br/>- KAMIS County Price Spread & Markup Calc"]
        S_TABLES["silver_pos_sales.parquet<br/>silver_kamis_prices.parquet<br/>silver_inventory.parquet"]
    end

    subgraph GoldLayer ["5. Medallion Gold Layer (Analytical Marts)"]
        G_SALES["gold_daily_store_sales"]
        G_PRICE["gold_price_competitiveness"]
        G_PROD["gold_product_profitability"]
        G_RISK["gold_inventory_risk"]
    end

    subgraph Warehouse ["6. PostgreSQL Dimensional Star Schema"]
        DIM_DATE["dim_date"]
        DIM_STORE["dim_store"]
        DIM_PROD["dim_product"]
        DIM_SUPP["dim_supplier"]
        DIM_MKT["dim_kamis_market"]
        DIM_CPI["dim_knbs_cpi"]
        FACT_SALES["fact_pos_sales"]
        FACT_INV["fact_inventory_snapshot"]
        FACT_KAMIS["fact_kamis_market_price"]
        FACT_INTEL["fact_price_intelligence"]
    end

    subgraph Governance ["7. Governance & Serving"]
        AIRFLOW["Airflow DAG (@daily / @monthly)"]
        DQ_GATE["8 Automated Assertions (DataQualityEngine)"]
        AUDIT_LOG["Audit Ledger (data_quality_audit_log)"]
        DASHBOARD["Streamlit Analytics Dashboard (:8501)"]
    end

    KAMIS --> B_KAMIS
    KNBS --> B_KNBS
    OP_GEN --> POS_STREAM
    OP_GEN --> INV_STREAM
    POS_STREAM --> B_STREAM
    INV_STREAM --> B_STREAM
    OP_GEN --> B_POS
    OP_GEN --> B_INV

    B_KAMIS --> S_CLEAN
    B_KNBS --> S_CLEAN
    B_POS --> S_CLEAN
    B_INV --> S_CLEAN
    B_STREAM --> S_CLEAN
    S_CLEAN --> S_DEFLATE --> S_TABLES

    S_TABLES --> G_SALES
    S_TABLES --> G_PRICE
    S_TABLES --> G_PROD
    S_TABLES --> G_RISK

    G_SALES --> FACT_SALES
    G_PRICE --> FACT_INTEL
    S_TABLES --> FACT_INV
    S_TABLES --> FACT_KAMIS

    FACT_SALES --> DQ_GATE
    FACT_INTEL --> DQ_GATE
    DQ_GATE --> AUDIT_LOG
    FACT_SALES --> DASHBOARD
    FACT_INTEL --> DASHBOARD
    AIRFLOW -.-> S_CLEAN
    AIRFLOW -.-> DQ_GATE
```

---

## 2. Medallion Layer Responsibilities

1. **Bronze Layer (`data/bronze/`):**
   - Preserves raw source data in immutable, partitioned Parquet format.
   - Appends ingestion lineage metadata (`_bronze_ingested_at`, `_source_file`).
   - Acts as replayable storage for backfills and auditing.

2. **Silver Layer (`data/silver/`):**
   - Cleans and deduplicates records (removing redundant `transaction_id`s).
   - Standardises disparate trading units (divides wholesale 90kg/50kg bags by respective divisors into canonical base units: 1 KG / 1 L).
   - Deflates nominal sales to real 2019 constant prices using KNBS Food CPI index.
   - Enriches sales with prevailing county KAMIS market price benchmarks.

3. **Gold Layer (`data/gold/`):**
   - Pre-aggregates dimensional metrics: Daily store sales, county price competitiveness indexes, product margin rankings, and inventory stockout risk.
   - Direct feeder into the PostgreSQL Kimball dimensional model.
