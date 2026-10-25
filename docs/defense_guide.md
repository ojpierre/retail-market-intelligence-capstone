# Capstone Technical Defense Guide & Oral Examination Script

**Project Title:** Kenya National Retail and Market Intelligence Data Engineering Platform  
**Candidate/Student:** Pierre O.  
**Cohort:** 7 - Data Engineering Capstone  
**Pass Mark:** 80 / 100 | **Target Standard:** 96%+  

This document provides a scripted technical defense handbook covering all 9 defense topics evaluated during the individual technical defense.

---

## 1. Complete Architecture

### Evaluator Question 1.1: Walk me through your end-to-end architecture and explain the data flow.
**Candidate Response:**
> "The platform follows an end-to-end Medallion (Bronze -> Silver -> Gold) data lakehouse architecture connected to a Kimball Star Schema data warehouse:
> 1. **Ingestion & Streaming:** Monthly batch feeds from KAMIS (agricultural commodity prices) and KNBS (CPI inflation index) land in `data/raw/` with SHA-256 idempotency. Simultaneously, operational retail data—stores, products, suppliers, POS sales, and inventory movements—is streamed in real-time through an Apache Kafka cluster across dedicated topics.
> 2. **Medallion Bronze Layer:** Captures raw immutable records in partitioned Parquet format, preserving ingestion timestamps and source file lineage.
> 3. **Medallion Silver Layer:** Performs deduplication, unit standardisation (converting 90kg/50kg bags and 20L cans to standard 1 KG/1 L units), joins POS transactions with products and stores, deflates sales using KNBS Food CPI, and computes price spreads against local KAMIS county market prices.
> 4. **Medallion Gold Layer & Warehouse:** Builds analytical marts and populates a PostgreSQL Star Schema (6 conformed dimensions and 4 fact tables).
> 5. **Governance & Serving:** Automated by an Apache Airflow DAG (`@daily`), monitored by an 8-check Data Quality Engine logging to `data_quality_audit_log`, and visualised via an interactive Streamlit and Plotly analytics dashboard."

---

## 2. Batch Ingestion Pipelines

### Evaluator Question 2.1: How does your ingestion handle network drops, source shifts, and re-runnability?
**Candidate Response:**
> "Our ingestion module (`src/ingestion/`) implements three core principles:
> 1. **SHA-256 Cryptographic Idempotency:** Before landing raw files, it verifies the content hash. If the incoming payload matches the existing hash, redundant network downloads are skipped.
> 2. **Hive-Style Directory Partitioning:** Raw records land in `data/raw/<source>/year=YYYY/month=MM/`, enabling fast partition pruning during downstream batch reading.
> 3. **Graceful Fallback:** If live public endpoints experience downtime, our ingestion engine falls back to high-fidelity empirical benchmark generators, ensuring continuity of pipeline testing and execution."

---

## 3. Kafka Streaming Implementation

### Evaluator Question 3.1: Explain your Kafka topic design, partition keys, and how you handle corrupted events.
**Candidate Response:**
> "We designed four distinct topics:
> - `retail.pos.transactions` (6 partitions, partitioned by `store_id`): Guarantees in-order processing of sales per store.
> - `retail.inventory.movements` (4 partitions, partitioned by `product_id`): Guarantees sequential stock balances per SKU.
> - `market.price.alerts` (3 partitions): Captures real-time stockout warnings and price variance alerts.
> - `retail.dlq.errors` (Dead Letter Queue): Captures corrupted or schema-violating JSON payloads.
> 
> In `src/streaming/consumer.py`, every incoming message undergoes strict schema validation. If required fields like `transaction_id`, `quantity`, or `unit_price_kes` are missing or invalid, the consumer routes the record to the Dead Letter Queue without crashing the consumer group."

---

## 4. Spark Transformations & Medallion Design

### Evaluator Question 4.1: What specific transformations take place in your Silver and Gold layers?
**Candidate Response:**
> "In the Silver layer:
> 1. **Unit Standardisation:** Wholesale prices quoted per 90kg bag, 50kg bag, or 20L jerrican are divided by their canonical conversion factors to derive standard prices per 1 KG or 1 L.
> 2. **KNBS Inflation Deflation:** We join POS transactions with KNBS Food CPI on year and month:
>    `real_line_total_kes = (nominal_line_total_kes / cpi_food) * 100`
>    This strips away macroeconomic inflation to reveal genuine volume sales growth.
> 3. **Market Price Spreads:** Slices KAMIS county retail prices to calculate the retailer's price competitiveness index.
> 
> In the Gold layer, we aggregate four specialized marts: Daily Store Sales, Price Competitiveness, Product Profitability Rankings, and Inventory Stockout Volatility."

---

## 5. PostgreSQL Data Warehouse & Dimensional Model

### Evaluator Question 5.1: Walk me through your dimensional model, surrogate keys, and analytical queries.
**Candidate Response:**
> "The warehouse is structured as a Kimball Star Schema:
> - **Conformed Dimensions:** `dim_date`, `dim_store`, `dim_product`, `dim_supplier`, `dim_kamis_market`, `dim_knbs_cpi`. All dimensions use synthetic integer surrogate keys, with `dim_date` utilizing smart integer keys (e.g., `20240315`).
> - **Fact Tables:**
>   - `fact_pos_sales` (25,000 line items): Granular transaction sales with additive measures (revenue, cost, gross profit).
>   - `fact_inventory_snapshot` (10,000 logs): Stock levels and reorder flags.
>   - `fact_kamis_market_price` (2,400 observations): Wholesale and retail open-market rates.
>   - `fact_price_intelligence` (17,043 comparisons): Retailer shelf price vs. KAMIS market benchmark.
> - **Performance:** B-tree indexes are created on all foreign keys in the fact tables to optimize analytical JOINs."

---

## 6. Airflow Workflows & Orchestration

### Evaluator Question 6.1: How is your DAG structured and how does it handle failure and backfills?
**Candidate Response:**
> "The DAG (`dags/retail_market_intelligence_dag.py`) executes a linear dependency graph:
> `[ingest_kamis, ingest_knbs, ingest_operational] >> spark_bronze >> spark_silver >> spark_gold >> load_warehouse >> data_quality_gate`
> - `max_active_runs = 1` prevents database lock contention between simultaneous daily runs.
> - `retries = 2` with 5-minute exponential backoff handles transient network failures.
> - The `data_quality_gate` acts as a circuit breaker: if validation assertions fail, the task raises an unhandled exception, failing the DAG and preventing corrupted data from being promoted to production dashboards."

---

## 7. Data Quality Mechanisms & Governance

### Evaluator Question 7.1: What validation checks do you enforce and where is lineage audited?
**Candidate Response:**
> "Our `DataQualityEngine` (`src/quality/engine.py`) enforces 8 discrete validation checks:
> 1. Fact table completeness (non-empty).
> 2. Dimension completeness (all 6 dimensions populated).
> 3. Price boundary assertion (`unit_price_kes > 0` and `< 50,000`).
> 4. Transaction uniqueness (zero duplicate `transaction_id`s).
> 5. Referential integrity (zero orphan foreign keys).
> 6. Observation date boundary (no future sale dates).
> 7. KAMIS price non-negativity.
> 8. Gross profit margin bounds (-50% to 100%).
> 
> When bad batches are caught, they are quarantined to `data/quarantine/`. Every run records batch metrics, execution time, and full JSON assertion logs into `data_quality_audit_log`."

---

## 8. Deployment & Monitoring Setup

### Evaluator Question 8.1: How is the solution containerised and monitored?
**Candidate Response:**
> "The entire platform is containerised via `docker/Dockerfile` and `docker-compose.yml`, orchestrating 6 services:
> - `postgres`: PostgreSQL 16 warehouse with automated schema initialization.
> - `zookeeper` and `kafka`: Apache Kafka cluster with automated topic provisioning.
> - `pipeline-master`: Container executing the master ETL and streaming ingest.
> - `dashboard`: Streamlit interactive analytics dashboard exposed on port 8501.
> - Health checks: Automated `pg_isready` and Kafka topic list checks ensure dependent services only start when upstream infrastructure is healthy."

---

## 9. Failure Handling & Pipeline Recovery Demonstration

### Evaluator Question 9.1: Demonstrate how your platform recovers when a task fails midway.
**Candidate Response:**
> "1. **Idempotent Warehouse Reloads:** If a fact table load fails midway, executing `python src/main.py --mode replace` uses clean transactional table truncations (`DELETE FROM fact_pos_sales;`) before reloading, ensuring zero duplicate rows or broken surrogate key mappings.
> 2. **Dead Letter Queue Recovery:** In the Kafka stream, unparseable events are routed to `retail.dlq.errors` rather than halting the consumer group.
> 3. **Airflow Task Clear:** In the Airflow UI or CLI, clearing a failed task instance allows seamless re-execution from the point of failure."
