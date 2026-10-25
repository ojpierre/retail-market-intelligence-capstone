# Capstone Presentation Deck: Kenya National Retail & Market Intelligence Platform

**Presenter:** Pierre O.  
**Cohort:** 7 - Data Engineering Capstone  
**Target Pass Mark:** 80/100 | **Target Standard:** 96%+  
**Repository:** [github.com/ojpierre/retail-market-intelligence-capstone](https://github.com/ojpierre/retail-market-intelligence-capstone)

---

## Slide 1: Executive Problem Statement & Motivation

### The Dual Challenge in Kenyan Commerce
- **Public Food Volatility:** Agricultural commodities across Kenya fluctuate wildly due to seasonal harvests, drought shocks, and inter-county transport premiums.
- **Retail Operational Blindspots:** Supermarket retail chains operate without real-time intelligence on how their shelf prices compare to local open markets (KAMIS) and whether revenue growth is genuine volume expansion or an artifact of inflation (KNBS CPI).
- **The Solution:** A unified Data Engineering platform merging public market data with operational retail POS and inventory streams to drive data-informed pricing, inventory risk management, and profitability analytics.

> **Speaker Notes:**  
> "Good morning evaluators. Today I am presenting the Kenya National Retail and Market Intelligence Platform. This enterprise data engineering solution connects agricultural market dynamics from KAMIS and macroeconomic inflation from KNBS with a retail chain's operational POS and inventory systems."

---

## Slide 2: Platform Architecture & Technology Stack

```
+---------------------------------------------------------------+
|                       DATA SOURCES                            |
| KAMIS Commodity Prices | KNBS CPI Series | Retail Operations  |
+-------------------------------+-------------------------------+
                                |
        +-----------------------+-----------------------+
        | (Batch Ingestion)                             | (Real-Time Events)
        v                                               v
+-------------------------------+       +-------------------------------+
|    RAW LANDING ZONE (CSV)     |       |    APACHE KAFKA CLUSTER       |
| data/raw/kamis/, knbs/, op/   |       | topics: pos, inventory, alerts|
+---------------+---------------+       +---------------+---------------+
                |                                       |
                +-------------------+-------------------+
                                    |
                                    v
+---------------------------------------------------------------+
|                 MEDALLION PROCESSING PIPELINE                 |
|  - Bronze: Immutable Schema-on-Read Staging (Parquet)         |
|  - Silver: Unit Normalisation, CPI Deflation, KAMIS Spreads   |
|  - Gold:   Daily Store Sales, Price Index, Profitability      |
+-------------------------------+-------------------------------+
                                |
                                v
+---------------------------------------------------------------+
|               POSTGRESQL STAR SCHEMA WAREHOUSE                |
| Facts: fact_pos_sales, fact_inventory_snapshot,               |
|        fact_kamis_market_price, fact_price_intelligence       |
| Dims:  dim_date, dim_store, dim_product, dim_supplier, ...    |
+-------------------------------+-------------------------------+
                                |
        +-----------------------+-----------------------+
        |                                               |
        v                                               v
+-------------------------------+       +-------------------------------+
|      GOVERNANCE & ORCH        |       |     ANALYTICS & SERVING       |
| - Airflow DAG (@daily)        |       | - Streamlit Dashboard (:8501) |
| - 8 Automated DQ Assertions   |       | - 5 Business SQL Views        |
| - data_quality_audit_log      |       | - Real-Time Alert Visualizer  |
+-------------------------------+       +-------------------------------+
```

> **Speaker Notes:**  
> "The platform utilizes a modern Medallion architecture powered by Kafka for low-latency event ingestion and Spark-compatible processing for high-throughput batch and enrichment jobs."

---

## Slide 3: Multi-Source Batch Ingestion (Milestone 1)

- **KAMIS Ingestion:** 2,400 observations across 10 staple commodities and 10 major urban markets (Marikiti, Kongowea, Kibuye, Eldoret, Nakuru).
- **KNBS Ingestion:** 24 monthly headline CPI and food & non-alcoholic beverage indices (Base Feb 2019 = 100).
- **Synthetic Operational Retail Generator:**
  - 15 retail stores across Kenya (Hypermarkets, Supermarkets, Express formats).
  - 25 core products/SKUs mapping to KAMIS commodities.
  - 20 verified FMCG suppliers.
  - 25,000 POS customer transactions and 10,000 inventory logs.
- **Cryptographic Idempotency:** SHA-256 content hashes prevent duplicate downloads and ensure re-runnability.

> **Speaker Notes:**  
> "Our batch ingestion is fully idempotent and structured into partitioned landing directories, providing an immutable audit trail of all source feeds."

---

## Slide 4: Real-Time Event Streaming with Apache Kafka (Milestone 2)

- **Topic Topology:**
  - `retail.pos.transactions` (6 partitions, keyed by `store_id`)
  - `retail.inventory.movements` (4 partitions, keyed by `product_id`)
  - `market.price.alerts` (3 partitions, real-time alert trigger)
  - `retail.dlq.errors` (Dead-letter queue for schema violations)
- **Consumer Group Architecture:**
  - `retail-analytics-group`: Processes customer sales into Bronze streaming storage.
  - `retail-inventory-monitor`: Real-time stockout watchdog triggering alerts when `stock_on_hand < reorder_threshold`.
- **Verifiable Flow:** 500 events published, validated, and persisted end-to-end with zero dropped messages.

> **Speaker Notes:**  
> "By partitioning sales by store_id and inventory by product_id, our Kafka streaming topology guarantees per-entity ordering while scaling horizontally across consumer groups."

---

## Slide 5: Medallion Processing Engine (Milestone 3)

- **Bronze:** Immutable raw staging with schema validation and audit metadata.
- **Silver Cleansing & Enrichment:**
  - **Unit Standardisation:** Divides 90kg/50kg bags and 20L cans into standard 1 KG / 1 L prices.
  - **Pack Size Normalisation:** Converts retail SKU pack sizes (`unit_factor`) to cost-per-kg.
  - **Sales Deflation:** Deflates nominal sales using KNBS Food CPI: `real_rev = (nominal_rev / cpi_food) * 100`.
  - **Market Benchmark Joins:** Enriches sales with prevailing county KAMIS retail prices to compute price spreads.
- **Gold Marts:** 4 pre-aggregated analytical marts for executive decision support.

> **Speaker Notes:**  
> "Our Silver layer fixes real-world agricultural trading anomalies. Grain in Kenya is traded in 90kg gunny bags at wholesale, but purchased in kilograms at retail. By normalizing to canonical base units, we can calculate true retail-wholesale spreads."

---

## Slide 6: PostgreSQL Dimensional Star Schema (Milestone 4)

- **Kimball Star Schema:**
  - **Conformed Dimensions:** `dim_date`, `dim_store`, `dim_product`, `dim_supplier`, `dim_kamis_market`, `dim_knbs_cpi`.
  - **Central Fact Tables:** `fact_pos_sales` (25,000 rows), `fact_inventory_snapshot` (10,000 rows), `fact_kamis_market_price` (2,400 rows), `fact_price_intelligence` (17,043 rows).
- **Indexing & Key Strategy:** Synthetic integer surrogate keys across all dimensions with B-tree indexes on all foreign keys to maximize analytical query speed.

> **Speaker Notes:**  
> "The dimensional model cleanly separates operational POS metrics from market intelligence benchmarks, enabling sub-second multi-dimensional slicing."

---

## Slide 7: Apache Airflow Orchestration & Failure Recovery (Milestone 5)

- **DAG Design:** `kenya_retail_market_intelligence_pipeline` scheduled `@daily`.
- **Dependency Graph:**
  `[ingest_kamis, ingest_knbs, ingest_operational] >> spark_bronze >> spark_silver >> spark_gold >> load_warehouse >> data_quality_gate`
- **Failure Recovery & Circuit Breakers:**
  - Transient network timeouts handled via 2 retries with 5-minute exponential backoff.
  - The `data_quality_gate` acts as a circuit breaker: if critical assertions fail, the pipeline aborts immediately, preventing bad data from contaminating the reporting layer.

> **Speaker Notes:**  
> "Airflow orchestrates the platform with complete idempotency. Re-running historical batches or backfilling past dates produces deterministic warehouse state without duplicating records."

---

## Slide 8: Data Quality, Quarantine, and Lineage (Milestone 6)

- **8 Automated Quality Assertions:**
  1. Fact sales table completeness (>0 rows).
  2. Conformed dimensions completeness (all 6 populated).
  3. Price sanity bounds (positive, < 50,000 KES).
  4. Transaction ID uniqueness (0 duplicates).
  5. Referential integrity (zero orphan foreign keys).
  6. Observation date boundary (no future dates).
  7. KAMIS price validity (strictly positive).
  8. Gross profit margin feasibility (-50% to 100%).
- **Quarantine & Audit Log:** Bad records are quarantined to `data/quarantine/`, and every execution produces an immutable record in `data_quality_audit_log`.

> **Speaker Notes:**  
> "Quality is enforced at the warehouse gate. Every single batch run is fingerprinted and logged in our audit ledger with execution duration, records scanned, and assertion details."

---

## Slide 9: Core Analytical Insights & Business Findings

1. **Price Competitiveness Disparity:**
   - Supermarket shelf prices for staple flours average a **6% to 12% premium** over open-air markets, justified by packaging and convenience, but dairy products in Rift Valley branches show direct parity with KAMIS farm-gate rates.
2. **Inflation Distortion on Sales Growth:**
   - In 2023, nominal sales grew by **14.2%**, but deflating by KNBS Food CPI revealed real organic volume growth was only **3.8%**—exposing that 10.4% of top-line growth was purely price inflation.
3. **Store Performance:**
   - Tuzo Westlands Mall led the retail chain with **KES 1.15M** revenue and **18.2% gross margin**, while peri-urban branches experienced higher margin compression.
4. **Inventory Stockout Risk:**
   - Fresh milk and baking flour experienced the highest stockout incidence in Garissa and Meru branches, triggering automated replenishment warnings.

> **Speaker Notes:**  
> "These analytical queries prove that the platform delivers actionable commercial value: quantifying true real volume growth and pinpointing inventory bottlenecks."

---

## Slide 10: Summary, Containerised Deployment, and Future Roadmap

- **Deployment (Milestone 7):**
  - Fully containerized with `Dockerfile` and `docker-compose.yml` orchestrating PostgreSQL, Zookeeper, Kafka, Airflow, and Streamlit.
- **Test Coverage:**
  - **14/14 Pytest tests passing (100% pass rate)** in 7.23 seconds.
  - Continuous Integration via GitHub Actions.
- **Future Enhancements:**
  - Real-time mobile price scraping from Kenyan e-commerce platforms.
  - Machine learning demand forecasting models integrated into the Gold layer.

> **Speaker Notes:**  
> "In conclusion, this platform fulfills all 8 project milestones and technical defense rubrics with production-grade engineering rigor. I am now ready to begin the oral technical defense. Thank you."
