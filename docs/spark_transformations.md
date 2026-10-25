# Milestone 3: Apache Spark & Medallion Layer Transformations

## 1. Medallion Pipeline Catalog

```
[Raw Ingestion / Streaming Sinks]
              |
              v
       [Bronze Layer] (Immutable Parquet, Lineage Metadata)
              |
              v
       [Silver Layer] (Deduplication, Unit Normalisation, CPI Deflation, KAMIS Joins)
              |
              v
       [Gold Layer]   (Aggregated Daily Store Sales, Price Competitiveness, Profitability)
```

---

## 2. Before & After Transformation Catalog

| Ingestion Issue / Raw State | Transformation Applied | Cleaned Silver / Gold State | Business Impact |
|---|---|---|---|
| **Disparate KAMIS Packaging** (90kg grain bags, 50kg tuber bags, 20L oil cans) | Dynamic divisor mapping via `KAMIS_UNIT_DIVISORS` | `norm_wholesale_price_kes` per canonical base unit (`1 KG` or `1 L`) | Enables direct mathematical comparison between wholesale market prices and supermarket shelf packs |
| **Retail SKU Pack Size Variance** (e.g., 2kg flour, 500ml milk, 400g bread) | Division by `unit_factor` | `retail_price_per_std_unit_kes` | Allows pricing analysts to evaluate true cost-per-kg across different packaging formats |
| **Inflation-Distorted Sales Figures** (Top-line sales rise due to price hikes rather than volume) | Joining with KNBS Food CPI series: `real_rev = (nominal_rev / cpi_food) * 100` | `real_line_total_kes` in constant 2019 KES | Exposes true real organic sales growth versus inflationary revenue increases |
| **Missing County Market Benchmarks** | Spatial and temporal join: `ON county, year, month, commodity` | `kamis_retail_benchmark_kes`, `spread_vs_market_kes`, `competitiveness_index_pct` | Identifies if a supermarket branch is overcharging or underpricing relative to local open markets |
| **Duplicate POS Transmissions** | `drop_duplicates(subset=["transaction_id"])` | Unique transaction ledger | Prevents double-counting revenue in financial analytics |
