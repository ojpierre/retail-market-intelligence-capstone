# Milestone 2: Apache Kafka Streaming Architecture & Event Processing

## 1. Topic Design & Partitioning Strategy

| Topic Name | Partitions | Key Field | Cleanup Policy | Retention | Purpose |
|---|---|---|---|---|---|
| `retail.pos.transactions` | 6 | `store_id` | `delete` | 7 days | High-velocity point-of-sale customer sales transactions. Keyed by `store_id` to guarantee in-order processing per retail branch. |
| `retail.inventory.movements` | 4 | `product_id` | `delete` | 7 days | Real-time stock movements (Restocks, Shrinkage, Sales Dispatch). Keyed by `product_id` for accurate per-SKU stock reconciliation. |
| `market.price.alerts` | 3 | `alert_type` | `delete` | 30 days | Real-time events triggered on stockout warnings and significant supermarket vs. KAMIS market price variances. |
| `retail.dlq.errors` | 2 | `error_type` | `compact` | 30 days | Dead Letter Queue capturing schema-violating or unparseable event payloads for post-mortem debugging. |

---

## 2. Consumer Groups & Workflow

- **`retail-analytics-group`**: Subscribes to `retail.pos.transactions` to maintain real-time store revenue velocity, committing offsets in batches.
- **`retail-inventory-monitor`**: Subscribes to `retail.inventory.movements`. Evaluates current stock-on-hand against `reorder_threshold`. If `stock_on_hand < reorder_threshold`, fires a `STOCKOUT_WARNING` alert to `market.price.alerts`.
- **`retail-governance-audit`**: Subscribes to `retail.dlq.errors` to alert data engineering on upstream schema drifts.

---

## 3. Streaming Verification & Message Flow Evidence

Executing `src/streaming/event_processor.py` produces verifiable end-to-end event delivery:
- **500 POS sales events** published and consumed with zero dropped messages.
- **500 inventory movement events** processed with real-time stockout threshold evaluations.
- Stream persisted to Bronze staging at `data/bronze/streaming/stream_retail-analytics-group_consumed.parquet`.
