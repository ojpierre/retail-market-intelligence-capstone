import sys
from pathlib import Path
import json
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import text
import streamlit as st

BASE_DIR = Path(__file__).resolve().parent.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from src.warehouse import get_engine

st.set_page_config(
    page_title="Kenya National Retail & Market Intelligence Platform",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🛒 Kenya National Retail & Market Intelligence Platform")
st.caption("End-to-End Enterprise Analytics: KAMIS Market Benchmarks | KNBS Inflation Deflation | Retail POS Performance")

engine = get_engine()


@st.cache_data(ttl=60)
def load_kpis():
    with engine.connect() as conn:
        kpi_df = pd.read_sql("""
            SELECT 
                COUNT(s.sales_id) AS total_sales_txn,
                ROUND(SUM(s.nominal_revenue_kes), 2) AS total_nominal_rev,
                ROUND(SUM(s.real_revenue_kes), 2) AS total_real_rev,
                ROUND(SUM(s.gross_profit_kes), 2) AS total_profit,
                ROUND(AVG(s.gross_margin_pct), 2) AS avg_margin
            FROM fact_pos_sales s
        """, conn)
    return kpi_df.iloc[0]


@st.cache_data(ttl=60)
def load_store_perf():
    with engine.connect() as conn:
        df = pd.read_sql("""
            SELECT 
                st.store_id, st.store_name, st.county, st.region, st.store_format,
                ROUND(SUM(s.nominal_revenue_kes), 2) AS nominal_revenue,
                ROUND(SUM(s.real_revenue_kes), 2) AS real_revenue,
                ROUND(SUM(s.gross_profit_kes), 2) AS gross_profit,
                ROUND((SUM(s.gross_profit_kes) / SUM(s.nominal_revenue_kes)) * 100.0, 2) AS margin_pct,
                COUNT(s.sales_id) AS orders_count
            FROM fact_pos_sales s
            JOIN dim_store st ON s.store_key = st.store_key
            GROUP BY st.store_id, st.store_name, st.county, st.region, st.store_format
            ORDER BY nominal_revenue DESC
        """, conn)
    return df


@st.cache_data(ttl=60)
def load_price_intelligence():
    with engine.connect() as conn:
        df = pd.read_sql("""
            SELECT 
                st.county,
                pr.product_name,
                pr.category,
                ROUND(AVG(pi.retail_shelf_price_kes), 2) AS shelf_price,
                ROUND(AVG(pi.kamis_retail_benchmark_kes), 2) AS kamis_market_price,
                ROUND(AVG(pi.spread_vs_market_kes), 2) AS spread_kes,
                ROUND(AVG(pi.competitiveness_index_pct), 2) AS comp_index
            FROM fact_price_intelligence pi
            JOIN dim_store st ON pi.store_key = st.store_key
            JOIN dim_product pr ON pi.product_key = pr.product_key
            GROUP BY st.county, pr.product_name, pr.category
            ORDER BY spread_kes DESC
        """, conn)
    return df


@st.cache_data(ttl=60)
def load_inflation_trends():
    with engine.connect() as conn:
        df = pd.read_sql("""
            SELECT 
                dt.year, dt.month, dt.month_name,
                cpi.cpi_food, cpi.food_inflation_yoy,
                ROUND(SUM(s.nominal_revenue_kes), 2) AS nominal_revenue,
                ROUND(SUM(s.real_revenue_kes), 2) AS real_revenue
            FROM fact_pos_sales s
            JOIN dim_date dt ON s.date_key = dt.date_key
            JOIN dim_knbs_cpi cpi ON dt.year = cpi.year AND dt.month = cpi.month
            GROUP BY dt.year, dt.month, dt.month_name, cpi.cpi_food, cpi.food_inflation_yoy
            ORDER BY dt.year, dt.month
        """, conn)
    return df


@st.cache_data(ttl=60)
def load_audit_lineage():
    with engine.connect() as conn:
        df = pd.read_sql("""
            SELECT audit_id, batch_id, pipeline_stage, total_checks, passed_checks,
                   status, records_scanned, execution_duration_sec, recorded_at
            FROM data_quality_audit_log
            ORDER BY audit_id DESC
        """, conn)
    return df


kpis = load_kpis()

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Gross Sales (Nominal)", f"KES {kpis['total_nominal_rev']:,.0f}")
col2.metric("Real Deflated Sales", f"KES {kpis['total_real_rev']:,.0f}", f"-{(kpis['total_nominal_rev'] - kpis['total_real_rev']):,.0f} Inflation")
col3.metric("Gross Profit", f"KES {kpis['total_profit']:,.0f}")
col4.metric("Avg Margin %", f"{kpis['avg_margin']:.1f}%")
col5.metric("Total POS Txns", f"{int(kpis['total_sales_txn']):,}")

st.divider()

tab1, tab2, tab3, tab4, tab5 = st.tabs([
    "🏪 Store Performance",
    "🔍 Price Intelligence vs. KAMIS",
    "📈 Inflation & Real Sales Deflation",
    "📦 Inventory & Stockout Risk",
    "🛡️ Data Quality & Audit Ledger",
])

with tab1:
    st.subheader("Supermarket Store Sales & Profitability Rankings")
    df_store = load_store_perf()
    
    fig_store = px.bar(
        df_store,
        x="store_name",
        y="nominal_revenue",
        color="margin_pct",
        title="Revenue by Store Branch (Colored by Gross Margin %)",
        labels={"nominal_revenue": "Nominal Revenue (KES)", "store_name": "Branch", "margin_pct": "Margin %"},
        color_continuous_scale="Viridis",
    )
    fig_store.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig_store, use_container_width=True)

    st.dataframe(df_store, use_container_width=True)

with tab2:
    st.subheader("Price Competitiveness: Supermarket Shelf Price vs. KAMIS Open Market")
    df_pi = load_price_intelligence()

    col_a, col_b = st.columns([1, 2])
    with col_a:
        counties = ["All"] + sorted(list(df_pi["county"].unique()))
        sel_county = st.selectbox("Select County:", counties)
    
    if sel_county != "All":
        df_pi_filtered = df_pi[df_pi["county"] == sel_county].copy()
    else:
        df_pi_filtered = df_pi.copy()

    df_pi_filtered["spread_magnitude"] = df_pi_filtered["spread_kes"].abs().clip(lower=2.0)

    fig_spread = px.scatter(
        df_pi_filtered,
        x="kamis_market_price",
        y="shelf_price",
        color="category",
        size="spread_magnitude",
        size_max=22,
        hover_name="product_name",
        hover_data={
            "product_name": False,
            "county": True,
            "shelf_price": ":.2f",
            "kamis_market_price": ":.2f",
            "spread_kes": ":.2f",
            "comp_index": ":.2f",
            "spread_magnitude": False,
        },
        title=f"Shelf Price vs. KAMIS Market Benchmark (County: {sel_county})",
        labels={
            "kamis_market_price": "KAMIS Retail Price (KES/kg or L)",
            "shelf_price": "Supermarket Price (KES)",
            "category": "Category",
            "spread_kes": "Spread (KES)",
            "comp_index": "Competitiveness Index (%)",
        },
    )
    if not df_pi_filtered.empty:
        max_val = max(df_pi_filtered["kamis_market_price"].max(), df_pi_filtered["shelf_price"].max()) * 1.05
        fig_spread.add_trace(
            go.Scatter(
                x=[0, max_val],
                y=[0, max_val],
                mode="lines",
                name="KAMIS Parity (Shelf = Market)",
                line=dict(color="gray", dash="dash"),
            )
        )
    st.plotly_chart(fig_spread, use_container_width=True)
    st.dataframe(df_pi_filtered.drop(columns=["spread_magnitude"]), use_container_width=True)

with tab3:
    st.subheader("Nominal Revenue vs. Deflated Real Sales (Constant 2019 Prices)")
    df_inf = load_inflation_trends()

    fig_inf = go.Figure()
    fig_inf.add_trace(go.Bar(
        x=[f"{r['year']}-{r['month']:02d}" for _, r in df_inf.iterrows()],
        y=df_inf["nominal_revenue"],
        name="Nominal Sales (KES)",
        marker_color="#3366CC",
    ))
    fig_inf.add_trace(go.Bar(
        x=[f"{r['year']}-{r['month']:02d}" for _, r in df_inf.iterrows()],
        y=df_inf["real_revenue"],
        name="Real Deflated Sales (Constant 2019 KES)",
        marker_color="#109618",
    ))
    fig_inf.add_trace(go.Scatter(
        x=[f"{r['year']}-{r['month']:02d}" for _, r in df_inf.iterrows()],
        y=df_inf["food_inflation_yoy"],
        name="KNBS Food Inflation YoY (%)",
        yaxis="y2",
        mode="lines+markers",
        line=dict(color="#FF9900", width=3),
    ))

    fig_inf.update_layout(
        title="24-Month Revenue Dynamics: Nominal vs Real Growth Deflated by KNBS Food CPI",
        yaxis=dict(title="Revenue (KES)"),
        yaxis2=dict(title="Food Inflation %", overlaying="y", side="right"),
        barmode="group",
        legend=dict(x=0.01, y=0.99),
    )
    st.plotly_chart(fig_inf, use_container_width=True)
    st.dataframe(df_inf, use_container_width=True)

with tab4:
    st.subheader("Inventory Movements & Stockout Alert Monitoring")
    with engine.connect() as conn:
        inv_df = pd.read_sql("""
            SELECT 
                st.store_name, st.county, pr.product_name, pr.category,
                inv.stock_on_hand, inv.reorder_threshold,
                COUNT(CASE WHEN inv.is_below_reorder = 1 OR inv.is_below_reorder = TRUE THEN 1 END) AS stockout_count
            FROM fact_inventory_snapshot inv
            JOIN dim_store st ON inv.store_key = st.store_key
            JOIN dim_product pr ON inv.product_key = pr.product_key
            GROUP BY st.store_name, st.county, pr.product_name, pr.category, inv.stock_on_hand, inv.reorder_threshold
            ORDER BY stockout_count DESC, inv.stock_on_hand ASC
            LIMIT 20
        """, conn)

    fig_inv = px.bar(
        inv_df,
        x="product_name",
        y="stock_on_hand",
        color="stockout_count",
        hover_data=["store_name", "county", "reorder_threshold"],
        title="Current Stock on Hand for Most Vulnerable SKUs (Colored by Stockout Alerts)",
        color_continuous_scale="Reds",
    )
    fig_inv.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig_inv, use_container_width=True)
    st.dataframe(inv_df, use_container_width=True)

with tab5:
    st.subheader("Data Quality Gate & Audit Lineage Ledger")
    df_audit = load_audit_lineage()
    st.dataframe(df_audit, use_container_width=True)

    with engine.connect() as conn:
        last_audit = conn.execute(text("SELECT details_json FROM data_quality_audit_log ORDER BY audit_id DESC LIMIT 1")).scalar()
        if last_audit:
            st.json(json.loads(last_audit))
