import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from datetime import datetime
import time
import os
from dotenv import load_dotenv

st.set_page_config(
    page_title="BuzzBurst Media — ROI Dashboard",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database Connection
load_dotenv()

DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT")
DB_NAME     = os.getenv("DB_NAME")

PLOTLY_THEME = {
    "paper_bgcolor" : "#1e2130",
    "plot_bgcolor"  : "#1e2130",
    "font_color"    : "#8b9bc8",
    "gridcolor"     : "#2e3250",
}


@st.cache_resource
def get_engine():
    # Try Streamlit secrets first (cloud deployment)
    try:
        import streamlit as st
        db_user     = st.secrets["DB_USER"]
        db_password = st.secrets["DB_PASSWORD"]
        db_host     = st.secrets["DB_HOST"]
        db_port     = st.secrets["DB_PORT"]
        db_name     = st.secrets["DB_NAME"]
    except Exception:
        #Fall back to .env for local development
        db_user     = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_host     = os.getenv("DB_HOST")
        db_port     = os.getenv("DB_PORT")
        db_name     = os.getenv("DB_NAME")

    #Neon requires SSL
    try:
        # ── Streamlit Cloud — reads from secrets.toml ────────
        url = (
            f"postgresql+psycopg2://{st.secrets['DB_USER']}:{st.secrets['DB_PASSWORD']}"
            f"@{st.secrets['DB_HOST']}:{st.secrets['DB_PORT']}/{st.secrets['DB_NAME']}"
            f"?sslmode=require"
        )
        print("[DB] Using Streamlit secrets")
    except Exception as e:
        print(f"[DB] Secrets failed ({e}), using local .env")
        # ── Local development — reads from .env ──────────────
        url = (
            f"postgresql+psycopg2://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'asad123')}"
            f"@{os.getenv('DB_HOST', 'localhost')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'buzzburst_gold')}"
            # No sslmode for local postgres
        )

    return create_engine(url)

# Data Loaders (cached for 10 seconds)

@st.cache_data(ttl=10)
def load_marketing_performance():
    engine = get_engine()
    df = pd.read_sql("""
        SELECT
            fmp.date_key,
            TO_DATE(fmp.date_key::TEXT, 'YYYYMMDD')  AS date,
            COALESCE(fmp.total_ad_spend::NUMERIC, 0)  AS total_ad_spend,
            COALESCE(fmp.total_revenue::NUMERIC, 0)   AS total_revenue,
            COALESCE(fmp.paying_users, 0)             AS paying_users,
            fmp.roas::NUMERIC                         AS roas,
            fmp.cac::NUMERIC                          AS cac
        FROM fact_marketing_performance fmp
        WHERE fmp.date_key IS NOT NULL
        ORDER BY fmp.date_key
    """, engine)
    return df


@st.cache_data(ttl=10)
def load_roas_by_platform():
    engine = get_engine()
    df = pd.read_sql("""
        WITH spend_by_platform AS (
            SELECT
                dp.platform,
                SUM(fa.total_cost::NUMERIC) AS total_spend
            FROM fact_ad_spend fa
            JOIN dim_platform dp ON fa.platform_key = dp.platform_key
            GROUP BY dp.platform
        ),
        total_revenue AS (
            SELECT COALESCE(SUM(amount::NUMERIC), 0) AS grand_revenue
            FROM fact_sales
        )
        SELECT
            s.platform,
            COALESCE(s.total_spend, 0) AS total_spend,
            t.grand_revenue AS total_revenue,
            CASE
                WHEN s.total_spend > 0
                THEN ROUND((t.grand_revenue / s.total_spend)::NUMERIC, 4)
                ELSE 0
            END  AS roas
        FROM spend_by_platform s
        CROSS JOIN total_revenue t
        ORDER BY roas DESC
    """, engine)
    return df

@st.cache_data(ttl=10)
def load_cac_over_time():
    engine = get_engine()
    df = pd.read_sql("""
        SELECT
            TO_DATE(date_key::TEXT, 'YYYYMMDD') AS date,
            cac::NUMERIC AS cac,
            total_ad_spend::NUMERIC AS total_ad_spend,
            paying_users
        FROM fact_marketing_performance
        WHERE cac IS NOT NULL
        ORDER BY date_key
    """, engine)
    return df


@st.cache_data(ttl=10)
def load_cac_over_time():
    engine = get_engine()
    df = pd.read_sql("""
        SELECT
            TO_DATE(date_key::TEXT, 'YYYYMMDD') AS date,
            cac::NUMERIC  AS cac,
            total_ad_spend::NUMERIC AS total_ad_spend,
            COALESCE(paying_users, 0) AS paying_users
        FROM fact_marketing_performance
        WHERE cac IS NOT NULL
        AND date_key IS NOT NULL
        ORDER BY date_key
    """, engine)
    return df


@st.cache_data(ttl=10)
def load_live_sales_feed():
    engine = get_engine()
    df = pd.read_sql("""
        SELECT
            fs.sale_id,
            TO_DATE(fs.date_key::TEXT, 'YYYYMMDD') AS date,
            COALESCE(du.full_name, 'Unknown')       AS customer,
            COALESCE(dp.product_name, 'Unknown')    AS product,
            COALESCE(fs.amount::NUMERIC, 0)         AS amount
        FROM fact_sales fs
        LEFT JOIN dim_user    du ON fs.user_key    = du.user_key
        LEFT JOIN dim_product dp ON fs.product_key = dp.product_key
        WHERE fs.date_key IS NOT NULL
        ORDER BY fs.date_key DESC, fs.sale_id DESC
        LIMIT 50
    """, engine)
    return df


@st.cache_data(ttl=10)
def load_daily_sales_chart():
    engine = get_engine()
    df = pd.read_sql("""
        SELECT
            TO_DATE(date_key::TEXT, 'YYYYMMDD') AS date,
            SUM(amount::NUMERIC) AS daily_revenue,
            COUNT(sale_id) AS transactions
        FROM fact_sales
        WHERE date_key IS NOT NULL
        GROUP BY date_key
        ORDER BY date_key
    """, engine)
    return df


@st.cache_data(ttl=10)
def load_summary_kpis():
    engine = get_engine()
    row = pd.read_sql("""
        SELECT
            COALESCE(SUM(total_revenue::NUMERIC), 0)  AS total_revenue,
            COALESCE(SUM(total_ad_spend::NUMERIC), 0) AS total_spend,
            COALESCE(AVG(roas::NUMERIC), 0) AS avg_roas,
            COALESCE(AVG(cac::NUMERIC), 0) AS avg_cac,
            COALESCE(SUM(paying_users), 0) AS total_customers
        FROM fact_marketing_performance
        WHERE date_key IS NOT NULL
    """, engine).iloc[0]
    return row


# Sidebar

with st.sidebar:
    st.markdown("## ⚡ BuzzBurst Media")
    st.markdown("**Production ROI Dashboard**")
    st.markdown("---")

    st.markdown("### 🔄 Auto Refresh")
    auto_refresh = st.toggle("Live Mode", value=True)
    refresh_rate = st.slider(
        "Refresh every (seconds)",
        min_value=5,
        max_value=60,
        value=10,
        step=5
    )

    st.markdown("---")
    st.markdown("### Data Source")
    st.markdown("PostgreSQL `buzzburst_gold`")
    st.markdown(f"Last updated: `{datetime.now().strftime('%H:%M:%S')}`")

    st.markdown("---")
    st.markdown("### Metric Formulas")
    st.markdown("""
    **ROAS** = Revenue ÷ Ad Spend

    **CAC** = Ad Spend ÷ Paying Users

    > ROAS > 1.0 = profitable
    > ROAS < 1.0 = losing money
    """)

    st.markdown("---")
    st.caption("BuzzBurst Media Data Platform v1.0")



# Header

col_title, col_badge = st.columns([6, 1])
with col_title:
    st.markdown(
        "# ⚡ BuzzBurst ROI Dashboard"
        "<span class='live-badge'>LIVE</span>",
        unsafe_allow_html=True
    )
with col_badge:
    st.markdown(f"**{datetime.now().strftime('%d %b %Y')}**")

st.markdown("---")


# ROW 1 — SUMMARY KPIs

st.markdown("<p class='section-header'>📈 Summary KPIs</p>", unsafe_allow_html=True)

try:
    kpis = load_summary_kpis()

    k1, k2, k3, k4, k5 = st.columns(5)

    with k1:
        st.metric(
            "Total Revenue",
            f"${kpis['total_revenue']:,.2f}",
        )
    with k2:
        st.metric(
            "Total Ad Spend",
            f"${kpis['total_spend']:,.2f}",
        )
    with k3:
        overall_roas = kpis['total_revenue'] / kpis['total_spend'] if kpis['total_spend'] > 0 else 0
        st.metric(
            "Overall ROAS",
            f"{overall_roas:.2f}x",
            delta="Profitable" if overall_roas > 1 else "Unprofitable",
        )
    with k4:
        st.metric(
            "Avg CAC",
            f"${kpis['avg_cac']:,.2f}",
        )
    with k5:
        st.metric(
            "Total Customers",
            f"{int(kpis['total_customers']):,}",
        )

except Exception as e:
    st.error(f"Error loading KPIs: {e}")

st.markdown("<br>", unsafe_allow_html=True)


# ROW 2 — ROAS BY PLATFORM + CAC OVER TIME

st.markdown(
    "<p class='section-header'>🎯 ROAS by Platform & CAC Trend</p>",
    unsafe_allow_html=True
)

col_roas, col_cac = st.columns(2)

#ROAS by Platform
with col_roas:
    try:
        roas_df = load_roas_by_platform()

        # Color code: blue if ROAS > 1, red if below
        roas_df["color"] = roas_df["roas"].apply(
            lambda x: "#5667fa" if x >= 1 else "#ff4b6e"
        )

        fig_roas = go.Figure()

        fig_roas.add_trace(go.Bar(
            x=roas_df["platform"],
            y=roas_df["roas"],
            marker_color=roas_df["color"],
            text=[f"{v:.2f}x" for v in roas_df["roas"]],
            textposition="outside",
            textfont=dict(color="#ffffff", size=13, family="monospace"),
        ))

        # ROAS = 1 breakeven line
        fig_roas.add_hline(
            y=1.0,
            line_dash="dash",
            line_color="#ff4b6e",
            annotation_text="Breakeven",
            annotation_font_color="#ff4b6e",
        )

        fig_roas.update_layout(
            title=dict(
                text="ROAS per Platform",
                font=dict(color="#ffffff", size=16)
            ),
            paper_bgcolor=PLOTLY_THEME["paper_bgcolor"],
            plot_bgcolor=PLOTLY_THEME["plot_bgcolor"],
            font=dict(color=PLOTLY_THEME["font_color"]),
            xaxis=dict(
                gridcolor=PLOTLY_THEME["gridcolor"],
                title="Platform"
            ),
            yaxis=dict(
                gridcolor=PLOTLY_THEME["gridcolor"],
                title="ROAS",
                zeroline=False
            ),
            showlegend=False,
            margin=dict(t=50, b=40, l=40, r=40),
            height=350,
        )

        st.plotly_chart(fig_roas, use_container_width=True)

        # Platform details table
        st.markdown("**Platform Breakdown**")
        display_roas = roas_df[["platform", "total_spend", "roas"]].copy()
        display_roas.columns = ["Platform", "Ad Spend ($)", "ROAS"]
        display_roas["Ad Spend ($)"] = display_roas["Ad Spend ($)"].apply(lambda x: f"${x:,.2f}")
        display_roas["ROAS"] = display_roas["ROAS"].apply(lambda x: f"{x:.4f}x")
        st.dataframe(display_roas, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Error loading ROAS data: {e}")

#CAC Over Time
with col_cac:
    try:
        cac_df = load_cac_over_time()

        fig_cac = go.Figure()

        # CAC line
        fig_cac.add_trace(go.Scatter(
            x=cac_df["date"],
            y=cac_df["cac"],
            mode="lines+markers",
            name="CAC",
            line=dict(color="#a78bfa", width=2),
            marker=dict(size=5, color="#a78bfa"),
            fill="tozeroy",
            fillcolor="rgba(167, 139, 250, 0.1)",
        ))

        # Average CAC line
        avg_cac = cac_df["cac"].mean()
        fig_cac.add_hline(
            y=avg_cac,
            line_dash="dot",
            line_color="#f59e0b",
            annotation_text=f"Avg ${avg_cac:.0f}",
            annotation_font_color="#f59e0b",
        )

        fig_cac.update_layout(
            title=dict(
                text="Customer Acquisition Cost Over Time",
                font=dict(color="#ffffff", size=16)
            ),
            paper_bgcolor=PLOTLY_THEME["paper_bgcolor"],
            plot_bgcolor=PLOTLY_THEME["plot_bgcolor"],
            font=dict(color=PLOTLY_THEME["font_color"]),
            xaxis=dict(
                gridcolor=PLOTLY_THEME["gridcolor"],
                title="Date"
            ),
            yaxis=dict(
                gridcolor=PLOTLY_THEME["gridcolor"],
                title="CAC ($)"
            ),
            showlegend=False,
            margin=dict(t=50, b=40, l=40, r=40),
            height=350,
        )

        st.plotly_chart(fig_cac, use_container_width=True)

        # CAC summary
        c1, c2, c3 = st.columns(3)
        c1.metric("Min CAC",  f"${cac_df['cac'].min():,.2f}")
        c2.metric("Avg CAC",  f"${cac_df['cac'].mean():,.2f}")
        c3.metric("Max CAC",  f"${cac_df['cac'].max():,.2f}")

    except Exception as e:
        st.error(f"Error loading CAC data: {e}")

st.markdown("<br>", unsafe_allow_html=True)


# ROW 3 — Live revenue + Marketing performance

st.markdown(
    "<p class='section-header'>💰 Live Revenue Feed</p>",
    unsafe_allow_html=True
)

col_rev, col_perf = st.columns([3, 2])

#Daily Revenue Chart
with col_rev:
    try:
        sales_df = load_daily_sales_chart()

        fig_sales = go.Figure()

        # Revenue bars
        fig_sales.add_trace(go.Bar(
            x=sales_df["date"],
            y=sales_df["daily_revenue"],
            name="Revenue",
            marker_color="#00ff88",
            opacity=0.8,
        ))

        # Transactions line on second axis
        fig_sales.add_trace(go.Scatter(
            x=sales_df["date"],
            y=sales_df["transactions"],
            name="Transactions",
            mode="lines+markers",
            line=dict(color="#f59e0b", width=2),
            marker=dict(size=4),
            yaxis="y2",
        ))

        fig_sales.update_layout(
            title=dict(
                text="Daily Revenue & Transactions",
                font=dict(color="#ffffff", size=16)
            ),
            paper_bgcolor=PLOTLY_THEME["paper_bgcolor"],
            plot_bgcolor=PLOTLY_THEME["plot_bgcolor"],
            font=dict(color=PLOTLY_THEME["font_color"]),
            xaxis=dict(
                gridcolor=PLOTLY_THEME["gridcolor"],
                title="Date"
            ),
            yaxis=dict(
                gridcolor=PLOTLY_THEME["gridcolor"],
                title="Revenue ($)",
                side="left"
            ),
            yaxis2=dict(
                title="Transactions",
                overlaying="y",
                side="right",
                showgrid=False,
                color="#f59e0b"
            ),
            legend=dict(
                bgcolor="rgba(0,0,0,0)",
                font=dict(color="#ffffff")
            ),
            margin=dict(t=50, b=40, l=40, r=40),
            height=380,
            barmode="overlay",
        )

        st.plotly_chart(fig_sales, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading revenue chart: {e}")

#ROAS Over Time
with col_perf:
    try:
        perf_df = load_marketing_performance()
        perf_df = perf_df.dropna(subset=["roas"])

        fig_perf = go.Figure()

        fig_perf.add_trace(go.Scatter(
            x=perf_df["date"],
            y=perf_df["roas"],
            mode="lines+markers",
            line=dict(color="#60a5fa", width=2),
            marker=dict(
                size=6,
                color=perf_df["roas"].apply(
                    lambda x: "#00ff88" if x >= 1 else "#ff4b6e"
                ),
                line=dict(color="#0f1117", width=1)
            ),
            fill="tozeroy",
            fillcolor="rgba(96, 165, 250, 0.08)",
        ))

        fig_perf.add_hline(
            y=1.0,
            line_dash="dash",
            line_color="#ff4b6e",
            annotation_text="Breakeven",
            annotation_font_color="#ff4b6e",
        )

        fig_perf.update_layout(
            title=dict(
                text="ROAS Over Time",
                font=dict(color="#ffffff", size=16)
            ),
            paper_bgcolor=PLOTLY_THEME["paper_bgcolor"],
            plot_bgcolor=PLOTLY_THEME["plot_bgcolor"],
            font=dict(color=PLOTLY_THEME["font_color"]),
            xaxis=dict(gridcolor=PLOTLY_THEME["gridcolor"]),
            yaxis=dict(gridcolor=PLOTLY_THEME["gridcolor"], title="ROAS"),
            showlegend=False,
            margin=dict(t=50, b=40, l=40, r=40),
            height=380,
        )

        st.plotly_chart(fig_perf, use_container_width=True)

    except Exception as e:
        st.error(f"Error loading ROAS trend: {e}")

st.markdown("<br>", unsafe_allow_html=True)


# ROW 4 — LIVE SALES FEED TABLE

st.markdown(
    "<p class='section-header'>🔴 Live Sales Feed — Last 50 Transactions</p>",
    unsafe_allow_html=True
)

try:
    feed_df = load_live_sales_feed()

    # Format for display
    feed_display = feed_df.copy()
    feed_display["amount"] = feed_display["amount"].apply(
        lambda x: f"${x:,.2f}"
    )
    feed_display["customer"] = feed_display["customer"].fillna("Unknown")
    feed_display.columns = [
        "Sale ID", "Date", "Customer", "Product", "Amount"
    ]

    st.dataframe(
        feed_display,
        use_container_width=True,
        hide_index=True,
        height=400,
    )

    # Show count
    engine = get_engine()
    total_sales = pd.read_sql(
        "SELECT COUNT(*) AS cnt FROM fact_sales", engine
    ).iloc[0]["cnt"]
    st.caption(
        f"Showing last 50 of {total_sales:,} total transactions — "
        f"updates every {refresh_rate}s"
    )

except Exception as e:
    st.error(f"Error loading sales feed: {e}")


# AUTO REFRESH
if auto_refresh:
    time.sleep(refresh_rate)
    st.cache_data.clear()   # clear cache so next rerun fetches fresh data
    st.rerun()
