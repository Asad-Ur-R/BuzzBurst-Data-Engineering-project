# ⚡ BuzzBurst Media — Production Data Platform

> A end-to-end Modern Data Stack built for a fast-growing marketing startup 
> that scaled from 0 to 50,000 users in six months.

![Python](https://img.shields.io/badge/Python-3.11-blue?style=flat-square&logo=python)
![PySpark](https://img.shields.io/badge/PySpark-3.x-orange?style=flat-square&logo=apache-spark)
![Prefect](https://img.shields.io/badge/Prefect-3.x-purple?style=flat-square)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-blue?style=flat-square&logo=postgresql)
![Streamlit](https://img.shields.io/badge/Streamlit-Live-red?style=flat-square&logo=streamlit)

---

## 🎯 The Problem

BuzzBurst Media had data scattered across 4 disconnected systems:

| System | Data | Problem |
|--------|------|---------|
| Google Ads | Ad spend | Inconsistent dates, platform typos |
| Stripe | Sales | Multi-currency, negative values |
| HubSpot | Customer data | Duplicates, messy emails |
| Google Sheets | Influencer data | Metadata locked in JSON strings |

**No one could answer:** *"How much did we spend on Facebook ads, 
and how many sales did that generate?"*

---

## 🏗️ The Solution — Medallion Architecture
```
Raw CSVs (8 sources)
      ↓
 BRONZE LAYER    — Preserve raw data as Parquet (no changes)
      ↓
 SILVER LAYER    — Clean, deduplicate, normalize, flatten
      ↓
 GOLD LAYER      — Star Schema with business metrics
      ↓
 POSTGRESQL      — Production database
      ↓
 STREAMLIT       — Live ROI Dashboard
```
### Fact Tables
| Table | Grain | Key Metrics |
|-------|-------|-------------|
| `fact_marketing_performance` | One row per day | ROAS, CAC, Revenue, Ad Spend |
| `fact_ad_spend` | One row per platform per day | Total cost by platform |
| `fact_sales` | One row per transaction | Amount in USD |

### Dimension Tables
`dim_user` · `dim_product` · `dim_platform` · `dim_date` · `dim_influencer`

---
## 📊 Dashboard Features

- **ROAS per Platform** — Which ad platform gives the best return
- **CAC Over Time** — How much it costs to acquire one customer
- **Live Revenue Chart** — Updates automatically when new data arrives
- **Live Sales Feed** — Real-time transaction table
- **Auto-refresh** — Every 10 seconds, no manual reload needed

---
## ⚡ Lambda Architecture (Real-Time Updates)

Drop a CSV into `data/incoming/` and watch the dashboard update live:
```
New CSV dropped into data/incoming/
           ↓
    Watchdog detects file
           ↓
    Process only NEW rows (incremental)
           ↓
    UPSERT into PostgreSQL (INSERT ON CONFLICT)
           ↓
    Dashboard auto-refreshes within 10 seconds
```

---

## 🚀 How to Run

### Prerequisites
```bash
pip install pyspark prefect streamlit plotly pandas 
            sqlalchemy psycopg2-binary watchdog
```

PostgreSQL must be installed and running locally.

### 1. Set up the database
```sql
CREATE DATABASE buzzburst_gold;
```

### 2. Run the full pipeline
```bash
python dags/dag.py
```

### 3. Start the live watchdog
```bash
python scripts/lambda_processor.py
```

### 4. Launch the dashboard
```bash
streamlit run dashboard/dashboard.py
```

### 5. Simulate live data
```bash
copy "data\incoming\test_data\new_sales_dump.csv" "data\incoming\sales_dump.csv"
```

Watch the dashboard update in real-time.

---

## 📁 Project Structure
```
BuzzBurst-Data-Platform/
├── scripts/
│   ├── bronze_ingest.py        # Raw CSV → Parquet
│   ├── silver_transform.py     # Clean + structure data
│   ├── gold.py                 # Star Schema + business metrics
│   ├── gold_to_postgres.py     # Load Gold → PostgreSQL
│   └── lambda_processor.py     # Real-time incremental updates
├── dags/
│   └── dag.py                  # Prefect orchestration DAG
├── dashboard/
│   └── dashboard.py            # Streamlit live dashboard
├── data/
│   ├── cloud_source/           # Raw input CSVs
│   ├── bronze/                 # Bronze Parquet files
│   ├── silver/                 # Silver Parquet files
│   ├── gold/                   # Gold Parquet files
│   └── incoming/               # Drop new CSVs here
└── README.md
```

---

## 🧹 Key Data Engineering Challenges Solved

**1. Identity Resolution**
Recognized `"fb"`, `"facebook"`, `"facebã¶k"` as the same platform
using standardization logic

**2. Currency Normalization**
Converted EUR → USD using regex amount extraction + exchange rate

**3. JSON Flattening**
Exploded `{"handle":"@buzz","reach":5000}` string into queryable columns
using PySpark's `from_json()`

**4. Multi-format Date Parsing**
Handled `01/15/2026` vs `15/01/2026` ambiguity using dual-parse logic

**5. FK Integrity Enforcement**
Zero orphan records using `left_anti` join validation before every Gold write

**6. UNKNOWN User Pattern**
Preserved 100% of sales data by mapping unmatched users to a 
sentinel `user_key=0` row

---

## 📈 Key Results

| Metric | Value |
|--------|-------|
| Total transactions processed | 500 |
| Dimensions built | 5 |
| Fact tables | 3 |
| Days of data | 42 |
| Best ROAS day | 5.87x |
| Worst ROAS day | 0.21x |
| Pipeline run time | ~3 minutes |

---

## 👤 Author

**Asad Ur Rehman**  
[LinkedIn](https://www.linkedin.com/in/asad-ur-rehman-108439285/) · [GitHub](https://github.com/Asad-Ur-R)

---

## NOTE: Data folders are not uploaded. Add source CSV files to `data/cloud_source/`
