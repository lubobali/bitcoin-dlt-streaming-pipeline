# Databricks notebook source
# MAGIC %md
# MAGIC # Bitcoin Trade Analysis — Plotly Charts
# MAGIC
# MAGIC Visual analysis of the Bitcoin trade data processed by our DLT pipeline.
# MAGIC Reads from the Silver and Quarantine tables in `bootcamp_students.lubo_btc`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup

# COMMAND ----------

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd

# Configure your catalog.schema here
CATALOG_SCHEMA = "your_catalog.your_schema"

# Read from pipeline tables
silver_df = spark.table(f"{CATALOG_SCHEMA}.btc_trades_silver").toPandas()
quarantine_df = spark.table(f"{CATALOG_SCHEMA}.btc_trades_quarantine").toPandas()

print(f"Silver (clean) trades: {len(silver_df)}")
print(f"Quarantined (bad) trades: {len(quarantine_df)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chart 1: BTC Price Over Time

# COMMAND ----------

fig = px.line(
    silver_df.sort_values("trade_time"),
    x="trade_time",
    y="price",
    title="BTC-USD Trade Price Over Time",
    labels={"trade_time": "Time", "price": "Price (USD)"},
    markers=True,
)
fig.update_layout(
    template="plotly_dark",
    height=500,
    yaxis_tickprefix="$",
    yaxis_tickformat=",.2f",
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chart 2: Trade Volume (BTC) Per Trade

# COMMAND ----------

fig = px.bar(
    silver_df.sort_values("trade_time"),
    x="trade_time",
    y="size",
    title="BTC Trade Volume Per Trade",
    labels={"trade_time": "Time", "size": "Volume (BTC)"},
    color="size",
    color_continuous_scale="Viridis",
)
fig.update_layout(
    template="plotly_dark",
    height=500,
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chart 3: Trade Value (USD) Distribution

# COMMAND ----------

fig = px.histogram(
    silver_df,
    x="trade_value_usd",
    nbins=15,
    title="Distribution of Trade Values (USD)",
    labels={"trade_value_usd": "Trade Value (USD)", "count": "Number of Trades"},
    color_discrete_sequence=["#00d4aa"],
)
fig.update_layout(
    template="plotly_dark",
    height=500,
    xaxis_tickprefix="$",
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chart 4: Price vs Volume Scatter

# COMMAND ----------

fig = px.scatter(
    silver_df,
    x="price",
    y="size",
    size="trade_value_usd",
    color="exchange_id",
    title="Price vs Volume (bubble size = USD value)",
    labels={"price": "Price (USD)", "size": "Volume (BTC)", "exchange_id": "Exchange"},
    color_continuous_scale="Plasma",
)
fig.update_layout(
    template="plotly_dark",
    height=500,
    xaxis_tickprefix="$",
    xaxis_tickformat=",.2f",
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chart 5: Data Quality — Clean vs Quarantined

# COMMAND ----------

quality_data = pd.DataFrame({
    "Category": ["Clean (Silver)", "Quarantined"],
    "Count": [len(silver_df), len(quarantine_df)],
})

fig = px.pie(
    quality_data,
    values="Count",
    names="Category",
    title="Data Quality: Clean vs Quarantined Trades",
    color="Category",
    color_discrete_map={"Clean (Silver)": "#00d4aa", "Quarantined": "#ff4444"},
    hole=0.4,
)
fig.update_layout(
    template="plotly_dark",
    height=500,
)
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Chart 6: Quarantine Reasons Breakdown

# COMMAND ----------

if len(quarantine_df) > 0:
    fig = px.bar(
        quarantine_df.groupby("quarantine_reason").size().reset_index(name="count"),
        x="quarantine_reason",
        y="count",
        title="Why Trades Were Quarantined",
        labels={"quarantine_reason": "Reason", "count": "Number of Trades"},
        color="quarantine_reason",
        color_discrete_sequence=["#ff4444", "#ff8800", "#ffcc00"],
    )
    fig.update_layout(
        template="plotly_dark",
        height=500,
        showlegend=False,
    )
    fig.show()
else:
    print("No quarantined trades to display.")
