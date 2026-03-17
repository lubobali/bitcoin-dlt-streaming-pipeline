# Databricks notebook source
# MAGIC %md
# MAGIC # Bitcoin Real-Time Streaming Pipeline — Delta Live Tables
# MAGIC
# MAGIC This pipeline ingests **real-time Bitcoin trade data** from the
# MAGIC [Polygon.io WebSocket API](https://polygon.io/docs/crypto/ws_getting-started)
# MAGIC and processes it through a **Bronze → Silver → Gold** medallion architecture
# MAGIC using Delta Live Tables.
# MAGIC
# MAGIC The pipeline uses the **quarantine pattern** to route bad rows to a
# MAGIC separate table instead of silently dropping them.
# MAGIC
# MAGIC | Layer      | Description |
# MAGIC |------------|-------------|
# MAGIC | Bronze     | Raw trade messages from Polygon WebSocket |
# MAGIC | View       | Intermediate enriched view (pre-split for quality routing) |
# MAGIC | Silver     | Validated trades — only rows passing all quality checks |
# MAGIC | Quarantine | Failed trades — rows that didn't pass quality checks |
# MAGIC | Gold       | 1-minute OHLCV candlestick aggregations |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Imports & Setup

# COMMAND ----------

import dlt
from pyspark.sql import functions as F
from pyspark.sql import types as T
from databricks.sdk.runtime import dbutils

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Set these as **pipeline parameters** in the DLT pipeline settings UI:
# MAGIC
# MAGIC | Key | Default | Description |
# MAGIC |-----|---------|-------------|
# MAGIC | `secret_scope`    | `polygon`  | Databricks secret scope containing the API key |
# MAGIC | `secret_key`      | `api_key`  | Secret key name for the Polygon API key |
# MAGIC | `polygon_ws_url`  | `wss://socket.polygon.io/crypto` | WebSocket endpoint |
# MAGIC | `polygon_raw_path`| `/mnt/polygon/btc_trades/` | Cloud storage path for raw trade JSON |

# COMMAND ----------

# Pipeline parameters (set in DLT pipeline configuration)
SECRET_SCOPE = spark.conf.get("secret_scope", "polygon")
SECRET_KEY = spark.conf.get("secret_key", "api_key")
POLYGON_API_KEY = dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)

POLYGON_WS_URL = spark.conf.get("polygon_ws_url", "wss://socket.polygon.io/crypto")
SUBSCRIPTION_CHANNEL = "XT.X:BTC-USD"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trade Message Schema
# MAGIC
# MAGIC Defines the expected schema for Polygon crypto trade messages.
# MAGIC Reference: https://polygon.io/docs/crypto/ws_crypto_trades

# COMMAND ----------

trade_schema = T.StructType([
    T.StructField("ev", T.StringType(), True),       # Event type
    T.StructField("pair", T.StringType(), True),      # Crypto pair
    T.StructField("p", T.DoubleType(), True),         # Price
    T.StructField("s", T.DoubleType(), True),         # Size
    T.StructField("t", T.LongType(), True),           # Timestamp (Unix ms)
    T.StructField("c", T.ArrayType(T.IntegerType()), True),  # Conditions
    T.StructField("i", T.StringType(), True),         # Trade ID
    T.StructField("x", T.IntegerType(), True),        # Exchange ID
    T.StructField("r", T.LongType(), True),           # Received timestamp
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helper — Polygon Stream Reader
# MAGIC
# MAGIC Reads raw Polygon WebSocket trade messages that have been written
# MAGIC as JSON files to cloud storage by a separate ingest job.

# COMMAND ----------

def get_polygon_stream():
    """
    Reads from cloud storage where a separate ingest job writes
    raw Polygon WebSocket messages as JSON lines.
    Uses Spark Structured Streaming with the JSON format.
    """
    raw_path = spark.conf.get("polygon_raw_path", "/mnt/polygon/btc_trades/")

    return (
        spark.readStream
        .format("json")
        .schema(trade_schema)
        .option("maxFilesPerTrigger", 1)
        .load(raw_path)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — Raw Bitcoin Trade Events

# COMMAND ----------

@dlt.table(
    name="btc_trades_bronze",
    comment="Raw Bitcoin trade events from Polygon.io WebSocket.",
    table_properties={"quality": "bronze"},
)
def btc_trades_bronze():
    raw = get_polygon_stream()

    return (
        raw
        .select(
            F.col("ev").alias("event_type"),
            F.col("pair").alias("crypto_pair"),
            F.col("p").alias("price"),
            F.col("s").alias("size"),
            F.col("t").alias("trade_timestamp_ms"),
            F.col("c").alias("conditions"),
            F.col("i").alias("trade_id"),
            F.col("x").alias("exchange_id"),
            F.col("r").alias("received_timestamp_ms"),
        )
        .withColumn(
            "trade_time",
            (F.col("trade_timestamp_ms") / 1000).cast("timestamp"),
        )
        .withColumn("ingest_time", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Intermediate View — Enriched Trades (Pre-Split)
# MAGIC
# MAGIC All bronze trades are enriched here first, then split into clean (silver)
# MAGIC and quarantine tables based on data quality checks. This avoids reading
# MAGIC bronze twice and ensures silver + quarantine are complementary.

# COMMAND ----------

@dlt.view(
    name="btc_trades_enriched_v",
    comment="Intermediate view: all enriched trades before quality split.",
)
def btc_trades_enriched_v():
    bronze = dlt.read_stream("btc_trades_bronze")

    return (
        bronze
        .select(
            "trade_id",
            "event_type",
            "crypto_pair",
            "price",
            "size",
            F.round(F.col("price") * F.col("size"), 2).alias("trade_value_usd"),
            "trade_time",
            "exchange_id",
            "conditions",
            "ingest_time",
        )
        .withColumn("enriched_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — Validated Trades (Clean Rows Only)
# MAGIC
# MAGIC Only trades that pass ALL data quality checks make it to silver.
# MAGIC Bad rows are routed to the quarantine table instead.

# COMMAND ----------

@dlt.table(
    name="btc_trades_silver",
    comment="Validated Bitcoin trades — only rows passing all quality checks.",
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("valid_price", "price IS NOT NULL AND price > 0")
@dlt.expect_or_drop("valid_size", "size IS NOT NULL AND size > 0")
@dlt.expect_or_drop("valid_trade_time", "trade_time IS NOT NULL")
@dlt.expect_or_drop("valid_event_type", "event_type = 'XT'")
def btc_trades_silver():
    return dlt.read_stream("btc_trades_enriched_v")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine — Failed Quality Checks
# MAGIC
# MAGIC Captures trades that fail silver-layer validation so they can be
# MAGIC investigated later. Routes trades with null prices, zero sizes,
# MAGIC or unexpected event types to a quarantine table.
# MAGIC
# MAGIC This table can be used for:
# MAGIC - **Debugging**: Identify malformed trade messages
# MAGIC - **Reprocessing**: Retry after fixing upstream issues
# MAGIC - **Monitoring**: Alert if quarantine volume exceeds a threshold

# COMMAND ----------

@dlt.table(
    name="btc_trades_quarantine",
    comment="Quarantined Bitcoin trades that failed data quality checks.",
    table_properties={"quality": "quarantine"},
)
def btc_trades_quarantine():
    enriched = dlt.read_stream("btc_trades_enriched_v")

    return (
        enriched
        .filter(
            (F.col("price").isNull()) |
            (F.col("price") <= 0) |
            (F.col("size").isNull()) |
            (F.col("size") <= 0) |
            (F.col("trade_time").isNull()) |
            (F.col("event_type") != "XT")
        )
        .withColumn(
            "quarantine_reason",
            F.when(F.col("price").isNull() | (F.col("price") <= 0), "Invalid price")
             .when(F.col("size").isNull() | (F.col("size") <= 0), "Invalid size")
             .when(F.col("trade_time").isNull(), "Missing trade time")
             .when(F.col("event_type") != "XT", "Unexpected event type")
             .otherwise("Unknown"),
        )
        .withColumn("quarantined_at", F.current_timestamp())
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — 1-Minute OHLCV Candlesticks
# MAGIC
# MAGIC Aggregates silver trades into 1-minute candlestick bars with:
# MAGIC - **O**pen, **H**igh, **L**ow, **C**lose prices
# MAGIC - **V**olume (total BTC traded)
# MAGIC - Trade count
# MAGIC - Total USD value

# COMMAND ----------

@dlt.table(
    name="btc_candles_1m",
    comment="1-minute OHLCV candlestick bars for BTC-USD.",
    table_properties={"quality": "gold"},
)
def btc_candles_1m():
    silver = (
        dlt.read_stream("btc_trades_silver")
        .withWatermark("trade_time", "2 minutes")
    )

    return (
        silver
        .groupBy(
            F.window("trade_time", "1 minute").alias("candle_window"),
        )
        .agg(
            F.first("price").alias("open"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.last("price").alias("close"),
            F.sum("size").alias("volume_btc"),
            F.sum("trade_value_usd").alias("volume_usd"),
            F.count("*").alias("trade_count"),
        )
        .select(
            F.col("candle_window.start").alias("candle_start"),
            F.col("candle_window.end").alias("candle_end"),
            "open",
            "high",
            "low",
            "close",
            "volume_btc",
            "volume_usd",
            "trade_count",
        )
    )
