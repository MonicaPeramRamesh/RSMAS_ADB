# Databricks notebook source
import dlt
from pyspark.sql.functions import (
    col, lower, when, lit, current_timestamp, expr, coalesce, to_timestamp
)

# ---------------------------------------------------------
# Read runtime configuration from pipeline
# ---------------------------------------------------------
CATALOG = spark.conf.get("rsclp.catalog")
SCHEMA = spark.conf.get("rsclp.schema")

# =========================================================
# 1️⃣ Paths
# =========================================================
supplier_pricehistory_path = dbutils.secrets.get(scope='rsclp-scope', key='master-data-path')+ "product/supplier_costprice_updates/"

# =========================================================
# 2️⃣ Bronze Layer – Raw Product Price Updates
# =========================================================
@dlt.table(
    name=f"{CATALOG}.{SCHEMA}.supplier_costprice_updates",
    comment="Raw product price updates ingested from Excel to Delta staging"
)
def product_price_updates():
    # ✅ Static read to ensure schema stability
    df = (
        spark.readStream.format("delta")
        .option("header", True)
        .load(supplier_pricehistory_path)
    )

    # ✅ Normalize IsActive into DeleteFlag (DLT expects this pattern)
    df = df.withColumn(
        "DeleteFlag",
        when(lower(col("IsActive")) == "false", lit(True))
        .when(lower(col("IsActive")) == "true", lit(False))
        .otherwise(lit(False))
    )

    return df


# =========================================================
# 3️⃣ Silver Layer – Product Price History (SCD Type 2)
# =========================================================

# Create target table if not exists
try:
    dlt.create_target_table(
        name=f"{CATALOG}.{SCHEMA}.supplier_costprice_history",
        comment="SCD Type 2 table storing full history of product price changes per store"
    )
except Exception as e:
    print(f"Target table already exists: {e}")

# =========================================================
# 4️⃣ Apply SCD Type 2 logic
# =========================================================
dlt.apply_changes(
    target=f"{CATALOG}.{SCHEMA}.supplier_costprice_history",
    source=f"{CATALOG}.{SCHEMA}.supplier_costprice_updates",
    keys=["ProductID"],                 # Business keys
    sequence_by=col("UpdatedOn"),                # Sequencing column
    stored_as_scd_type="2",                        # Type 2 = history tracking
    apply_as_deletes=expr("DeleteFlag = true"),    # Handle inactive products
    except_column_list=[
        "DeleteFlag", "source_file", "sheet_name","IsActive",
        "load_date", "ingested_at", "UpdatedOn"
    ]
)
