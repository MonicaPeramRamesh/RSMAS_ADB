import dlt
from pyspark.sql.functions import col, lower, when, lit, expr

bronze_path = dbutils.secrets.get(scope='rsmas-dev-scope', key='master-data-path')+"/product/product_updates/"
silver_path = dbutils.secrets.get(scope='rsmas-dev-scope', key='master-data-path')+"product/product_master/"

# =========================================================
# 1️⃣  Bronze Layer - Product Updates
# =========================================================
@dlt.table(
    name="dev_rsmas_catalog.rsmas_productmaster_schema.product_updates",
    comment="Raw product updates ingested from Excel uploads converted to CSV"
)
def product_updates():
    df = (
        spark.readStream.format("delta")
        .option("header", True)
        .load(bronze_path)
    )
    
    # Normalize DeleteFlag
    df = df.withColumn(
        "DeleteFlag",
        when(lower(col("DeleteFlag")) == "true", lit(True))
        .when(lower(col("DeleteFlag")) == "false", lit(False))
        .otherwise(lit(False))  # default to False if null or missing
    )
    
    return df

# =========================================================
# 2️⃣  Silver Layer - Product Master (SCD Type 2)
# =========================================================
dlt.create_target_table(
    name="dev_rsmas_catalog.rsmas_productmaster_schema.product_master",
    comment="SCD Type 2 Product Master table storing full history of inserts, updates, and deletes",
)

dlt.apply_changes(
    target="dev_rsmas_catalog.rsmas_productmaster_schema.product_master",
    source="dev_rsmas_catalog.rsmas_productmaster_schema.product_updates",
    keys=["ProductID"],
    sequence_by=col("StartDate"),
    stored_as_scd_type="2",
    apply_as_deletes=expr("DeleteFlag = true"),  # ✅ simplified
    except_column_list=["DeleteFlag", "source_file", "sheet_name", "load_date","ingested_at", "StartDate"]
)
