import dlt
from pyspark.sql.functions import col, lower, when, lit, expr

# ---------------------------------------------------------
# Read runtime configuration from pipeline
# ---------------------------------------------------------
CATALOG = spark.conf.get("rsclp.catalog")
SCHEMA = spark.conf.get("rsclp.schema")
SECRET_SCOPE = spark.conf.get("rsclp.secret.scope")

BASE_PATH = dbutils.secrets.get(
    scope='rsclp-scope',
    key="master-data-path"
)

BRONZE_PATH = f"{BASE_PATH}/product/product_updates/"
SILVER_PATH = f"{BASE_PATH}/product/product_master/"

# =========================================================
# 1️⃣ Bronze Layer – Product Updates
# =========================================================
@dlt.table(
    name=f"{CATALOG}.{SCHEMA}.product_updates",
    comment="Raw product updates ingested from Excel uploads converted to CSV"
)
def product_updates():
    df = (
        spark.readStream
        .format("delta")
        .option("header", True)
        .load(BRONZE_PATH)
    )

    df = df.withColumn(
        "DeleteFlag",
        when(lower(col("DeleteFlag")) == "true", lit(True))
        .when(lower(col("DeleteFlag")) == "false", lit(False))
        .otherwise(lit(False))
    )

    return df

# =========================================================
# 2️⃣ Silver Layer – Product Master (SCD Type 2)
# =========================================================
dlt.create_target_table(
    name=f"{CATALOG}.{SCHEMA}.product_master",
    comment="SCD Type 2 Product Master with full history"
)

dlt.apply_changes(
    target=f"{CATALOG}.{SCHEMA}.product_master",
    source=f"{CATALOG}.{SCHEMA}.product_updates",
    keys=["ProductID"],
    sequence_by=col("StartDate"),
    stored_as_scd_type="2",
    apply_as_deletes=expr("DeleteFlag = true"),
    except_column_list=[
        "DeleteFlag",
        "source_file",
        "sheet_name",
        "load_date",
        "ingested_at",
        "StartDate"
    ]
)
