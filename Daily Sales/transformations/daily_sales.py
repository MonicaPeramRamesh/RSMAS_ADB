"""
================================================================================
Retail Sales Management System - Delta Live Tables Pipeline
================================================================================
Purpose: Process daily sales data through Bronze -> Silver -> Gold layers
Pre-requisites: 
  - ADF pipeline validates schema (column names, count) before triggering DLT
  - Product master table must exist: dev_rsclp_catalog.rsclp_productmaster_schema.product_master
  
Layers:
  - Bronze: Raw ingestion from ADLS CSV files
  - Silver: Cleansed, exploded, enriched with product master
  - Gold: Aggregated metrics for analytics
  - Quarantine: Invalid/rejected records
  
Author: Data Engineering Team
Last Updated: 2025-01-15
================================================================================
"""

from datetime import datetime
from zoneinfo import ZoneInfo
import logging

import dlt
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType
)

# Current processing date
processing_date = datetime.now(ZoneInfo("Europe/London")).date()

# ---------------------------------------------------------
# Read runtime configuration from pipeline
# ---------------------------------------------------------
CATALOG = spark.conf.get("rsclp.catalog")
SCHEMA = spark.conf.get("rsclp.schema")

# ---------------------------
# Logging setup
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(stage)s] %(message)s'
)
logger = logging.getLogger("dlt_daily_sales")

def log(stage: str, message: str, level: str = "info"):
    extra = {"stage": stage}
    if level == "info":
        logger.info(message, extra=extra)
    elif level == "warning":
        logger.warning(message, extra=extra)
    elif level == "error":
        logger.error(message, extra=extra)
    else:
        logger.info(message, extra=extra)

# ------------------------------
# Secret Retrieval with Error Handling
# ------------------------------
try:
    bronze_path = dbutils.secrets.get(scope='rsclp-scope', key='bronze-path')
    silver_path = dbutils.secrets.get(scope='rsclp-scope', key='silver-path')
    gold_path   = dbutils.secrets.get(scope='rsclp-scope', key='gold-path')
    source_path = dbutils.secrets.get(scope='rsclp-scope', key='source-path')
    jdbc_url = dbutils.secrets.get(scope='rsclp-scope', key='jdbc-url')
    database = dbutils.secrets.get(scope='rsclp-scope', key='rsclp-database')
    username = dbutils.secrets.get(scope='rsclp-scope', key='rsclp-db-username')
    password = dbutils.secrets.get(scope='rsclp-scope', key='rsclp-db-password')
    driver = dbutils.secrets.get(scope='rsclp-scope', key='rsclp-db-driver')
    hostname = dbutils.secrets.get(scope='rsclp-scope', key='rsclp-db-hostname')
    port = dbutils.secrets.get(scope='rsclp-scope', key='rsclp-db-port')
    log("INIT", "Secrets retrieved successfully")
except Exception as e:
    log("INIT", f"CRITICAL: Failed to retrieve secrets: {e}", level="error")
    raise

# ------------------------------
# Schema Definitions
# ------------------------------
sales_schema = StructType([
    StructField('TransactionID', StringType(), True),
    StructField('TransactionDateTime', StringType(), True),
    StructField('StoreID', StringType(), True),
    StructField('ProductDetailsJson', StringType(), True),
    StructField('PaymentMethod', StringType(), True)
])

product_schema = ArrayType(StructType([
    StructField('ProductID', StringType(), True),
    StructField('Quantity', IntegerType(), True),
    StructField('UnitPrice', IntegerType(), True)
]))

# Quality expectations for Silver layer
daily_sales_silver_rules = {
    "valid_product": "ProductID IS NOT NULL",
    "valid_quantity": "Quantity > 0",
    "valid_price": "UnitPrice > 0"
}

# ------------------------------
# BRONZE LAYER: Raw Data Ingestion
# ------------------------------
@dlt.table(
    name=f'{CATALOG}.rsclp_bronze_schema.daily_sales',
    comment='Bronze layer: Raw incremental ingestion of daily sales from ADLS (schema pre-validated by ADF)',
    partition_cols=['TransactionDate'],
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "data_classification": "raw",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "TransactionDate,StoreID",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.deletedFileRetentionDuration": "interval 7 days"
    }
)
def daily_sales_bronze():
    """
    Bronze layer: Raw data ingestion from CSV files.
    
    Features:
    - Cloud Files auto loader for scalable ingestion
    - Minimal transformations (only TransactionDate parsing)
    - Audit columns: LoadTimestamp, SourceFileName, ProcessingDate
    - Partitioned by TransactionDate for query performance
    - Schema strictly enforced (pre-validated by ADF)
    
    Note: ADF pipeline validates column names and count before triggering this DLT
    
    Returns:
        DataFrame: Raw sales data with audit columns
    """
    log("BRONZE", "Starting raw ingestion")
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", ";")
        .option("cloudFiles.includeExistingFiles", "true")
        .schema(sales_schema)
        .load(f"{source_path}/daily_sales/")
        .withColumn("TransactionDate", F.to_date(F.to_timestamp(F.col("TransactionDateTime"), "yyyy-MM-dd HH:mm:ss")))
        .withColumn("LoadTimestamp", F.from_utc_timestamp(F.current_timestamp(), "Europe/London"))
        .withColumn("SourceFileName", F.col("_metadata.file_path"))
        .withColumn("ProcessingDate", F.lit(processing_date))
    )
    log("BRONZE", "Raw ingestion completed")
    return df

# ---------------------------
# parsed_sales_view: parse JSON once (may be null)
# ---------------------------
@dlt.view(
    name="parsed_sales_view",
    comment="Parsed JSON view; parsed_json can be null for invalid JSON"
)
def parsed_sales_view():
    """
    Parse ProductDetailsJson into an array (parsed_json). Keep original rows.
    parsed_json = NULL => invalid JSON
    parsed_json != NULL => can be exploded
    """
    log("INTERMEDIATE", "Parsing ProductDetailsJson into parsed_json (single parse)")
    bronze_df = dlt.read_stream(f"{CATALOG}.rsclp_bronze_schema.daily_sales")
    parsed = bronze_df.withColumn("parsed_json", F.from_json(F.col("ProductDetailsJson"), product_schema))
    log("INTERMEDIATE", "parsed_sales_view created")
    return parsed

# ---------------------------
# exploded_sales_view: explode parsed_json once (used by Silver & Quarantine)
# ---------------------------
@dlt.view(
    name="exploded_sales_view",
    comment="Exploded product-level rows (uses parsed_sales_view and filters valid JSON)"
)
def exploded_sales_view():
    log("INTERMEDIATE", "Creating exploded_sales_view from parsed_sales_view")
    parsed = dlt.read_stream("parsed_sales_view")
    exploded = (
        parsed
        .filter(F.col("parsed_json").isNotNull())
        .withColumn("product_obj", F.explode(F.col("parsed_json")))
        .withColumn("ProductID", F.col("product_obj.ProductID"))
        .withColumn("Quantity", F.col("product_obj.Quantity"))
        .withColumn("UnitPrice", F.col("product_obj.UnitPrice"))
        .withColumn("TotalAmount", F.col("Quantity") * F.col("UnitPrice"))
        .select(
            "TransactionID",
            "TransactionDateTime",
            "TransactionDate",
            "StoreID",
            "ProductDetailsJson",
            "ProductID",
            "Quantity",
            "UnitPrice",
            "TotalAmount",
            "PaymentMethod",
            "LoadTimestamp",
            "SourceFileName",
            "ProcessingDate"
        )
    )
    log("INTERMEDIATE", "exploded_sales_view created")
    return exploded

# ------------------------------
# SILVER LAYER: Cleansed & Enriched Data
# ------------------------------
@dlt.table(
    name=f"{CATALOG}.rsclp_silver_schema.daily_sales",
    comment='Silver layer: Cleansed sales data with exploded JSON, enriched with product master, quality validated',
    partition_cols=['TransactionDate'],
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "data_classification": "cleansed",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "TransactionDate,StoreID,ProductID",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true",  # Enable CDC for downstream consumption
        "delta.deletedFileRetentionDuration": "interval 7 days"
    }
)
@dlt.expect_all(daily_sales_silver_rules)
def daily_sales_silver():
    """
    Silver layer: Business logic transformations and data enrichment.
    
    Transformations:
    1. Parse ProductDetailsJson (array of products)
    2. Explode array to individual product lines
    3. Calculate TotalAmount per product line
    4. Enrich with product master (ProductName, Category)
    5. Apply quality rules via DLT expectations
    6. Inner join filters out invalid products (sent to quarantine)
    
    Quality Rules:
    - ProductID must not be null
    - Quantity must be > 0
    - UnitPrice must be > 0
    
    Returns:
        DataFrame: Cleansed and enriched sales data at product line level
    """
    log("SILVER", "Reading exploded_sales_view for silver processing")
    exploded = dlt.read_stream("exploded_sales_view")

    # Filter to only valid product-lines (these become Silver)
    filtered = exploded.dropna(subset=[
    "TransactionID", "TransactionDateTime", "TransactionDate", 
    "StoreID", "ProductID", "Quantity", "UnitPrice"
    ]).filter(
    (F.col("Quantity") > 0) & (F.col("UnitPrice") > 0)
    )

    log("SILVER", "Reading product master and broadcasting for enrichment")
    product_df = spark.read.table(f"{CATALOG}.rsclp_productmaster_schema.product_master")
    product_small = product_df.select(
        "ProductID", "ProductName", "CategoryID", "DepartmentID", "Description", "UnitOfMeasure", "__START_AT", "__END_AT"
    ).filter(F.col('__END_AT').isNull())

    enriched = (
        filtered
        .join(F.broadcast(product_small), on="ProductID", how="inner")
        .select(
            "TransactionID",
            "TransactionDateTime",
            "TransactionDate",
            "StoreID",
            "ProductID",
            "ProductName",
            "CategoryID",
            "DepartmentID",
            "Description",
            "UnitOfMeasure",
            "Quantity",
            "UnitPrice",
            "TotalAmount",
            "PaymentMethod",
            "LoadTimestamp",
            "SourceFileName",
            "ProcessingDate"
        )
    )

    log("SILVER", "Silver enrichment completed")
    return enriched

# ------------------------------
# GOLD LAYER: Aggregated Analytics
# ------------------------------
@dlt.table(
    name=f'{CATALOG}.rsclp_gold_schema.daily_sales',
    comment='Gold layer: Aggregated daily sales metrics by date, store, product, and price point for analytics and reporting',
    partition_cols=["TransactionDate"],
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "data_classification": "aggregated",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "TransactionDate,StoreID",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.deletedFileRetentionDuration": "interval 30 days"
    }
)
def daily_sales_gold():
    """
    Gold layer: Aggregated business metrics for analytics and reporting.
    
    Aggregation Level:
    - TransactionDate
    - StoreID
    - ProductID (with ProductName, Category)
    - UnitPrice (to handle price changes within same day)
    
    Metrics:
    - TotalQuantity: Sum of all quantities sold
    - TotalAmount: Sum of all revenue
    - LoadTimestamp: Latest load time for this aggregate
    - FirstLoadTimestamp: First time this combination was loaded
    
    Use Cases:
    - Daily sales reporting
    - Store performance analysis
    - Product performance tracking
    - Price point analysis
    
    Returns:
        DataFrame: Aggregated sales metrics
    """
    log("GOLD", "Reading silver for aggregation")
    # 1. Read only new Silver rows (incremental)
    silver_stream = dlt.read_stream(f"{CATALOG}.rsclp_silver_schema.daily_sales")

    # 2. Add watermark (OPTIONAL - does NOT change what data is read)
    #    Allows Spark to discard very late events
    silver_stream = silver_stream.withWatermark("LoadTimestamp", "2 hours")

    # 3. Perform micro-batch aggregation on NEW data only
    agg_new = (
        silver_stream
        .groupBy(
            "TransactionDate",
            "StoreID",
            "ProductID",
            "ProductName",
            "CategoryID",
            "UnitPrice"
        )
        .agg(
            F.sum("Quantity").alias("TotalQuantity"),
            F.sum("TotalAmount").alias("TotalAmount"),
            F.max("LoadTimestamp").alias("LoadTimestamp"),
            F.max("ProcessingDate").alias("ProcessingDate")
        )
    )

    # 4. Return NEW aggregated records (duplicates allowed)
    return agg_new

# ------------------------------
# QUARANTINE LAYER: Invalid Records
# ------------------------------
@dlt.table(
    name=f'{CATALOG}.rsclp_invalid_data_schema.invalid_daily_sales',
    comment='Quarantine layer: All invalid/rejected sales records with comprehensive failure reasons for data quality monitoring',
    partition_cols=['TransactionDate'],
    table_properties={
        "quality": "quarantine",
        "layer": "quarantine",
        "data_classification": "invalid",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "TransactionDate,InvalidationType,Reason",
        "delta.deletedFileRetentionDuration": "interval 90 days"
    }
)
def invalid_daily_sales():
    """
    Quarantine layer: Captures all invalid records with detailed failure reasons.
    
    Invalid Record Categories:
    1. JSON_PARSE_ERROR: ProductDetailsJson cannot be parsed
    2. PRODUCT_NOT_FOUND: ProductID doesn't exist in product master
    3. QUALITY_RULE_VIOLATION: Failed DLT expectations (null ProductID, invalid Quantity/UnitPrice)
    
    Features:
    - Single JSON parse (efficient)
    - left_anti join for clean "not found" logic
    - Multiple quality violations combined into single reason string
    - InvalidationType for easy filtering and monitoring
    
    Use Cases:
    - Data quality monitoring
    - Root cause analysis
    - Vendor data issue identification
    - Reprocessing after data fixes
    
    Returns:
        DataFrame: All invalid records with failure reasons
    """
    log("QUARANTINE", "Collecting invalid records for quarantine")

    parsed_stream = dlt.read_stream("parsed_sales_view")
    exploded_stream = dlt.read_stream("exploded_sales_view")
    bronze_stream = dlt.read_stream(f"{CATALOG}.rsclp_bronze_schema.daily_sales")

    product_df = (
        spark.read.table(f"{CATALOG}.rsclp_productmaster_schema.product_master").filter(F.col('__END_AT').isNull())
            .select("ProductID")
            .cache()
    )

    # Extract folder date expression (reuse everywhere)
    folder_date_expr = F.regexp_extract(
        F.col("SourceFileName"),
        r"/daily_sales/(\d{4}-\d{2}-\d{2})/",
        1
    )

    # =====================================================================
    # 1. JSON Parse Errors
    # =====================================================================
    invalid_json = (
        parsed_stream.filter(F.col("parsed_json").isNull())
            .withColumn("ProductID", F.lit(None))
            .withColumn("Quantity", F.lit(None))
            .withColumn("UnitPrice", F.lit(None))
            .withColumn("Reason", F.lit("JSON parse failed"))
            .withColumn("InvalidationType", F.lit("JSON_PARSE_ERROR"))
            # Inlined TransactionDateCategory
            .withColumn(
                "TransactionDateCategory",
                F.when(
                    (F.col("TransactionDate").isNull()) |
                    (F.col("TransactionDate") == "") |
                    (F.col("TransactionDate") == "-"),
                    folder_date_expr
                ).otherwise(F.col("TransactionDate"))
            )
    )

    # =====================================================================
    # 2. Product Not Found
    # =====================================================================
    invalid_product = (
        exploded_stream.join(product_df, "ProductID", "left_anti")
            .withColumn("Reason", F.lit("Product not found"))
            .withColumn("InvalidationType", F.lit("PRODUCT_NOT_FOUND"))
            # Inlined TransactionDateCategory
            .withColumn(
                "TransactionDateCategory",
                F.when(
                    (F.col("TransactionDate").isNull()) |
                    (F.col("TransactionDate") == "") |
                    (F.col("TransactionDate") == "-"),
                    folder_date_expr
                ).otherwise(F.col("TransactionDate"))
            )
    )

    # =====================================================================
    # 3. Quality Violations
    # =====================================================================
    invalid_quality = (
    exploded_stream.filter(
        (F.col("ProductID").isNull()) |
        (F.col("ProductID") == "") |                    # Empty string check
        (F.col("Quantity").isNull()) |                  # ✅ Explicit NULL check
        (F.col("Quantity") <= 0) |
        (F.col("UnitPrice").isNull()) |                 # ✅ Explicit NULL check
        (F.col("UnitPrice") <= 0)
    )
    .withColumn(
        "Reason",
        F.concat_ws("; ",
            F.when(F.col("ProductID").isNull() | (F.col("ProductID") == ""), F.lit("ProductID null")),
            F.when(F.col("Quantity").isNull(), F.lit("Quantity null")),
            F.when((F.col("Quantity").isNotNull()) & (F.col("Quantity") <= 0), F.lit("Quantity <=0")),
            F.when(F.col("UnitPrice").isNull(), F.lit("UnitPrice null")),
            F.when((F.col("UnitPrice").isNotNull()) & (F.col("UnitPrice") <= 0), F.lit("UnitPrice <=0"))
        )
    )
    .withColumn("InvalidationType", F.lit("QUALITY_VIOLATION"))
    )

    # =====================================================================
    # 4. Missing Columns
    # =====================================================================
    missing_columns = (
        bronze_stream.filter(
            F.col("TransactionID").isNull() |
            F.col("TransactionDateTime").isNull() |
            F.col("StoreID").isNull() |
            F.col("PaymentMethod").isNull()
        )
        .withColumn(
            "Reason",
            F.concat_ws("; ",
                F.when(F.col("TransactionID").isNull(), F.lit("TransactionID null")),
                F.when(F.col("TransactionDateTime").isNull(), F.lit("DateTime null")),
                F.when(F.col("StoreID").isNull(),F.lit("StoreID is null")),
                F.when(F.col("PaymentMethod").isNull(),F.lit("PaymentMethod is null"))
            )
        )
        .withColumn("InvalidationType", F.lit("MISSING_COLUMNS"))
        .withColumn("ProductID", F.lit(None))
        .withColumn("Quantity", F.lit(None))
        .withColumn("UnitPrice", F.lit(None))
        # Inlined TransactionDateCategory
        .withColumn(
            "TransactionDateCategory",
            F.when(
                (F.col("TransactionDate").isNull()) |
                (F.col("TransactionDate") == "") |
                (F.col("TransactionDate") == "-"),
                folder_date_expr
            ).otherwise(F.col("TransactionDate"))
        )
    )

    # =====================================================================
    # Final union
    # =====================================================================
    invalid_sales_final = (
        invalid_json
        .unionByName(invalid_product, allowMissingColumns=True)
        .unionByName(invalid_quality, allowMissingColumns=True)
        .unionByName(missing_columns, allowMissingColumns=True)
    )

    return invalid_sales_final
    

# ================================================================================
# MONITORING VIEWS
# ================================================================================

# ------------------------------
# Invalid Sales Summary View
# ------------------------------
@dlt.table(
    name=f'{CATALOG}.rsclp_monitor_schema.invalid_sales_summary',
    comment='Monitoring view: Aggregated invalid sales by date, type, and reason - for data quality dashboards'
)
def invalid_sales_summary():
    """
    Invalid Sales Summary: Aggregates quarantine records for monitoring.
    
    Groups by:
    - TransactionDate
    - InvalidationType (JSON_PARSE_ERROR, PRODUCT_NOT_FOUND, QUALITY_RULE_VIOLATION)
    - Reason (specific failure reason)
    
    Metrics:
    - InvalidCount: Number of invalid records
    - FirstSeenTimestamp: When this issue first appeared
    - LastSeenTimestamp: Most recent occurrence
    - AffectedFileCount: Number of source files with this issue
    
    Use Cases:
    - Daily data quality reports
    - Alerting on unusual invalid record spikes
    - Trending data quality over time
    
    Returns:
        DataFrame: Aggregated invalid sales summary
    """
    quarantine_stream = dlt.read_stream(f"{CATALOG}.rsclp_invalid_data_schema.invalid_daily_sales") \
                           .withWatermark("LoadTimestamp", "1 hour")
    return (quarantine_stream.groupBy("TransactionDateCategory", "InvalidationType", "Reason")
            .agg(
                F.count("*").alias("InvalidCount"),
                F.min("LoadTimestamp").alias("FirstSeen"),
                F.max("LoadTimestamp").alias("LastSeen"),
                F.approx_count_distinct("SourceFileName").alias("AffectedFiles")
            ))
    


# ------------------------------
# Pipeline Health Metrics View
# ------------------------------
@dlt.table(
    name=f'{CATALOG}.rsclp_monitor_schema.daily_pipeline_metrics',
    comment='Monitoring view: Daily pipeline health metrics - valid vs invalid records with success rate'
)
def daily_pipeline_metrics():
    """
    Pipeline Health Metrics: Compares valid vs invalid records by date.

    Metrics:
    - ValidRecords: Count of records that passed to Silver layer
    - InvalidRecords: Count of records in quarantine
    - TotalRecords: Sum of valid + invalid
    - SuccessRate: Percentage of valid records (ValidRecords/TotalRecords * 100)

    Use Cases:
    - Daily pipeline health checks
    - SLA monitoring (e.g., success rate must be > 95%)
    - Trend analysis over time
    - Alerting on degraded data quality

    Returns:
        DataFrame: Daily pipeline metrics
    """
    # VALID RECORDS
    silver = (dlt.read_stream(f"{CATALOG}.rsclp_silver_schema.daily_sales")
                .withWatermark("LoadTimestamp", "1 hour")
                .withColumn("is_valid", F.lit(1))
                .withColumn("is_invalid", F.lit(0))
                # Add date category
                .withColumn(
                    "TransactionDateCategory",
                    F.when(F.col("TransactionDate").isNotNull(), F.col("TransactionDate"))
                     .otherwise(
                         F.regexp_extract(F.col("SourceFileName"), r"/daily_sales/(\d{4}-\d{2}-\d{2})/", 1)
                     )
                )
                .withColumn(
                    "TransactionDateCategory",
                    F.when(F.col("TransactionDateCategory") == "", F.lit("UNKNOWN"))
                     .otherwise(F.col("TransactionDateCategory"))
                )
                .select("TransactionDateCategory", "is_valid", "is_invalid")
    )

    invalid = (dlt.read_stream(f"{CATALOG}.rsclp_invalid_data_schema.invalid_daily_sales")
                  .withWatermark("LoadTimestamp", "1 hour")
                  .withColumn("is_valid", F.lit(0))
                  .withColumn("is_invalid", F.lit(1))
                  .withColumn(
                      "TransactionDateCategory",
                      F.when(F.col("TransactionDate").isNotNull(), F.col("TransactionDate"))
                       .otherwise(
                           F.regexp_extract(F.col("SourceFileName"), r"/daily_sales/(\d{4}-\d{2}-\d{2})/", 1)
                       )
                  )
                  .withColumn(
                      "TransactionDateCategory",
                      F.when(F.col("TransactionDateCategory") == "", F.lit("UNKNOWN"))
                       .otherwise(F.col("TransactionDateCategory"))
                  )
                  .select("TransactionDateCategory", "is_valid", "is_invalid")
    )

    combined = silver.unionByName(invalid)

    aggregated = (combined.groupBy("TransactionDateCategory")
                        .agg(
                            F.sum("is_valid").alias("ValidRecords"),
                            F.sum("is_invalid").alias("InvalidRecords")
                        )
                        .withColumn("TotalRecords", F.col("ValidRecords") + F.col("InvalidRecords"))
                        .withColumn(
                            "SuccessRate",
                            F.when(F.col("TotalRecords") == 0, 0.0)
                             .otherwise(F.round(F.col("ValidRecords") / F.col("TotalRecords") * 100, 2))
                        )
                        .withColumn(
                            "MetricsGeneratedAt",
                            F.from_utc_timestamp(F.current_timestamp(), "Europe/London")
                        )
    )

    return aggregated


