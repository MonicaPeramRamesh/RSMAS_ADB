"""
================================================================================
Retail Sales Management System - Inbound Deliveries DLT Pipeline
================================================================================
Purpose: Process inbound delivery data through Bronze -> Silver -> Gold layers
Pre-requisites: 
  - ADF pipeline validates schema before triggering DLT
  - Product master table: dev_rsclp_catalog.rsclp_productmaster_schema.product_master
  - Supplier cost price: dev_rsclp_catalog.rsclp_productmaster_schema.supplier_costprice_history
  
Layers:
  - Bronze: Raw ingestion from ADLS CSV files
  - Silver: Cleansed, validated, enriched with product/supplier data
  - Gold: Aggregated metrics for inventory updates
  - Quarantine: Invalid/rejected records with detailed reasons
  
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
    StructType, StructField, StringType, DoubleType
)

# Current processing date
processing_date = datetime.now(ZoneInfo("Europe/London")).date()

# ---------------------------
# Logging setup
# ---------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(stage)s] %(message)s'
)
logger = logging.getLogger("dlt_inbound_deliveries")

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
    gold_path = dbutils.secrets.get(scope='rsclp-scope', key='gold-path')
    source_path = dbutils.secrets.get(scope='rsclp-scope', key='source-path')
    log("INIT", "Secrets retrieved successfully")
except Exception as e:
    log("INIT", f"CRITICAL: Failed to retrieve secrets: {e}", level="error")
    raise

# ------------------------------
# Schema Definitions
# ------------------------------
deliveries_schema = StructType([
    StructField("ReceiptID", StringType(), True),
    StructField("ShipmentDate", StringType(), True),
    StructField("StoreID", StringType(), True),
    StructField("SupplierID", StringType(), True),
    StructField("ProductID", StringType(), True),
    StructField("ProductName", StringType(), True),
    StructField("QuantityShipped", DoubleType(), True),
    StructField("UnitOfMeasure", StringType(), True),
    StructField("UnitCost", DoubleType(), True),
    StructField("TotalCost", DoubleType(), True),
    StructField("ExpiryDate", StringType(), True),
    StructField("Remarks", StringType(), True)
])

# Quality expectations for Silver layer
deliveries_silver_rules = {
    "valid_receipt_id": "ReceiptID IS NOT NULL",
    "valid_product": "ProductID IS NOT NULL",
    "valid_quantity": "QuantityShipped > 0",
    "valid_cost": "UnitCost >= 0",
    "valid_store": "StoreID IS NOT NULL"
}

# ------------------------------
# BRONZE LAYER: Raw Data Ingestion
# ------------------------------
@dlt.table(
    name='dev_rsclp_catalog.rsclp_bronze_schema.inbound_deliveries',
    comment='Bronze layer: Raw incremental ingestion of inbound deliveries from ADLS',
    partition_cols=['ShipmentDate'],
    table_properties={
        "quality": "bronze",
        "layer": "bronze",
        "data_classification": "raw",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "ShipmentDate,StoreID",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.deletedFileRetentionDuration": "interval 7 days"
    }
)
def inbound_deliveries_bronze():
    """
    Bronze layer: Raw data ingestion from CSV files.
    
    Features:
    - Cloud Files auto loader for scalable ingestion
    - Minimal transformations (DeliveryDate parsing from ShipmentDate)
    - Partitioned by DeliveryDate for query performance (analogous to TransactionDate)
    - Schema strictly enforced
    - Audit columns: LoadTimestamp, SourceFileName, ProcessingDate
    
    Returns:
        DataFrame: Raw delivery data with audit columns
    """
    log("BRONZE", "Starting raw ingestion of inbound deliveries")
    df = (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.includeExistingFiles", "true")
        .schema(deliveries_schema)
        .load(f"{source_path}/inbound_deliveries/")
        .withColumn("LoadTimestamp", F.from_utc_timestamp(F.current_timestamp(), "Europe/London"))
        .withColumn("SourceFileName", F.col("_metadata.file_path"))
        .withColumn("ProcessingDate", F.lit(processing_date))
    )
    log("BRONZE", "Raw ingestion completed")
    return df

# No intermediate views needed - data is already flat (no JSON/arrays to parse)

# ------------------------------
# SILVER LAYER: Cleansed & Enriched Data
# ------------------------------
@dlt.table(
    name="dev_rsclp_catalog.rsclp_silver_schema.inbound_deliveries",
    comment='Silver layer: Validated deliveries enriched with product master and supplier pricing',
    partition_cols=['ShipmentDate'],
    table_properties={
        "quality": "silver",
        "layer": "silver",
        "data_classification": "cleansed",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "ShipmentDate,StoreID,ProductID",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true",
        "delta.deletedFileRetentionDuration": "interval 7 days"
    }
)
@dlt.expect_all(deliveries_silver_rules)
def inbound_deliveries_silver():
    """
    Silver layer: Business logic transformations and data enrichment.
    
    Transformations:
    1. Read bronze and filter valid records
    2. Enrich with product master (ProductName, Category, etc.)
    3. Enrich with supplier pricing and validate price match
    4. Apply quality rules via DLT expectations
    5. Inner join with product master filters out invalid products
    6. Filter out supplier price mismatches
    
    Quality Rules:
    - ReceiptID must not be null
    - ProductID must not be null
    - QuantityShipped must be > 0
    - UnitCost must be >= 0
    - StoreID must not be null
    - UnitCost must match supplier pricing (within tolerance)
    
    Returns:
        DataFrame: Cleansed and enriched delivery data (ONLY fully valid records)
    """
    log("SILVER", "Reading bronze for silver processing")
    bronze_stream = dlt.read_stream("dev_rsclp_catalog.rsclp_bronze_schema.inbound_deliveries")
    
    # Filter valid records
    filtered = bronze_stream.dropna(subset=[
        "ReceiptID", "ShipmentDate", "StoreID", "ProductID", "QuantityShipped", "UnitCost"
    ]).filter(
        (F.col("QuantityShipped") > 0) & (F.col("UnitCost") >= 0)
    )
    
    # Product master (rename to avoid clashes)
    product_df = (
        spark.read.table("dev_rsclp_catalog.rsclp_productmaster_schema.product_master")
        .filter(F.col("__END_AT").isNull())
        .select(
            "ProductID",
            F.col("ProductName").alias("EnrichedProductName"),
            "CategoryID", "DepartmentID", "Description",
            F.col("UnitOfMeasure").alias("EnrichedUnitOfMeasure")
        )
    )
    
    # Supplier pricing
    supplier_df = (
        spark.read.table("dev_rsclp_catalog.rsclp_productmaster_schema.supplier_costprice_history")
        .filter(F.col("__END_AT").isNull())
        .select(
            F.col("ProductID").alias("SupplierProductID"),
            F.col("SupplierID").alias("SupplierSupplierID"),
            F.col("SupplierPrice")
        )
    )
    
    # Join + validate + select in ONE chain
    return (
        filtered
        .join(F.broadcast(product_df), on="ProductID", how="inner")  # Filters invalid products
        .join(F.broadcast(supplier_df), 
              (F.col("ProductID") == F.col("SupplierProductID")) & 
              (F.col("SupplierID") == F.col("SupplierSupplierID")), 
              "inner")
        .filter(F.abs(F.col("UnitCost") - F.col("SupplierPrice")) <= 0.01)  # Price validation
        .select(
            "ReceiptID", "ShipmentDate", "StoreID", "SupplierID", "ProductID",
            F.col("EnrichedProductName").alias("ProductName"),
            "CategoryID", "DepartmentID", "Description",
            F.col("EnrichedUnitOfMeasure").alias("UnitOfMeasure"),
            "QuantityShipped", "UnitCost", "TotalCost", "ExpiryDate",
            "LoadTimestamp", "SourceFileName", "ProcessingDate"
        )
    )

# ------------------------------
# GOLD LAYER: Aggregated Analytics
# ------------------------------
@dlt.table(
    name='dev_rsclp_catalog.rsclp_gold_schema.inbound_deliveries',
    comment='Gold layer: Aggregated delivery metrics by date, store, product, and cost for inventory updates',
    partition_cols=["ShipmentDate"],
    table_properties={
        "quality": "gold",
        "layer": "gold",
        "data_classification": "aggregated",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "ShipmentDate,StoreID",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.deletedFileRetentionDuration": "interval 30 days"
    }
)
def inbound_deliveries_gold():
    """
    Gold layer: Aggregated business metrics for inventory updates.
    
    Aggregation Level:
    - DeliveryDate (from ShipmentDate - analogous to TransactionDate)
    - StoreID
    - ProductID
    - UnitCost (to handle price changes)
    
    Metrics:
    - DeliveredQuantity: Sum of all quantities delivered
    - TotalDeliveryCost: Sum of all costs
    - DeliveryCount: Number of receipts
    - LastShipmentDate: Most recent shipment date
    - LoadTimestamp: Latest load time
    
    Use Cases:
    - Inventory replenishment tracking
    - Cost analysis
    - Supplier performance monitoring
    
    Returns:
        DataFrame: Aggregated delivery metrics
    """
    log("GOLD", "Reading silver for aggregation")
    silver_stream = dlt.read_stream("dev_rsclp_catalog.rsclp_silver_schema.inbound_deliveries")
    
    # Add watermark
    silver_stream = silver_stream.withWatermark("LoadTimestamp", "2 hours")
    
    # Aggregate
    agg_new = (
        silver_stream
        .groupBy(
            "ShipmentDate",
            "StoreID",
            "ProductID",
            "ProductName",
            "CategoryID",
            "UnitCost"
        )
        .agg(
            F.sum("QuantityShipped").alias("DeliveredQuantity"),
            F.sum("TotalCost").alias("TotalDeliveryCost"),
            F.max("LoadTimestamp").alias("LoadTimestamp"),
            F.max("ProcessingDate").alias("ProcessingDate")
        )
    )
    
    log("GOLD", "Gold aggregation completed")
    return agg_new

# ------------------------------
# QUARANTINE LAYER: Invalid Records
# ------------------------------
@dlt.table(
    name='dev_rsclp_catalog.rsclp_invalid_data_schema.invalid_inbound_deliveries',
    comment='Quarantine layer: All invalid/rejected delivery records with comprehensive failure reasons',
    partition_cols=['ShipmentDate'],
    table_properties={
        "quality": "quarantine",
        "layer": "quarantine",
        "data_classification": "invalid",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "ShipmentDate,InvalidationType,Reason",
        "delta.deletedFileRetentionDuration": "interval 90 days"
    }
)
def invalid_inbound_deliveries():
    """
    Quarantine layer: Captures all invalid records with detailed failure reasons.
    
    Invalid Record Categories:
    1. MISSING_COLUMNS: Required fields are null (BLOCKS Silver)
    2. PRODUCT_NOT_FOUND: ProductID doesn't exist in product master (BLOCKS Silver)
    3. QUALITY_RULE_VIOLATION: Failed DLT expectations - invalid Quantity/Cost (BLOCKS Silver)
    4. SUPPLIER_PRICE_MISMATCH: UnitCost doesn't match supplier pricing (BLOCKS Silver)
    
    Features:
    - left_anti join for clean "not found" logic
    - Multiple quality violations combined into single reason string
    - InvalidationType for easy filtering and monitoring
    - ALL invalid records are blocked from Silver layer
    
    Use Cases:
    - Data quality monitoring
    - Root cause analysis
    - Vendor data issue identification
    - Price discrepancy tracking with suppliers
    - Reprocessing after data fixes
    
    Returns:
        DataFrame: All invalid records with failure reasons
    """
    log("QUARANTINE", "Collecting invalid delivery records")
    
    bronze_stream = dlt.read_stream("dev_rsclp_catalog.rsclp_bronze_schema.inbound_deliveries")
    
    # Active product master
    product_df = (
        spark.read.table("dev_rsclp_catalog.rsclp_productmaster_schema.product_master")
        .filter(F.col("__END_AT").isNull())
        .select("ProductID")
        .cache()
    )
    
    # Extract folder date for partitioning
    folder_date_expr = F.regexp_extract(
        F.col("SourceFileName"),
        r"/inbound_deliveries/(\d{4}-\d{2}-\d{2})/",
        1
    )
    
    # =====================================================================
    # 1. Missing Columns
    # =====================================================================
    missing_columns = (
        bronze_stream.filter(
            F.col("ReceiptID").isNull() |
            F.col("ShipmentDate").isNull() |
            F.col("StoreID").isNull() |
            F.col("ProductID").isNull() |
            F.col("QuantityShipped").isNull() |
            F.col("UnitCost").isNull()
        )
        .withColumn(
            "Reason",
            F.concat_ws("; ",
                F.when(F.col("ReceiptID").isNull(), F.lit("ReceiptID null")),
                F.when(F.col("ShipmentDate").isNull(), F.lit("ShipmentDate null")),
                F.when(F.col("StoreID").isNull(), F.lit("StoreID null")),
                F.when(F.col("ProductID").isNull(), F.lit("ProductID null")),
                F.when(F.col("QuantityShipped").isNull(), F.lit("QuantityShipped null")),
                F.when(F.col("UnitCost").isNull(), F.lit("UnitCost null"))
            )
        )
        .withColumn("InvalidationType", F.lit("MISSING_COLUMNS"))
        .withColumn(
            "ShipmentDateCategory",
            F.when(
                (F.col("ShipmentDate").isNull()) |
                (F.col("ShipmentDate") == "") |
                (F.col("ShipmentDate") == "-"),
                folder_date_expr
            ).otherwise(F.col("ShipmentDate"))
        )
    )
    
    # =====================================================================
    # 2. Product Not Found (using validated records only)
    # =====================================================================
    valid_for_product_check = (
        bronze_stream.dropna(subset=[
            "ReceiptID", "ShipmentDate", "StoreID", "ProductID", "QuantityShipped", "UnitCost"
        ]).filter(
            (F.col("QuantityShipped") > 0) & (F.col("UnitCost") >= 0)
        )
    )
    
    invalid_product = (
        valid_for_product_check
        .join(product_df, "ProductID", "left_anti")
        .withColumn("Reason", F.lit("Product not found"))
        .withColumn("InvalidationType", F.lit("PRODUCT_NOT_FOUND"))
        .withColumn(
            "ShipmentDateCategory",
            F.when(
                (F.col("ShipmentDate").isNull()) |
                (F.col("ShipmentDate") == "") |
                (F.col("ShipmentDate") == "-"),
                folder_date_expr
            ).otherwise(F.col("ShipmentDate"))
        )
    )
    
    # =====================================================================
    # 3. Quality Violations (using validated records only)
    # =====================================================================
    valid_for_quality_check = (
        bronze_stream.dropna(subset=["ProductID"])
    )
    
    invalid_quality = (
        valid_for_quality_check.filter(
            (F.col("QuantityShipped") <= 0) |
            (F.col("UnitCost") < 0)
        )
        .withColumn(
            "Reason",
            F.concat_ws("; ",
                F.when(F.col("QuantityShipped") <= 0, F.lit("QuantityShipped <=0")),
                F.when(F.col("UnitCost") < 0, F.lit("UnitCost <0"))
            )
        )
        .withColumn("InvalidationType", F.lit("QUALITY_VIOLATION"))
        .withColumn(
            "ShipmentDateCategory",
            F.when(
                (F.col("ShipmentDate").isNull()) |
                (F.col("ShipmentDate") == "") |
                (F.col("ShipmentDate") == "-"),
                folder_date_expr
            ).otherwise(F.col("ShipmentDate"))
        )
    )
    
    # =====================================================================
    # 4. Supplier Price Mismatch (for monitoring - does NOT block Silver)
    # =====================================================================
    # Active supplier pricing
    supplier_df = (
        spark.read.table("dev_rsclp_catalog.rsclp_productmaster_schema.supplier_costprice_history")
        .filter(F.col("__END_AT").isNull())
        .select("ProductID", "SupplierID", "SupplierPrice")
    )
    
    # Check price mismatches on records that pass all other validations
    with_supplier = valid_for_product_check.join(
        supplier_df,
        (valid_for_product_check["ProductID"] == supplier_df["ProductID"]) &
        (valid_for_product_check["SupplierID"] == supplier_df["SupplierID"]),
        "left"
    )
    
    invalid_supplier = (
        with_supplier
        .filter(
            F.col("SupplierPrice").isNotNull() &
            (F.abs(F.col("UnitCost") - F.col("SupplierPrice")) > 0.01)
        )
        .withColumn(
            "Reason",
            F.concat(
                F.lit("Supplier price mismatch: Expected "),
                F.col("SupplierPrice"),
                F.lit(", Got "),
                F.col("UnitCost")
            )
        )
        .withColumn("InvalidationType", F.lit("SUPPLIER_PRICE_MISMATCH"))
        .withColumn(
            "ShipmentDateCategory",
            F.when(
                (F.col("ShipmentDate").isNull()) |
                (F.col("ShipmentDate") == "") |
                (F.col("ShipmentDate") == "-"),
                folder_date_expr
            ).otherwise(F.col("ShipmentDate"))
        )
       .select(
        bronze_stream.ReceiptID,
        bronze_stream.ShipmentDate,
        bronze_stream.StoreID,
        bronze_stream.SupplierID,
        bronze_stream.ProductID,
        bronze_stream.ProductName,
        bronze_stream.QuantityShipped,
        bronze_stream.UnitCost,
        bronze_stream.TotalCost,
        bronze_stream.ExpiryDate,
        bronze_stream.Remarks,
        bronze_stream.LoadTimestamp,
        bronze_stream.SourceFileName,
        bronze_stream.ProcessingDate,
        "Reason",
        "InvalidationType",
        "ShipmentDateCategory"
    )

    )
    
    # =====================================================================
    # Final union
    # =====================================================================
    invalid_final = (
        missing_columns
        .unionByName(invalid_product, allowMissingColumns=True)
        .unionByName(invalid_quality, allowMissingColumns=True)
        .unionByName(invalid_supplier, allowMissingColumns=True)
    )
    
    log("QUARANTINE", "Invalid delivery records collected")
    return invalid_final

# ================================================================================
# MONITORING VIEWS
# ================================================================================

@dlt.table(
    name='dev_rsclp_catalog.rsclp_monitor_schema.invalid_deliveries_summary',
    comment='Monitoring view: Aggregated invalid deliveries by date, type, and reason'
)
def invalid_deliveries_summary():
    """
    Invalid Deliveries Summary: Aggregates quarantine records for monitoring.
    
    Returns:
        DataFrame: Aggregated invalid delivery summary
    """
    quarantine_stream = (
        dlt.read_stream("dev_rsclp_catalog.rsclp_invalid_data_schema.invalid_inbound_deliveries")
        .withWatermark("LoadTimestamp", "1 hour")
    )
    
    return (
        quarantine_stream
        .groupBy("ShipmentDateCategory", "InvalidationType", "Reason")
        .agg(
            F.count("*").alias("InvalidCount"),
            F.min("LoadTimestamp").alias("FirstSeen"),
            F.max("LoadTimestamp").alias("LastSeen"),
            F.approx_count_distinct("SourceFileName").alias("AffectedFiles")
        )
    )

@dlt.table(
    name='dev_rsclp_catalog.rsclp_monitor_schema.deliveries_pipeline_metrics',
    comment='Monitoring view: Daily delivery pipeline health metrics'
)
def deliveries_pipeline_metrics():
    """
    Pipeline Health Metrics: Compares valid vs invalid delivery records.
    
    Returns:
        DataFrame: Daily pipeline metrics
    """
    # Valid records
    silver = (
        dlt.read_stream("dev_rsclp_catalog.rsclp_silver_schema.inbound_deliveries")
        .withWatermark("LoadTimestamp", "1 hour")
        .withColumn("is_valid", F.lit(1))
        .withColumn("is_invalid", F.lit(0))
        .withColumn(
            "ShipmentDateCategory",
            F.when(F.col("ShipmentDate").isNotNull(), F.col("ShipmentDate"))
            .otherwise(
                F.regexp_extract(F.col("SourceFileName"), r"/inbound_deliveries/(\d{4}-\d{2}-\d{2})/", 1)
            )
        )
        .withColumn(
            "ShipmentDateCategory",
            F.when(F.col("ShipmentDateCategory") == "", F.lit("UNKNOWN"))
            .otherwise(F.col("ShipmentDateCategory"))
        )
        .select("ShipmentDateCategory", "is_valid", "is_invalid")
    )
    
    # Invalid records
    invalid = (
        dlt.read_stream("dev_rsclp_catalog.rsclp_invalid_data_schema.invalid_inbound_deliveries")
        .withWatermark("LoadTimestamp", "1 hour")
        .withColumn("is_valid", F.lit(0))
        .withColumn("is_invalid", F.lit(1))
        .withColumn(
            "ShipmentDateCategory",
            F.when(F.col("ShipmentDateCategory").isNotNull(), F.col("ShipmentDateCategory"))
            .otherwise(
                F.regexp_extract(F.col("SourceFileName"), r"/inbound_deliveries/(\d{4}-\d{2}-\d{2})/", 1)
            )
        )
        .withColumn(
            "ShipmentDateCategory",
            F.when(F.col("ShipmentDateCategory") == "", F.lit("UNKNOWN"))
            .otherwise(F.col("ShipmentDateCategory"))
        )
        .select("ShipmentDateCategory", "is_valid", "is_invalid")
    )
    
    combined = silver.unionByName(invalid)
    
    return (
        combined
        .groupBy("ShipmentDateCategory")
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
    