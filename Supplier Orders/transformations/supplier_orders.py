"""
================================================================================
Retail Sales Management System - AI Supplier Orders DLT Pipeline
================================================================================
Purpose: Process AI-generated supplier orders through Bronze -> Silver -> Gold
Source: AI Model (Demand Forecasting) - Parquet files

Future-Proof Design:
  ✓ Works with 1 store or 100 stores (no code changes)
  ✓ Can add warehouse routing later (config change only)
  ✓ Follows daily sales pattern exactly

Author: Data Engineering Team
Last Updated: 2025-12-07
================================================================================
"""

from datetime import datetime
from zoneinfo import ZoneInfo
import logging
import dlt
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *

# Current processing date
processing_date = datetime.now(ZoneInfo("Europe/London")).date()

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] [%(stage)s] %(message)s')
logger = logging.getLogger("dlt_supplier_orders")

def log(stage: str, message: str, level: str = "info"):
    extra = {"stage": stage}
    getattr(logger, level)(message, extra=extra)

# Secret Retrieval
try:
    source_path = dbutils.secrets.get(scope='rsmas-dev-scope', key='source-path')
    jdbc_url = dbutils.secrets.get(scope='rsmas-dev-scope', key='jdbc-url')
    username = dbutils.secrets.get(scope='rsmas-dev-scope', key='rsmas-db-username')
    password = dbutils.secrets.get(scope='rsmas-dev-scope', key='rsmas-db-password')
    log("INIT", "Secrets retrieved successfully")
except Exception as e:
    log("INIT", f"CRITICAL: Failed to retrieve secrets: {e}", level="error")
    raise

# ==========================================
# Schema Definitions (parquet input schema)
# ==========================================
supplier_orders_schema = StructType([

    # ORDER HEADER
    StructField("OrderID", StringType(), True),
    StructField("GeneratedDateTime", TimestampType(), True),
    StructField("OrderDate", DateType(), True),

    StructField("ModelMetadata", StructType([
        StructField("ModelVersion", StringType(), True),
        StructField("ModelType", StringType(), True),
        StructField("TrainingDate", DateType(), True),
        StructField("OverallConfidence", DoubleType(), True)
    ]), True),

    # LINE ITEMS (AI generated)
    StructField("LineItems", ArrayType(StructType([
        StructField("LineNumber", IntegerType(), True),

        StructField("ProductID", StringType(), True),
        StructField("ProductName", StringType(), True),
        StructField("CategoryID", StringType(), True),

        StructField("SupplierID", StringType(), True),
        StructField("StoreID", StringType(), True),

        StructField("UnitOfMeasure", StringType(), True),

        StructField("StoreInventory", StructType([
            StructField("CurrentStock", IntegerType(), True),
            StructField("SafetyStock", IntegerType(), True),
            StructField("ReorderPoint", IntegerType(), True)
        ]), True),

        StructField("ForecastedDemand", IntegerType(), True),
        StructField("ForecastConfidence", DoubleType(), True),

        StructField("RecommendedQuantity", IntegerType(), True),

        StructField("UnitCost", DoubleType(), True),
        StructField("TotalCost", DoubleType(), True),

        StructField("ExpectedDeliveryDate", DateType(), True),

        StructField("Priority", StringType(), True),

        StructField("HistoricalMetrics", StructType([
            StructField("AvgDailySales", DoubleType(), True),
            StructField("StockoutRisk", DoubleType(), True),
            StructField("SeasonalityFactor", DoubleType(), True)
        ]), True)
    ])), True),

    # ORDER SUMMARY (ONLY USEFUL FIELDS)
    StructField("OrderSummary", StructType([
        StructField("TotalLineItems", IntegerType(), True),
        StructField("TotalOrderValue", DoubleType(), True),
        StructField("HighPriorityItems", IntegerType(), True),
        StructField("ItemsNeedingStockoutPrevention", IntegerType(), True)
    ]), True)
])


# Basic quality expectations (for visibility)
supplier_orders_silver_rules = {
    "valid_order_id": "OrderID IS NOT NULL",
    "valid_store": "StoreID IS NOT NULL",
    "valid_product": "ProductID IS NOT NULL",
    "valid_supplier": "SupplierID IS NOT NULL",

    "valid_quantity": "RecommendedQuantity > 0",
    "valid_unit_cost": "UnitCost > 0",

    "valid_expected_delivery": "ExpectedDeliveryDate IS NOT NULL",
    "valid_generated_datetime": "GeneratedDateTime IS NOT NULL",

    "valid_forecast_confidence": "ForecastConfidence >= 0 AND ForecastConfidence <= 1"
}

# ==================== BRONZE LAYER ====================
@dlt.table(
    name='rsmas_catalog.rsmas_bronze_schema.supplier_orders',
    comment='Bronze: Raw AI supplier orders from Parquet',
    partition_cols=['OrderDate'],
    table_properties={
        "quality": "bronze", "layer": "bronze", "data_classification": "raw",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "OrderDate,StoreID",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.deletedFileRetentionDuration": "interval 7 days"
    }
)
def supplier_orders_bronze():
    """
    Bronze layer: Raw data ingestion from Parquet files.
    
    Features:
    - Cloud Files auto loader for scalable ingestion
    - Reads nested Parquet structure as-is
    - Audit columns: LoadTimestamp, SourceFileName, ProcessingDate
    - Partitioned by GenerationDate (when AI created the order)
    - Future-proof: Works with 1 or N stores automatically
    
    Returns:
        DataFrame: Raw AI orders with audit columns
    """
    log("BRONZE", "Starting raw ingestion")
    orders_bronze = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.includeExistingFiles", "true")
        .schema(supplier_orders_schema)
        .load(f"{source_path}/supplier_orders/")
        .withColumn("LoadTimestamp", F.from_utc_timestamp(F.current_timestamp(), "Europe/London"))
        .withColumn("SourceFileName", F.col("_metadata.file_path"))
        .withColumn("ProcessingDate", F.lit(processing_date))
    )
    log("BRONZE", "Raw ingestion completed")
    return orders_bronze

# ==================== INTERMEDIATE VIEWS ====================
@dlt.view(name="parsed_orders_view", comment="Parsed orders; validates PredictedItems exists")
def parsed_orders_view():
    """
    Parsed orders view: Validates that PredictedItems array exists.
    Analogous to parsed_sales_view in daily sales.
    
    Returns:
        DataFrame: Orders with validated nested structure
    """
    return (
        dlt.read_stream("rsmas_catalog.rsmas_bronze_schema.supplier_orders")
        .withColumn("has_items", F.when(F.col("LineItems").isNotNull(), F.lit(True)).otherwise(F.lit(False)))
    )

@dlt.view(name="exploded_orders_view", comment="Exploded product-level order items")
def exploded_orders_view():
    """
    Exploded orders view: Converts nested array to flat product-level records.
    Analogous to exploded_sales_view in daily sales.
    
    Returns:
        DataFrame: Flattened order items (one row per product per store)
    """
    return (
        dlt.read_stream("parsed_orders_view")
        .filter(F.col("has_items") == True)
        .withColumn("item", F.explode(F.col("LineItems")))
        .select(
            "OrderID", "GeneratedDateTime", "OrderDate",
            F.col("item.StoreID").alias("StoreID"),
            F.col("item.ProductID").alias("ProductID"),
            F.col("item.ProductName").alias("ProductName"),
            F.col("item.CategoryID").alias("CategoryID"),
            F.col("item.UnitOfMeasure").alias("UnitOfMeasure"),
            F.col("item.SupplierID").alias("SupplierID"),                  
            F.col("item.StoreInventory").alias("StoreInventory"),
            F.col("item.ForecastedDemand").alias("ForecastedDemand"),
            F.col("item.ForecastConfidence").alias("ForecastConfidence"),
            F.col("item.RecommendedQuantity").alias("RecommendedQuantity"),
            F.col("item.UnitCost").alias("UnitCost"),
            F.col("item.TotalCost").alias("TotalCost"),
            F.col("item.Priority").alias("Priority"),
            F.col("item.HistoricalMetrics").alias("HistoricalMetrics"),
            F.col("item.ExpectedDeliveryDate").alias("ExpectedDeliveryDate"),
            "LoadTimestamp", "SourceFileName", "ProcessingDate"
        )
    )

# ==================== SILVER LAYER ====================
@dlt.table(
    name="rsmas_catalog.rsmas_silver_schema.supplier_orders",
    comment='Silver: Validated orders (only valid records)',
    partition_cols=['OrderDate'],
    table_properties={
        "quality": "silver", "layer": "silver", "data_classification": "cleansed",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "OrderDate,StoreID,SupplierID",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true",
        "delta.deletedFileRetentionDuration": "interval 7 days"
    }
)
@dlt.expect_all(supplier_orders_silver_rules)
def supplier_orders_silver():
    """
    Silver layer: Business logic validations and enrichment.
    
    Validations:
    1. Required fields not null/"-"
    2. RecommendedQuantity > 0
    3. ExpectedDeliveryDate >= OrderDate (not in past)
    4. ProductID exists in product master
    5. SupplierID exists in supplier cost price
    6. UnitCost matches supplier pricing
    7. RecommendedQuantity <= 3 × Threshold (from SQL Server)
    
    Future-Proof:
    - Works with any number of stores
    - Threshold table has ProductID + StoreID columns
    - Can add warehouse logic later by checking StoreID pattern
    
    Returns:
        DataFrame: Only valid orders ready for PDF generation
    """
    log("SILVER", "Validating and enriching orders")
    exploded = dlt.read_stream("exploded_orders_view")
    
    log("SILVER", "Validating and enriching orders")
    exploded = dlt.read_stream("exploded_orders_view")
    
    # Basic validations
    filtered = exploded.dropna(subset=["OrderID", "StoreID", "ProductID", "SupplierID", "RecommendedQuantity"]).filter(
        (F.col("ProductID") != "-") & (F.col("SupplierID") != "-") &
        (F.col("RecommendedQuantity") > 0) &
        (F.col("UnitCost") > 0) &
        (F.col("ExpectedDeliveryDate") >= F.col("OrderDate"))
    )
    
    # Product master with explicit aliases
    product_df = (
        spark.read.table("rsmas_catalog.rsmas_productmaster_schema.product_master")
        .filter(F.col("__END_AT").isNull())
        .select(
            F.col("ProductID").alias("pm_ProductID"), 
            F.col("ProductName").alias("pm_ProductName"), 
            F.col("CategoryID").alias("pm_CategoryID"), 
            F.col("DepartmentID").alias("pm_DepartmentID"), 
            F.col("Description").alias("pm_Description"),
            F.col("UnitOfMeasure").alias("pm_UnitOfMeasure")
        )
    )
    
    # Supplier pricing with explicit aliases
    supplier_df = (
        spark.read.table("rsmas_catalog.rsmas_productmaster_schema.supplier_costprice_history")
        .filter(F.col("__END_AT").isNull())
        .select(
            F.col("ProductID").alias("sph_ProductID"), 
            F.col("SupplierID").alias("sph_SupplierID"), 
            F.col("SupplierPrice").alias("sph_SupplierPrice")
        )
    )
    
    # Threshold with explicit aliases
    threshold_df = (
        spark.read.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", "dbo.InventorySafetyThresholds")
        .option("user", username)
        .option("password", password)
        .load()
        .select(
            F.col("ProductID").alias("th_ProductID"),
            F.col("StoreID").alias("th_StoreID"),
            F.col("ThresholdQuantity").alias("th_ThresholdQuantity")
        )
    )
    
    # Join and validate with explicit column references
    joined_result = (
        filtered
        .join(F.broadcast(product_df), filtered.ProductID == product_df.pm_ProductID, "inner")
        .join(F.broadcast(supplier_df), 
              (filtered.ProductID == supplier_df.sph_ProductID) & 
              (filtered.SupplierID == supplier_df.sph_SupplierID), "inner")
        .filter(F.abs(filtered.UnitCost - supplier_df.sph_SupplierPrice) <= 0.01)
        .join(F.broadcast(threshold_df), 
              (filtered.ProductID == threshold_df.th_ProductID) & 
              (filtered.StoreID == threshold_df.th_StoreID), "left")
        .filter(F.col("th_ThresholdQuantity").isNull() | 
                (filtered.RecommendedQuantity <= (threshold_df.th_ThresholdQuantity * 3)))
        .select(
            filtered.OrderID,
            filtered.GeneratedDateTime, 
            filtered.StoreID, 
            filtered.OrderDate, 
            filtered.ExpectedDeliveryDate, 
            filtered.ProductID, 
            F.coalesce(product_df.pm_ProductName, filtered.ProductName).alias("ProductName"),
            product_df.pm_CategoryID.alias("CategoryID"), 
            product_df.pm_DepartmentID.alias("DepartmentID"), 
            product_df.pm_Description.alias("Description"), 
            product_df.pm_UnitOfMeasure.alias("UnitOfMeasure"),
            filtered.SupplierID, 
            filtered.StoreInventory, 
            filtered.ForecastedDemand, 
            filtered.ForecastConfidence,
            filtered.RecommendedQuantity, 
            F.round(filtered.UnitCost, 2).alias("UnitCost"),
            F.round(filtered.TotalCost, 2).alias("TotalCost"), 
            filtered.Priority, 
            filtered.HistoricalMetrics,
            supplier_df.sph_SupplierPrice.alias("SupplierPrice"), 
            threshold_df.th_ThresholdQuantity.alias("ThresholdQuantity"), 
            filtered.LoadTimestamp, 
            filtered.SourceFileName, 
            filtered.ProcessingDate
        )
    )
    return joined_result


# ==================== GOLD LAYER ====================
@dlt.table(
    name='rsmas_catalog.rsmas_gold_schema.supplier_orders',
    comment='Gold: Aggregated by Supplier+DeliveryDate for PDF generation',
    partition_cols=["OrderDate"],
    table_properties={
        "quality": "gold", "layer": "gold", "data_classification": "aggregated",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "OrderDate,SupplierID",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.deletedFileRetentionDuration": "interval 30 days"
    }
)
def supplier_orders_gold():
    """
    Gold layer: Aggregated orders for PDF generation.
    
    Aggregation Level:
    - GenerationDate (when AI created orders)
    - SupplierID (one PDF per supplier)
    - ExpectedDeliveryDate (supplier might deliver on different dates)
    
    Metrics:
    - TotalStores: How many stores ordering
    - TotalItems: How many distinct products
    - TotalQuantity: Sum of all quantities
    - TotalOrderValue: Sum of all costs
    - CriticalItems: Count of critical priority items
    
    Use Cases:
    - Generate one PDF per supplier
    - Supplier sees all stores they need to deliver to
    - Grouped by delivery date (might have multiple delivery dates)
    
    Future-Proof:
    - Automatically handles multiple stores (TotalStores aggregation)
    - Can filter by StoreID pattern for warehouse routing later
    
    Returns:
        DataFrame: Aggregated orders by supplier
    """
    log("GOLD", "Aggregating by supplier")
    orders_gold_df = dlt.read_stream("rsmas_catalog.rsmas_silver_schema.supplier_orders").select(
        "OrderID", "OrderDate", "ExpectedDeliveryDate",
        "SupplierID", "StoreID",
        "ProductID", "ProductName", "UnitOfMeasure",
        "RecommendedQuantity", "UnitCost", "TotalCost",
        "Priority",
        "LoadTimestamp", "ProcessingDate"
    )
    return orders_gold_df

# ==================== QUARANTINE LAYER ====================
@dlt.table(
    name='rsmas_catalog.rsmas_invalid_data_schema.invalid_supplier_orders',
    comment='Quarantine: All invalid orders with reasons',
    partition_cols=['OrderDate'],
    table_properties={
        "quality": "quarantine", "layer": "quarantine", "data_classification": "invalid",
        "pipelines.autoOptimize.managed": "true",
        "pipelines.autoOptimize.zOrderCols": "OrderDate,InvalidationType,Reason",
        "delta.deletedFileRetentionDuration": "interval 90 days"
    }
)
def invalid_supplier_orders():
    """
    Quarantine layer: Captures all invalid orders with detailed failure reasons.
    
    Invalid Order Categories:
    1. MISSING_ITEMS: PredictedItems array is null/empty
    2. MISSING_COLUMNS: Required fields are null or "-"
    3. INVALID_QUANTITY: RecommendedQuantity <= 0
    4. PAST_DELIVERY_DATE: ExpectedDeliveryDate < GenerationDate
    5. PRODUCT_NOT_FOUND: ProductID doesn't exist in master
    6. SUPPLIER_NOT_FOUND: SupplierID doesn't exist in supplier master
    7. PRICE_MISMATCH: UnitCost doesn't match supplier pricing
    8. EXCEEDS_THRESHOLD: RecommendedQuantity > 3 × Threshold
    
    Returns:
        DataFrame: All invalid orders with failure reasons
    """
    log("QUARANTINE", "Collecting invalid orders WITH GENERIC REASONS")
    
    parsed = dlt.read_stream("parsed_orders_view")
    exploded = dlt.read_stream("exploded_orders_view") 

    # Master tables (unchanged)
    product_df = spark.read.table("rsmas_catalog.rsmas_productmaster_schema.product_master")\
        .filter(F.col("__END_AT").isNull())\
        .select(F.col("ProductID").alias("pm_ProductID")).cache()
    
    supplier_df = spark.read.table("rsmas_catalog.rsmas_productmaster_schema.supplier_costprice_history")\
        .filter(F.col("__END_AT").isNull())\
        .select(F.col("ProductID").alias("sph_ProductID"), 
                F.col("SupplierID").alias("sph_SupplierID"), 
                F.col("SupplierPrice").alias("sph_SupplierPrice")).cache()
    
    threshold_df = spark.read.format("jdbc")\
        .option("url", jdbc_url)\
        .option("dbtable", "dbo.InventorySafetyThresholds")\
        .option("user", username)\
        .option("password", password)\
        .load()\
        .select(F.col("ProductID").alias("th_ProductID"),
                F.col("StoreID").alias("th_StoreID"),
                F.col("ThresholdQuantity").alias("th_ThresholdQuantity")).cache()     

    folder_date = F.regexp_extract(F.col("SourceFileName"), r"/supplier_orders/(\d{4}-\d{2}-\d{2})/", 1) 

    # --- 1. Missing Items (unchanged) ---
    inv_no_items = parsed.filter(F.col("has_items") == False)\
        .withColumn("ProductID", F.lit(None))\
        .withColumn("SupplierID", F.lit(None))\
        .withColumn("RecommendedQuantity", F.lit(None))\
        .withColumn("Reason", F.lit("LineItems array null/empty"))\
        .withColumn("InvalidationType", F.lit("MISSING_ITEMS"))\
        .withColumn("OrderDateCategory", F.when(F.col("OrderDate").isNull(), folder_date).otherwise(F.col("OrderDate"))) 

    # --- 2. Missing Columns (unchanged) ---
    inv_missing = exploded.filter(
        F.col("OrderID").isNull() | F.col("StoreID").isNull() | F.col("ProductID").isNull() | 
        F.col("SupplierID").isNull() | F.col("RecommendedQuantity").isNull() | 
        (F.col("ProductID") == "-") | (F.col("SupplierID") == "-")
    ).withColumn(
        "Reason", F.lit("Missing required fields")
    ).withColumn("InvalidationType", F.lit("MISSING_COLUMNS"))\
     .withColumn("OrderDateCategory", F.when(F.col("OrderDate").isNull(), folder_date).otherwise(F.col("OrderDate"))) 

    # --- 3-4. Basic validations (unchanged) ---
    valid_struct = exploded.dropna(subset=["OrderID", "StoreID", "ProductID", "SupplierID", "RecommendedQuantity"])\
        .filter((F.col("ProductID") != "-") & (F.col("SupplierID") != "-")) 

    inv_qty = valid_struct.filter(F.col("RecommendedQuantity") <= 0)\
        .withColumn("Reason", F.lit("RecommendedQuantity <= 0"))\
        .withColumn("InvalidationType", F.lit("INVALID_QUANTITY"))\
        .withColumn("OrderDateCategory", F.when(F.col("OrderDate").isNull(), folder_date).otherwise(F.col("OrderDate"))) 

    valid_qty = valid_struct.filter(F.col("RecommendedQuantity") > 0) 
    inv_date = valid_qty.filter(F.col("ExpectedDeliveryDate") < F.col("OrderDate"))\
        .withColumn("Reason", F.lit("ExpectedDeliveryDate in past"))\
        .withColumn("InvalidationType", F.lit("PAST_DELIVERY_DATE"))\
        .withColumn("OrderDateCategory", F.when(F.col("OrderDate").isNull(), folder_date).otherwise(F.col("OrderDate"))) 

    valid_date = valid_qty.filter(F.col("ExpectedDeliveryDate") >= F.col("OrderDate")) 

    # --- 5. Product not found (unchanged) ---
    inv_product = valid_date.join(product_df, valid_date.ProductID == product_df.pm_ProductID, "left_anti")\
        .withColumn("Reason", F.lit("ProductID not in master"))\
        .withColumn("InvalidationType", F.lit("PRODUCT_NOT_FOUND"))\
        .withColumn("OrderDateCategory", F.when(F.col("OrderDate").isNull(), folder_date).otherwise(F.col("OrderDate"))) 

    valid_product = valid_date.join(product_df, valid_date.ProductID == product_df.pm_ProductID, "inner") 

    # --- 6-7. Supplier + Price (GENERIC!) ---
    with_supplier = valid_product.join(supplier_df, 
        (valid_product.ProductID == supplier_df.sph_ProductID) & 
        (valid_product.SupplierID == supplier_df.sph_SupplierID), "left")

    inv_supplier_notfound = with_supplier.filter(supplier_df.sph_SupplierPrice.isNull())\
        .withColumn("Reason", F.lit("SupplierID/ProductID combination not found"))\
        .withColumn("InvalidationType", F.lit("SUPPLIER_NOT_FOUND"))\
        .withColumn("OrderDateCategory", F.when(F.col("OrderDate").isNull(), folder_date).otherwise(F.col("OrderDate"))) 

    # 🔥 FIXED: Generic price mismatch reason
    inv_price_mismatch = with_supplier.filter(F.abs(valid_product.UnitCost - supplier_df.sph_SupplierPrice) > 0.01)\
        .withColumn("Reason", F.lit("UnitCost deviates >0.01 from supplier price"))\
        .withColumn("InvalidationType", F.lit("PRICE_MISMATCH"))\
        .withColumn("OrderDateCategory", F.when(F.col("OrderDate").isNull(), folder_date).otherwise(F.col("OrderDate"))) 

    inv_negative_price = with_supplier.filter(valid_product.UnitCost < 0)\
        .withColumn("Reason", F.lit("UnitCost negative"))\
        .withColumn("InvalidationType", F.lit("NEGATIVE_UNIT_PRICE"))\
        .withColumn("OrderDateCategory", F.when(F.col("OrderDate").isNull(), folder_date).otherwise(F.col("OrderDate"))) 

    # --- 8. Threshold (GENERIC!) ---
    valid_supplier = with_supplier.filter(
        supplier_df.sph_SupplierPrice.isNotNull() & 
        (F.abs(valid_product.UnitCost - supplier_df.sph_SupplierPrice) <= 0.01) &
        (valid_product.UnitCost >= 0)
    )

    with_threshold = valid_supplier.join(threshold_df, 
        (valid_supplier.ProductID == threshold_df.th_ProductID) & 
        (valid_supplier.StoreID == threshold_df.th_StoreID), "left")
    
    # 🔥 FIXED: Generic threshold reason
    inv_threshold = with_threshold.filter(
        threshold_df.th_ThresholdQuantity.isNotNull() & 
        (valid_supplier.RecommendedQuantity > (threshold_df.th_ThresholdQuantity * 3))
    ).withColumn("Reason", F.lit("RecommendedQuantity exceeds 3x safety threshold"))\
     .withColumn("InvalidationType", F.lit("EXCEEDS_THRESHOLD"))\
     .withColumn("OrderDateCategory", F.when(F.col("OrderDate").isNull(), folder_date).otherwise(F.col("OrderDate"))) 

    return inv_no_items.unionByName(inv_missing, allowMissingColumns=True)\
        .unionByName(inv_qty, allowMissingColumns=True)\
        .unionByName(inv_date, allowMissingColumns=True)\
        .unionByName(inv_product, allowMissingColumns=True)\
        .unionByName(inv_supplier_notfound, allowMissingColumns=True)\
        .unionByName(inv_price_mismatch, allowMissingColumns=True)\
        .unionByName(inv_negative_price, allowMissingColumns=True)\
        .unionByName(inv_threshold, allowMissingColumns=True)


# ==================== MONITORING VIEWS ====================
@dlt.table(name='rsmas_catalog.rsmas_monitor_schema.invalid_orders_summary', comment='Monitoring: Aggregated invalid orders')
def invalid_orders_summary():
    return (
        dlt.read_stream("rsmas_catalog.rsmas_invalid_data_schema.invalid_supplier_orders")
        .withWatermark("LoadTimestamp", "1 hour")
        .groupBy("OrderDateCategory", "InvalidationType", "Reason")
        .agg(
            F.count("*").alias("InvalidCount"), 
            F.max("LoadTimestamp").alias("LastSeen"), 
            F.approx_count_distinct("SourceFileName").alias("AffectedFiles")
        )
    )
@dlt.table(name='rsmas_catalog.rsmas_monitor_schema.orders_pipeline_metrics', comment='Monitoring: Pipeline health metrics')
def orders_pipeline_metrics():
    silver = (
        dlt.read_stream("rsmas_catalog.rsmas_silver_schema.supplier_orders")
        .withWatermark("LoadTimestamp", "1 hour")
        .withColumn("is_valid", F.lit(1))
        .withColumn("is_invalid", F.lit(0))
        .withColumn("OrderDateCategory", F.when(F.col("OrderDate").isNotNull(), F.col("OrderDate")).otherwise(F.lit("UNKNOWN")))
        .select("OrderDateCategory", "is_valid", "is_invalid")
    )
    
    invalid = (
        dlt.read_stream("rsmas_catalog.rsmas_invalid_data_schema.invalid_supplier_orders")
        .withWatermark("LoadTimestamp", "1 hour")
        .withColumn("is_valid", F.lit(0))
        .withColumn("is_invalid", F.lit(1))
        .select("OrderDateCategory", "is_valid", "is_invalid")
    )
    
    return (
        silver.unionByName(invalid)
        .groupBy("OrderDateCategory")
        .agg(
            F.sum("is_valid").alias("ValidRecords"), 
            F.sum("is_invalid").alias("InvalidRecords")
        )
        .withColumn("TotalRecords", F.col("ValidRecords") + F.col("InvalidRecords"))
        .withColumn("SuccessRate", F.when(F.col("TotalRecords") == 0, 0.0).otherwise(F.round(F.col("ValidRecords") / F.col("TotalRecords") * 100, 2)))
        .withColumn("MetricsGeneratedAt", F.from_utc_timestamp(F.current_timestamp(), "Europe/London"))
    )