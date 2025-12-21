import dlt
from pyspark.sql.functions import (
    col, coalesce, lit, round, current_timestamp,
    sum as _sum, avg as _avg
)
from datetime import datetime
from zoneinfo import ZoneInfo

# Current date in BST/GMT automatically
today_bst = datetime.now(ZoneInfo("Europe/London")).strftime("%Y-%m-%d")


@dlt.table(
    name="rsmas_catalog.rsmas_gold_schema.daily_profits",
    comment="Daily profits by product, store, and date",
    partition_cols=["loa_date"]
)
def daily_profits():

    # -------------------------------
    # 1️⃣ INBOUND DELIVERIES (Cost Data)
    # -------------------------------
    inbound_df = (
        spark.read.table("rsmas_catalog.rsmas_gold_schema.inbound_deliveries")
        .filter(col("load_date") == today_bst)
        .select(
            col("ProductID").alias("in_ProductID"),
            col("StoreID").alias("in_StoreID"),
            col("DeliveredQuantity"),
            col("UnitCost").alias("CostPrice"),
            col("load_date").alias("in_load_date")
        )
        .groupBy("in_ProductID", "in_StoreID", "in_load_date")
        .agg(
            _sum("DeliveredQuantity").alias("DeliveredQuantity"),
            _avg("CostPrice").alias("CostPrice")  # supports store-level cost in future
        )
    )

    # -------------------------------
    # 2️⃣ DAILY SALES (Revenue Data)
    # -------------------------------
    sales_df = (
        spark.read.table("rsmas_catalog.rsmas_gold_schema.daily_sales")
        .filter(col("TransactionDate") == today_bst)
        .select(
            col("ProductID").alias("s_ProductID"),
            col("StoreID").alias("s_StoreID"),
            col("UnitPrice").alias("SellingPrice"),
            col("TotalQuantity").alias("TotalQuantitySold"),
            col("TransactionDate").alias("s_load_date")
        )
        .groupBy("s_ProductID", "s_StoreID", "s_load_date")
        .agg(
            _sum("TotalQuantitySold").alias("TotalQuantitySold"),
            _avg("SellingPrice").alias("SellingPrice")
        )
    )

    # -------------------------------
    # 3️⃣ FULL OUTER JOIN (Product + Store + Date)
    # -------------------------------
    joined_df = inbound_df.join(
        sales_df,
        (col("in_ProductID") == col("s_ProductID")) &
        (col("in_StoreID") == col("s_StoreID")) &
        (col("in_load_date") == col("s_load_date")),
        "full_outer"
    )

    # -------------------------------
    # 4️⃣ PROFIT CALCULATION
    # -------------------------------
    result_df = (
        joined_df
        .withColumn("ProductID", coalesce(col("in_ProductID"), col("s_ProductID")))
        .withColumn("StoreID", coalesce(col("in_StoreID"), col("s_StoreID")))
        .withColumn("load_date", coalesce(col("in_load_date"), col("s_load_date")))

        # Handle missing values safely
        .withColumn("DeliveredQuantity", coalesce(col("DeliveredQuantity"), lit(0.0)))
        .withColumn("CostPrice", coalesce(col("CostPrice"), lit(0.0)))
        .withColumn("TotalQuantitySold", coalesce(col("TotalQuantitySold"), lit(0.0)))
        .withColumn("SellingPrice", coalesce(col("SellingPrice"), lit(0.0)))

        # Profit Formula:
        # (SellingPrice - CostPrice) * SoldQuantity
        .withColumn("Profit",
            round((col("SellingPrice") - col("CostPrice")) * col("TotalQuantitySold"), 2)
        )

        .withColumn("ingestion_timestamp", current_timestamp())

        .select(
            "load_date",
            "ProductID",
            "StoreID",
            "DeliveredQuantity",
            "CostPrice",
            "TotalQuantitySold",
            "SellingPrice",
            "Profit",
            "ingestion_timestamp"
        )
    )

    return result_df
