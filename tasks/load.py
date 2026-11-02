# Databricks notebook source
# MAGIC %md
# MAGIC # Load the sales
# MAGIC
# MAGIC Runs as the first task of the job you build in notebook 05. It's the sales
# MAGIC path from notebooks 03 and 04 in one notebook: Auto Loader picks up any new
# MAGIC extracts into `bronze_sales`, a `MERGE` brings `silver_sales` up to date, and
# MAGIC `gold_fact_sales` is rebuilt from silver. Every step is idempotent, so the
# MAGIC task is safe to retry and to repair. It reads the catalog and schema from
# MAGIC the job parameters.

# COMMAND ----------

from pyspark.sql import functions as F

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
spark.catalog.setCurrentCatalog(catalog)
spark.catalog.setCurrentDatabase(schema)

SALES = f"/Volumes/{catalog}/{schema}/raw/sales"
CHECKPOINT = f"/Volumes/{catalog}/{schema}/raw/_checkpoints/bronze_sales"

# COMMAND ----------

# Bronze: the same stream and checkpoint as notebook 03, so only new extracts are loaded.
(spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", CHECKPOINT)
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(SALES)
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable("bronze_sales")
    .awaitTermination())

# COMMAND ----------

# Silver: the cleaning from notebook 04, merged on the business key.
(spark.table("bronze_sales")
    .filter(F.col("transactionID").isNotNull())
    .dropDuplicates(["transactionID"])
    .select(
        F.col("transactionID").cast("bigint").alias("transactionID"),
        F.col("customerID").cast("bigint").alias("customerID"),
        F.col("franchiseID").cast("bigint").alias("franchiseID"),
        F.try_to_timestamp(F.col("dateTime")).alias("dateTime"),
        "product",
        F.col("quantity").cast("int").alias("quantity"),
        F.col("unitPrice").cast("decimal(12,2)").alias("unitPrice"),
        F.col("totalPrice").cast("decimal(12,2)").alias("totalPrice"),
        F.lower(F.trim(F.col("paymentMethod"))).alias("paymentMethod"),
    )
    .createOrReplaceTempView("sales_clean"))

spark.sql("""
    MERGE INTO silver_sales AS t
    USING sales_clean AS s
    ON t.transactionID = s.transactionID
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
""")

# COMMAND ----------

# Gold: rebuilt in full from silver.
(spark.table("silver_sales")
    .select("transactionID", "customerID", "franchiseID", "dateTime", "product",
            "quantity", "unitPrice", "totalPrice", "paymentMethod")
    .write.mode("overwrite").clusterBy("dateTime").saveAsTable("gold_fact_sales"))

print(f"gold_fact_sales: {spark.table('gold_fact_sales').count()} rows")
