# Databricks notebook source
# MAGIC %md
# MAGIC # Check the gold table
# MAGIC
# MAGIC Runs as the second task of the job you build in notebook 05, after
# MAGIC `load`. It reads its settings from the job parameters, counts the rows in
# MAGIC `gold_fact_sales`, and raises an error if there are fewer than `min_rows`.
# MAGIC The error stops the notebook and marks the task, and so the job run, as
# MAGIC failed.

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
min_rows = int(dbutils.widgets.get("min_rows"))

rows = spark.table(f"{catalog}.{schema}.gold_fact_sales").count()
if rows < min_rows:
    raise ValueError(f"gold_fact_sales has {rows} rows, expected at least {min_rows}")
print(f"gold_fact_sales: {rows} rows, at least {min_rows} as expected")
