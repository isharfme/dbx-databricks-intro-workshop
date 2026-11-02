# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Spark concepts
# MAGIC
# MAGIC By the end of this notebook you'll know where your code runs, why Spark
# MAGIC waits until you ask for a result, and how the SQL you already write maps to
# MAGIC PySpark. Everything works on small DataFrames built right here in the
# MAGIC notebook, with no files or tables to set up, so the focus stays on Spark
# MAGIC itself. This is the solved version of
# MAGIC `02-spark-concepts`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Understand the cluster: driver and workers
# MAGIC
# MAGIC Spark spreads one piece of work over many machines. On a classic cluster
# MAGIC there are two roles:
# MAGIC
# MAGIC ```
# MAGIC                 your notebook
# MAGIC                      │
# MAGIC              ┌───────▼────────┐
# MAGIC              │     driver     │   plans the query, splits it into tasks,
# MAGIC              └───────┬────────┘   collects the results
# MAGIC        ┌─────────────┼─────────────┐
# MAGIC  ┌─────▼─────┐ ┌─────▼─────┐ ┌─────▼─────┐
# MAGIC  │  worker   │ │  worker   │ │  worker   │   each runs tasks in parallel,
# MAGIC  │ (executor)│ │ (executor)│ │ (executor)│   one per partition of the data
# MAGIC  └───────────┘ └───────────┘ └───────────┘
# MAGIC ```
# MAGIC
# MAGIC The data is split into *partitions*, and each task processes one partition.
# MAGIC That's why Spark scales: twice the workers, roughly twice the partitions
# MAGIC processed at once. Your Python code runs on the driver; it describes the
# MAGIC work, and the workers do it.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compare classic and serverless compute
# MAGIC
# MAGIC | | Classic compute | Serverless compute |
# MAGIC | --- | --- | --- |
# MAGIC | Who manages the machines | You: instance types, worker count, autoscaling, runtime version | Databricks |
# MAGIC | Start-up | Minutes, while the cluster boots | Seconds |
# MAGIC | Inspecting execution | Spark UI | Query profile |
# MAGIC | Typical use | Workloads that need specific machines, libraries or settings | The default for notebooks, jobs and pipelines |
# MAGIC
# MAGIC The driver/worker model is the same underneath; serverless just takes the
# MAGIC sizing and operation off your hands. Some low-level features aren't
# MAGIC available on serverless — for example, DataFrame caching and continuous
# MAGIC streaming triggers — and that won't get in your way today.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Know what the Databricks Runtime and Photon are
# MAGIC
# MAGIC The **Databricks Runtime** (DBR) is the software image the compute runs:
# MAGIC Apache Spark plus Databricks' own improvements, Delta Lake, and a set of
# MAGIC preinstalled libraries. It is versioned, and the long-term-support (LTS)
# MAGIC releases are the ones you'd pin production jobs to on classic compute. On
# MAGIC serverless, Databricks keeps the runtime up to date for you.
# MAGIC
# MAGIC **Photon** is Databricks' native, vectorized query engine. It executes the
# MAGIC same DataFrame and SQL operations as Spark, only faster, and you don't change
# MAGIC a line of code to use it. You'll spot it in query plans as operators whose
# MAGIC names start with `Photon`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a DataFrame from inline data
# MAGIC
# MAGIC A **DataFrame** is Spark's table: named, typed columns and rows, spread over
# MAGIC partitions. Most of the time you'd read one from a table or files; to keep
# MAGIC this notebook self-contained we build two small ones from Python lists.
# MAGIC
# MAGIC The Python you need for this, in passing:
# MAGIC
# MAGIC - A list is `[a, b, c]`; a tuple `(a, b, c)` is one row.
# MAGIC - `from x import y` makes a library available. By convention, Spark's
# MAGIC   built-in functions are imported as `F`, so `F.sum`, `F.year`, `F.col`.
# MAGIC - Long expressions are wrapped in parentheses so they can span several
# MAGIC   lines, one step per line.
# MAGIC
# MAGIC The schema is a DDL string, written exactly as it would be in
# MAGIC `CREATE TABLE`.

# COMMAND ----------

from datetime import date

from pyspark.sql import functions as F

orders = [
    ("SO-001", "C01", 1250.00, "EUR", date(2024, 11, 5)),
    ("SO-002", "C02", 430.50, "EUR", date(2024, 11, 18)),
    ("SO-003", "C03", 2890.00, "GBP", date(2024, 12, 2)),
    ("SO-004", "C01", 760.00, "EUR", date(2025, 1, 9)),
    ("SO-005", "C04", 15400.00, "NOK", date(2025, 1, 21)),
    ("SO-006", "C02", 1980.00, "EUR", date(2025, 2, 3)),
    ("SO-007", "C05", 320.00, "EUR", date(2025, 2, 14)),
    ("SO-008", "C03", 1105.75, "GBP", date(2025, 3, 1)),
    ("SO-009", "C01", 5400.00, "EUR", date(2025, 3, 17)),
    ("SO-010", "C05", 990.00, "EUR", date(2025, 4, 2)),
    ("SO-011", "C04", 8200.00, "NOK", date(2025, 4, 22)),
    ("SO-012", "C02", 640.00, "EUR", date(2025, 5, 6)),
]
customers = [("C01", "DE"), ("C02", "FR"), ("C03", "GB"), ("C04", "NO"), ("C05", "DE")]

orders_df = spark.createDataFrame(
    orders, "order_id STRING, customer_id STRING, amount DOUBLE, currency STRING, order_date DATE")
customers_df = spark.createDataFrame(customers, "customer_id STRING, country STRING")

display(orders_df)

# COMMAND ----------

# MAGIC %md
# MAGIC `printSchema()` shows the columns and types, the equivalent of `DESCRIBE`:

# COMMAND ----------

orders_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Translate SQL to PySpark
# MAGIC
# MAGIC Every clause you know from SQL has a DataFrame method. You chain them in the
# MAGIC order you'd think through the query, and each method returns a new DataFrame.
# MAGIC
# MAGIC | SQL | PySpark |
# MAGIC | --- | --- |
# MAGIC | `SELECT a, b` | `.select("a", "b")` |
# MAGIC | `SELECT a * 2 AS c` | `.withColumn("c", F.col("a") * 2)` (adds `c`, keeps the other columns) |
# MAGIC | `WHERE a > 10` | `.filter(F.col("a") > 10)` |
# MAGIC | `GROUP BY a` + `SUM(b)` | `.groupBy("a").agg(F.sum("b").alias("total"))` |
# MAGIC | `JOIN t ON x.k = t.k` | `.join(t, "k")` (or `.join(t, "k", "left")`) |
# MAGIC | `ORDER BY total DESC` | `.orderBy(F.desc("total"))` |
# MAGIC | `DISTINCT` | `.distinct()` |
# MAGIC | `LIMIT 10` | `.limit(10)` |
# MAGIC
# MAGIC `F.col("a")` refers to a column, and the functions in `F` are the same
# MAGIC built-in functions SQL has: `F.year`, `F.upper`, `F.coalesce`, `F.when`
# MAGIC (for `CASE WHEN`) and so on. Here's a query in both forms:
# MAGIC
# MAGIC ```sql
# MAGIC SELECT order_id, amount, upper(currency) AS currency
# MAGIC FROM orders WHERE currency = 'EUR'
# MAGIC ```

# COMMAND ----------

eur_orders = (
    orders_df
    .filter(F.col("currency") == "EUR")
    .select("order_id", "amount", F.upper("currency").alias("currency"))
)
display(eur_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC SQL stays available the whole time. Register a DataFrame as a temporary
# MAGIC view and query it with `spark.sql()`; both forms produce the same plan, so
# MAGIC pick whichever reads better for the step at hand.

# COMMAND ----------

orders_df.createOrReplaceTempView("orders")
display(spark.sql("SELECT order_id, amount, upper(currency) AS currency FROM orders WHERE currency = 'EUR'"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.1 — Filter and derive
# MAGIC
# MAGIC From `orders_df`, keep only the orders with an `amount` above 1,000 and add a
# MAGIC column `order_year` with the year of `order_date` (`F.year`). Keep the result
# MAGIC in `big_orders`.

# COMMAND ----------

big_orders = (
    orders_df
    .filter(F.col("amount") > 1000)
    .withColumn("order_year", F.year("order_date"))
)
display(big_orders)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Separate planning from execution
# MAGIC
# MAGIC Spark distinguishes two kinds of operations:
# MAGIC
# MAGIC - **Transformations** — `filter`, `select`, `withColumn`, `join`, `groupBy`
# MAGIC   — describe a new DataFrame. They return immediately and don't touch the
# MAGIC   data.
# MAGIC - **Actions** — `display`, `count`, `collect`, `show`, writing a table —
# MAGIC   ask for a result. Only an action makes Spark do work.
# MAGIC
# MAGIC This is *lazy evaluation*. While you chain transformations, Spark only builds
# MAGIC up a plan. When an action arrives, its optimizer looks at the whole plan at
# MAGIC once — pushing filters down, dropping columns nobody uses, choosing join
# MAGIC strategies — and only then runs it on the workers. It's the same thing your
# MAGIC database does with a SQL statement; in Spark it just spans several lines of
# MAGIC code.
# MAGIC
# MAGIC `explain()` prints the plan without running it. Read it from the bottom up:
# MAGIC the scan of the data is at the bottom, the result at the top.

# COMMAND ----------

eur_orders.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC To see what actually ran, use the **query profile**. On serverless, every
# MAGIC cell that runs a query offers it under the cell's output (*See performance*).
# MAGIC It shows the executed operators as a graph with the rows and time per step.
# MAGIC On classic compute the Spark UI shows the same information as jobs, stages
# MAGIC and tasks.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Join and aggregate
# MAGIC
# MAGIC Joins and aggregations work like their SQL counterparts. Here are total
# MAGIC order amounts per customer, with the country joined on:

# COMMAND ----------

per_customer = (
    orders_df
    .groupBy("customer_id")
    .agg(F.sum("amount").alias("total_amount"), F.count("*").alias("orders"))
    .join(customers_df, "customer_id")
    .orderBy(F.desc("total_amount"))
)
display(per_customer)

# COMMAND ----------

# MAGIC %md
# MAGIC These are the operations that move data between workers. To sum per
# MAGIC customer, all rows for one customer must end up on the same worker, so Spark
# MAGIC redistributes the rows by key across the network. That step is a
# MAGIC **shuffle**, and in a plan it appears as an `Exchange`. Shuffles are usually
# MAGIC the most expensive part of a query; when one side of a join is small, Spark
# MAGIC avoids shuffling the big side by sending a copy of the small one to every
# MAGIC worker instead (a *broadcast* join).

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.2 — Join and aggregate
# MAGIC
# MAGIC The orders come in different currencies, and adding EUR to NOK makes no
# MAGIC sense, so work with the EUR orders only. Join `orders_df` to `customers_df`, keep the `EUR` orders, and
# MAGIC total the `amount` per `country` as `total_amount`, largest first. Keep the
# MAGIC result in `revenue_by_country`.
# MAGIC
# MAGIC Then run `revenue_by_country.explain()` and find two things in the plan: the
# MAGIC join operator and the `Exchange` that the aggregation needs.

# COMMAND ----------

revenue_by_country = (
    orders_df
    .join(customers_df, "customer_id")
    .filter(F.col("currency") == "EUR")
    .groupBy("country")
    .agg(F.sum("amount").alias("total_amount"))
    .orderBy(F.desc("total_amount"))
)
display(revenue_by_country)

# COMMAND ----------

revenue_by_country.explain()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2.3 — Spot the action
# MAGIC
# MAGIC Read the cell below before you run it. Which line makes Spark actually
# MAGIC process data? Decide, then run it and check the answer underneath.

# COMMAND ----------

step1 = orders_df.filter(F.col("amount") > 500)
step2 = step1.withColumn("order_month", F.date_trunc("month", "order_date"))
step3 = step2.join(customers_df, "customer_id")
step4 = step3.groupBy("country", "order_month").agg(F.sum("amount").alias("total_amount"))
step4.show()

# COMMAND ----------

# MAGIC %md
# MAGIC **Answer.** Only `step4.show()`. The first four lines are transformations:
# MAGIC they return instantly and only extend the plan. `show()` is an action, so
# MAGIC Spark optimizes the plan built by all four steps and runs it as a single
# MAGIC query — open its query profile and you'll see filter, join and aggregation
# MAGIC in one graph.

# COMMAND ----------

# MAGIC %md
# MAGIC Everything in this notebook ran on data that lived inside the notebook. The
# MAGIC same DataFrame operations work unchanged on tables and files of any size;
# MAGIC the next notebook points them at real files, arriving one delivery at a
# MAGIC time.
# MAGIC
# MAGIC Next: open `03-auto-loader` from the file browser on the left.
