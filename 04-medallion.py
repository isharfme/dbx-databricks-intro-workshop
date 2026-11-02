# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Medallion, layer by layer
# MAGIC
# MAGIC By the end of this notebook you'll have the Bakehouse sales, franchises and
# MAGIC customers in your own schema as bronze, silver and gold tables: extracts
# MAGIC loaded as received, cleaned and deduplicated silver tables, and a gold fact
# MAGIC and dimensions shaped for querying. The table you loaded with Auto Loader in
# MAGIC notebook 03 is your first bronze table; everything here builds on it. The
# MAGIC full version of everything here is `reference/04-medallion`; open it from the
# MAGIC file browser on the left if you get stuck.

# COMMAND ----------

# MAGIC %md
# MAGIC The config makes your schema in `dbacademy` the default for this session, so
# MAGIC table names below don't need the catalog and schema in front of them, in
# MAGIC Python and in `%sql` cells alike.

# COMMAND ----------

import re

from pyspark.sql import functions as F

SCHEMA = re.sub(r"\W", "_", spark.sql("SELECT current_user()").first()[0].split("@")[0])  # your own schema, named after your user
spark.catalog.setCurrentCatalog("dbacademy")
spark.catalog.setCurrentDatabase(SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Understand bronze, silver and gold
# MAGIC
# MAGIC The medallion architecture is a convention for organising a lakehouse into
# MAGIC layers, where each layer makes a promise about the data in it. Data moves
# MAGIC forward one layer at a time, and each step does one kind of work:
# MAGIC
# MAGIC | Layer | What it promises | What the step into it does |
# MAGIC | --- | --- | --- |
# MAGIC | Bronze | Exactly what the source sent, plus where and when it arrived | Load files incrementally; add audit columns; no cleaning |
# MAGIC | Silver | One clean, typed row per business key | Drop invalid rows, deduplicate, standardise codes, cast types |
# MAGIC | Gold | Shaped for the queries you expect | Select, join, aggregate; lay out for fast reads |
# MAGIC
# MAGIC Each layer is designed around the job it does.
# MAGIC
# MAGIC **Bronze keeps exactly what the source sent.** It's the record of what
# MAGIC arrived, so you can always replay it: if a silver rule turns out to be wrong,
# MAGIC you fix the rule and rebuild silver from bronze, and you never have to ask the
# MAGIC source system for the extract again. That's why bronze doesn't reject
# MAGIC anything. Columns are kept as strings, and anything unexpected goes into
# MAGIC `_rescued_data` instead of failing the load, as the card numbers did in
# MAGIC notebook 03. A source that adds a column or sends `"n/a"` in an amount field
# MAGIC doesn't stop ingestion; bronze takes it in, and the problem surfaces
# MAGIC downstream where you can deal with it. It's the same idea as a staging area
# MAGIC in a classic ETL tool, except that bronze is kept and queryable rather than
# MAGIC truncated after each load.
# MAGIC
# MAGIC **Silver is the source of truth.** This is where the schema is enforced:
# MAGIC types are cast, codes standardised, invalid rows dropped and duplicates
# MAGIC removed, so there's one clean row per business key. Everything downstream,
# MAGIC every report and model, builds on silver, so the rules for what a valid sale
# MAGIC is live here and only here.
# MAGIC
# MAGIC **Gold is an optimisation layer.** It's shaped for the questions you expect
# MAGIC people to ask: joined and aggregated ahead of time, and laid out for fast
# MAGIC reads, so a dashboard reads a small, ready-made table instead of recomputing
# MAGIC the same joins and sums on every query. You can add or reshape a gold table
# MAGIC whenever a new question comes up, because silver holds the detail.
# MAGIC
# MAGIC In this notebook you build each layer by hand, one step at a time, so you can
# MAGIC see what each one takes. The techniques — cleaning, `MERGE`, star schemas,
# MAGIC clustering — are the everyday tools of a data engineer on Databricks,
# MAGIC whatever runs them in the end.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Complete bronze with the master data
# MAGIC
# MAGIC `bronze_sales` is already there from notebook 03. The sales refer to two
# MAGIC kinds of master data, franchises and customers, by `franchiseID` and
# MAGIC `customerID`. Master data is small and changes rarely, so source systems
# MAGIC usually send it whole rather than as a stream of new rows; here we read it
# MAGIC straight from the Bakehouse tables. Each load replaces the previous copy,
# MAGIC which is what `CREATE OR REPLACE TABLE ... AS SELECT` does, and adds the same
# MAGIC `_ingested_at` audit column as `bronze_sales`:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE bronze_franchises AS
# MAGIC SELECT *, current_timestamp() AS _ingested_at FROM samples.bakehouse.sales_franchises;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE bronze_customers AS
# MAGIC SELECT *, current_timestamp() AS _ingested_at FROM samples.bakehouse.sales_customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build silver
# MAGIC
# MAGIC We build `silver_sales` together, then you do the same for customers. Here
# MAGIC is what the sales extracts sent that we need to fix:
# MAGIC
# MAGIC - **Duplicates.** About 3% of the sales from 7 to 12 May arrived twice.
# MAGIC   Silver promises one row per business key, `transactionID`.
# MAGIC - **Messy codes.** `paymentMethod` sometimes arrives in capitals with a
# MAGIC   trailing space.
# MAGIC - **Types.** Everything is still a string from bronze.
# MAGIC - **Missing keys.** None in these extracts, but a sale without a
# MAGIC   `transactionID` can't be used, so silver drops any that arrive.
# MAGIC
# MAGIC Silver also decides which columns it keeps. The card numbers that arrived
# MAGIC in `_rescued_data` stay in bronze: nobody analysing revenue needs them, and
# MAGIC leaving them out keeps them out of every table built on silver.
# MAGIC
# MAGIC Read the cleaning step as a `SELECT ... FROM bronze_sales WHERE ...`:
# MAGIC `filter` is the `WHERE`, `dropDuplicates` keeps one row per key, and
# MAGIC `select` lists the output columns with their expressions. `cast` converts a
# MAGIC string to a type. For the timestamp we use `try_to_timestamp`, which returns
# MAGIC NULL when a string isn't a valid timestamp: serverless runs Spark in ANSI
# MAGIC mode, where a plain conversion raises an error on the first bad value
# MAGIC instead.

# COMMAND ----------

sales_clean = (
    spark.table("bronze_sales")
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
)
sales_clean.createOrReplaceTempView("sales_clean")
display(sales_clean.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Nothing has been written yet; `sales_clean` is only a plan, as you saw in
# MAGIC notebook 02. `createOrReplaceTempView` gives it a name, `sales_clean`, that
# MAGIC SQL cells can read; the view exists only in this session and stores nothing.
# MAGIC The next section writes it.
# MAGIC
# MAGIC Notice that the filter drops rows silently: nothing records how many sales it
# MAGIC removed, or why. Counting what a rule removes, and deciding whether a
# MAGIC violation should drop the row or stop the load, is what *expectations* in
# MAGIC Spark Declarative Pipelines are for; notebook 05 comes back to that.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make silver rerun-safe with `MERGE`
# MAGIC
# MAGIC Writing a DataFrame to a Delta table can go three ways:
# MAGIC
# MAGIC | Write | What happens on a rerun | Fits |
# MAGIC | --- | --- | --- |
# MAGIC | `mode("overwrite")` | The table is replaced with the new result | Small tables you can always rebuild completely |
# MAGIC | `mode("append")` | The same rows are added again, so you get duplicates | Only when the input is guaranteed to be new |
# MAGIC | `MERGE INTO` | Matching keys are updated, new keys inserted | Incremental loads into a keyed table |
# MAGIC
# MAGIC Jobs get retried, people rerun cells, and a failed load gets restarted
# MAGIC halfway. A load is *idempotent* when running it twice leaves the table in the
# MAGIC same state as running it once, and that's the property you want for silver.
# MAGIC `MERGE` gives it to you: the second run finds every key already there and
# MAGIC updates it with the same values.
# MAGIC
# MAGIC The next two cells switch to SQL. PySpark requires more code to implement the
# MAGIC same functionality, and a notebook can switch languages from cell to cell, so
# MAGIC we use the simpler implementation where it is simpler.
# MAGIC
# MAGIC First, the table. Declaring its columns and types up front is silver
# MAGIC enforcing the schema: `dateTime` is a `TIMESTAMP`, the prices are
# MAGIC `DECIMAL(12,2)`, and neither the key nor the timestamp may be NULL. A write
# MAGIC that doesn't fit fails instead of quietly changing the table. `IF NOT EXISTS`
# MAGIC makes the cell safe to rerun.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_sales (
# MAGIC   transactionID BIGINT NOT NULL,
# MAGIC   customerID    BIGINT,
# MAGIC   franchiseID   BIGINT,
# MAGIC   dateTime      TIMESTAMP NOT NULL,
# MAGIC   product       STRING,
# MAGIC   quantity      INT,
# MAGIC   unitPrice     DECIMAL(12,2),
# MAGIC   totalPrice    DECIMAL(12,2),
# MAGIC   paymentMethod STRING
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC Then the `MERGE`, from the `sales_clean` view into the table, matching on
# MAGIC `transactionID`:

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO silver_sales AS t
# MAGIC USING sales_clean AS s
# MAGIC ON t.transactionID = s.transactionID
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC The result shows what the `MERGE` did: `num_inserted_rows` is every sale on
# MAGIC the first run, and `num_updated_rows` is 0 because the table was empty. Run
# MAGIC the `MERGE` cell again: now every key is already there, so
# MAGIC `num_updated_rows` is every sale and `num_inserted_rows` is 0. The row count
# MAGIC doesn't change:

# COMMAND ----------

print(f"silver_sales: {spark.table('silver_sales').count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC `MERGE` needs at most one source row per key, which is one more reason to
# MAGIC deduplicate before it; with duplicates in the source, Delta stops the `MERGE`
# MAGIC with an error rather than guess which row wins.
# MAGIC
# MAGIC The franchises fit the first row of the table above: a few dozen rows, sent
# MAGIC whole, so silver simply rebuilds them with `mode("overwrite")` on every load.
# MAGIC Silver keeps the columns reports need and renames `name`, which on its own
# MAGIC wouldn't say whose name it is:

# COMMAND ----------

(spark.table("bronze_franchises")
    .select("franchiseID", F.col("name").alias("franchise_name"), "city", "country")
    .write.mode("overwrite").saveAsTable("silver_franchises"))

display(spark.table("silver_franchises"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.1 — Silver customers
# MAGIC
# MAGIC Build `silver_customers` from `bronze_customers` with a `MERGE`, the same way
# MAGIC as the sales. The customer master is clean today, but silver's rules are
# MAGIC there for the day it isn't. First the cleaning, in Python:
# MAGIC
# MAGIC 1. Drop rows where `customerID` is NULL.
# MAGIC    `.filter(F.col("X").isNotNull())` keeps only the rows where `X` has a value.
# MAGIC 2. Keep one row per `customerID`.
# MAGIC    `.dropDuplicates(["X"])` keeps one row for each value of `X`.
# MAGIC 3. Keep `customerID`, `first_name`, `last_name`, `city` and `country`; the
# MAGIC    contact details and the address stay in bronze. Trim the names, and trim
# MAGIC    and upper-case `country`.
# MAGIC    `F.trim(F.col("X"))` removes spaces at both ends, `F.upper(...)` upper-cases,
# MAGIC    and `.alias("X")` names the result, so
# MAGIC    `F.upper(F.trim(F.col("X"))).alias("X")` does both and keeps the name.
# MAGIC
# MAGIC The cell registers the result as the view `customers_clean`.

# COMMAND ----------

customers_clean = (
    spark.table("bronze_customers")
    .filter(...)                                   # keep rows where customerID has a value
    .dropDuplicates(...)                           # one row per customerID
    .select(
        "customerID",
        ...,                                       # first_name, trimmed
        ...,                                       # last_name, trimmed
        "city",
        ...,                                       # country, trimmed and upper-case
    )
)
customers_clean.createOrReplaceTempView("customers_clean")
display(customers_clean)

# COMMAND ----------

# MAGIC %md
# MAGIC Then the table and the `MERGE`, in SQL as before. The table is given; complete
# MAGIC the `MERGE` so it merges `customers_clean` into `silver_customers` on
# MAGIC `customerID`, following the sales `MERGE` above. Run the cell; the check
# MAGIC compares the result with the source.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS silver_customers (
# MAGIC   customerID BIGINT NOT NULL,
# MAGIC   first_name STRING,
# MAGIC   last_name  STRING,
# MAGIC   city       STRING,
# MAGIC   country    STRING
# MAGIC );
# MAGIC
# MAGIC MERGE INTO ... AS t                              -- the target table
# MAGIC USING ... AS s                                   -- the view with the cleaned rows
# MAGIC ON ...                                           -- match on the key
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# Check
sc = spark.table("silver_customers")
n = sc.count()
expected = spark.table("samples.bakehouse.sales_customers").select("customerID").distinct().count()
assert n == expected, f"Expected {expected} customers, found {n}"
assert set(sc.columns) == {"customerID", "first_name", "last_name", "city", "country"}, f"Unexpected columns: {sc.columns}"
assert sc.select("customerID").distinct().count() == n, "There is more than one row for some customers"
assert sc.filter(F.col("country") != F.upper(F.trim(F.col("country")))).count() == 0, \
    "country should be trimmed and upper-case"
assert sc.filter(F.col("first_name") != F.trim(F.col("first_name"))).count() == 0, "first_name should be trimmed"
print("Check passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build gold
# MAGIC
# MAGIC Gold is where you stop thinking about the source and start thinking about
# MAGIC the reader. A common shape is a *star schema*: a narrow **fact** table with
# MAGIC one row per event (here, per sale) holding the keys, the timestamp and the
# MAGIC measures, and **dimension** tables holding the descriptive attributes you
# MAGIC filter and group by (here, the franchise's name and city, and the customer's
# MAGIC name and country). Queries join the fact to the dimensions on the keys.
# MAGIC
# MAGIC Gold is also where you lay the data out for the queries you expect. Most
# MAGIC questions about sales filter on a date range, so the fact table is written
# MAGIC with `clusterBy("dateTime")`. *Liquid clustering* keeps rows with similar
# MAGIC timestamps in the same files, so a query for one day reads only the files
# MAGIC that can contain that day and skips the rest. Clustering columns are set
# MAGIC when the table is created or overwritten.
# MAGIC
# MAGIC All three tables are rebuilt completely with `mode("overwrite")`: gold is
# MAGIC fully derived from silver, so a full rebuild is cheap, idempotent and always
# MAGIC consistent. Silver already holds only valid sales, so the fact takes all of
# MAGIC them.

# COMMAND ----------

(spark.table("silver_franchises")
    .write.mode("overwrite").saveAsTable("gold_dim_franchise"))

(spark.table("silver_customers")
    .write.mode("overwrite").saveAsTable("gold_dim_customer"))

(spark.table("silver_sales")
    .select("transactionID", "customerID", "franchiseID", "dateTime", "product",
            "quantity", "unitPrice", "totalPrice", "paymentMethod")
    .write.mode("overwrite").clusterBy("dateTime").saveAsTable("gold_fact_sales"))

display(spark.table("gold_fact_sales").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 4.2 — Query the star schema
# MAGIC
# MAGIC Write the query a franchise report would run: the revenue per franchise
# MAGIC city and day. Keep the result in a DataFrame named `revenue_by_city`.
# MAGIC
# MAGIC 1. Join the fact with the franchise dimension on `franchiseID`.
# MAGIC    `df.join(other, "X")` joins two DataFrames on the column `X`, like
# MAGIC    `JOIN other USING (X)` in SQL.
# MAGIC 2. Add `sale_date`, the date of `dateTime`.
# MAGIC    `.withColumn("NEW", expr)` adds a column, and `F.to_date(F.col("X"))`
# MAGIC    turns a timestamp into a date.
# MAGIC 3. Group by `city` and `sale_date` and sum `totalPrice` into a column named
# MAGIC    `revenue`. `.groupBy("A", "B").agg(F.sum("X").alias("Y"))` is
# MAGIC    `SELECT A, B, sum(X) AS Y ... GROUP BY A, B`.

# COMMAND ----------

fact = spark.table("gold_fact_sales")
dim = spark.table("gold_dim_franchise")

revenue_by_city = (
    fact
    .join(...)                                     # the franchise dimension, on franchiseID
    .withColumn("sale_date", ...)                  # the date of dateTime
    .groupBy(...)                                  # the two columns to group by
    .agg(...)                                      # the sum of totalPrice, named revenue
)
display(revenue_by_city.orderBy("sale_date", "city"))

# COMMAND ----------

# Check
assert set(revenue_by_city.columns) == {"city", "sale_date", "revenue"}, f"Unexpected columns: {revenue_by_city.columns}"
assert revenue_by_city.filter("city IS NULL OR sale_date IS NULL OR revenue IS NULL").count() == 0, \
    "Some rows have a NULL city, sale_date or revenue"
expected_total = fact.join(dim, "franchiseID").agg(F.sum("totalPrice")).first()[0]
got_total = revenue_by_city.agg(F.sum("revenue")).first()[0]
assert got_total == expected_total, f"Total revenue should be {expected_total}, got {got_total}"
assert revenue_by_city.select("city", "sale_date").distinct().count() == revenue_by_city.count(), \
    "Expected one row per city and sale_date"
expected_days = fact.select(F.to_date("dateTime")).distinct().count()
assert revenue_by_city.select("sale_date").distinct().count() == expected_days, f"Expected {expected_days} days"
print("Check passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC If a report runs this query often, gold can store its result too: one more
# MAGIC gold table, rebuilt from the fact and the dimension after every load, so
# MAGIC the join and the aggregation run once per load rather than once per read. A
# MAGIC plain view is different: it stores nothing and recomputes on every read, so
# MAGIC it gives consumers a stable interface but doesn't make anything faster.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trace lineage in Catalog Explorer
# MAGIC
# MAGIC Unity Catalog records which tables each write read from, without any setup
# MAGIC on your side. Open `gold_fact_sales` in *Catalog Explorer* and select the
# MAGIC *Lineage* tab: you'll see it was built from `silver_sales`, and you can
# MAGIC follow `silver_sales` back to `bronze_sales`. When a number in a report
# MAGIC looks wrong, this is the quickest way to find which table to look at.
# MAGIC
# MAGIC Next: open `05-jobs-operations` from the file browser on the left.
