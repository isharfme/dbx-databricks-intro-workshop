# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Auto Loader
# MAGIC
# MAGIC By the end of this notebook you'll have loaded a stream of CSV extracts into
# MAGIC a table with Auto Loader, seen it load every file exactly once however often
# MAGIC you run it, and seen it keep loading when the source changes its schema. The
# MAGIC finished version is `reference/03-auto-loader`; open it from the file
# MAGIC browser on the left if you get stuck.

# COMMAND ----------

# MAGIC %md
# MAGIC The config makes your schema the current one, as in notebook 01, and names
# MAGIC the two folders this notebook uses, both in your `raw` volume: `sales`,
# MAGIC where the extracts arrive, and `_checkpoints/bronze_sales`, where Auto Loader
# MAGIC keeps track of them. The last line creates the volume if you skipped
# MAGIC Exercise 1.1.

# COMMAND ----------

import os
import re

from pyspark.sql import functions as F

SCHEMA = re.sub(r"\W", "_", spark.sql("SELECT current_user()").first()[0].split("@")[0])  # your own schema, named after your user
spark.catalog.setCurrentCatalog("dbacademy")
spark.catalog.setCurrentDatabase(SCHEMA)

SALES = f"/Volumes/dbacademy/{SCHEMA}/raw/sales"
CHECKPOINT = f"/Volumes/dbacademy/{SCHEMA}/raw/_checkpoints/bronze_sales"
spark.sql("CREATE VOLUME IF NOT EXISTS raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Meet the source system
# MAGIC
# MAGIC The data in this workshop is Bakehouse, a sample dataset of a fictional chain
# MAGIC of cookie franchises. It sits in the catalog `samples`, which the workspace
# MAGIC receives as a read-only share: in *Catalog* it's listed under *Shares
# MAGIC received*. Its schema `bakehouse` holds the tables of Bakehouse's sales
# MAGIC system. Every sale is one row in `sales_transactions`:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM samples.bakehouse.sales_transactions LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC Sales systems rarely let a data platform query their database directly.
# MAGIC More often they export what's new as files, every day or every few hours,
# MAGIC and drop them where the data team can pick them up. In this notebook you
# MAGIC play that sales system: you'll export the sales from
# MAGIC `samples.bakehouse.sales_transactions` as CSV files into the `sales` folder
# MAGIC of your volume, a few days at a time.
# MAGIC
# MAGIC The `deliver` function writes one extract. `toPandas()` collects a small
# MAGIC DataFrame onto the driver as a pandas DataFrame, and pandas' `to_csv` writes
# MAGIC it as a single file, the way you wrote `numbers.csv` in notebook 01. The
# MAGIC export leaves out `cardNumber`; nobody on the data team needs card numbers.

# COMMAND ----------

def deliver(df, file_name):
    """Write one extract as a single CSV file into the sales folder."""
    os.makedirs(SALES, exist_ok=True)
    df.toPandas().to_csv(f"{SALES}/{file_name}", index=False)
    print(f"Delivered {file_name}: {df.count()} rows")


EXTRACT_COLUMNS = ["transactionID", "customerID", "franchiseID", "dateTime", "product",
                   "quantity", "unitPrice", "totalPrice", "paymentMethod"]
sales = spark.table("samples.bakehouse.sales_transactions")
sale_date = F.to_date("dateTime")

# COMMAND ----------

# MAGIC %md
# MAGIC Bakehouse is clean sample data, and real extracts rarely are, so the export
# MAGIC roughs it up a little, the way a real source system might:
# MAGIC
# MAGIC - The first extract, the sales from 1 to 6 May 2024, writes some payment
# MAGIC   methods in capitals with a trailing space.
# MAGIC - The second, 7 to 12 May, sends about 3% of its sales twice.
# MAGIC
# MAGIC Notebook 04 cleans both up. Run the cell to deliver the two extracts:

# COMMAND ----------

first = (
    sales.filter(sale_date <= "2024-05-06")
    .select(EXTRACT_COLUMNS)
    .withColumn("paymentMethod",
                F.when(F.col("transactionID") % 5 == 0, F.concat(F.upper("paymentMethod"), F.lit(" ")))
                 .otherwise(F.col("paymentMethod")))
)
second = sales.filter(sale_date.between("2024-05-07", "2024-05-12")).select(EXTRACT_COLUMNS)
second = second.unionByName(second.filter(F.col("transactionID") % 30 == 0))  # the resent sales

deliver(first, "sales_2024-05-01_06.csv")
deliver(second, "sales_2024-05-07_12.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load each file exactly once
# MAGIC
# MAGIC A table fed by extracts must load every file exactly once: no gaps, no
# MAGIC double loads. The extracts keep arriving, so you can't reload the whole
# MAGIC folder every time, and writing it by hand means keeping a list of the files
# MAGIC you've already loaded. **Auto Loader** does it for you. It reads a folder as a
# MAGIC *stream*, loads only the files that arrived since its last run, and keeps
# MAGIC its progress in a *checkpoint*. We keep files on a volume here, but Auto
# MAGIC Loader reads from cloud object storage in the same way.
# MAGIC
# MAGIC An Auto Loader load has three statements:
# MAGIC
# MAGIC 1. **Define the source** with `spark.readStream`. Nothing runs yet; like any
# MAGIC    DataFrame, it's a plan.
# MAGIC    - `.format("cloudFiles")` selects Auto Loader, and the option
# MAGIC      `cloudFiles.format` says what the files are (`csv`, `json`, …). The
# MAGIC      option `header` says the first line of every file holds the column
# MAGIC      names.
# MAGIC    - `cloudFiles.schemaLocation` is a folder where Auto Loader stores the
# MAGIC      schema it inferred from the files, so later runs use the same one. For
# MAGIC      CSV and JSON it infers every column as a string.
# MAGIC    - `cloudFiles.schemaEvolutionMode` says what to do when the files change
# MAGIC      shape; the next section is about that.
# MAGIC    - `.load(path)` is the folder to watch.
# MAGIC    - The two `withColumn` lines add audit columns: `_ingested_at`, when the
# MAGIC      row was loaded, and `_source_file`, the file it came from, taken from the
# MAGIC      hidden `_metadata` column Databricks provides for every file-based read.
# MAGIC 2. **Start the stream** with `.writeStream`.
# MAGIC    - `checkpointLocation` is a folder where the stream records which files it
# MAGIC      has processed. It's what makes the next run skip them. Keep it next to
# MAGIC      the data it belongs to, but never inside the folder Auto Loader reads.
# MAGIC    - `.trigger(availableNow=True)` processes everything that's new and then
# MAGIC      stops, so a scheduled job can run it like a batch load.
# MAGIC    - `.toTable(name)` appends the rows to a table, creating it on the first
# MAGIC      run, and returns a handle to the running stream.
# MAGIC 3. **Wait** with `.awaitTermination()`, which returns once the run has
# MAGIC    processed everything new.
# MAGIC
# MAGIC After the run, the stream's `recentProgress` reports what it did. Its
# MAGIC `numFilesProcessed` metric counts only the files this run read, so you can
# MAGIC see the load is incremental.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decide what happens when the schema changes
# MAGIC
# MAGIC Sooner or later a source system changes its extract: a new release adds a
# MAGIC column, renames one, or starts sending text where there used to be numbers.
# MAGIC The table you load into is the first layer of everything downstream, so
# MAGIC its job is to take in whatever arrives, and never to lose data or stop
# MAGIC loading because of it. Notebook 04 explains that layer, *bronze*.
# MAGIC
# MAGIC Auto Loader's answer is the *rescued data column*, `_rescued_data`. When it
# MAGIC infers the schema, it adds this column, and anything in a file that doesn't
# MAGIC fit the schema lands there as a small JSON document, together with the path
# MAGIC of the file it came from: a column the schema doesn't have, a value of the
# MAGIC wrong type, or a column name in a different case. The load carries on, and
# MAGIC nothing is lost.
# MAGIC
# MAGIC What Auto Loader does with a *new* column is set by
# MAGIC `cloudFiles.schemaEvolutionMode`. The three modes that matter most:
# MAGIC
# MAGIC | Mode | When a file brings a new column |
# MAGIC | --- | --- |
# MAGIC | `addNewColumns` (the default when Auto Loader infers the schema) | The stream stops with an error after adding the column to the schema; the next run carries on with the new schema. Run it in a job with retries, and the retry picks up the new column. |
# MAGIC | `rescue` | The schema never changes and the stream never stops; the new column's values go into `_rescued_data`. |
# MAGIC | `failOnNewColumns` | The stream stops and stays stopped until someone changes the schema or removes the file. |
# MAGIC
# MAGIC We use `rescue`: the table keeps the columns everyone has agreed on, and a
# MAGIC change from the source shows up in `_rescued_data` instead of in the
# MAGIC table's schema. Someone then decides whether the new column belongs in the
# MAGIC table.
# MAGIC
# MAGIC You can also tell Auto Loader more about the schema up front:
# MAGIC
# MAGIC | Option | How | When it fits |
# MAGIC | --- | --- | --- |
# MAGIC | Infer | Nothing to configure | Exploring a new source, and loads that must never fail |
# MAGIC | Infer with hints | `cloudFiles.schemaHints`, e.g. `"quantity INT"` | You know a few columns, the rest may change |
# MAGIC | Declare | `.schema("transactionID STRING, quantity INT, ...")` | You have an agreed contract with the source |
# MAGIC
# MAGIC A declared schema adds no `_rescued_data` column unless you also set the
# MAGIC option `rescuedDataColumn`, so set it whenever you declare one. In this
# MAGIC notebook Auto Loader infers the schema.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.1 — Your first Auto Loader stream
# MAGIC
# MAGIC Fill in the five `...` so the cell loads the CSV files in the `sales`
# MAGIC folder into the table `bronze_sales`. Each placeholder's comment says what
# MAGIC goes there; the list above has the values.
# MAGIC
# MAGIC Below the load, the cell prints how many new files this run read and shows
# MAGIC the table's rows per source file, newest first.

# COMMAND ----------

# TODO: fill in the five ... placeholders
sales_stream = (
    spark.readStream
    .format(...)                                   # the format name that selects Auto Loader
    .option("cloudFiles.format", ...)              # the type of the files
    .option("header", "true")
    .option("cloudFiles.schemaLocation", CHECKPOINT)
    .option("cloudFiles.schemaEvolutionMode", "rescue")
    .load(...)                                     # the folder the extracts arrive in
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

sales_query = (
    sales_stream.writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(...)                                  # process everything new, then stop
    .toTable(...)                                  # the table to append to, as a string
)
sales_query.awaitTermination()

# What this run read: numFilesProcessed counts only the files Auto Loader hadn't seen.
new_files = sum(
    int(s["metrics"].get("numFilesProcessed", 0))
    for p in sales_query.recentProgress
    for s in p["sources"]
)
print(f"This run ingested {new_files} new file(s); bronze_sales now holds "
      f"{spark.table('bronze_sales').count()} rows.")

display(
    spark.table("bronze_sales")
    .groupBy("_source_file")
    .agg(F.count("*").alias("rows"), F.min("_ingested_at").alias("first_ingested"))
    .orderBy(F.col("first_ingested").desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC The first run read both files. Run the load cell once more without
# MAGIC delivering anything: it reports zero new files, and the row counts don't
# MAGIC change, because the checkpoint already lists both files.
# MAGIC
# MAGIC Look at the table's column types, too. Every column is a `string`, and
# MAGIC there's the `_rescued_data` column Auto Loader added:

# COMMAND ----------

spark.table("bronze_sales").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Change the schema at the source
# MAGIC
# MAGIC A new release of the sales system now exports card numbers as well: the
# MAGIC third extract, 13 to 17 May, has an extra column at the end, `cardNumber`.
# MAGIC Nobody told the data team. Deliver it:

# COMMAND ----------

third = sales.filter(sale_date >= "2024-05-13").select(EXTRACT_COLUMNS + ["cardNumber"])
deliver(third, "sales_2024-05-13_17.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Now run the load cell of Exercise 3.1 again. It reports one new file, and
# MAGIC the load succeeds even though the file doesn't match the table: the schema
# MAGIC of `bronze_sales` hasn't changed. The card numbers are in `_rescued_data`.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3.2 — Find the rescued data
# MAGIC
# MAGIC Show the rows of `bronze_sales` that have rescued data, with the card number
# MAGIC pulled out of the JSON. Keep the result in `rescued`.
# MAGIC
# MAGIC - `.filter(F.col("X").isNotNull())` keeps the rows where `X` has a value.
# MAGIC - `F.get_json_object("X", "$.name")` reads the field `name` from the JSON
# MAGIC   document in column `X`.

# COMMAND ----------

# TODO: fill in the two ... placeholders
rescued = (
    spark.table("bronze_sales")
    .filter(...)                                   # rows where _rescued_data has a value
    .select(
        "transactionID",
        "_source_file",
        F.get_json_object("_rescued_data", ...).alias("cardNumber"),   # the JSON path of the cardNumber field
    )
)
display(rescued)

# COMMAND ----------

# MAGIC %md
# MAGIC Every file was loaded exactly once, however often you ran the load, and the
# MAGIC unexpected column cost you nothing: the load kept running, the table's
# MAGIC schema stayed as agreed, and every card number is still there, in
# MAGIC `_rescued_data`, should someone decide it's needed after all. That's what
# MAGIC makes a load safe to schedule and safe to rerun.
# MAGIC
# MAGIC `bronze_sales` is the first table of the next notebook, where it becomes the
# MAGIC bronze layer of a medallion architecture. Next: open `04-medallion` from the
# MAGIC file browser on the left.
