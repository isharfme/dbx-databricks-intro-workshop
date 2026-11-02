# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — The workspace
# MAGIC
# MAGIC By the end of this notebook you'll know your way around the Databricks
# MAGIC workspace, and you'll have created a volume of your own, written a file into
# MAGIC it with plain Python and queried it with SQL. The finished
# MAGIC version is `reference/01-workspace`; open it from the file browser on the
# MAGIC left if you get stuck.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pick your compute
# MAGIC
# MAGIC Every cell runs on *compute*, the machines that execute your code, so attach
# MAGIC the notebook to compute before you run anything. There are four kinds:
# MAGIC
# MAGIC | Compute | What it is |
# MAGIC | --- | --- |
# MAGIC | Serverless | Databricks provides, starts and scales the machines; there's nothing for you to size, start or stop. We use it throughout this workshop. |
# MAGIC | Serverless GPU (PuPr) | Serverless with GPUs, for training and fine-tuning deep learning models. |
# MAGIC | All-purpose cluster | A cluster you create, size and start yourself, for interactive work, and can share with others. It runs, and costs, until it's stopped or terminates automatically. |
# MAGIC | Job cluster | A cluster that a job creates when a run starts and terminates when the run ends. It serves that one run, not interactive work. |
# MAGIC
# MAGIC Pick **Serverless** in the compute selector at the top right of the
# MAGIC notebook. Notebook 02 explains what runs underneath.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Find your way around the workspace
# MAGIC
# MAGIC The left sidebar is where everything starts. The entries you'll use today:
# MAGIC
# MAGIC | Sidebar entry | What it's for |
# MAGIC | --- | --- |
# MAGIC | *Workspace* | Your files, notebooks and Git folders, in a folder tree |
# MAGIC | *Catalog* | Catalog Explorer: browse tables, volumes and their permissions in Unity Catalog |
# MAGIC | *Jobs & Pipelines* | Scheduled jobs (notebook 05) and declarative pipelines |
# MAGIC | *SQL Editor* | A plain SQL editor, if you'd rather not use a notebook |
# MAGIC
# MAGIC A notebook is a sequence of cells. Each cell holds code or text, runs on
# MAGIC its own, and shows its output right underneath. Variables live in the
# MAGIC notebook's session, so a later cell can use what an earlier cell defined.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run cells and mix SQL, Python and markdown
# MAGIC
# MAGIC Run a cell with **Shift+Enter**, or use *Run all* at the top to run the
# MAGIC whole notebook. The notebook's default language is Python; a *magic
# MAGIC command* on the first line switches one cell to another language: `%sql`,
# MAGIC `%md` for text like this cell, `%python`.
# MAGIC
# MAGIC Start with Python. `spark` is the entry point to Spark and is already
# MAGIC defined in every notebook:

# COMMAND ----------

print(f"Spark version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC The same session runs SQL. This cell is plain SQL; its result shows up as a
# MAGIC table:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT current_user() AS me, current_date() AS today

# COMMAND ----------

# MAGIC %md
# MAGIC The two languages share the session. `spark.sql()` runs the same query from
# MAGIC Python and returns a DataFrame. `.first()` gives you its first row, and you
# MAGIC pick the values out of the row by column name:

# COMMAND ----------

row = spark.sql("SELECT current_user() AS me, current_date() AS today").first()
me = row["me"]
today = row["today"]
print(f"{me}, today is {today}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Look at results with `display()`
# MAGIC
# MAGIC `print()` shows plain text. `display()` renders a DataFrame as an
# MAGIC interactive table: sort and filter the columns, add a chart with the **+**
# MAGIC next to the table tab, or generate a data profile with per-column
# MAGIC statistics. SQL cells use `display()` automatically.

# COMMAND ----------

display(spark.sql("SELECT id, id * id AS squared FROM range(10)"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Browse Unity Catalog
# MAGIC
# MAGIC Unity Catalog is where every data asset lives and is governed. It has a
# MAGIC three-level namespace, the same idea as `database.schema.table` in most SQL
# MAGIC databases, with one more level on top:
# MAGIC
# MAGIC ```
# MAGIC catalog
# MAGIC  └── schema
# MAGIC       ├── table / view       ->  <catalog>.<schema>.<table>
# MAGIC       └── volume (files)     ->  /Volumes/<catalog>/<schema>/<volume>/...
# MAGIC ```
# MAGIC
# MAGIC Tables hold structured data. Volumes hold files — CSV, JSON, images,
# MAGIC anything — on the same cloud storage, addressed by a path. Access to both is
# MAGIC controlled the same way, with `GRANT` statements on the catalog, schema or
# MAGIC object.
# MAGIC
# MAGIC You share this workspace with the other participants, so everything you
# MAGIC build goes into a schema of your own in the catalog `dbacademy`, named after
# MAGIC your user. The schema already exists. The config cell works out its name and
# MAGIC makes it the current catalog and schema, so names further down need no
# MAGIC prefix:

# COMMAND ----------

import re

SCHEMA = re.sub(r"\W", "_", spark.sql("SELECT current_user()").first()[0].split("@")[0])  # your own schema, named after your user
spark.catalog.setCurrentCatalog("dbacademy")
spark.catalog.setCurrentDatabase(SCHEMA)
print(f"Your schema: dbacademy.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Keep files in a volume
# MAGIC
# MAGIC Source systems often deliver data as files: an export from an ERP system, a
# MAGIC CSV a partner uploads, JSON from an application. Traditionally, a place to
# MAGIC put those files means a request to the infrastructure team: a storage
# MAGIC bucket, credentials, a mount, permissions. A **volume** takes that request
# MAGIC away. It's a folder of files on cloud storage that Unity Catalog manages for
# MAGIC you, so anyone allowed to create objects in a schema can create one in
# MAGIC seconds, and it's governed like a table, with the same `GRANT`s.
# MAGIC
# MAGIC Once a volume exists, you work with its files in whatever way suits the task:
# MAGIC
# MAGIC | | From the UI | From code |
# MAGIC | --- | --- | --- |
# MAGIC | Create a volume | *Catalog* → your schema → **Create** → **Volume** | `CREATE VOLUME raw` in SQL |
# MAGIC | Add files | **Upload to this volume** on the volume's page | Any Python that writes files, at the path `/Volumes/<catalog>/<schema>/<volume>/...` |
# MAGIC | Look at files | Browse the volume in *Catalog* | `dbutils.fs.ls(path)` |
# MAGIC | Process files | — | `read_files` in SQL |
# MAGIC
# MAGIC The path is the key part: a volume looks like an ordinary folder to your
# MAGIC code, so plain Python can read and write its files with `open()`, and SQL
# MAGIC can query them as tables. You'll use a volume called `raw` for the rest
# MAGIC of the workshop: notebook 03 plays a source system that drops its extracts
# MAGIC there.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 1.1 — Create a volume and write a file into it
# MAGIC
# MAGIC 1. In *Catalog*, open the catalog `dbacademy`, then your schema (printed by
# MAGIC    the config cell). Click **Create** → **Volume**, name it `raw`, keep the
# MAGIC    type **Managed**, and create it. Managed means Unity Catalog chooses the
# MAGIC    storage location for you.
# MAGIC 2. In the cell below, write a CSV file into the volume with plain Python. The
# MAGIC    cell is complete apart from the three `...`:
# MAGIC    - `os.makedirs` creates the folder `random` in the volume.
# MAGIC    - `open(path, "w")` opens the file for writing, and `csv.writer` turns
# MAGIC      each Python list into one line of comma-separated values.
# MAGIC    - `random.randint(1, 1000)` returns a random whole number between 1 and
# MAGIC      1,000.
# MAGIC 3. Open the volume in *Catalog* again: the folder `random` and the file
# MAGIC    `numbers.csv` are there. Click the file to preview it.

# COMMAND ----------

import csv
import os
import random

folder = f"/Volumes/dbacademy/{SCHEMA}/raw/random"
os.makedirs(folder, exist_ok=True)

with open(f"{folder}/numbers.csv", "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(...)                           # the header: a list with the column names "id" and "value"
    for i in range(100):
        writer.writerow([i, ...])                  # a random whole number between 1 and 1,000

print(f"Wrote {folder}/numbers.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC Now query the file. In SQL, `read_files` reads a file or a whole folder as a
# MAGIC table; `header => true` says the first line holds the column names. A SQL
# MAGIC cell can't see Python variables, so replace `<your schema>` with your schema
# MAGIC name. You should see 100 numbers with an average somewhere around 500:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) AS numbers, avg(value) AS average
# MAGIC FROM read_files('/Volumes/dbacademy/<your schema>/raw/random', format => 'csv', header => true)

# COMMAND ----------

# MAGIC %md
# MAGIC The volume you just created needed no ticket and no storage credentials, and
# MAGIC it already shows up in Unity Catalog with your schema's permissions. You could
# MAGIC equally have uploaded `numbers.csv` from your laptop with **Upload to this
# MAGIC volume**; the code that reads it wouldn't change.
# MAGIC
# MAGIC Next: open `02-spark-concepts` from the file browser on the left.
