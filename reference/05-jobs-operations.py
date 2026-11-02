# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Jobs and operations
# MAGIC
# MAGIC By the end of this notebook the sales load from notebooks 03 and 04 will run
# MAGIC as a job that checks its own result, fails loudly when the data is wrong,
# MAGIC retries and notifies, and can be repaired without rerunning what already
# MAGIC succeeded. You'll also have rolled a table back after a bad load, and you'll
# MAGIC know when to build a load like this one with Spark Declarative Pipelines
# MAGIC instead. This is the solved version of
# MAGIC `05-jobs-operations`.

# COMMAND ----------

import re

from pyspark.sql import functions as F

SCHEMA = re.sub(r"\W", "_", spark.sql("SELECT current_user()").first()[0].split("@")[0])  # your own schema, named after your user
spark.catalog.setCurrentCatalog("dbacademy")
spark.catalog.setCurrentDatabase(SCHEMA)
print(f"Your schema: {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Orchestrate with a job
# MAGIC
# MAGIC So far you've run every step by hand, cell by cell. A **job** runs them
# MAGIC unattended. It's a set of **tasks** (a notebook, a SQL query, a Python
# MAGIC script, a pipeline, and other types) with dependencies between them,
# MAGIC **parameters** passed to the tasks, and **triggers** such as a schedule. If
# MAGIC you've used an ETL scheduler, it's the same idea, built into the workspace.
# MAGIC
# MAGIC The job you'll build has two notebook tasks, both in the `tasks` folder
# MAGIC next to this notebook:
# MAGIC
# MAGIC - `load` is the sales path from notebooks 03 and 04 in one notebook:
# MAGIC   Auto Loader into `bronze_sales`, the `MERGE` into `silver_sales`, and a
# MAGIC   rebuild of `gold_fact_sales`.
# MAGIC - `check-gold` counts the rows in `gold_fact_sales` and raises an error if
# MAGIC   there are fewer than `min_rows`, which makes the task, and so the job run,
# MAGIC   fail.
# MAGIC
# MAGIC Both read the job parameters `catalog` and `schema` with
# MAGIC `dbutils.widgets.get`, which is how a notebook task receives parameters, and
# MAGIC the check also reads `min_rows`. Making the check its own task, rather than
# MAGIC a cell at the end of the load, keeps "build the data" and "decide whether it's
# MAGIC good enough to publish" as separate, visible steps.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.1 — Orchestrate it
# MAGIC
# MAGIC 1. In the sidebar, click **Jobs & Pipelines**, then **Create** → **Job**.
# MAGIC    Name the job `<your schema>_sales_job`.
# MAGIC 2. Add the first task: type **Notebook**, name `load`, path: the `load`
# MAGIC    notebook in the `tasks` folder, compute serverless.
# MAGIC 3. Add a second task: type **Notebook**, name `check_gold`, path: the
# MAGIC    `tasks/check-gold` notebook, compute serverless, and make it depend on
# MAGIC    `load`.
# MAGIC 4. Add three job parameters: `catalog` = `dbacademy`, `schema` = your
# MAGIC    schema (printed by the config cell), `min_rows` = `1000`. Job parameters
# MAGIC    are passed to every task, so both notebooks pick them up.
# MAGIC 5. Add a schedule trigger that runs daily, and set it to **paused**. The
# MAGIC    schedule is part of the job's definition, but nothing runs on its own in
# MAGIC    the lab environment.
# MAGIC 6. Click **Run now**.
# MAGIC
# MAGIC Open the run. You should see `load` succeed, then `check_gold` succeed after
# MAGIC it. Click `check_gold` to see its output: the number of rows in
# MAGIC `gold_fact_sales`. No new extracts arrived since notebook 03, so the load
# MAGIC found no new files and the tables hold the same rows as before; that's the
# MAGIC idempotency from notebook 04 at work. Then run the check below.

# COMMAND ----------

# Check — run after the job run has finished
last = spark.sql("DESCRIBE HISTORY gold_fact_sales LIMIT 1").first()  # the history records which job wrote each version
assert last["job"] is not None, "The last write to gold_fact_sales didn't come from a job; has the job run?"
gold_rows = spark.table("gold_fact_sales").count()
assert gold_rows == spark.table("silver_sales").count(), "gold_fact_sales should have one row per sale in silver_sales"
print(f"Check passed: {gold_rows} rows in gold_fact_sales, written by job {last['job']['jobName']}.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fail loudly
# MAGIC
# MAGIC A job only knows a task failed if the task raises an error. Anything that
# MAGIC catches the error and carries on (a `print`, a log line, a `pass`) turns a
# MAGIC failure into a green run. That is the most expensive kind of bug: nobody looks
# MAGIC at a green run, and the reports downstream quietly show stale or wrong numbers.
# MAGIC
# MAGIC The rule in a job task: catch an exception only if you can do something
# MAGIC useful with it (retry a call, or add context and re-raise). Otherwise let it
# MAGIC propagate. Python's `raise` plays the role of `THROW` in a SQL script; it stops
# MAGIC the notebook, marks the task as failed, and puts the message in the run output,
# MAGIC where whoever is on call will read it. So make the message say what was wrong
# MAGIC and what was expected.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.2 — Fail loudly
# MAGIC
# MAGIC The cell below counts the rows in `gold_fact_sales`. Replace the `...` with
# MAGIC a check that raises an error when there are fewer than `min_rows` rows. In
# MAGIC Python that's two lines:
# MAGIC
# MAGIC ```python
# MAGIC if rows < min_rows:
# MAGIC     raise ValueError(f"...")   # a message that says how many rows there were and how many were expected
# MAGIC ```
# MAGIC
# MAGIC No `try`/`except`: raising is the whole point. Run the cell with
# MAGIC `min_rows = 1000`; it should print the row count. Then set
# MAGIC `min_rows = 100000` and run it again; the cell should fail and show your
# MAGIC message in red.

# COMMAND ----------

min_rows = 1000   # run with 1000, then with 100000

rows = spark.table("gold_fact_sales").count()
if rows < min_rows:
    raise ValueError(f"gold_fact_sales has {rows} rows, expected at least {min_rows}")
print(f"gold_fact_sales: {rows} rows, at least {min_rows} as expected")

# COMMAND ----------

# MAGIC %md
# MAGIC This is exactly what `tasks/check-gold` does as the second task of your job:
# MAGIC open it from the file browser on the left. The only difference is that it
# MAGIC reads `min_rows` from the job parameters instead of a variable in the cell.
# MAGIC When it raises, the task goes red, and that's the signal the next section
# MAGIC builds on.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure retries, timeouts and failure notifications
# MAGIC
# MAGIC Some failures are transient: a source system briefly unreachable, a file
# MAGIC still being written when the job starts. For those a retry is the right
# MAGIC answer, and the job does it for you. Other failures, like the check above
# MAGIC finding too few rows, will fail again however often you retry; for those you
# MAGIC want a person to find out quickly. Each task in a job has three settings for
# MAGIC this, which you'll find in the task's configuration panel:
# MAGIC
# MAGIC - **Retries.** Click **Add** next to *Retries* to set how many times the task is retried and how long to wait between attempts.
# MAGIC - **Timeout.** Under **Metric thresholds**, choose *Run duration* and set a *Timeout*, so a task that hangs is stopped instead of running (and costing) forever. With retries configured, the timeout applies to each attempt.
# MAGIC - **Notifications.** Send a message *on Failure* to an email address or to a system destination such as a chat channel or a webhook. You can set notifications per task or for the whole job.
# MAGIC
# MAGIC > Note. In the lab workspace, email notifications may not reach you. Configure
# MAGIC > one anyway; the setting is what matters here, and you'll see it in the job's
# MAGIC > settings.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Repair a failed run
# MAGIC
# MAGIC When a multi-task run fails, you rarely want to rerun all of it. In a real
# MAGIC job the load is the slow, expensive part; the check is a few seconds.
# MAGIC **Repair run** reruns only the tasks that failed or were skipped, plus anything
# MAGIC that depends on them, and leaves the successful tasks alone. In the repair
# MAGIC dialog you can also override job parameters, which is how you fix a run that
# MAGIC failed because of a wrong parameter.
# MAGIC
# MAGIC Repair reruns each task from the beginning. That's safe only if the task is
# MAGIC idempotent, which is one more reason the loads in notebooks 03 and 04 use a
# MAGIC checkpoint, `MERGE` and overwrite rather than blind appends.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.3 — Break and repair
# MAGIC
# MAGIC Work in the job you created in Exercise 5.1, under *Jobs & Pipelines*.
# MAGIC
# MAGIC 1. Open the `check_gold` task and set **Retries** to 2 with a short interval (a minute is plenty). Under **Metric thresholds**, add a *Run duration* timeout of 10 minutes. Add a failure notification to your email address.
# MAGIC 2. Click the caret next to **Run now** and choose **Run now with different settings**. Set the job parameter `min_rows` to `100000`, which your gold table can't possibly reach, and run it.
# MAGIC 3. Watch the run. The `load` task succeeds; `check_gold` fails, is retried twice, and fails each time. Open the failed task and read the error: it's the `ValueError` message from `check-gold`.
# MAGIC 4. On the failed run, click **Repair run**. In the dialog, set `min_rows` back to `1000` and start the repair.
# MAGIC
# MAGIC **What you should see:** the repaired run shows the `load` task as it was
# MAGIC (it isn't rerun), and `check_gold` now succeeds.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Know what system tables are
# MAGIC
# MAGIC The job's run history covers one job. For a view across everything in the
# MAGIC account, Databricks keeps **system tables**: read-only tables in the Unity
# MAGIC Catalog `system` catalog that Databricks maintains for you. They're grouped
# MAGIC into schemas by topic, for example `system.billing` (usage and cost),
# MAGIC `system.access` (audit logs and lineage) and `system.lakeflow` (job and
# MAGIC pipeline runs). Because they're ordinary tables, you query them with SQL like
# MAGIC anything else, and build dashboards and alerts on them: failed jobs across all
# MAGIC teams, the most expensive jobs this month, who read a sensitive table.
# MAGIC
# MAGIC Access is governed like any other data: an admin grants it schema by schema,
# MAGIC so what you can see depends on your workspace's setup.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recover from a bad load with time travel
# MAGIC
# MAGIC Every write to a Delta table creates a new table version, and the previous
# MAGIC versions stay readable for a retention period. `DESCRIBE HISTORY` lists the
# MAGIC versions with their timestamp, operation and who ran it. You can read an old
# MAGIC version like any table, and `RESTORE TABLE ... TO VERSION AS OF n` makes
# MAGIC version `n` current again, as a new version on top, so the restore itself
# MAGIC shows up in the history too. For a bad load, this replaces "restore last
# MAGIC night's backup" with a single statement that takes seconds.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 5.4 — Undo a bad load
# MAGIC
# MAGIC First, remember the row count and the total revenue of your
# MAGIC `gold_fact_sales`, so the check can compare later:

# COMMAND ----------

expected_rows = spark.table("gold_fact_sales").count()
expected_total = spark.table("gold_fact_sales").agg(F.sum("totalPrice")).first()[0]
print(f"gold_fact_sales: {expected_rows} rows, total revenue {expected_total}")

# COMMAND ----------

# MAGIC %md
# MAGIC The next cell plays the part of a buggy job: it sets every price to zero.
# MAGIC It's in SQL because an `UPDATE` is one statement there; in Python the same
# MAGIC change takes more code through the Delta table API. Run it.

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE gold_fact_sales SET totalPrice = 0

# COMMAND ----------

# MAGIC %md
# MAGIC Now repair it. `DESCRIBE HISTORY` is also a single SQL statement, so this
# MAGIC cell stays in SQL. Find the `UPDATE` you just ran; the version just below it
# MAGIC is the last good one.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY gold_fact_sales

# COMMAND ----------

# MAGIC %md
# MAGIC Before restoring, read the good version to make sure it's the right one.
# MAGIC `spark.read.option("versionAsOf", n).table(...)` reads the table as it was at
# MAGIC version `n`. Set `GOOD_VERSION` and run the cell: the total should be the one
# MAGIC printed before the bad load.

# COMMAND ----------

GOOD_VERSION = ...   # the version number just before the UPDATE, from the history above

old = spark.read.option("versionAsOf", GOOD_VERSION).table("gold_fact_sales")
print(f"version {GOOD_VERSION}: {old.count()} rows, total revenue {old.agg(F.sum('totalPrice')).first()[0]}")

# COMMAND ----------

# MAGIC %md
# MAGIC Now restore it. `RESTORE` is another single SQL statement, so this cell is
# MAGIC SQL too. Replace `<version>` with your `GOOD_VERSION` number.

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE gold_fact_sales TO VERSION AS OF <version>

# COMMAND ----------

# Check
restored = spark.table("gold_fact_sales")
assert restored.count() == expected_rows, \
    f"Expected {expected_rows} rows after the restore, found {restored.count()}"
total = restored.agg(F.sum("totalPrice")).first()[0]
assert total == expected_total, f"Total revenue should be {expected_total} again, is {total}"
print("Check passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC Run `DESCRIBE HISTORY` once more: the bad update is still there, followed
# MAGIC by a `RESTORE` entry. Nothing was erased; you moved forward to a good state,
# MAGIC and the audit trail shows exactly what happened.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Decide when to use Spark Declarative Pipelines
# MAGIC
# MAGIC You built the sales load by hand: Auto Loader with a checkpoint you chose,
# MAGIC cleaning, a `MERGE` you wrote, a gold rebuild, and a job that runs the steps
# MAGIC in the order you wired up. Every piece of it is a tool you'll keep using.
# MAGIC For a load like this one, though, there's a second way to build it.
# MAGIC Databricks' guidance on choosing an ETL approach:
# MAGIC
# MAGIC | Approach | When to use it |
# MAGIC | --- | --- |
# MAGIC | Lakeflow pipelines, built on Apache Spark Declarative Pipelines (SDP) | New batch and incremental ETL workloads |
# MAGIC | dbt on Databricks | Your team already maintains a dbt project |
# MAGIC | Spark notebooks and jobs | Complex transformation logic that you want to express directly in Spark |
# MAGIC | Stored procedures | Procedural SQL that you want to package and reuse |
# MAGIC
# MAGIC Databricks recommends Lakeflow pipelines for most ETL workloads. In a
# MAGIC pipeline you declare what each table should contain, as a query, and the
# MAGIC pipeline takes over the parts you wrote by hand here:
# MAGIC
# MAGIC | By hand, in this workshop | In a declarative pipeline |
# MAGIC | --- | --- |
# MAGIC | Checkpoint and schema locations you choose and manage | Managed by the pipeline |
# MAGIC | The order of the steps, wired up in cells and job tasks | Derived from which table reads which, and shown as a graph |
# MAGIC | A `MERGE` you write so reruns are safe | Streaming tables process each file once; materialized views are kept in line with their source |
# MAGIC | Filters that drop bad rows silently | Expectations, with passed and failed counts recorded on every update |
# MAGIC | Applying a change feed with a hand-written `MERGE` | `AUTO CDC`, including history as a slowly changing dimension of type 2 |
# MAGIC
# MAGIC What doesn't change is everything underneath: the same Spark, the same
# MAGIC Auto Loader, the same Delta tables in Unity Catalog. Jobs don't go away
# MAGIC either. A pipeline knows how to build its tables, but not when to run;
# MAGIC a job runs it, as a pipeline task, with the same triggers, retries,
# MAGIC notifications and repair you used today.
# MAGIC
# MAGIC That's the end of the workshop. You built a load that ingests each file
# MAGIC once, cleans and merges into silver, rebuilds gold, runs as a job that
# MAGIC checks its own result, fails loudly, retries, and can be repaired and rolled
# MAGIC back.
