# Data Engineering with PySpark on Databricks

A hands-on lab for data engineers who know SQL and ETL well but are new to
Databricks, Python and PySpark. Over five notebooks you'll find your way around
the workspace and create a volume of your own, learn how Spark runs your code,
load a stream of file extracts incrementally with Auto Loader, build a bronze →
silver → gold medallion by hand, and run it as a job you can monitor, break and
repair.

## The notebooks

| Notebook | What it covers |
| -------- | -------------- |
| `01-workspace` — The workspace | Compute options, workspace, notebook cells, `display()`, Unity Catalog, volumes from the UI and from code |
| `02-spark-concepts` — Spark concepts | Driver and workers, serverless, Databricks Runtime and Photon, DataFrames through their SQL equivalents, lazy evaluation |
| `03-auto-loader` — Auto Loader | Incremental file ingestion, checkpoints, schema inference, schema changes and `_rescued_data` |
| `04-medallion` — Medallion, layer by layer | Bronze, silver and gold; cleaning; idempotent loads with `MERGE`; a star schema; liquid clustering; lineage |
| `05-jobs-operations` — Jobs and operations | Orchestration with a job, failing loudly, retries and notifications, repairing runs, system tables, time travel, and when to use Spark Declarative Pipelines |

Work through them in order; each one builds on what the previous one left
behind.

## How the lab works

Each notebook mixes explanation with code cells you run, and exercises you
complete yourself. Every exercise is a cell with `...` placeholders or steps
you carry out in the UI, and most are followed by a check cell that confirms
your result with `assert`. If you get stuck, the `reference` folder in the file
browser on the left has a solved version of every notebook to check against.

You share this workspace with the other participants, so everything you build
goes into a schema of your own. Each notebook starts with a config cell that
sets the catalog to the lab's catalog, `dbacademy`, and derives the schema from
your user name, so you don't have to set anything and nobody collides with
anyone else; your schema already exists.

## The data

Bakehouse is a sample dataset of a fictional chain of cookie franchises: sales
transactions, and the franchises and customers behind them. It's in the
`samples.bakehouse` schema, which the workspace receives as a read-only share.
In notebook 03 you play Bakehouse's sales system and export its sales as CSV
files into a volume of your own, a few days at a time and not always tidily,
the way extracts from a source system arrive.
