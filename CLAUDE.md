# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Coding principles (non-negotiable)

The owner must be able to read and understand every line. Simple beats clever.

1. **KISS** – Pick the most obvious solution. Plain SQL and plain Python functions. No metaprogramming, no clever Jinja, no class hierarchies when a function will do.
2. **YAGNI** – Build only what is needed now. Don't add config options, abstractions, macros, or "future-proof" layers ahead of time.
3. **DRY** – When the same logic appears 3+ times, pull it into one place: a Python helper, a dbt macro, or an upstream model. Two copies are fine if extracting them would make things harder to read.
4. **SOLID (applied pragmatically)** – Each asset/model/function does one thing. Dependencies (DuckDB, dbt) come in through Dagster resources, never hard-coded inside functions.
5. Make small, focused changes. Explain *why* in comments only where the reason is not obvious (e.g. Starke data quirks).

## Communication

6. When returning a list, number the items instead of using "-" bullets. **Numbers must be unique across the whole reply or document**: never restart at 1 in a new section, keep counting. Nested points use the parent number (8.1, 8.2). The owner references items by number ("fix 3").

## Architecture

7. ELT pipeline for a physiotherapy practice:
   `Starke Praxis (MSSQL, LAN) → Dagster assets (pandas) → DuckDB raw_starke.* → dbt → DuckDB stg_starke.* / core.* → Superset`
   7.1. **Superset** (separate, not in this repo) reads its **own copy** of `duckdb/starke_praxis.duckdb`. The pipeline copies the file after dbt finishes, so Superset never opens the file Dagster writes to. Only `core` models are meant for Superset.
   7.2. **Target platform: Docker on Unraid.** `rpi/setup` is the old Raspberry Pi setup.
   7.3. **Planned:** a Dagster job that extracts every 6 hours. Add it only after the pipeline is verified end-to-end.
   7.4. One Python project for the whole repo: root `pyproject.toml` + `uv.lock` (Python 3.13), venv in root `.venv`. Dagster and dbt share it.
8. **`sa_dagster/`** – Orchestration. Dagster loads `sa_dagster.sa_dagster` (set in `[tool.dagster]` in `pyproject.toml`); its `__init__.py` builds the `Definitions` object (assets, jobs, resources). `sa_dagster/` is also `DAGSTER_HOME`: `dagster.yaml` plus the run history Dagster writes there (`history/`, `schedules/`, `storage/`, all git-ignored).
   8.1. `assets/assets.py`: network discovery (finds the MSSQL host on the LAN and writes it to `config.ini`), extraction of 5 Starke tables into `raw_starke.raw_*`, and pre-run helper assets (git pull, `uv sync`, `dbt parse`, code-location reload). Shared helpers: `get_mssql_engine()`, `load_starke_table()`, `save_config()`.
   8.2. `assets/dbt.py`: one `@dbt_assets` that runs `dbt build`. It reads `sa_dbt/target/manifest.json`, so **that manifest must exist** (run `dbt parse`) before Dagster can load.
   8.3. `assets/constants.py`: env vars loaded via `python-dotenv` (`DUCKDB_PATH`, `STARKE_PRAXIS_PORT/USER/PASSWORD`, `CONFIG_FILE_PATH`).
   8.4. Runtime state (server IP, DB name, `run_network_discovery` flag) is stored in root `config.ini`. It is not passed between assets.
9. **`sa_dbt/`** – Transformations on DuckDB (`duckdb/starke_praxis.duckdb`).
   9.1. `models/source/starke_praxis.yml`: sources with `meta.dagster.asset_key` matching the Python asset names. This is what links Python and dbt lineage in Dagster.
   9.2. `models/staging/starke_praxis/stg_*.sql`: rename German source columns to English snake_case, cast types, and convert Starke dates.
   9.3. `models/core/`: `fct_*` / `dim_*` tables (materialized as `table`).
   9.4. `macros/generate_schema_name.sql`: on target `prod`, models go to their custom schema (`stg_starke`, `core`). On any other target, **everything goes into the target schema** (`dbt_dev`).
   9.5. dbt behaviour flags live under `flags:` in `dbt_project.yml`, not in `profiles.yml`.

### Domain quirks

10. **Starke dates are integers counting days since 1801-01-01**: `DATE '1801-01-01' + to_days(col::INT)`. Values like `0` or `< 70000` mean "no date".
11. **PII is hashed in the MSSQL extraction query** (`HASHBYTES('SHA2_512', ...)`) before it reaches DuckDB. Never pull patient names, birth dates, insurance numbers, or doctor names unhashed.
12. Key German terms: Rezept = receipt/prescription, Termin = appointment, Rechnung = invoice, Rechnungszeile = invoice line item, Mitarbeiter = employee, Zuzahlung = co-payment, Pauschale = flat rate, Mahndatum = dunning date.

## Commands

13. Dependencies are managed with **uv** in the repo root. Add a package with `uv add <pkg>` (never `pip install`) and commit `uv.lock`.
14. The dbt profile lives in `sa_dbt/profiles.yml`, so run dbt **from inside `sa_dbt/`**. The default target is **`prod`**: pass `--target dev` while developing.

```bash
# Setup (repo root)
uv sync                                    # installs Python 3.13 + all packages into .venv

# dbt (cd sa_dbt)
uv run dbt parse                                  # (re)generate target/manifest.json – Dagster needs it
uv run dbt build --target dev                     # run + test everything into schema dbt_dev
uv run dbt build --target dev --select stg_receipts+   # one model + downstream
uv run dbt test  --target dev --select stg_invoices    # tests of one model
uv run dbt show  --target dev --select dim_receipts --limit 10

# Dagster (repo root; needs a .env with the variables in 8.3 and DAGSTER_HOME=<repo>/sa_dagster)
./dagster_dev_startup.sh                          # sets DAGSTER_HOME + uv run dagster dev, UI on http://localhost:3000
uv run dagster definitions validate               # check that all definitions load
uv run pytest sa_dagster/sa_dagster_tests         # tests (currently empty)
uv run pytest sa_dagster/sa_dagster_tests/test_assets.py::test_name   # single test
```

## Best practices per tool

### dbt

15. Layers: `source` → `stg_` (1:1 with a source table, rename/cast only, no joins) → `fct_`/`dim_` in `core` (joins, business logic). Add an `intermediate/` layer only when a core model gets too complex.
16. Always use `{{ ref() }}` / `{{ source() }}`. Never hard-code schema names inside models.
17. Every model's primary key gets `unique` + `not_null` under `data_tests:` (not the old `tests:` key). Use `severity: warn` for known data issues, as on `stg_invoices.receipt_id`.
18. Every new model needs a YAML entry with `config.meta.dagster.group`. Every new source table needs `meta.dagster.asset_key` matching its Python asset name.
19. Match the existing SQL style: UPPERCASE keywords, columns qualified with the table name (`stg_receipts.receipt_id`), `WHERE 1=1`, `AS` for every alias, booleans named `is_*`.
20. If the Starke date conversion has to be written again, use a single macro (DRY) instead of copying the expression.

### Dagster

21. One asset = one table or one side effect. Declare order with `deps=[...]` and logical grouping with `group_name`.
22. Connect to MSSQL only via `get_mssql_engine(database)`, and write `config.ini` only via `save_config()`.
23. Access DuckDB only through the `DuckDBResource` parameter (`duckdb.get_connection()` as a context manager). Never open your own connection.
24. Use `context.log` instead of `print()`, and `subprocess.run(..., check=True)` instead of `os.system` so that failures fail the run.
25. Jobs select assets by group name (`AssetSelection.groups(...)`), so a renamed group silently empties a job. Check `jobs/__init__.py` when you change groups.
26. Schedules and sensors only run once they are added to `Definitions(...)`.

### DuckDB

27. **DuckDB allows either one read-write process or several read-only processes, never both at once.** That's why Superset gets a copy (see 7.1). Close IDE connections to the pipeline's file before running the pipeline.
28. Raw loads replace whole tables with one atomic `CREATE OR REPLACE TABLE`. New raw tables: write the query and call `load_starke_table(duckdb, "raw_<name>", query)`. Also add the table to `models/source/starke_praxis.yml`.
29. DuckDB SQL is allowed (trailing commas, `DATE_DIFF`, `IFNULL`, reusing an alias in the same `SELECT`), but don't mix in MSSQL syntax.
30. Use `NUMERIC(10,4)` for money columns, as staging already does.

### Python

31. Python 3.13 (pinned in `pyproject.toml`; dagster-dbt doesn't support 3.14 yet). Secrets only from `.env` (git-ignored). `config.ini` holds runtime state, not secrets.

## Known issues (verify before relying on them)

32. `fct_accounting_documents` selects `total_gross` from both `stg_invoices` and `stg_invoice_line_items` (duplicate column name).
33. The pre-run assets (`git_pull`, `install_requirements`, `parse_dbt`) still use `os.system`, so failures don't fail the run (see 24).
