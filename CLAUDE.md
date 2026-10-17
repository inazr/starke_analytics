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
7. **Never fix anything on your own that is a design decision.** Diagnose (read-only checks are fine), write the finding into `TODO.md` with options, and only change what the owner explicitly names.
8. One topic at a time, and be specific: give the full name and path ("Microsoft ODBC Driver 18 for SQL Server, `/opt/homebrew/lib/libmsodbcsql.18.dylib`"), not "the driver".
   8.1. **Keep answers short.** Answer the question asked in a few sentences first. No long background, no repeated context, no extra sections.

## Architecture

9. ELT pipeline for a physiotherapy practice:
   `Starke Praxis (MSSQL, LAN) → Dagster assets (pandas) → DuckDB raw_starke.* → dbt → DuckDB stg_starke.* / core.* → Superset`
   9.1. **Superset** (separate, not in this repo) reads its **own copy** of `duckdb/starke_praxis.duckdb`. The pipeline copies the file after dbt finishes, so Superset never opens the file Dagster writes to. Only `core` models are meant for Superset.
   9.2. **Target platform: Docker on Unraid.** `rpi/setup` is the old Raspberry Pi setup.
   9.3. **Planned:** a Dagster job that extracts every 6 hours. Add it only after the pipeline is verified end-to-end.
   9.4. One Python project for the whole repo: root `pyproject.toml` + `uv.lock` (Python 3.13), venv in root `.venv`. Dagster and dbt share it.
10. **`sa_dagster/`** – Orchestration. Dagster loads `sa_dagster.sa_dagster` (set in `[tool.dagster]` in `pyproject.toml`); its `__init__.py` builds the `Definitions` object (assets, jobs, resources). `sa_dagster/` is also `DAGSTER_HOME`: `dagster.yaml` plus the run history Dagster writes there (`history/`, `schedules/`, `storage/`, all git-ignored).
   10.1. `assets/assets.py`: network discovery (finds the MSSQL host on the LAN and writes it to `config.ini`), extraction of 5 Starke tables into `raw_starke.raw_*`, and pre-run helper assets (git pull, `uv sync`, `dbt parse`, code-location reload). Shared helpers: `get_mssql_engine()`, `load_starke_table()`, `save_config()`.
   10.2. `assets/dbt.py`: one `@dbt_assets` that runs `dbt build`. It reads `sa_dbt/target/manifest.json`, so **that manifest must exist** (run `dbt parse`) before Dagster can load.
   10.3. `assets/constants.py`: secrets from the root `.env` via `python-dotenv` (`STARKE_PRAXIS_PORT`, `STARKE_PRAXIS_USER`, `STARKE_PRAXIS_PASSWORD`). The DuckDB file comes from `DUCKDB_PATH` in `.env` (absolute path), passed to `DuckDBResource` as `EnvVar("DUCKDB_PATH")`. `config.ini` comes from `CONFIG_FILE_PATH` in `.env` (absolute path). Only `sa_dbt/` is derived from the repo location.
   10.4. Runtime state (server IP, DB name, `run_network_discovery` flag) is stored in root `config.ini`. It is not passed between assets.
11. **`sa_dbt/`** – Transformations on DuckDB (`duckdb/starke_praxis.duckdb`).
   11.1. `models/source/starke_praxis.yml`: sources with `meta.dagster.asset_key` matching the Python asset names. This is what links Python and dbt lineage in Dagster.
   11.2. `models/staging/starke_praxis/stg_*.sql`: rename German source columns to English snake_case, cast types, and convert Starke dates.
   11.3. `models/core/`: `fct_*` / `dim_*` tables (materialized as `table`).
   11.4. `macros/generate_schema_name.sql`: on target `prod`, models go to their custom schema (`stg_starke`, `core`). On any other target, **everything goes into the target schema** (`dbt_dev`).
   11.5. dbt behaviour flags live under `flags:` in `dbt_project.yml`, not in `profiles.yml`.

### Domain quirks

12. **Starke dates are integers counting days since 1801-01-01**: `DATE '1801-01-01' + to_days(col::INT)`. Values like `0` or `< 70000` mean "no date".
13. **PII is hashed in the MSSQL extraction query** (`HASHBYTES('SHA2_512', ...)`) before it reaches DuckDB. Never pull patient names, birth dates, insurance numbers, or doctor names unhashed.
14. Key German terms: Rezept = receipt/prescription, Termin = appointment, Rechnung = invoice, Rechnungszeile = invoice line item, Mitarbeiter = employee, Zuzahlung = co-payment, Pauschale = flat rate, Mahndatum = dunning date.
15. **Invoices (`RECHNUNG`) aren't 1:1 with receipts.** `REZ_Nr = 0` means "no receipt" (mostly cancellations, status `S`/`G`). One receipt gets several invoices, one per recipient (`invoice_recipient_type`, e.g. `Z → K`). Probable meaning: Z = Zuzahlung, K = Krankenkasse, P = Privat, A = Ausfall. Take `receipt_id` from the invoice line items.

## Commands

16. Dependencies are managed with **uv** in the repo root. Add a package with `uv add <pkg>` (never `pip install`) and commit `uv.lock`.
17. The dbt profile lives in `sa_dbt/profiles.yml`, so run dbt **from inside `sa_dbt/`**. The default target is **`prod`**: pass `--target dev` while developing.
18. Never print `.env`: it holds the Starke credentials. In ad-hoc scripts use `load_dotenv('.env')`, because plain `load_dotenv()` fails in stdin scripts.

```bash
# Setup (repo root)
uv sync                                    # installs Python 3.13 + all packages into .venv

# dbt (cd sa_dbt)
uv run dbt parse                                  # (re)generate target/manifest.json – Dagster needs it
uv run dbt build --target dev                     # run + test everything into schema dbt_dev
uv run dbt build --target dev --select stg_receipts+   # one model + downstream
uv run dbt test  --target dev --select stg_invoices    # tests of one model
uv run dbt show  --target dev --select dim_receipts --limit 10

# Dagster (repo root; needs a .env with the variables in 10.3 and DAGSTER_HOME=<repo>/sa_dagster)
./dagster_dev_startup.sh                          # sets DAGSTER_HOME + uv run dagster dev, UI on http://localhost:3000
uv run dagster definitions validate               # check that all definitions load
uv run pytest sa_dagster/sa_dagster_tests         # tests (currently empty)
uv run pytest sa_dagster/sa_dagster_tests/test_assets.py::test_name   # single test

# First run / new network: run job `discover_network` (writes server IP + DB name to config.ini)
# Test run: materialize groups `extraction_entry_point` + `extract_from_starke` + dbt assets. Don't run `pre_dbt_run` locally (git pull, uv sync, reload).

# Inspect data read-only (only while Dagster isn't writing)
.venv/bin/python -c "import duckdb; print(duckdb.connect('duckdb/starke_praxis.duckdb', read_only=True).sql('SELECT ...'))"
```

## Best practices per tool

### dbt

19. Layers: `source` → `stg_` (1:1 with a source table, rename/cast only, no joins) → `fct_`/`dim_` in `core` (joins, business logic). Add an `intermediate/` layer only when a core model gets too complex.
20. Always use `{{ ref() }}` / `{{ source() }}`. Never hard-code schema names inside models.
21. Every model's primary key gets `unique` + `not_null` under `data_tests:` (not the old `tests:` key). Use `severity: warn` for known data issues.
22. Every new model needs a YAML entry with `config.meta.dagster.group`. Every new source table needs `meta.dagster.asset_key` matching its Python asset name.
23. Match the existing SQL style: UPPERCASE keywords, columns qualified with the table name (`stg_receipts.receipt_id`), `WHERE 1=1`, `AS` for every alias, booleans named `is_*`.
24. If the Starke date conversion has to be written again, use a single macro (DRY) instead of copying the expression.

### Dagster

25. One asset = one table or one side effect. Declare order with `deps=[...]` and logical grouping with `group_name`.
26. Connect to MSSQL only via `get_mssql_engine(database)`, and write `config.ini` only via `save_config()`.
27. Access DuckDB only through the `DuckDBResource` parameter (`duckdb.get_connection()` as a context manager). Never open your own connection.
28. Use `context.log` instead of `print()`, and `subprocess.run(..., check=True)` instead of `os.system` so that failures fail the run.
29. Jobs select assets by group name (`AssetSelection.groups(...)`), so a renamed group silently empties a job. Check `jobs/__init__.py` when you change groups.
30. Schedules and sensors only run once they are added to `Definitions(...)`.

### DuckDB

31. **DuckDB allows either one read-write process or several read-only processes, never both at once.** That's why Superset gets a copy (see 9.1). Close IDE connections to the pipeline's file before running the pipeline.
32. Raw loads replace whole tables with one atomic `CREATE OR REPLACE TABLE`. New raw tables: write the query and call `load_starke_table(duckdb, "raw_<name>", query)`. Also add the table to `models/source/starke_praxis.yml`.
33. DuckDB SQL is allowed (trailing commas, `DATE_DIFF`, `IFNULL`, reusing an alias in the same `SELECT`), but don't mix in MSSQL syntax.
34. Use `NUMERIC(10,4)` for money columns, as staging already does.

### Python

35. Python 3.13 (pinned in `pyproject.toml`; dagster-dbt doesn't support 3.14 yet). Secrets only from `.env` (git-ignored). `config.ini` holds runtime state, not secrets.

## Local environment quirks (macOS)

36. **ODBC driver needs OpenSSL 3.** `msodbcsql18` (Homebrew) loads OpenSSL from the fixed path `/opt/homebrew/opt/openssl/lib/`. Homebrew points that path to `openssl@4`, which the driver doesn't support (error: `OpenSSL library could not be loaded`). Fix: `ln -sfn ../Cellar/openssl@3/3.6.4 /opt/homebrew/opt/openssl`. This breaks again after every `openssl@4` upgrade. Docker/Linux doesn't have this problem.
37. **macOS Local Network permission:** without it, the network scan fails silently in a few milliseconds. Allow the IDE/terminal under System Settings → Privacy & Security → Local Network, then **restart Dagster**.
38. **Login timeout to MSSQL** usually means the wrong IP in `config.ini`: if the scan finds nothing, it saves the last address it tried (`x.x.x.254`).

## Open points

39. All known issues and pending decisions are in `TODO.md` (numbered; the owner references them as "TODO n"). Add new findings there instead of fixing them.
