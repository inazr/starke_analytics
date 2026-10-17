# Open points

Collected while getting the pipeline to run. Nothing here gets fixed until you pick an item.

## Blocking the test run (found in the logs)

1. **"Microsoft ODBC Driver 18 for SQL Server" can't load OpenSSL 3.** This is the Homebrew package `msodbcsql18`, file `/opt/homebrew/lib/libmsodbcsql.18.dylib`. It's what lets Python talk to the Starke SQL Server: `sqlalchemy` → `pyodbc` → this driver → SQL Server. For the encrypted login it loads OpenSSL at runtime (`libssl.3.dylib` / `libcrypto.3.dylib` in `/opt/homebrew/lib`).
   1.1. Error: `SSL Provider: OpenSSL library could not be loaded, make sure OpenSSL 1.0, 1.1, or 3.0 is installed`.
   1.2. Cause: installing `msodbcsql18` pulled in `openssl@4` and removed the `openssl@3` links. `/opt/homebrew/lib` now only has `libssl.4.dylib` and `libcrypto.4.dylib`, which this driver doesn't support.
   1.3. `openssl@3` is still installed (`/opt/homebrew/opt/openssl@3`), just no longer linked.
   1.4. Root cause: the Microsoft formula says `depends_on "openssl"` without a version. Homebrew now resolves that name to `openssl@4` and made `openssl@3` keg-only. The driver loads OpenSSL from one fixed path, `/opt/homebrew/opt/openssl/lib/`. The upstream fix would be `depends_on "openssl@3"`.
   1.5. **Worked around locally (2026-10-17):** `ln -sfn ../Cellar/openssl@3/3.6.4 /opt/homebrew/opt/openssl`. Login to `10.0.0.120` now works and finds the database `SP9_400502848`.
   1.6. This breaks again whenever Homebrew updates `openssl@4` and resets the link; run the same command again. To undo: `brew reinstall openssl@4`. The problem doesn't exist in Docker/Linux (TODO 24).
2. **The network scan from Dagster finds nothing.** In the Dagster run it finished in 161 ms without a hit. From Claude's shell the same scan took 26 s and found port 1433 open on `10.0.0.120`.
   2.1. Cause: macOS asked whether PyCharm may access the local network. You allowed it, but the Dagster process that was already running had been started before that.
   2.2. Next step: restart Dagster and run `discover_network` again.
3. **The scan silently saves a wrong IP when it finds nothing.** The loop simply ends, and the last address tried (`10.0.0.254`) is written to `config.ini` as the "Starke server". The next step then fails with a misleading login timeout.
4. **Login timeout errors are slow and unclear.** The failed run waited about 17 s and only said `Login timeout expired`, with no hint of which host was tried.

## Paths and `.env`

5. **Done (2026-10-17):** `DUCKDB_PATH` and `CONFIG_FILE_PATH` come from `.env` again. Dagster uses `EnvVar("DUCKDB_PATH")` on `DuckDBResource`, dbt uses `env_var('DUCKDB_PATH')` in `profiles.yml` (dbt 1.12 loads the root `.env` itself). Only `sa_dbt/` is derived from the repo location.
6. **Done:** a missing `config.ini` fails loudly (`FileNotFoundError` in `assets.py`). Kept.
7. **Done:** CLAUDE.md 10.3 describes 5.

## Configuration and environment

8. **Done (2026-10-17):** removed `DUCKDB_USER` and `DUCKDB_PASSWORD` from `.env`. Added `.env.example` with the 5 variables the code uses.
9. **Done (2026-10-17):** `config.ini` is no longer tracked (`git rm --cached`; the `.gitignore` entry now applies). Added `config.ini.example` with placeholder values. A new setup copies it to the path in `CONFIG_FILE_PATH`.
10. **Done (2026-10-17):** the placeholder database name (`SP9_123456789`) now only lives in `config.ini.example` (TODO 9); `get_correct_db` writes the real one. The `run_network_discovery` flag was removed: it never started discovery, and its only reader was `create_duckdb_with_schema`. A new setup runs the `discover_network` job once by hand.
11. **Done (2026-10-17):** switched to `dg dev`. Added `dagster-dg-cli` and `[tool.dg]` / `[tool.dg.project]` to `pyproject.toml` (code location name stays `sa_dagster`). `dagster_dev_startup.sh` runs `dg dev --no-check-yaml`, because without that flag `dg` requires a `defs/` folder for components, which this project doesn't use. `[tool.dagster]` was removed; validation is now `dg check defs --no-check-yaml`.
12. **Accepted (2026-10-17): gRPC log noise** (`fork_posix.cc ... skipping fork() handlers`). `grpcio` logs it at INFO level whenever a Dagster process with gRPC threads starts a subprocess (dbt, git, uv). Harmless, so it stays. If it should go later: `export GRPC_VERBOSITY=ERROR` in `dagster_dev_startup.sh` (logging only) or `GRPC_ENABLE_FORK_SUPPORT=false` (changes behaviour).

## Dagster code

13. **There's no extraction job.** The broken sync job was removed. A new job is needed for `extraction_entry_point`, `extract_from_starke` and dbt, plus the 6-hour schedule once everything is verified.
14. **The Superset copy is decided but not built:** copy `starke_praxis.duckdb` after dbt finishes.
15. **The pre-run assets use `os.system`:** `git_pull`, `install_requirements`, `parse_dbt`. Failures don't fail the run.
16. **`git_pull` was kept.** Decide whether it's still needed, especially with Docker.
17. **`check_server_online_status` uses `print()`** and stops the run through `run_launcher.terminate`.
18. **Done (2026-10-17):** `create_duckdb_with_schema` now always creates the schema (the flag is gone, see TODO 10).
19. **`get_correct_db` takes the first `SP9_%` database** (`[0][0]`). It fails if there are none and is ambiguous if there are several.
20. **The password goes into the connection URL as plain text.** Special characters such as `@` or `/` would break the URL; `sqlalchemy.URL.create` would handle that.
21. **dagster-dbt can rebuild the dbt manifest by itself** (`prepare_if_dev()`). That could replace `parse_dbt` and `reload_code_location`.
22. **There are no tests:** `sa_dagster_tests/test_assets.py` is empty.

## dbt / SQL (later)

23. **`fct_accounting_documents` has `total_gross` twice**, once from `stg_invoices` and once from `stg_invoice_line_items`.
28. **Warning `unique_stg_invoices_receipt_id` (253 duplicate values out of 3,272 invoices). The test has been removed (2026-10-17); the findings below stay for the SQL work.** The test assumes one invoice per receipt, but the source data doesn't work that way:
    28.1. **`receipt_id = 0` is a placeholder for "no receipt"** and appears on 305 invoices. 300 of them are cancellation invoices (status `S`/`G`); 5 have status `L`. That alone counts as 1 of the 253.
    28.2. **252 real receipts have 2–3 invoices to different recipients**, with no cancellations involved. Most common sequences: `Z → K` (184), `A → K` (35), `A → P` (12), `A → A → K` (7). Probable meaning, to be confirmed: Z = Zuzahlung (patient co-payment), K = Krankenkasse (statutory insurance), P = Privat, A = Ausfallrechnung (fee for a missed appointment).
    28.3. Even `(receipt_id, invoice_recipient_type)` isn't unique: 12 receipts have two `A` invoices.
    28.4. This also explains the existing comment in `fct_accounting_documents`: `--stg_invoices.receipt_id -- this looks to be incorrect`.
    28.5. Option: in `stg_invoices`, use `NULLIF(raw_invoices.REZ_Nr, 0) AS receipt_id` (the same NULLIF pattern as the other columns), and replace `unique` with a `relationships` test to `stg_receipts.receipt_id`.
29. **Your dbt run only built the 5 staging views.** It reported "Found 8 models", but the 3 core tables (`fct_receipts_to_appointments`, `fct_accounting_documents`, `dim_receipts`) weren't in it. Check which assets/job were selected; for `tag:stg_starke+` to include the core models, the job has to select them as well.

## Deployment

24. **Docker on Unraid:** needs a Dockerfile/compose file, a volume for `DAGSTER_HOME` and the DuckDB file, and the ODBC driver inside the image.
25. **`rpi/setup` is obsolete** once Docker is in place.
26. **Superset's DuckDB driver** must be able to read files written by DuckDB 1.5.
27. **Merge the branch** `overhaul-uv-latest-versions` into `main` once everything runs.
