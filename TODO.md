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
2. **Obsolete (2026-10-17):** the LAN scan was removed (TODO 31). Discovery only uses allowlisted Tailscale peers.
3. **Done (2026-10-17):** discovery (`find_starke_server`) verifies each candidate by its `SP9_` database and raises `Failure` unless exactly one is found. It no longer saves the last address tried.
4. **Login timeout errors are slow and unclear.** The failed run waited about 17 s and only said `Login timeout expired`, with no hint of which host was tried. **Partly fixed by TODO 17:** an unreachable server now fails after 1 s with the host name. Still open: a reachable server with wrong login or database name.

## Paths and `.env`

5. **Done (2026-10-17):** `DUCKDB_PATH` and `CONFIG_FILE_PATH` come from `.env` again. Dagster uses `EnvVar("DUCKDB_PATH")` on `DuckDBResource`, dbt uses `env_var('DUCKDB_PATH')` in `profiles.yml` (dbt 1.12 loads the root `.env` itself). Only `sa_dbt/` is derived from the repo location.
6. **Done:** a missing `config.ini` fails loudly (`FileNotFoundError` in `assets.py`). Kept.
7. **Done:** CLAUDE.md 10.3 describes 5.

## Configuration and environment

8. **Done (2026-10-17):** removed `DUCKDB_USER` and `DUCKDB_PASSWORD` from `.env`. Added `.env.example` with the 5 variables the code uses.
9. **Done (2026-10-17):** `config.ini` is no longer tracked (`git rm --cached`; the `.gitignore` entry now applies). Added `config.ini.example` with placeholder values. A new setup copies it to the path in `CONFIG_FILE_PATH`.
10. **Done (2026-10-17):** the placeholder database name (`SP9_123456789`) now only lives in `config.ini.example` (TODO 9); discovery writes the real one. The `run_network_discovery` flag was removed: it never started discovery, and its only reader was `create_duckdb_with_schema`. A new setup runs the `discover_network` job once by hand.
11. **Done (2026-10-17):** switched to `dg dev`. Added `dagster-dg-cli` and `[tool.dg]` / `[tool.dg.project]` to `pyproject.toml` (code location name stays `sa_dagster`). `dagster_dev_startup.sh` runs `dg dev --no-check-yaml`, because without that flag `dg` requires a `defs/` folder for components, which this project doesn't use. `[tool.dagster]` was removed; validation is now `dg check defs --no-check-yaml`.
12. **Accepted (2026-10-17): gRPC log noise** (`fork_posix.cc ... skipping fork() handlers`). `grpcio` logs it at INFO level whenever a Dagster process with gRPC threads starts a subprocess (dbt, git, uv). Harmless, so it stays. If it should go later: `export GRPC_VERBOSITY=ERROR` in `dagster_dev_startup.sh` (logging only) or `GRPC_ENABLE_FORK_SUPPORT=false` (changes behaviour).

## Dagster code

13. **Job done (2026-10-17), schedule open.** Job `extract_and_transform` selects the groups `extraction_entry_point`, `extract_from_starke`, `staging`, `core` (15 assets). The old job `dbt_run_stg_starke` was removed. Still open: the 6-hour schedule for this job, once the pipeline is verified end-to-end.
14. **Done (2026-10-17):** asset `copy_duckdb_for_superset` (group `superset`, part of `extract_and_transform`) runs after dbt: `CHECKPOINT`, copy to `<target>.tmp`, then atomic `os.replace` to `SUPERSET_DUCKDB_PATH` (`.env`). Locally `duckdb/superset/starke_praxis.duckdb`.
15. **Done (2026-10-17):** removed the whole `pre_dbt_run` group (`git_pull`, `install_requirements`, `parse_dbt`, `reload_code_location`). A pipeline that updates its own code is an anti-pattern; code updates become a deployment step: build a new image with `uv sync` + `dbt parse`, restart the code-location container (TODO 24). Locally: `dg dev` runs `dbt parse` on every reload (TODO 21).
16. **Done (2026-10-17):** `git_pull` removed with TODO 15.
17. **Done (2026-10-17):** `check_server_online_status` raises `Failure` with host, port and the hint to run `discover_network`, so the run is Failed (not Canceled) and downstream assets are skipped. `print()`, `run_launcher.terminate` and the global socket timeout are gone.
18. **Done (2026-10-17):** `create_duckdb_with_schema` now always creates the schema (the flag is gone, see TODO 10).
19. **Done and tested (2026-10-17):** the database lookup (now in `find_starke_server`) uses `LIKE 'SP9[_]%'` (a plain `_` is a wildcard), sorts by name, and raises `Failure` unless exactly one Starke database is found across all reachable servers. Test run found `100.88.163.113` (Tailscale peer `Tower`) with `SP9_400502848`.
20. **Done (2026-10-18):** `get_mssql_engine` builds the URL with `sqlalchemy.URL.create`, which escapes special characters and prints the password as `***`.
21. **Done (2026-10-18):** `dbt_project.prepare_if_dev()` in `project.py` runs `dbt parse` on every code load under `dg dev` (about 2.4 s). No effect in production; there the manifest comes from the image build (TODO 24).
22. **There are no tests:** `sa_dagster_tests/test_assets.py` is empty.

## dbt / SQL (later)

23. **`fct_accounting_documents` has `total_gross` twice**, once from `stg_invoices` and once from `stg_invoice_line_items`.
28. **Warning `unique_stg_invoices_receipt_id` (253 duplicate values out of 3,272 invoices). The test has been removed (2026-10-17); the findings below stay for the SQL work.** The test assumes one invoice per receipt, but the source data doesn't work that way:
    28.1. **`receipt_id = 0` is a placeholder for "no receipt"** and appears on 305 invoices. 300 of them are cancellation invoices (status `S`/`G`); 5 have status `L`. That alone counts as 1 of the 253.
    28.2. **252 real receipts have 2–3 invoices to different recipients**, with no cancellations involved. Most common sequences: `Z → K` (184), `A → K` (35), `A → P` (12), `A → A → K` (7). Probable meaning, to be confirmed: Z = Zuzahlung (patient co-payment), K = Krankenkasse (statutory insurance), P = Privat, A = Ausfallrechnung (fee for a missed appointment).
    28.3. Even `(receipt_id, invoice_recipient_type)` isn't unique: 12 receipts have two `A` invoices.
    28.4. This also explains the existing comment in `fct_accounting_documents`: `--stg_invoices.receipt_id -- this looks to be incorrect`.
    28.5. Option: in `stg_invoices`, use `NULLIF(raw_invoices.REZ_Nr, 0) AS receipt_id` (the same NULLIF pattern as the other columns), and replace `unique` with a `relationships` test to `stg_receipts.receipt_id`.
29. **Done (2026-10-17):** run `1d4887e3` of job `extract_and_transform` built all 3 core tables (`fct_receipts_to_appointments`, `fct_accounting_documents`, `dim_receipts`). The old job `dbt_run_stg_starke`, which only built staging, was removed (TODO 13).

30. **The Superset copy contains all schemas, not only `core`** (found by an automated security review, 2026-10-17). `copy_duckdb_for_superset` copies the whole file, so Superset can also read `raw_starke` and `stg_starke`. CLAUDE.md 9.1 says only `core` is meant for Superset. Impact is limited because PII is already hashed during extraction (CLAUDE.md 13).
    30.1. Option: build the copy from `core` only (`ATTACH` a new file, `CREATE TABLE ... AS SELECT * FROM core.<model>` per model, then the same atomic rename).
    30.2. Option: keep the full copy and only expose `core` in Superset (dataset permissions).
    30.3. Option: also restrict the file permissions of `SUPERSET_DUCKDB_PATH`.

31. **Done (2026-10-17):** discovery only logs in to online Tailscale peers on an allowlist in `config.ini` (since TODO 32: node IDs), skipping shared-in peers. The LAN scan and `get_local_ip` were removed. Before, the Starke login went to every host with port 1433 open (found by an automated security review).

32. **Done (2026-10-18):** the allowlist matches the Tailscale node `ID` (`allowed_tailscale_node_ids` in `config.ini`; `Tower` = `nBmu8w3x6h11CNTRL`) instead of the self-chosen `HostName` (found by an automated security review). Must be updated if `Tower` is re-registered in Tailscale. Still possible later: TLS certificate validation (`TrustServerCertificate=no`).

## Deployment

24. **Docker on Unraid:** needs a Dockerfile/compose file, a volume for `DAGSTER_HOME` and the DuckDB file, and the ODBC driver inside the image.
    24.1. **Discovery needs Tailscale in the container:** `find_starke_server` calls `tailscale status --json` and fails without the `tailscale` command. Options: host networking on Unraid with the Tailscale CLI available, Tailscale inside the image, or set the server in `config.ini` by hand and don't run `discover_network`.
25. **`rpi/setup` is obsolete** once Docker is in place.
26. **Superset's DuckDB driver** must be able to read files written by DuckDB 1.5.
27. **Merge the branch** `overhaul-uv-latest-versions` into `main` once everything runs.
