#!/bin/bash
# PreToolUse hook: stop Claude from connecting to the live Starke SQL Server.
# Only the Dagster pipeline, started by the owner, may talk to it. Claude uses the DuckDB copy.
command=$(jq -r '.tool_input.command // empty')
# Fail closed: if jq fails or finds no command, don't let the command through unchecked
if [ -z "$command" ]; then
  echo "Blocked: the Starke hook could not read the command." >&2
  exit 2
fi

# Only commands that can open a connection (not grep/cat/sed reading the code)
runs_code='(python|uv run|(^|[ ;&|])(nc|ncat|telnet|sqlcmd|bcp|isql|dg|dagster|curl) )'
# Signs of a connection to the Starke server
starke='(get_mssql_engine|load_starke_table|find_starke_server|check_server_online_status|mssql|pyodbc|1433|100\.88\.163\.113|dg launch|job execute|asset materialize|materialize\()'

if echo "$command" | grep -Eq "$runs_code" && echo "$command" | grep -Eiq "$starke"; then
  echo "Blocked: Claude must never connect to the live Starke SQL Server (owner rule, 2026-10-18). Use the DuckDB copy read-only, or write the question into TODO.md for the owner." >&2
  exit 2
fi
exit 0
