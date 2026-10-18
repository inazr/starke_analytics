from dagster import asset, AssetExecutionContext, Failure

import socket
import sqlalchemy
import os
import shutil
import subprocess
import json
import configparser

import pandas as pd
from dagster_duckdb import DuckDBResource

from .constants import (STARKE_PRAXIS_PORT, STARKE_PRAXIS_USER, STARKE_PRAXIS_PASSWORD, CONFIG_FILE_PATH,
                        SUPERSET_DUCKDB_PATH)
from .dbt import dbt_analytics

config = configparser.ConfigParser()
if not config.read(CONFIG_FILE_PATH):
    raise FileNotFoundError(f'Config file not found: {CONFIG_FILE_PATH}')

SET_PANDAS_ANALYZE_SAMPLE = """
                            SET GLOBAL pandas_analyze_sample=10000
                            ;
                           """


def save_config() -> None:
    with open(CONFIG_FILE_PATH, 'w') as configfile:
        config.write(configfile)


def get_mssql_engine(database: str, server: str | None = None) -> sqlalchemy.Engine:
    # Default: the server saved by discover_network
    if server is None:
        server = config.get('NETWORK', 'last_known_starke_mssql_server')
    # URL.create escapes special characters and hides the password when printed
    url = sqlalchemy.URL.create(
        'mssql+pyodbc',
        username=STARKE_PRAXIS_USER,
        password=STARKE_PRAXIS_PASSWORD,
        host=server,
        database=database,
        query={'driver': 'ODBC Driver 18 for SQL Server'},
    )
    return sqlalchemy.create_engine(url, connect_args={"TrustServerCertificate": "yes"})


def load_starke_table(duckdb: DuckDBResource, table_name: str, query: str) -> None:
    """Copy the result of a Starke MSSQL query into raw_starke.<table_name> (full replace)."""
    engine = get_mssql_engine(config.get('STARKE_PRAXIS', 'database'))
    with engine.connect() as con:
        df_result = pd.DataFrame(con.execute(sqlalchemy.text(query)).fetchall())

    with duckdb.get_connection() as conn:
        conn.execute(SET_PANDAS_ANALYZE_SAMPLE)
        conn.register("df_result", df_result)
        # CREATE OR REPLACE is atomic: the old table stays until the new one is ready
        conn.execute(f"CREATE OR REPLACE TABLE raw_starke.{table_name} AS SELECT * FROM df_result;")


def get_allowed_tailscale_ips(context: AssetExecutionContext) -> list[str]:
    """IPv4 addresses of the online Tailscale peers whose node ID is in allowed_tailscale_node_ids (config.ini)."""
    # Only allowlisted hosts get the Starke login; never scan the LAN, any device there could open port 1433.
    # Match the node ID, not the HostName: any device in the tailnet can rename itself, but not change its ID.
    allowed = [node_id.strip() for node_id in config.get('NETWORK', 'allowed_tailscale_node_ids').split(',')]

    if shutil.which('tailscale') is None:
        raise Failure('tailscale command not found. Install Tailscale or set the server in config.ini by hand.')

    result = subprocess.run(['tailscale', 'status', '--json'], capture_output=True, text=True, check=True)
    peers = json.loads(result.stdout).get('Peer') or {}

    allowed_peers = [peer for peer in peers.values()
                     if peer['ID'] in allowed
                     and peer['Online']
                     and not peer.get('ShareeNode')]  # shared in from another tailnet
    ips = [peer['TailscaleIPs'][0] for peer in allowed_peers]  # IPv4 first, then IPv6
    context.log.info(f'Online allowlisted Tailscale peers: {[(peer["HostName"], ip) for peer, ip in zip(allowed_peers, ips)]}')
    return ips


def is_port_open(ip: str, timeout: float) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.settimeout(timeout)
        return s.connect_ex((ip, STARKE_PRAXIS_PORT)) == 0


def get_starke_databases(context: AssetExecutionContext, server: str) -> list[str]:
    """Names of all Starke databases (SP9_...) on a server. Empty if the login fails."""
    query = """
                SELECT 
                        sysdatabases.name
                FROM
                        master.sys.sysdatabases
                WHERE   1=1
                  -- [_] is a literal underscore; a plain _ in LIKE matches any character
                  AND   sysdatabases.name LIKE 'SP9[_]%'
                ORDER BY
                        sysdatabases.name
                  ;
            """

    try:
        with get_mssql_engine('master', server).connect() as con:
            return [row[0] for row in con.execute(sqlalchemy.text(query))]
    except sqlalchemy.exc.DBAPIError as error:
        # An open port 1433 can be a different SQL Server where the Starke login doesn't work
        context.log.warning(f'Login to {server} failed: {error}')
        return []


@asset(group_name="discover_network")
def find_starke_server(context: AssetExecutionContext) -> None:
    # Tailscale peers can be relayed, so allow 1 s per host
    open_ips = [ip for ip in get_allowed_tailscale_ips(context) if is_port_open(ip, 1)]
    context.log.info(f'Port {STARKE_PRAXIS_PORT} is open on: {open_ips}')

    # An open port only makes a host a candidate; the SP9_ database proves it is the Starke server
    found = [(ip, database) for ip in open_ips for database in get_starke_databases(context, ip)]

    if len(found) != 1:
        raise Failure(f'Expected exactly one Starke database (SP9_...) on the allowlisted servers, found: {found}. '
                      f'Check allowed_tailscale_node_ids (IDs: tailscale status --json), or set last_known_starke_mssql_server and database in config.ini by hand.')

    server, database = found[0]
    config.set('NETWORK', 'last_known_starke_mssql_server', server)
    config.set('STARKE_PRAXIS', 'database', database)
    save_config()
    context.log.info(f'Starke server: {server}, database: {database}')


@asset(group_name="extraction_entry_point")
def check_server_online_status(context: AssetExecutionContext) -> None:
    server = config.get('NETWORK', 'last_known_starke_mssql_server')

    if not is_port_open(server, 1):
        raise Failure(f'Starke SQL Server {server}:{STARKE_PRAXIS_PORT} is not reachable. '
                      f'If its IP changed, run the job discover_network.')

    context.log.info(f'Starke SQL Server {server}:{STARKE_PRAXIS_PORT} is reachable.')


@asset(deps=[check_server_online_status],
       group_name="extraction_entry_point")
def create_duckdb_with_schema(duckdb: DuckDBResource) -> None:
    create_schema_query = """
                            CREATE SCHEMA IF NOT EXISTS raw_starke
                            ;
                          """

    with duckdb.get_connection() as conn:
        conn.execute(create_schema_query)


@asset(deps=[create_duckdb_with_schema],
       group_name="extract_from_starke")
def raw_appointments(duckdb: DuckDBResource) -> None:
    query = """
                SELECT 
                        TERMIN.Nr,
                        TERMIN.REZ_Nr,
                        TERMIN.Datum,
                        TERMIN.MIT_Kurzname,
                        TERMIN.Brutto,
                        TERMIN.Zuzahlung,
                        TERMIN.Kennzeichen,
                        TERMIN.Ausgefallen,
                        TERMIN.Multi,
                        TERMIN.Begruendung,
                        CONVERT(VARCHAR(32), HASHBYTES('SHA2_512', TERMIN.Text), 2) AS Text, -- possible personal data in text
                        TERMIN.ChangeDate,
                        TERMIN.ChangeTime,
                        TERMIN.BereitsErhalten,
                        TERMIN.Teletherapie,
                        TERMIN.AzhUnterbrechung,
                        TERMIN.DcStatus
                FROM
                        dbo.TERMIN
                WHERE   1=1
                ;      
            """

    load_starke_table(duckdb, "raw_appointments", query)


@asset(deps=[create_duckdb_with_schema],
       group_name="extract_from_starke")
def raw_employees(duckdb: DuckDBResource) -> None:
    query = """
                SELECT 
                        MITARBEITER.Nr,
                        MITARBEITER.Kurzname,
                        MITARBEITER.Name,
                        MITARBEITER.Nachname,
                        MITARBEITER.Vorname,
                        MITARBEITER.Geschlecht,
                        MITARBEITER.Titel,
                        MITARBEITER.Namenszusatz,
                        MITARBEITER.Geburtsdatum,
                        MITARBEITER.IK,
                        MITARBEITER.Abrechnungscode,
                        MITARBEITER.Rechte,
                        MITARBEITER.Passwort,
                        MITARBEITER.Praxistermine,
                        MITARBEITER.Hausbesuche,
                        MITARBEITER.Wegegelder,
                        MITARBEITER.Aktiv,
                        MITARBEITER.Passiv,
                        MITARBEITER.Privat,
                        MITARBEITER.Porto,
                        MITARBEITER.Ausgeschieden,
                        MITARBEITER.Kontonummer,
                        MITARBEITER.Kreditinstitut,
                        MITARBEITER.BLZ,
                        MITARBEITER.Notiz,
                        MITARBEITER.ADR_Nr,
                        MITARBEITER.ChangeDate,
                        MITARBEITER.ChangeTime,
                        MITARBEITER.Sollzeit,
                        MITARBEITER.Qualifikation,
                        MITARBEITER.Arbeitszeit,
                        MITARBEITER.Ort,
                        MITARBEITER.Berufsurkunde
                FROM
                        dbo.MITARBEITER
                WHERE   1=1
                ;      
            """

    load_starke_table(duckdb, "raw_employees", query)


@asset(deps=[create_duckdb_with_schema],
       group_name="extract_from_starke")
def raw_receipts(duckdb: DuckDBResource) -> None:
    query = """
                SELECT
                        REZEPT.Nr,
                        REZEPT.Euro,
                        REZEPT.Art,
                        REZEPT.Datum,
                        REZEPT.Soll,
                        REZEPT.FrequenzVon,
                        REZEPT.FrequenzBis,
                        REZEPT.FrequenzOK,
                        REZEPT.Ist,
                        REZEPT.Termine,
                        REZEPT.Ausgefallen,
                        REZEPT.Verordnung,
                        REZEPT.Behandlungsbeginn,
                        REZEPT.Hausbesuch,
                        REZEPT.Heim,
                        REZEPT.Bericht,
                        REZEPT.Gruppentherapie,
                        REZEPT.Kilometer,
                        REZEPT.Brutto,
                        REZEPT.Zuzahlung,
                        REZEPT.Pauschale,
                        REZEPT.Netto,
                        REZEPT.MwSt,
                        REZEPT.Ausfall,
                        REZEPT.IK,
                        REZEPT.Abrechnungscode,
                        REZEPT.Zustand,
                        REZEPT.Voranschlag,
                        REZEPT.ZUZ_Status,
                        REZEPT.ZUZ_Datum,
                        REZEPT.ZUZ_MIT_Kurzname,
                        REZEPT.ZUZ_Betrag,
                        REZEPT.ZUZ_Mahnung,
                        REZEPT.Eigenanteil,
                        REZEPT.Arbeitsunfall,
                        REZEPT.Unfalltag,
                        REZEPT.Unfallbetrieb,
                        REZEPT.BVG,
                        REZEPT.Genehmigungskennzeichen,
                        REZEPT.Genehmigungsdatum,
                        REZEPT.Ausland,
                        REZEPT.EWRCH,
                        CONVERT(VARCHAR(32), HASHBYTES('SHA2_512', REZEPT.PAT_Name), 2) AS PAT_Name, -- possible personal data in text
                        CONVERT(VARCHAR(32), HASHBYTES('SHA2_512', CAST(REZEPT.PAT_Geburtsdatum AS VARCHAR(6))), 2) AS PAT_Geburtsdatum, -- possible personal data in text
                        CONVERT(VARCHAR(32), HASHBYTES('SHA2_512', REZEPT.PAT_Versichertennummer), 2) AS PAT_Versichertennummer, -- possible personal data in text
                        REZEPT.PAT_VKGueltigkeit,
                        REZEPT.PAT_VKZ,
                        REZEPT.PAT_Status,
                        REZEPT.PAT_Frei,
                        REZEPT.PAT_FreiVon,
                        REZEPT.PAT_FreiBis,
                        REZEPT.DGZ_Nummer,
                        REZEPT.DGZ_Wechsel,
                        REZEPT.DIA_Nummer,
                        REZEPT.DIA_Diagnose,
                        REZEPT.HMROK,
                        REZEPT.MIT_Kurzname,
                        REZEPT.PAT_ADR_Nr,
                        REZEPT.PAT_EMP_Nr,
                        REZEPT.KAS_IK,
                        REZEPT.KAS_Name,
                        REZEPT.TAR_Name,
                        REZEPT.TAR_Gueltigkeit,
                        REZEPT.LTA_Nummer_0,
                        REZEPT.LTA_Nummer_1,
                        REZEPT.LTA_Nummer_2,
                        REZEPT.LTA_Faktor_0,
                        REZEPT.LTA_Faktor_1,
                        REZEPT.LTA_Faktor_2,
                        REZEPT.LTA_Weitere,
                        REZEPT.BET_Nummer,
                        REZEPT.ARZ_LANR,
                        REZEPT.ARZ_Nummer,
                        CONVERT(VARCHAR(32), HASHBYTES('SHA2_512', REZEPT.ARZ_Name), 2) AS ARZ_Name, -- possible personal data in text
                        REZEPT.Zahlungsart,
                        REZEPT.Erster,
                        REZEPT.Letzter,
                        REZEPT.ChangeDate,
                        REZEPT.ChangeTime,
                        REZEPT.Text,
                        REZEPT.BeginnOK,
                        REZEPT.FristOK,
                        REZEPT.AROK,
                        REZEPT.ICDOK,
                        REZEPT.REC_Nummer,
                        REZEPT.Ungeprueft,
                        REZEPT.DocGuid,
                        REZEPT.DocArchiv,
                        REZEPT.Entlass,
                        REZEPT.Anonym,
                        REZEPT.ZUZ_Aufforderung,
                        REZEPT.KorrekturNr,
                        REZEPT.Verarbeitungskennzeichen,
                        REZEPT.Formular,
                        REZEPT.HDG_Nummer,
                        REZEPT.LS,
                        REZEPT.Diagnosetext,
                        REZEPT.Dringlich,
                        REZEPT.Heilmittelbereich,
                        REZEPT.ScannedHeilmittel,
                        REZEPT.FrequenzEinheit,
                        REZEPT.ZUZ_Muendlich,
                        REZEPT.ZUZ_Exkasso,
                        REZEPT.ZUZ_DatumExkasso,
                        REZEPT.Ablaufdatum,
                        REZEPT.LhbBvb,
                        REZEPT.AzhVersion,
                        REZEPT.DcStatus,
                        REZEPT.DcRequested,
                        REZEPT.DcConfirmed,
                        REZEPT.DcDeclined,
                        REZEPT.UnterschriftArzt,
                        REZEPT.Aenderungen,
                        REZEPT.Langzeit,
                        REZEPT.Kostenzusage,
                        REZEPT.KostenzusageDatum
                FROM
                        dbo.REZEPT
                WHERE   1=1
                ;      
            """

    load_starke_table(duckdb, "raw_receipts", query)


@asset(deps=[create_duckdb_with_schema],
       group_name="extract_from_starke")
def raw_invoices(duckdb: DuckDBResource) -> None:
    query = """
                SELECT
                        RECHNUNG.Nr,
                        RECHNUNG.Nachberechnung,
                        RECHNUNG.Nummer,
                        RECHNUNG.Art,
                        RECHNUNG.Datum,
                        RECHNUNG.Von,
                        RECHNUNG.Bis,
                        RECHNUNG.EMP_Nr,
                        RECHNUNG.KAS_IK,
                        CASE WHEN RECHNUNG.KAS_IK = '' THEN CONVERT(VARCHAR(32), HASHBYTES('SHA2_512', RECHNUNG.Name), 2) ELSE RECHNUNG.Name END AS Name,
                        RECHNUNG.REZ_Nr,
                        RECHNUNG.SEN_Nr,
                        RECHNUNG.Zustand,
                        RECHNUNG.Betrag,
                        RECHNUNG.Gesamt_0,
                        RECHNUNG.Gesamt_1,
                        RECHNUNG.Gesamt_2,
                        RECHNUNG.Gesamt_3,
                        RECHNUNG.Mitglied_0,
                        RECHNUNG.Mitglied_1,
                        RECHNUNG.Mitglied_2,
                        RECHNUNG.Mitglied_3,
                        RECHNUNG.Familie_0,
                        RECHNUNG.Familie_1,
                        RECHNUNG.Familie_2,
                        RECHNUNG.Familie_3,
                        RECHNUNG.Rentner_0,
                        RECHNUNG.Rentner_1,
                        RECHNUNG.Rentner_2,
                        RECHNUNG.Rentner_3,
                        RECHNUNG.Sonstige_0,
                        RECHNUNG.Sonstige_1,
                        RECHNUNG.Sonstige_2,
                        RECHNUNG.Sonstige_3,
                        RECHNUNG.Ausland_0,
                        RECHNUNG.Ausland_1,
                        RECHNUNG.Ausland_2,
                        RECHNUNG.Ausland_3,
                        RECHNUNG.MwSt,
                        RECHNUNG.Rezepte,
                        RECHNUNG.Faelligkeitsdatum,
                        RECHNUNG.Mahndatum_0,
                        RECHNUNG.Mahndatum_1,
                        RECHNUNG.Mahndatum_2,
                        RECHNUNG.Mahndatum_3,
                        RECHNUNG.Eingangsdatum,
                        RECHNUNG.Teilzahlung,
                        RECHNUNG.ChangeDate,
                        RECHNUNG.ChangeTime,
                        RECHNUNG.Id,
                        RECHNUNG.Prefix,
                        RECHNUNG.StornoNummer,
                        RECHNUNG.REC_Nummer,
                        RECHNUNG.Abschreibung,
                        RECHNUNG.Abschreibungsdatum,
                        RECHNUNG.Versendet,
                        RECHNUNG.Kuerzung,
                        RECHNUNG.Land,
                        RECHNUNG.PLZ,
                        RECHNUNG.Ort,
                        RECHNUNG.Debitor,
                        RECHNUNG.Verarbeitungskennzeichen
                FROM
                        dbo.RECHNUNG
                
                WHERE   1=1
                ;      
            """

    load_starke_table(duckdb, "raw_invoices", query)


@asset(deps=[create_duckdb_with_schema],
       group_name="extract_from_starke")
def raw_invoice_line_items(duckdb: DuckDBResource) -> None:
    query = """
                SELECT
                        RECHNZEILE.Nr,
                        RECHNZEILE.RZE_Nr,
                        RECHNZEILE.Euro,
                        CONVERT(VARCHAR(32), HASHBYTES('SHA2_512', RECHNZEILE.PAT_Name), 2) AS PAT_Name,
                        YEAR(DATEADD(DAY, RECHNZEILE.PAT_Geburtsdatum, '1801-01-01')) AS PAT_Geburtsjahr,
                        RECHNZEILE.PAT_Status,
                        RECHNZEILE.REZ_Nr,
                        RECHNZEILE.LEI_Nummer,
                        RECHNZEILE.LTA_Nummer,
                        RECHNZEILE.Von,
                        RECHNZEILE.Bis,
                        RECHNZEILE.Faktor,
                        RECHNZEILE.Anzahl,
                        RECHNZEILE.Brutto,
                        RECHNZEILE.Zuzahlung,
                        RECHNZEILE.Netto,
                        RECHNZEILE.Eigenanteil,
                        RECHNZEILE.Text,
                        RECHNZEILE.REC_Nummer,
                        RECHNZEILE.Abrechnungscode,
                        RECHNZEILE.REZ_Datum,
                        RECHNZEILE.KAS_IK,
                        RECHNZEILE.KAS_Name,
                        RECHNZEILE.TAR_Name,
                        RECHNZEILE.DIA_Diagnose,
                        --RECHNZEILE.PAT_Versichertennummer, -- possible personal data
                        RECHNZEILE.PAT_VKZ,
                        RECHNZEILE.Arbeitsunfall,
                        RECHNZEILE.Unfalltag,
                        RECHNZEILE.Unfallbetrieb,
                        RECHNZEILE.Pauschale,
                        RECHNZEILE.ZuzPflichtig,
                        RECHNZEILE.Einzelpreis,
                        RECHNZEILE.MwSt,
                        RECHNZEILE.MwStSatz
                FROM
                        dbo.RECHNZEILE

                WHERE   1=1
                ;      
            """

    load_starke_table(duckdb, "raw_invoice_line_items", query)


# Only core is meant for Superset. The group comes from config.meta.dagster.group in the dbt YAML
core_assets = [key for key in dbt_analytics.keys if dbt_analytics.group_names_by_key[key] == "core"]


@asset(deps=core_assets, group_name="superset")
def copy_duckdb_for_superset(duckdb: DuckDBResource) -> None:
    # Superset reads its own copy, because DuckDB allows one writer or several readers, never both.
    # The copy only holds core: raw_starke and stg_starke are not meant for Superset.
    os.makedirs(os.path.dirname(SUPERSET_DUCKDB_PATH), exist_ok=True)
    tmp_path = SUPERSET_DUCKDB_PATH + '.tmp'
    if os.path.exists(tmp_path):
        os.remove(tmp_path)  # left over from a failed run

    with duckdb.get_connection() as conn:
        conn.execute(f"ATTACH '{tmp_path}' AS superset_copy;")
        conn.execute("CREATE SCHEMA superset_copy.core;")
        # Every table in core, so a new core model is copied without changing this code
        core_tables = conn.execute("""
                                   SELECT tables.table_name
                                   FROM   information_schema.tables AS tables
                                   WHERE  1=1
                                     AND  tables.table_catalog = current_database()
                                     AND  tables.table_schema = 'core'
                                   ;
                                   """).fetchall()
        for (table_name,) in core_tables:
            conn.execute(f'CREATE TABLE superset_copy.core."{table_name}" AS SELECT * FROM core."{table_name}";')
        # DETACH writes everything into the file, so no .wal file is left behind
        conn.execute("DETACH superset_copy;")

    # Atomic rename: Superset never sees a half-copied file
    os.replace(tmp_path, SUPERSET_DUCKDB_PATH)
