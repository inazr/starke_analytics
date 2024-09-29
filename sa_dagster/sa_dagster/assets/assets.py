import dagster_dbt
from dagster import asset, DagsterInstance, OpExecutionContext

import socket
import sqlalchemy
import os
import configparser

import pandas as pd
from dagster_duckdb import DuckDBResource

from .constants import STARKE_PRAXIS_PORT, STARKE_PRAXIS_USER, STARKE_PRAXIS_PASSWORD, CONFIG_FILE_PATH

from dagster_graphql import (
    DagsterGraphQLClient,
    ReloadRepositoryLocationInfo
)

config = configparser.ConfigParser()
config.read(CONFIG_FILE_PATH)

SET_PANDAS_ANALYZE_SAMPLE = """
                            SET GLOBAL pandas_analyze_sample=10000
                            ;
                           """


@asset(group_name="pre_dbt_run")
def git_pull():
    os.system('git pull')


@asset(group_name="git")
def git_push():
    os.system('git add .')
    os.system('git commit -m "auto commit"')
    os.system('git push')


@asset(deps=[git_pull],
       group_name="pre_dbt_run")
def install_requirements():
    os.system('pip install -r $DAGSTER_HOME/requirements.txt')


@asset(group_name="discover_network")
def get_local_ip():
    # https://stackoverflow.com/questions/166506/finding-local-ip-addresses-using-pythons-stdlib
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(0)
    try:
        # doesn't even have to be reachable
        s.connect(('10.254.254.254', 1))
        local_ip = s.getsockname()[0]
    except Exception:
        local_ip = '127.0.0.1'
    finally:
        s.close()

    config.set('NETWORK', 'local_ip', local_ip)

    with open(CONFIG_FILE_PATH, 'w') as configfile:
        config.write(configfile)


@asset(deps=[get_local_ip],
       group_name="discover_network")
def get_list_of_host_with_open_mssql_port():
    # https://www.tutorialspoint.com/python_penetration_testing/python_penetration_testing_network_scanner.htm

    local_ip = config.get('NETWORK', 'local_ip')

    ip_range = '.'.join(local_ip.split('.')[:3])

    for i in range(1, 255):
        ip: str = f'{ip_range}.{i}'
        socket.setdefaulttimeout(0.1)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = s.connect_ex((ip, STARKE_PRAXIS_PORT))
        # 0: open, 1: closed
        if result == 0:
            break

    config.set('NETWORK', 'last_known_starke_mssql_server', ip)

    with open(CONFIG_FILE_PATH, 'w') as configfile:
        config.write(configfile)


@asset(deps=[get_list_of_host_with_open_mssql_port],
       group_name="discover_network")
def get_correct_db():
    starke_mssql_server = config.get('NETWORK', 'last_known_starke_mssql_server')

    engine = sqlalchemy.create_engine(
        'mssql+pyodbc://' + STARKE_PRAXIS_USER + ':' + STARKE_PRAXIS_PASSWORD + '@' + starke_mssql_server + '/master?driver=ODBC+Driver+18+for+SQL+Server',
        connect_args={"TrustServerCertificate": "yes"})

    query = """
                SELECT 
                        sysdatabases.name
                FROM
                        master.sys.sysdatabases
                WHERE   1=1
                  AND   sysdatabases.name LIKE 'SP9_%'
                  ;      
            """

    con = engine.connect()
    
    result = con.execute(sqlalchemy.text(query))

    config.set('STARKE_PRAXIS', 'database', result.fetchall()[0][0])
    config.set('NETWORK', 'run_network_discovery', 'False')

    with open(CONFIG_FILE_PATH, 'w') as configfile:
        config.write(configfile)


@asset(deps=[],
       group_name="extraction_entry_point")
def check_server_online_status(context: OpExecutionContext) -> None:
    starke_mssql_server = config.get('NETWORK', 'last_known_starke_mssql_server')

    # 1 second timeout
    socket.setdefaulttimeout(1)
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    result = s.connect_ex((starke_mssql_server, STARKE_PRAXIS_PORT))

    if result == 0:
        config.set('NETWORK', 'run_network_discovery', 'False')
    else:
        config.set('NETWORK', 'run_network_discovery', 'True')

        # Terminate the run
        instance = DagsterInstance.get()
        termination_result = instance.run_launcher.terminate(context.run.run_id)
        print(f"Run {context.run.run_id} terminated successfully.")

    with open(CONFIG_FILE_PATH, 'w') as configfile:
        config.write(configfile)


@asset(deps=[check_server_online_status],
       group_name="extraction_entry_point")
def create_duckdb_with_schema(duckdb: DuckDBResource) -> None:
    create_schema_query = """
                            CREATE SCHEMA IF NOT EXISTS raw_starke
                            ;
                          """

    run_network_discovery = config.get('NETWORK', 'run_network_discovery')

    if run_network_discovery == 'False':
        with duckdb.get_connection() as conn:
            conn.execute(create_schema_query)


@asset(deps=[create_duckdb_with_schema],
       group_name="extract_from_starke")
def raw_appointments(duckdb: DuckDBResource) -> None:
    starke_mssql_server = config.get('NETWORK', 'last_known_starke_mssql_server')
    starke_praxis_db = config.get('STARKE_PRAXIS', 'database')

    engine = sqlalchemy.create_engine(
        'mssql+pyodbc://' + STARKE_PRAXIS_USER + ':' + STARKE_PRAXIS_PASSWORD + '@' + starke_mssql_server + '/' + starke_praxis_db + '?driver=ODBC+Driver+18+for+SQL+Server',
        connect_args={"TrustServerCertificate": "yes"})

    con = engine.connect()

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

    result = con.execute(sqlalchemy.text(query))

    df_result = pd.DataFrame(result.fetchall())

    with duckdb.get_connection() as conn:
        conn.execute(SET_PANDAS_ANALYZE_SAMPLE)
        conn.execute("CREATE TABLE IF NOT EXISTS raw_starke.raw_appointments_temp AS SELECT * FROM df_result;")
        conn.execute("DROP TABLE IF EXISTS raw_starke.raw_appointments;")
        conn.execute("ALTER TABLE raw_starke.raw_appointments_temp RENAME TO raw_appointments;")
        conn.execute("DROP TABLE IF EXISTS raw_starke.raw_appointments_temp;")


@asset(deps=[create_duckdb_with_schema],
       group_name="extract_from_starke")
def raw_employees(duckdb: DuckDBResource) -> None:
    starke_mssql_server = config.get('NETWORK', 'last_known_starke_mssql_server')
    starke_praxis_db = config.get('STARKE_PRAXIS', 'database')

    engine = sqlalchemy.create_engine(
        'mssql+pyodbc://' + STARKE_PRAXIS_USER + ':' + STARKE_PRAXIS_PASSWORD + '@' + starke_mssql_server + '/' + starke_praxis_db + '?driver=ODBC+Driver+18+for+SQL+Server',
        connect_args={"TrustServerCertificate": "yes"})

    con = engine.connect()

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

    result = con.execute(sqlalchemy.text(query))

    df_result = pd.DataFrame(result.fetchall())

    with duckdb.get_connection() as conn:
        conn.execute(SET_PANDAS_ANALYZE_SAMPLE)
        conn.execute("CREATE TABLE IF NOT EXISTS raw_starke.raw_employees_temp AS SELECT * FROM df_result;")
        conn.execute("DROP TABLE IF EXISTS raw_starke.raw_employees;")
        conn.execute("ALTER TABLE raw_starke.raw_employees_temp RENAME TO raw_employees;")
        conn.execute("DROP TABLE IF EXISTS raw_starke.raw_employees_temp;")


@asset(deps=[create_duckdb_with_schema],
       group_name="extract_from_starke")
def raw_receipts(duckdb: DuckDBResource) -> None:
    starke_mssql_server = config.get('NETWORK', 'last_known_starke_mssql_server')
    starke_praxis_db = config.get('STARKE_PRAXIS', 'database')

    engine = sqlalchemy.create_engine(
        'mssql+pyodbc://' + STARKE_PRAXIS_USER + ':' + STARKE_PRAXIS_PASSWORD + '@' + starke_mssql_server + '/' + starke_praxis_db + '?driver=ODBC+Driver+18+for+SQL+Server',
        connect_args={"TrustServerCertificate": "yes"})

    con = engine.connect()

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

    result = con.execute(sqlalchemy.text(query))
    df_result = pd.DataFrame(result.fetchall())

    with duckdb.get_connection() as conn:
        conn.execute(SET_PANDAS_ANALYZE_SAMPLE)
        conn.execute("CREATE TABLE IF NOT EXISTS raw_starke.raw_receipts_temp AS SELECT * FROM df_result;")
        conn.execute("DROP TABLE IF EXISTS raw_starke.raw_receipts;")
        conn.execute("ALTER TABLE raw_starke.raw_receipts_temp RENAME TO raw_receipts;")
        conn.execute("DROP TABLE IF EXISTS raw_starke.raw_receipts_temp;")


@asset(deps=[create_duckdb_with_schema],
       group_name="extract_from_starke")
def raw_invoices(duckdb: DuckDBResource) -> None:
    starke_mssql_server = config.get('NETWORK', 'last_known_starke_mssql_server')
    starke_praxis_db = config.get('STARKE_PRAXIS', 'database')

    engine = sqlalchemy.create_engine(
        'mssql+pyodbc://' + STARKE_PRAXIS_USER + ':' + STARKE_PRAXIS_PASSWORD + '@' + starke_mssql_server + '/' + starke_praxis_db + '?driver=ODBC+Driver+18+for+SQL+Server',
        connect_args={"TrustServerCertificate": "yes"})

    con = engine.connect()

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

    result = con.execute(sqlalchemy.text(query))
    df_result = pd.DataFrame(result.fetchall())

    with duckdb.get_connection() as conn:
        conn.execute(SET_PANDAS_ANALYZE_SAMPLE)
        conn.execute("CREATE TABLE IF NOT EXISTS raw_starke.raw_invoices_temp AS SELECT * FROM df_result;")
        conn.execute("DROP TABLE IF EXISTS raw_starke.raw_invoices;")
        conn.execute("ALTER TABLE raw_starke.raw_invoices_temp RENAME TO raw_invoices;")
        conn.execute("DROP TABLE IF EXISTS raw_starke.raw_invoices_temp;")


@asset(deps=[create_duckdb_with_schema],
       group_name="extract_from_starke")
def raw_invoice_line_items(duckdb: DuckDBResource) -> None:
    starke_mssql_server = config.get('NETWORK', 'last_known_starke_mssql_server')
    starke_praxis_db = config.get('STARKE_PRAXIS', 'database')

    engine = sqlalchemy.create_engine(
        'mssql+pyodbc://' + STARKE_PRAXIS_USER + ':' + STARKE_PRAXIS_PASSWORD + '@' + starke_mssql_server + '/' + starke_praxis_db + '?driver=ODBC+Driver+18+for+SQL+Server',
        connect_args={"TrustServerCertificate": "yes"})

    con = engine.connect()

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

    result = con.execute(sqlalchemy.text(query))
    df_result = pd.DataFrame(result.fetchall())

    with duckdb.get_connection() as conn:
        conn.execute(SET_PANDAS_ANALYZE_SAMPLE)
        conn.execute("CREATE TABLE IF NOT EXISTS raw_starke.raw_invoice_line_items_temp AS SELECT * FROM df_result;")
        conn.execute("DROP TABLE IF EXISTS raw_starke.raw_invoice_line_items;")
        conn.execute("ALTER TABLE raw_starke.raw_invoice_line_items_temp RENAME TO raw_invoice_line_items;")
        conn.execute("DROP TABLE IF EXISTS raw_starke.raw_invoice_line_items_temp;")


@asset(deps=[dagster_dbt.dbt_manifest_asset_selection.AssetKey("fct_receipts_to_appointments"),
             dagster_dbt.dbt_manifest_asset_selection.AssetKey("fct_accounting_documents"),
             dagster_dbt.dbt_manifest_asset_selection.AssetKey("dim_receipts")],
       group_name="rebuild_evidence_sources")
def copy_sources_to_evidence() -> None:
    os.system('cd $DAGSTER_HOME/../sa_evidence && npm run sources')


@asset(deps=[install_requirements], group_name="pre_dbt_run")
def reload_code_location() -> None:
    client = DagsterGraphQLClient("127.0.0.1", port_number=2999)
    reload_info: ReloadRepositoryLocationInfo = client.reload_repository_location(
        "sa_dagster"
    )
