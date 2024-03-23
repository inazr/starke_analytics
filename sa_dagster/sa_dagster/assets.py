from dagster import asset

import socket
import sqlalchemy
import os
import configparser

import pandas as pd
from dagster_duckdb import DuckDBResource

from dotenv import load_dotenv
load_dotenv()

STARKE_PRAXIS_PORT = int(os.getenv('STARKE_PRAXIS_PORT'))
STARKE_PRAXIS_USER = os.getenv('STARKE_PRAXIS_USER')
STARKE_PRAXIS_PASSWORD = os.getenv('STARKE_PRAXIS_PASSWORD')

CONFIG_FILE_PATH = os.getenv('CONFIG_FILE_PATH')

config = configparser.ConfigParser()
config.read(CONFIG_FILE_PATH)


@asset(group_name="git")
def git_pull():
    os.system('git pull')


@asset(group_name="git")
def git_push():
    os.system('git add .')
    os.system('git commit -m "auto commit"')
    os.system('git push')


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

    with open(CONFIG_FILE_PATH, 'w') as configfile:
        config.write(configfile)


@asset(deps=[get_correct_db],
       group_name="discover_network")
def create_starke_schema(duckdb: DuckDBResource) -> None:
    create_schema_query = """
                            CREATE SCHEMA IF NOT EXISTS starke
                            ;
                          """

    with duckdb.get_connection() as conn:
        conn.execute(create_schema_query)


@asset(deps=[create_starke_schema],
       group_name="extract_load")
def extract_load_termin(duckdb: DuckDBResource) -> None:
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
                        HASHBYTES('SHA2_256', TERMIN.Text) AS Text, -- possible personal data in text
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
        conn.execute("DROP TABLE IF EXISTS starke.raw_termine;")
        conn.execute("CREATE TABLE IF NOT EXISTS starke.raw_termine AS SELECT * FROM df_result;")


@asset(deps=[create_starke_schema],
       group_name="extract_load")
def extract_load_mitarbeiter(duckdb: DuckDBResource) -> None:
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
        conn.execute("DROP TABLE IF EXISTS starke.raw_mitarbeiter;")
        conn.execute("CREATE TABLE IF NOT EXISTS starke.raw_mitarbeiter AS SELECT * FROM df_result;")


@asset(deps=[create_starke_schema],
       group_name="extract_load")
def extract_load_rezept(duckdb: DuckDBResource) -> None:
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
                        HASHBYTES('SHA2_256', REZEPT.PAT_Name) AS PAT_Name, -- possible personal data in text
                        HASHBYTES('SHA2_256', CAST(REZEPT.PAT_Geburtsdatum AS VARCHAR(6))) AS PAT_Geburtsdatum, -- possible personal data in text
                        HASHBYTES('SHA2_256', REZEPT.PAT_Versichertennummer) AS PAT_Versichertennummer, -- possible personal data in text
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
                        HASHBYTES('SHA2_256', REZEPT.ARZ_Name) AS ARZ_Name, -- possible personal data in text
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
        conn.execute("DROP TABLE IF EXISTS starke.raw_rezept;")
        conn.execute("CREATE TABLE IF NOT EXISTS starke.raw_rezept AS SELECT * FROM df_result;")

    conn.close()
