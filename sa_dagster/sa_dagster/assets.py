from dagster import asset

import socket
import sqlalchemy
import os
import configparser

from dotenv import load_dotenv
load_dotenv()

STARKE_PRAXIS_PORT = int(os.getenv('STARKE_PRAXIS_PORT'))
STARKE_PRAXIS_USER = os.getenv('STARKE_PRAXIS_USER')
STARKE_PRAXIS_PASSWORD = os.getenv('STARKE_PRAXIS_PASSWORD')

CONFIG_FILE_PATH = os.getenv('CONFIG_FILE_PATH')

config = configparser.ConfigParser()
config.read(CONFIG_FILE_PATH)



@asset
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


@asset(deps=[get_local_ip])
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


@asset(deps=[get_list_of_host_with_open_mssql_port])
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



