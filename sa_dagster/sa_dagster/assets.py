from dagster import asset

import socket

import sqlalchemy
from sqlalchemy import create_engine

STARKE_PRAXIS_PORT = 1433
STARKE_PRAXIS_USER = 'sa'
STARKE_PRAXIS_PASSWORD = STARKE_PRAXIS_PASSWORD

available_possible_hosts = []
starke_praxis_host = None
starke_praxis_db = None

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

    with open('../data/temp/ip.txt', 'w') as f:
        f.write(local_ip)


@asset(deps=[get_local_ip])
def get_list_of_host_with_open_mssql_port():
    # https://www.tutorialspoint.com/python_penetration_testing/python_penetration_testing_network_scanner.htm

    with open('../data/temp/ip.txt', 'r') as f:
        local_ip = f.read()

    ip_range = '.'.join(local_ip.split('.')[:3])

    for i in range(1, 255):
        ip = f'{ip_range}.{i}'
        socket.setdefaulttimeout(0.1)

        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = s.connect_ex((ip, STARKE_PRAXIS_PORT))
        if result == 0:
            available_possible_hosts.append(ip)


@asset(deps=[get_list_of_host_with_open_mssql_port])
def check_starke_praxis_host():
    for ip in available_possible_hosts:
        try:
            #conn = pymssql.connect(server=ip, user=STARKE_PRAXIS_USER, password=STARKE_PRAXIS_PASSWORD)
            #conn.close()
            return ip
        except:
            pass