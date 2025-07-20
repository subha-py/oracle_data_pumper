import oracledb
import logging
import os
def connect_to_oracle(host, db_name, user='sys', password='cohesity'):
    connection = oracledb.connect(
        user=user,
        password=password,
        dsn=f"{host}:1521/{db_name}", mode=oracledb.AUTH_MODE_SYSDBA)

    print("Successfully connected to Oracle Database")
    print(connection)
    return connection


if __name__ == '__main__':
    conn = connect_to_oracle('10.14.70.149', 'bctstat')