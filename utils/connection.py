import oracledb
def connect_to_oracle(user, password, host, db_name):
    connection = oracledb.connect(
        user=user,
        password=password,
        dsn=f"{host}:1521/{db_name}", mode=oracledb.AUTH_MODE_SYSDBA)

    print("Successfully connected to Oracle Database")
    print(connection)
    return connection
if __name__ == '__main__':
    conn = connect_to_oracle('oracle', 'cohesity', '10.14.69.168', 'orcl1')