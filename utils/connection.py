import oracledb

def connect_to_oracle(host, db_name, user='sys', password='cohesity'):
    connection = oracledb.connect(
        user=user,
        password=password,
        dsn=f"{host}:1521/{db_name}", mode=oracledb.AUTH_MODE_SYSDBA)

    print("Successfully connected to Oracle Database")
    print(connection)
    return connection

def connect_to_oracle_pool(host, db_name, user='sys', password='cohesity'):
    pool = oracledb.create_pool(user=user, password=password, dsn=f"{host}:1521/{db_name}",
        mode=oracledb.AUTH_MODE_SYSDBA,  # or "hostname:port/service_name"
        min=20,  # keep 1 per CPU ready
        max=200,  # allow bursts up to 10x CPUs
        increment=20,
        getmode=oracledb.SPOOL_ATTRVAL_WAIT  # wait if pool is exhausted
    )
    return pool


if __name__ == '__main__':
    conn = connect_to_oracle('10.131.37.211', 'PROD1')