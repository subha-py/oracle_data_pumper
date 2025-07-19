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

def filter_host_map_by_listener_connectivity(hostmap):
    logger = logging.getLogger(os.environ.get("log_file_name"))
    for host,dbs in hostmap.items():
        db_list = dbs[:]
        for db in db_list:
            try:
                connect_to_oracle(host,db)
                logger.info(f'host -> {host}:{db} is reachable over listener')
            except:
                logger.fatal(f'host -> {host}:{db} is not reachable over listener')
                hostmap[host].remove(db)
    logger.info(f'Final hostmap on which data pumping will happen {hostmap}')
    return hostmap
if __name__ == '__main__':
    conn = connect_to_oracle('10.14.70.149', 'bctstat')