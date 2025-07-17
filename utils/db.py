from utils.ssh import execute_commands_on_host
from collections import defaultdict
from utils.connection import connect_to_oracle
import logging
import os
from utils.log import set_logger
class DB:
    def __init__(self, db_name, host, username='sys', password='cohesity', type='standalone'):
        self.db_name = db_name
        self.host = host
        self.username = username
        self.password = password
        self.connection = None
        self.log = set_logger(f"{self.host.ip}_{self.db_name}", os.path.join('logs', 'dbs'))
    def is_listener_connectivity_available(self):
        try:
            self.connection = self.connect()
            return True
        except Exception as e:
            self.log.fatal(f'Cannot connect to db - {self}')
            return False

    def connect(self):
        return connect_to_oracle(self.host, self.db_name)

    def __repr__(self):
        return f"{self.host}:{self.oracle_sid}"



def get_remote_oracle_dbs(host, oratab_path='/etc/oratab'):
    oracle_sids = []
    try:

        command = f"grep -v '^#' {oratab_path} | grep -v '^$'"
        stdout, stderr = execute_commands_on_host(host,[command])

        if stderr:
            raise Exception(f"Error reading oratab: {stderr.strip()}")

        for line in stdout.strip().splitlines():
            parts = line.strip().split(":")
            if len(parts) >= 2:
                sid = parts[0].strip().upper()
                oracle_sids.append(sid)
        return oracle_sids

    except Exception as e:
        print(f"Failed to fetch Oracle DBs: {e}")
        return []
def get_db_map_from_vms(ips):
    logger = logging.getLogger(os.environ.get("log_file_name"))
    result = defaultdict(list)
    for ip in ips:
        dbs = get_remote_oracle_dbs(ip)
        logger.info(f'for host -> {ip} got dbs -> {dbs}')
        result[ip] = dbs
    return result
if __name__ == '__main__':
    dbs = get_remote_oracle_dbs('10.14.69.139')
    print(dbs)