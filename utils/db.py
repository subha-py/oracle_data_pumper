from utils.ssh import execute_commands_on_host
from collections import defaultdict
from utils.connection import connect_to_oracle
import logging
import os
from utils.log import set_logger
import random
import sys
from oracledb.exceptions import DatabaseError, InterfaceError
import pathlib
from utils.tables import Table
class DB:
    def __init__(self, db_name, host, username='sys', password='cohesity', type='standalone'):
        self.db_name = db_name
        self.host = host
        self.username = username
        self.password = password
        self.connection = self.connect()
        self.log = set_logger(f"{self.host.ip}_{self.db_name}", os.path.join('logs', 'dbs'))
        self.is_healthy = True
        self.target_table_count = 100
        self.tables = []


        self.get_tables()

    def connect(self):
        return connect_to_oracle(self.host, self.db_name)
    def is_listener_connectivity_available(self):
        try:
            self.connection = self.connect()
        except Exception as e:
            self.log.fatal(f'Cannot connect to db - {self}')
            self.is_healthy = False

    def run_query(self, query):
        with self.connection.cursor() as cursor:
            try:
                cursor.execute(query)
                result = cursor.fetchall()
                return result
            except InterfaceError as e:
                if 'the executed statement does not return rows' in str(e):
                    self.log.info(f'query executed successfully - {query}')
            except Exception as e:
                self.log.info(f'cannot set query - {query} in {self.host}:{self} got error - {e}')
                return []

    def set_fra_limit(self):
        # todo: write set only get fra_limit is not set
        set_recovery_file_dest_size = 'alter system set db_recovery_file_dest_size=2000G scope=both'
        self.run_query(set_recovery_file_dest_size)

    def set_db_files_limit(self):
        # todo: write set only get db_files limit is not set
        set_db_files = 'alter system set db_files=20000 scope=spfile'
        self.run_query(set_db_files)

    def get_tables(self):
        query = "SELECT table_name from all_tables WHERE table_name LIKE '%TODOITEM%'"
        table_names = self.run_query(query)[0]
        for table in table_names:
            self.tables.append(Table(db=self,name=table))


    def delete_table(self, tablename='todoitem'):
        with self.connection.cursor() as cursor:
            cursor.execute(f"""
                begin
                    execute immediate 'drop table {tablename}';
                    exception when others then if sqlcode <> -942 then raise;
                    end if;
                end;""")
            try:
                cursor.execute(f"drop tablespace {tablename}ts \
                    INCLUDING CONTENTS AND DATAFILES")
            except DatabaseError as e:
                if "does not exist" in str(e):
                    return
            self.log.info(f'deleted table - {tablename}')

    def get_datafile_dir(self):
        self.log.info('Fetching datafile location')
        with self.connection.cursor() as cursor:
            if 'pdb' not in self.db_name.lower():
                cursor.execute("select value from v$parameter where name = 'db_create_file_dest'")
                result = cursor.fetchone()[0]
                result = os.path.join(result,self.db_name, 'datafile')
            else:
                cursor.execute("select FILE_NAME from dba_data_files")
                result = cursor.fetchone()[0]
                result = str(pathlib.Path(result).parent)
        self.log.info(f'Got datafile location - {result}')
        return result

    def create_tables(self):
        number_of_tables_to_be_created = self.target_table_count
        number_of_tables = len(self.tables)
        if len(self.host.dbs) > 2:
            self.target_table_count = self.target_table_count//2
        if number_of_tables < self.target_table_count:
            number_of_tables_to_be_created = self.target_table_count - number_of_tables
        for i in range(number_of_tables_to_be_created):
            self.tables.append(Table(db=self))
        self.is_healthy = len(self.tables) == self.target_table_count



    def is_pumpable(self):
        # can connect over listener
        # have db_files param set
        # have fra param set
        # have atleast 100 tables
        # todo: if cdb in name should have atleast one pdb reachable via listener
        # todo: if big in name should have only ony table with name todoitem
        self.is_listener_connectivity_available()
        self.set_fra_limit()
        self.set_db_files_limit()
        self.create_tables()
        return self.is_healthy

    def process_batch(self, batch_number, total_batches, lock):
        table_obj = random.choice(self.tables)
        table_obj.insert_batch(batch_number, total_batches, lock)

    def __repr__(self):
        return f"{self.host}:{self.db_name}"



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