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
from utils.memory import human_read_to_byte
from threading import Lock
import time
class DB:
    def __init__(self, db_name, host, username='sys', password='cohesity', type='standalone'):
        self.db_name = db_name
        self.host = host
        self.username = username
        self.password = password
        self.log = set_logger(f"{self.host.ip}_{self.db_name}", os.path.join('logs', 'dbs'))
        self.connection = self.connect()
        self.is_healthy = True
        self.target_table_count = 100
        self.tables = []
        self.fra_limit_set = None
        self.db_files_limit_set = None
        self.lock = Lock()
        self.get_fra_limit()
        self.get_dbfiles_limit()
        self.get_tables()

    def connect(self, max_retries=5, wait_seconds=60):
        for attempt in range(1, max_retries + 1):
            try:
                return connect_to_oracle(self.host, self.db_name)
            except Exception as e:
                self.log.info(f"[Attempt {attempt}/{max_retries}] Connection failed: {e}")
                if attempt < max_retries:
                    self.log.info(f"Retrying in {wait_seconds} seconds...")
                    time.sleep(wait_seconds)
                else:
                    self.log.info("All retries failed.")
                    return None
    def is_listener_connectivity_available(self):
        try:
            if not self.connection:
                self.connection = self.connect()
        except Exception as e:
            self.log.fatal(f'Cannot connect to db - {self}')
            self.is_healthy = False

    def run_query(self, query):
        retries = 5
        wait = 60
        for attempt in range(1, retries+1):
            self.log.info(f"\n--- Attempt {attempt} - running query {query} in  {self} ---")
            with self.connection.cursor() as cursor:
                try:
                    cursor.execute(query)
                    result = cursor.fetchall()
                    self.log.info(f'query - {query} successfully executed - {self}')
                    return result
                except InterfaceError as e:
                    if 'the executed statement does not return rows' in str(e):
                        self.log.info(f'query executed successfully - {query}')
                        return True
                except Exception as e:
                    self.log.info(f'cannot set query - {query} in {self.host}:{self} got error - {e}')
                    if 'ORA' in str(e):
                        self.log.info('Do not need to retry for this error')
                        return
            if attempt < retries:
                self.log.info(f"Retrying in {wait} seconds...")
                time.sleep(wait)
        return []

    def get_fra_limit(self):
        try:
            result = self.run_query("SELECT name,value FROM v$parameter WHERE name='db_recovery_file_dest_size'")[0][1]
            if int(result) >= human_read_to_byte('1024G'):
                self.fra_limit_set = True
            else:
                self.log.fatal('marking db as unhealthy since fra size is < 1024G')
                self.is_healthy = False
        except IndexError:
            self.log.fatal('Cannot get fra limit')
            self.is_healthy = False

    def set_fra_limit(self):
        if not self.fra_limit_set:
            set_recovery_file_dest_size = 'alter system set db_recovery_file_dest_size=2000G scope=both'
            self.run_query(set_recovery_file_dest_size)

    def get_dbfiles_limit(self):
        try:
            result = self.run_query("select value from v$parameter where name = 'db_files'")[0][0]
            if int(result) >= 1000:
                self.db_files_limit_set = True
            else:
                self.log.fatal('db files limit is less than < 1000, marking db as unhealthy')
                self.is_healthy = False

        except IndexError:
            self.log.fatal('Cannot get db files limit')
            self.is_healthy = False
    def set_db_files_limit(self):
        if not self.db_files_limit_set:
            set_db_files = 'alter system set db_files=20000 scope=spfile'
            self.run_query(set_db_files)

    def get_tables(self):
        if self.is_healthy:
            query = "SELECT table_name from all_tables WHERE table_name LIKE '%TODOITEM%'"
            table_names = self.run_query(query)
            for table in table_names:
                table_name = table[0]
                self.tables.append(Table(db=self,name=table_name))


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
        if self.is_healthy:
            number_of_tables = len(self.tables)
            if len(self.host.dbs) > 2:
                self.target_table_count = self.target_table_count//2
            number_of_tables_to_be_created = max(self.target_table_count - number_of_tables, 0)
            for i in range(number_of_tables_to_be_created):
                self.tables.append(Table(db=self))
            self.log.info(f'Number of tables loaded - {len(self.tables)}')



    def is_pumpable(self):
        # todo: if cdb in name should have atleast one pdb reachable via listener
        # todo: if big in name should have only ony table with name todoitem
        if not self.connection:
            self.is_listener_connectivity_available()
        self.set_fra_limit()
        self.set_db_files_limit()
        self.create_tables()
        if self.is_healthy:
            self.log.info(f'db is healthy - {self} and ready to pump data')
        else:
            self.log.info(f'unhealthy db - {self} cannot pump data in this db')
        return self.is_healthy

    def process_batch(self):
        table_obj = random.choice(self.tables)
        self.host.curr_number_of_batch += 1
        table_obj.insert_batch(self.host.curr_number_of_batch, self.host.total_number_of_batches, self.lock)

    def __repr__(self):
        return f"{self.host}:{self.db_name}"

if __name__ == '__main__':
    dbs = connect_to_oracle('10.131.37.81', 'SBTDB')
    print(dbs)