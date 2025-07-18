from utils.memory import (
    human_read_to_byte,
    get_number_of_rows_from_file_size,
    set_recovery_file_dest_size,
    bytes_to_human_read,
    get_databse_size
)
from oracledb.exceptions import DatabaseError
import os
import random
import string
import sys
import time
import datetime
import pathlib
from utils.tablespace import Tablespace
from string import ascii_letters
class Table:
    def __init__(self, db, name=None):
        self.name = name
        self.tablespace = None
        self.db = db
        self.batch_size = 10000
        self.random_size = False
        if self.is_created():
            # when table is already present no need to create a new one
            self.tablespace = Tablespace(db=self.db, table=self)
        else:
            # when table is not present need to create
            if self.name is None:
                random_string = ''.join(random.choices(ascii_letters, k=4))
                self.name = f"todoitem{random_string}"
            self.create()
    def is_created(self):
        if self.name is None:
            return False
        query = f"SELECT table_name FROM all_tables WHERE table_name LIKE '%{self.name}'"
        result = self.db.run_query(query)[0][0]
        return len(result) > 0

    def delete(self):
        with self.db.connection.cursor() as cursor:
            cursor.execute(f"""
                begin
                    execute immediate 'drop table {self.name}';
                    exception when others then if sqlcode <> -942 then raise;
                    end if;
                end;""")
            self.db.log.info(f'deleted table - {self.name}')
        if self.tablespace is not None:
            self.tablespace.delete()
    def create(self):
        self.delete()
        tablespace_name = f"{self.name}ts"
        self.tablespace = Tablespace(name=tablespace_name, db=self.db, table=self)
        self.db.log.info(f"Creating table {self.name}")
        with self.db.connection.cursor() as cursor:
            cursor.execute(f"""
                            create table {self.name} (
                                id number generated always as identity,
                                description varchar2(4000),
                                creation_ts timestamp with time zone default current_timestamp,
                                done number(1,0),
                                randomnumber number,
                                randomstring varchar2(4000),
                                primary key (id))
                                TABLESPACE {self.tablespace.name}""")
        self.db.log.info(f"Created table {self.name}")

    def create_row(self):
        ascii_letters = list(string.ascii_letters)
        task_number = random.randint(1, sys.maxsize)
        random_string = ''.join(random.choices(ascii_letters, k=10))
        toggle = random.choice([True, False])
        return f'Task:{random.randint(0, sys.maxsize) + 1}', toggle, task_number, random_string



    def insert_batch(self, batch_number, number_of_batches, lock, rows=None):
        rows = []
        for i in range(self.batch_size):
            rows.append(self.create_row())
        self.db.log.info(f"inserting into {self.name}: batch_number: {batch_number}/{number_of_batches}")
        with self.db.connection.cursor() as cursor:
            try:
                cursor.executemany(f"insert into {self.name} (description, done, randomnumber, randomstring) values(:1, :2, :3, :4)", rows)
            except DatabaseError as e:
                if 'unable to extend' in str(e):
                    # acquire lock
                    if not lock.locked():
                        start = time.time()
                        lock.acquire()
                        try:
                            self.db.log.info(f'Acquired lock by batch number - {batch_number}/{number_of_batches}')
                            self.db.log.info('Failed to insert data due to lack of space in tablespace')
                            self.tablespace.extend()
                            cursor.executemany(f"insert into {self.name} (description, done, \
                                randomnumber, randomstring) values(:1, :2, :3, :4)", rows)
                            self.db.log.info(f'lock released by batch number- \
                                    :{batch_number}/{number_of_batches}')
                            lock.release()
                            end = time.time()
                            self.db.log.info(f'lock is held by batch \
                                     - :{batch_number}/{number_of_batches} \
                                     for - {end - start} secs')
                        except DatabaseError as e:
                            if 'unable to extend' in str(e):
                                lock.release()
                                self.db.log.info(f'batch - :{batch_number}/{number_of_batches} \
                                        is going to recursion')
                                # go to recursion
                                self.insert_batch(batch_number, number_of_batches, lock, rows)
                                return
                        except Exception as e:
                            self.db.log.info(f'got exception - {e} - batch number - \
                                    :{batch_number}/{number_of_batches}')
                            lock.release()

                    else:
                        while lock.locked():
                            sleep_time = random.randint(180, 300)
                            self.db.log.info(f'{datetime.datetime.now()}:\
                                    batch number - :{batch_number}/{number_of_batches}\
                                     is going to sleep for {sleep_time} secs since \
                                    tablespace is expanding')
                            time.sleep(sleep_time)
                        try:
                            cursor.executemany(f"insert into {self.name} (description, done, \
                                randomnumber, randomstring) values(:1, :2, :3, :4)", rows)

                        except DatabaseError as e:
                            if 'unable to extend' in str(e):
                                self.db.log.info(f'batch - :{batch_number}/{number_of_batches} \
                                        is going to recursion')
                                # go to recursion
                                self.insert_batch(batch_number, number_of_batches, lock, rows)
                                return

        self.db.connection.commit()
        self.db.log.info(f'Committed batch number - :{batch_number}/{number_of_batches}')
        return


    def __repr__(self):
        return self.name



def is_table_created(connection, table):
    with connection.cursor() as cursor:
        cursor.execute("SELECT table_name FROM user_tables WHERE table_name = :1", [table.upper()])
        values = cursor.fetchall()
        return True if len(values) > 0 else False

def delete_todoitem_table(connection, tablename='todoitem'):
    with connection.cursor() as cursor:
        cursor.execute(f"""
            begin
                execute immediate 'drop table {tablename}';
                exception when others then if sqlcode <> -942 then raise;
                end if;
            end;""")
        try:
            cursor.execute(
                f"drop tablespace {tablename}ts \
                INCLUDING CONTENTS AND DATAFILES")
        except DatabaseError as e:
            if "does not exist" in str(e):
                return
        print(f'deleted table - {tablename}')

def get_datafile_dir(connection, db_name):
    print('Fetching datafile location')
    with connection.cursor() as cursor:
        if 'pdb' not in db_name.lower():
            cursor.execute(
                "select value from v$parameter where name = 'db_create_file_dest'")
            result = cursor.fetchone()[0]
            result = os.path.join(result, db_name, 'datafile')
        else:
            cursor.execute("select FILE_NAME from dba_data_files")
            result = cursor.fetchone()[0]
            result = str(pathlib.Path(result).parent)
    print(f'Got datafile location - {result}')
    return result

def get_curr_number_of_datafile(connection):
    with connection.cursor() as cursor:
        cmd = ("select count(file_name) from dba_data_files \
         where tablespace_name='TODOITEMTS'")
        cursor.execute(cmd)
        res = cursor.fetchone()
    return res[0]

def create_tablespace(connection, db_name, tablespace_name, datafile_size, autoextend):
    ascii_letters = list(string.ascii_letters)
    # random_string = ''.join(
    #     random.choices(ascii_letters, k=10))
    datafile_path = os.path.join(get_datafile_dir(connection, db_name), tablespace_name)

    if autoextend:
        cmd = (f"""create tablespace {tablespace_name} \
            datafile '{datafile_path}.dbf' size {datafile_size} AUTOEXTEND 
            ON NEXT {datafile_size} EXTENT MANAGEMENT LOCAL SEGMENT SPACE MANAGEMENT AUTO""")
    else:
        cmd = (f"create tablespace {tablespace_name} \
        datafile '{datafile_path}.dbf' size {datafile_size}")
    print(f'creating tablespace with name - {tablespace_name}')
    with connection.cursor() as cursor:
        cursor.execute(cmd)
    print(f'tablespace created with name - {tablespace_name}')
    return tablespace_name

def list_all_todoitem_tables(connection, multi_table):
    if multi_table:
        query = """
            SELECT table_name
            FROM all_tables
            WHERE table_name LIKE '%TODOITEM%'
            """
    else:
        query = """
                    SELECT table_name
                    FROM all_tables
                    WHERE table_name = 'TODOITEM'
                    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        tables = cursor.fetchall()
        return tables

def create_single_todoitem_table(connection, db_name, tablename, tablespace_name, datafile_size, autoextend):
    print(f"Creating tablespace {tablespace_name}")
    create_tablespace(connection, db_name, tablespace_name, datafile_size, autoextend)
    print("Tablespace created")
    print(f"Creating table {tablename}")
    with connection.cursor() as cursor:
        cursor.execute(f"""
            create table {tablename} (
                id number generated always as identity,
                description varchar2(4000),
                creation_ts timestamp with time zone default current_timestamp,
                done number(1,0),
                randomnumber number,
                randomstring varchar2(4000),
                primary key (id))
                TABLESPACE {tablespace_name}""")
    print(f"Created table {tablename}")

def create_todo_item_table(connection, db_name, datafile_size,
                               dest_recovery_size, autoextend, create_table, multi_table):
    set_recovery_file_dest_size(connection, dest_recovery_size)
    existing_tables = list_all_todoitem_tables(connection, multi_table)
    # if not all tables created, create the ones required
    def should_create():
        return create_table or not existing_tables

    if should_create():
        table_range = range(1, 1001) if multi_table else [""]
        for i in table_range:
            suffix = str(i) if multi_table else ""
            tablename = f"todoitem{suffix}"
            tablespace_name = f"todoitem{suffix}ts"
            delete_todoitem_table(connection, tablename)
            create_single_todoitem_table(connection, db_name, tablename, tablespace_name, datafile_size, autoextend)
    else:
        print("Tables already exist. Skipping creation.")

