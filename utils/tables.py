from utils.memory import set_recovery_file_dest_size
from oracledb.exceptions import DatabaseError
from oracledb import DB_TYPE_NUMBER, DB_TYPE_BOOLEAN
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
        self.batch_size = self.db.host.batch_size
        self.random_size = False
        self.is_healthy = True
        if self.is_created():
            # when table is already present no need to create a new one
            self.tablespace = Tablespace(db=self.db, table=self)
        else:
            # when table is not present need to create
            if self.name is None:
                random_string = ''.join(random.choices(ascii_letters, k=4))
                self.name = f"todoitem{random_string}"
            self.create()
        self.row_count = self.get_row_count()
        self.lowest_id = 0
        self.highest_id = 0
        self.get_id_range()
    def is_created(self):
        if self.name is None:
            return False
        query = f"SELECT table_name FROM all_tables WHERE table_name='{self.name}'"
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

    def create_row(self, row_id=None):
        ascii_letters = list(string.ascii_letters)
        task_number = random.randint(1, sys.maxsize)
        random_string = ''.join(random.choices(ascii_letters, k=10))
        toggle = random.choice([True, False])
        if row_id is not None:
            return f'Task:{random.randint(0, sys.maxsize) + 1}', toggle, task_number, random_string, row_id
        else:
            return f'Task:{random.randint(0, sys.maxsize) + 1}', toggle, task_number, random_string
    def get_random_ids(self):
        cmd = (f"""
            SELECT id FROM (
                SELECT id FROM todoitemoxub ORDER BY DBMS_RANDOM.VALUE
            ) WHERE ROWNUM <= {self.batch_size}
        """)
        ids = [row[0] for row in self.db.run_query(cmd)]
        return ids
    def get_random_id_queryless(self):
        return random.randint(self.lowest_id, self.highest_id)

    def update_batch(self, batch_number, number_of_batches, lock, rows=None):
        self.db.log.info(f"updating into {self.name}: batch_number: {batch_number}/{number_of_batches}")
        if rows is None:
            rows = []
            random_row = random.randint(self.lowest_id, abs(self.highest_id-self.batch_size)+1)
            for row_id in range(random_row, random_row+self.batch_size):
                rows.append(self.create_row(row_id=row_id))
        with self.db.connection_pool.acquire() as connection:
            with connection.cursor() as cursor:
                try:
                    cursor.executemany(f"""
                        UPDATE {self.name}
                           SET description = :1,
                               done = :2,
                               randomnumber = :3,
                               randomstring = :4
                         WHERE id = :5
                    """, rows)
                except DatabaseError as e:
                    if 'unable to extend' in str(e):
                        self.db.log.info(f'reached end of file skipping txn, marking table is unhealthy - {self.name}')
                        self.is_healthy = False
                        return
                        # acquire lock
                        # if not lock.locked():
                        #     start = time.time()
                        #     lock.acquire()
                        #     try:
                        #         self.db.log.info(f'Acquired lock by batch number - {batch_number}/{number_of_batches}')
                        #         self.db.log.info('Failed to insert data due to lack of space in tablespace')
                        #         self.tablespace.extend()
                        #         cursor.executemany(f"""
                        #                                 UPDATE {self.name}
                        #                                    SET description = :1,
                        #                                        done = :2,
                        #                                        randomnumber = :3,
                        #                                        randomstring = :4
                        #                                  WHERE id = :5
                        #                             """, rows, batcherrors=True)
                        #         self.db.log.info(f'lock released by batch number- \
                        #                 :{batch_number}/{number_of_batches}')
                        #         lock.release()
                        #         end = time.time()
                        #         self.db.log.info(f'lock is held by batch \
                        #                  - :{batch_number}/{number_of_batches} \
                        #                  for - {end - start} secs')
                        #     except DatabaseError as e:
                        #         if 'unable to extend' in str(e):
                        #             lock.release()
                        #             self.db.log.info(f'batch - :{batch_number}/{number_of_batches} \
                        #                     is going to recursion')
                        #             # go to recursion
                        #             self.update_batch(batch_number, number_of_batches, lock, rows)
                        #             return
                        #     except Exception as e:
                        #         self.db.log.info(f'got exception - {e} - batch number - \
                        #                 :{batch_number}/{number_of_batches}')
                        #         lock.release()
                        #
                        # else:
                        #     while lock.locked():
                        #         sleep_time = random.randint(180, 300)
                        #         self.db.log.info(f'{datetime.datetime.now()}:\
                        #                 batch number - :{batch_number}/{number_of_batches}\
                        #                  is going to sleep for {sleep_time} secs since \
                        #                 tablespace is expanding')
                        #         time.sleep(sleep_time)
                        #     try:
                        #         cursor.executemany(f"""
                        #                                 UPDATE {self.name}
                        #                                    SET description = :1,
                        #                                        done = :2,
                        #                                        randomnumber = :3,
                        #                                        randomstring = :4
                        #                                  WHERE id = :5
                        #                             """, rows, batcherrors=True)
                        #
                        #     except DatabaseError as e:
                        #         if 'unable to extend' in str(e):
                        #             self.db.log.info(f'batch - :{batch_number}/{number_of_batches} \
                        #                     is going to recursion')
                        #             # go to recursion
                        #             self.update_batch(batch_number, number_of_batches, lock, rows)
                        #             return
                    elif 'the database or network closed the connection' in str(e):
                        self.db.log.info('This happens when there is a connection error between db and pumper')
                        self.db.log.info(f'db is marked unhealthy, because {e}')
                        self.db.is_healthy = False
                        self.db.host.failed_number_of_batch += 1
                except Exception as e:
                    if 'object has not attribute' in str(e):
                        self.db.log.info('This happens when database closes the connection')
                        self.db.log.info(f'db is marked unhealthy, because {e}')
                        self.db.is_healthy = False
                        self.db.host.failed_number_of_batch += 1
            try:
                if self.db.is_healthy:
                    connection.commit()
            except AttributeError as e:
                self.db.log.info('This happens when db is shutdown while pumper is running')
                self.db.log.info('cannot commit this transaction, will mark this db as unhealthy')
                self.db.is_healthy = False
                self.db.host.failed_number_of_batch += 1
        del rows
        if self.db.is_healthy:
            # sleep_time = random.randint(1,5)
            self.db.log.info(f'{self.db}-{self}-Committed batch number - :{batch_number}/{number_of_batches}')
        return


    def insert_batch(self, batch_number, number_of_batches, lock, rows=None):
        if rows is None:
            rows = []
            for i in range(self.batch_size):
                rows.append(self.create_row())
        self.db.log.info(f"inserting into {self.name}: batch_number: {batch_number}/{number_of_batches}")
        with self.db.connection_pool.acquire() as connection:
            with connection.cursor() as cursor:
                try:
                    cursor.executemany(f"insert into {self.name} (description, done, randomnumber, randomstring) values(:1, :2, :3, :4)", rows)
                except DatabaseError as e:
                    if 'unable to extend' in str(e):
                        self.db.log.info(f'reached end of file skipping txn - {self.name}, marking table is unhealthy ')
                        self.is_healthy = False
                        return
                        # if not lock.locked():
                        #     start = time.time()
                        #     lock.acquire()
                        #     try:
                        #         self.db.log.info(f'Acquired lock by batch number - {batch_number}/{number_of_batches}')
                        #         self.db.log.info('Failed to insert data due to lack of space in tablespace')
                        #         self.tablespace.extend()
                        #         cursor.executemany(f"insert into {self.name} (description, done, \
                        #             randomnumber, randomstring) values(:1, :2, :3, :4)", rows)
                        #         self.db.log.info(f'lock released by batch number- \
                        #                 :{batch_number}/{number_of_batches}')
                        #         lock.release()
                        #         end = time.time()
                        #         self.db.log.info(f'lock is held by batch \
                        #                  - :{batch_number}/{number_of_batches} \
                        #                  for - {end - start} secs')
                        #     except DatabaseError as e:
                        #         if 'unable to extend' in str(e):
                        #             lock.release()
                        #             self.db.log.info(f'batch - :{batch_number}/{number_of_batches} \
                        #                     is going to recursion')
                        #             # go to recursion
                        #             self.insert_batch(batch_number, number_of_batches, lock, rows)
                        #             return
                        #     except Exception as e:
                        #         self.db.log.info(f'got exception - {e} - batch number - \
                        #                 :{batch_number}/{number_of_batches}')
                        #         lock.release()
                        #
                        # else:
                        #     while lock.locked():
                        #         sleep_time = random.randint(180, 300)
                        #         self.db.log.info(f'{datetime.datetime.now()}:\
                        #                 batch number - :{batch_number}/{number_of_batches}\
                        #                  is going to sleep for {sleep_time} secs since \
                        #                 tablespace is expanding')
                        #         time.sleep(sleep_time)
                        #     try:
                        #         cursor.executemany(f"insert into {self.name} (description, done, \
                        #             randomnumber, randomstring) values(:1, :2, :3, :4)", rows)
                        #
                        #     except DatabaseError as e:
                        #         if 'unable to extend' in str(e):
                        #             self.db.log.info(f'batch - :{batch_number}/{number_of_batches} \
                        #                     is going to recursion')
                        #             # go to recursion
                        #             self.insert_batch(batch_number, number_of_batches, lock, rows)
                        #             return
                    elif 'the database or network closed the connection' in str(e):
                        self.db.log.info('This happens when there is a connection error between db and pumper')
                        self.db.log.info(f'db is marked unhealthy, because {e}')
                        self.db.is_healthy = False
                        self.db.host.failed_number_of_batch += 1
                except Exception as e:
                    if 'object has not attribute' in str(e):
                        self.db.log.info('This happens when database closes the connection')
                        self.db.log.info(f'db is marked unhealthy, because {e}')
                        self.db.is_healthy = False
                        self.db.host.failed_number_of_batch += 1
            try:
                if self.db.is_healthy:
                    connection.commit()
            except AttributeError as e:
                self.db.log.info('This happens when db is shutdown while pumper is running')
                self.db.log.info('cannot commit this transaction, will mark this db as unhealthy')
                self.db.is_healthy = False
                self.db.host.failed_number_of_batch += 1
        del rows
        if self.db.is_healthy:
            # sleep_time = random.randint(1,5)
            self.db.log.info(f'{self.db}-{self}-Committed batch number - :{batch_number}/{number_of_batches}')
        return

    def get_row_count(self):
        if self.db.host.update_rows:
            if self.db.row_count_map.get(self.name).get('row_count') is not None:
                self.db.log.info(f'Got row_count from row_count_map - {self.name}')
                return self.db.row_count_map.get(self.name).get('row_count')
            cmd = f'SELECT COUNT(*) AS row_count FROM {self.name}'
            return self.db.run_query(cmd)[0][0]
        else:
            return 0
    def get_id_range(self):
        if self.db.host.update_rows:
            if self.db.row_count_map.get(self.name).get('lowest_id') is not None and \
                    self.db.row_count_map.get(self.name).get('highest_id') is not None:
                self.db.log.info(f'Got lowest id and highest id from row_count_map - {self.name}')
                self.lowest_id = self.db.row_count_map.get(self.name).get('lowest_id')
                self.highest_id = self.db.row_count_map.get(self.name).get('highest_id')
                return
            cmd = f'SELECT MIN(id) AS lowest_id, MAX(id) AS highest_id FROM {self.name}'
            output = self.db.run_query(cmd)[0]
            self.lowest_id, self.highest_id = output[0], output[1]
            return output
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

if __name__ == '__main__':
    from utils.connection import connect_to_oracle
    conn = connect_to_oracle('10.14.69.186', 'testsindb'.upper())
    tables = ['todoitem']
    for table in tables:
        delete_todoitem_table(conn, table)