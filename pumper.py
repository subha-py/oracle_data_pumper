import random
import string
import sys
import time
import datetime
import pathlib

from oracledb.exceptions import DatabaseError
import os
import concurrent.futures
from threading import Lock

from utils.memory import (
    human_read_to_byte,
    get_number_of_rows_from_file_size,
    set_recovery_file_dest_size,
    bytes_to_human_read
)
from utils.connection import connect_to_oracle

from utils.bct import enable_bct
import argparse

# todo: random datafile size adding to total size
# todo: datachurn
# todo: refactor coding modules



def is_table_created(connection):
    with connection.cursor() as cursor:
        cursor.execute(f"""
            SELECT table_name FROM user_tables WHERE table_name = 'TODOITEM'
        """)
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


def create_tablespace(connection, db_name, datafile_size, autoextend):
    ascii_letters = list(string.ascii_letters)
    random_string = ''.join(
        random.choices(ascii_letters, k=10))
    tablespace_name = 'todoitemts'
    datafile_path = os.path.join(get_datafile_dir(connection, db_name),
                                 tablespace_name)

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


def create_todo_item_table(connection, db_name, datafile_size,
                           dest_recovery_size, autoextend):
    set_recovery_file_dest_size(connection, dest_recovery_size)
    delete_todoitem_table(connection)
    tablespace_name = create_tablespace(connection, db_name, datafile_size,
                                        autoextend)
    print('creating table todoitem')
    with connection.cursor() as cursor:
        cursor.execute(f"""
            create table todoitem (
                id number generated always as identity,
                description varchar2(4000),
                creation_ts timestamp with time zone default current_timestamp,
                done number(1,0),
                randomnumber number,
                randomstring varchar2(4000),
                primary key (id))
                TABLESPACE {tablespace_name}""")
    print('created table todoitem')


def get_curr_number_of_datafile(connection):
    with connection.cursor() as cursor:
        cmd = ("select count(file_name) from dba_data_files \
         where tablespace_name='TODOITEMTS'")
        cursor.execute(cmd)
        res = cursor.fetchone()
    return res[0]


def process_batch(connection, datafile_dir, datafile_size, batch_size,
                  batch_number, lock, rows=None, number_of_batches=None, random_flag=False):
    if not rows:
        rows = []
    toggle = 0
    ascii_letters = list(string.ascii_letters)

    if len(rows) < 1:
        for i in range(batch_size):
            task_number = random.randint(1, sys.maxsize)
            random_string = ''.join(random.choices(ascii_letters, k=10))
            row = (f'Task:{i + 1}', toggle, task_number, random_string)
            rows.append(row)
            toggle = int(not toggle)

    with connection.cursor() as cursor:
        print(f'{datetime.datetime.now()}: Inserting batch number - {batch_number}/{number_of_batches}')

        try:
            cursor.executemany(
                "insert into todoitem (description, done, randomnumber, randomstring) values(:1, :2, :3, :4)",
                rows)

        except DatabaseError as e:
            if 'unable to extend' in str(e):
                # acquire lock
                if not lock.locked():
                    start = time.time()
                    lock.acquire()
                    try:
                        print(f'Acquired lock by batch number - {batch_number}/{number_of_batches}')
                        print('Failed to insert data due to lack of space in tablespace')

                        if random_flag:
                            min_size = human_read_to_byte("50M")
                            max_size = human_read_to_byte(datafile_size)
                            random_bytes = random.randint(min_size, max_size)
                            datafile_size = bytes_to_human_read(random_bytes)
                            print(f'Picked random datafile_size = {datafile_size}')

                        print(f'Extending tablespace by {datafile_size}')
                        random_string = ''.join(random.choices(ascii_letters, k=10))
                        cmd = f"""ALTER TABLESPACE todoitemts ADD DATAFILE 
                        '{datafile_dir}/todoitemts_{random_string}.dbf' 
                        SIZE {datafile_size}"""
                        cursor.execute(cmd)
                        print('tablespace successfully increased')
                        cursor.executemany(
                            "insert into todoitem (description, done, \
                        randomnumber, randomstring) values(:1, :2, :3, :4)",
                            rows)
                        print(
                            f'lock released by batch number- \
                            :{batch_number}/{number_of_batches}')
                        lock.release()
                        end = time.time()
                        print(
                            f'lock is held by batch \
                             - :{batch_number}/{number_of_batches} \
                             for - {end - start} secs')
                    except DatabaseError as e:
                        if 'unable to extend' in str(e):
                            lock.release()
                            print(
                                f'batch - :{batch_number}/{number_of_batches} \
                                is going to recursion')
                            # go to recursion
                            process_batch(connection, datafile_dir,
                                          datafile_size, batch_size,
                                          batch_number, lock, rows,
                                          number_of_batches)
                            return
                    except Exception as e:
                        print(
                            f'got exception - {e} - batch number - \
                            :{batch_number}/{number_of_batches}')
                        lock.release()

                else:
                    while lock.locked():
                        sleep_time = random.randint(180, 300)
                        print(
                            f'{datetime.datetime.now()}:\
                            batch number - :{batch_number}/{number_of_batches}\
                             is going to sleep for {sleep_time} secs since \
                            tablespace is expanding')
                        time.sleep(sleep_time)
                    try:
                        cursor.executemany(
                            "insert into todoitem (description, done, \
                        randomnumber, randomstring) values(:1, :2, :3, :4)",
                        rows)

                    except DatabaseError as e:
                        if 'unable to extend' in str(e):
                            print(
                                f'batch - :{batch_number}/{number_of_batches} \
                                is going to recursion')
                            # go to recursion
                            process_batch(connection, datafile_dir,
                                          datafile_size, batch_size,
                                          batch_number, lock, rows,
                                          number_of_batches)
                            return

    connection.commit()
    print(f'Committed batch number - :{batch_number}/{number_of_batches}')
    return


def pump_data(connection, db_name, total_size, datafile_size, batch_size,
              create_table=False, max_threads=128,
              dest_recovery_size='100G', random_flag=False, autoextend=False):
    if not is_table_created(connection) or create_table:
        create_todo_item_table(connection, db_name, datafile_size,
                               dest_recovery_size, autoextend)
    datafile_dir = get_datafile_dir(connection, db_name)
    target_number_of_datafile = human_read_to_byte(
        total_size) // human_read_to_byte(datafile_size)
    total_rows_required = get_number_of_rows_from_file_size(total_size)
    number_of_batches = total_rows_required // batch_size
    future_to_batch = {}
    workers = min(max_threads, number_of_batches)
    print('number of workers - {}'.format(workers))
    lock = Lock()
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=workers) as executor:
        for batch_number in range(1, number_of_batches + 1):
            arg = (
            connection, datafile_dir, datafile_size, batch_size, batch_number,
            lock, None, number_of_batches, random_flag)
            future_to_batch[
                executor.submit(process_batch, *arg)] = batch_number

    result = []
    for future in concurrent.futures.as_completed(future_to_batch):
        batch_number = future_to_batch[future]
        try:
            res = future.result()
            if not res:
                result.append(batch_number)
        except Exception as exc:
            print("%r generated an exception: %s" % (batch_number, exc))
            # todo: handle here sequentially for error batches

    return result

if __name__ == '__main__':
    connection = connect_to_oracle('sys', 'cohesity', '10.3.63.235',
                                   'FIDB0',)
    pump_data(connection, 'FIDB0', '1G', '100M', 100000, create_table=True,
              max_threads=128, dest_recovery_size='1T', random_flag=False,
              autoextend=True)