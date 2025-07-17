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
    bytes_to_human_read,
    get_databse_size
)
from utils.connection import connect_to_oracle
from utils.tables import *

from utils.bct import enable_bct
import argparse

# todo: random datafile size adding to total size
# todo: datachurn
# todo: refactor coding modules

def process_batch(connection, datafile_dir, datafile_size, batch_size,
                  batch_number, lock, tables, multi_table,
                  rows=None, number_of_batches=None, random_flag=False):
    if rows is None:
        rows = []

    if len(rows) < 1:
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
        try:
            if multi_table:
                table_entry = random.choice(tables)
                table = table_entry[0] if isinstance(table_entry, (list, tuple)) else str(table_entry)
            else:
                table= "todoitem"
            print(f"{datetime.datetime.now()}: inserting into {table}: batch_number: {batch_number}/{number_of_batches}")
            cursor.executemany(
                f"insert into {table} (description, done, randomnumber, randomstring) values(:1, :2, :3, :4)",
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
                        cmd = f"""ALTER TABLESPACE {table}ts ADD DATAFILE 
                        '{datafile_dir}/{table}ts_{random_string}.dbf' 
                        SIZE {datafile_size}"""
                        cursor.execute(cmd)
                        print('tablespace successfully increased')
                        cursor.executemany(
                            f"insert into {table} (description, done, \
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
                                          batch_number, lock,
                                          tables, multi_table,
                                          number_of_batches=number_of_batches,
                                          random_flag=random_flag, rows=rows)

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
                            f"insert into {table} (description, done, \
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
                                          batch_number, lock,
                                          tables, multi_table,
                                          number_of_batches=number_of_batches,
                                          random_flag=random_flag, rows=rows)
                            return

    connection.commit()
    print(f'Committed batch number - :{batch_number}/{number_of_batches}')
    return


def pump_data(connection, db_name, total_size, datafile_size, batch_size,
              create_table=False, max_threads=128,
              dest_recovery_size='100G', random_flag=False, autoextend=False,
              multi_table=False):
    datafile_dir = get_datafile_dir(connection, db_name)
    total_rows_required = get_number_of_rows_from_file_size(total_size)
    number_of_batches = total_rows_required // batch_size
    workers = min(max_threads, number_of_batches)
    print(f'Number of workers - {workers}')

    lock = Lock()
    future_to_batch = {}

    # Create tables and tablespaces
    create_todo_item_table(connection, db_name, datafile_size,
                           dest_recovery_size, autoextend, create_table, multi_table)

    tables = list_all_todoitem_tables(connection, multi_table)

    # Submit batches to thread pool
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        for batch_number in range(1, number_of_batches + 1):
            args = (
                connection, datafile_dir, datafile_size, batch_size, batch_number,
                lock, tables, multi_table
            )
            future = executor.submit(process_batch, *args,
                                     number_of_batches=number_of_batches,
                                     random_flag=random_flag)
            future_to_batch[future] = batch_number

    result = []
    for future in concurrent.futures.as_completed(future_to_batch):
        batch_number = future_to_batch[future]
        try:
            res = future.result()
            if not res:
                result.append(batch_number)
        except Exception as exc:
            print(f"Batch {batch_number} failed: {exc}")
    return result
def pump_data_sequential(connection, db_name, total_size, datafile_size, batch_size,
              create_table=False,
              dest_recovery_size='100G', random_flag=False, autoextend=False,
              multi_table=False):
    datafile_dir = get_datafile_dir(connection, db_name)
    total_rows_required = get_number_of_rows_from_file_size(total_size)
    number_of_batches = total_rows_required // batch_size

    lock = Lock()
    future_to_batch = {}

    # Create tables and tablespaces
    create_todo_item_table(connection, db_name, datafile_size, dest_recovery_size, autoextend, create_table, multi_table)

    tables = list_all_todoitem_tables(connection, multi_table)
    for batch_number in range(1, number_of_batches + 1):
        process_batch(connection, datafile_dir, datafile_size, batch_size, batch_number, lock, tables, multi_table, number_of_batches=number_of_batches, random_flag=random_flag)

if __name__ == '__main__':
    connection = connect_to_oracle('sys', 'cohesity', '10.14.69.168',
                                   'orcl1')
    # for i in range(1,1001):
    #     print(f"deleting todoitem{i}")
    # delete_todoitem_table(connection, f'TODOITEM')
    pump_data(
        connection=connection,
        db_name='orcl1',
        total_size='500G',
        datafile_size='100M',
        batch_size=10000,
        create_table=False,
        max_threads=128,
        dest_recovery_size='1T',
        random_flag=False,
        autoextend=True,
        # multi_table=True
    )