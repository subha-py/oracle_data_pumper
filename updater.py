from pumper import connect_to_oracle
import string
import random
import sys
import datetime
import time
import concurrent.futures
import argparse

parser = argparse.ArgumentParser(
    description='A program to update a db in oracle',
    usage='python3 updater.py --host 10.14.69.121 --db_name prodsb21\
        --user sys --password cohesity --batch_size 200000 --max_threads 128')
parser._action_groups.pop()
required = parser.add_argument_group('required arguments')
optional = parser.add_argument_group('optional arguments')

required.add_argument('--host', help='ip/hostname of the db',
                      type=str, required=True)
required.add_argument('--db_name', help='name of database',
                      type=str, required=True)
optional.add_argument('--user',
                      help='username of db (default:sys)',
                      default='sys', type=str)
optional.add_argument('--password',
                      help='password of db (default:cohesity)',
                      default='cohesity', type=str)
optional.add_argument('--batch_size',
                      help='number of rows in each batch (default:200000)',
                      default=100000, type=int)
optional.add_argument('--max_threads',
                      help='number of threads (default:128)',
                      default=128, type=int)


def get_todoitem_tables(connection):
    tables = []
    with connection.cursor() as cursor:
        cursor.execute("SELECT table_name FROM all_tables order by table_name")
        tables = cursor.fetchall()
        for table in tables:
            if 'TODOITEM' in table[0]:
                tables.append(table[0])
    return tables


def get_row_count(connection):
    with connection.cursor() as cursor:
        cursor.execute("select count(*) from todoitem")
        row_num = cursor.fetchone()[0]
        return int(row_num)


def get_rows(connection, start_index=0, end_index=10):
    with connection.cursor() as cursor:
        cursor.execute(
            f"select * from (select q.*, rownum rn from (SELECT * \
            FROM todoitem ORDER BY id) q ) where rn BETWEEN \
            {start_index} AND {end_index}")
        for row in cursor:
            print(row)


def process_batch(connection, batch_size, batch_number, rows=None,
                  number_of_batches=None, start_index=0):
    if not rows:
        rows = []
    toggle = 0
    ascii_letters = list(string.ascii_letters)
    if len(rows) < 1:
        for i in range(start_index, start_index + batch_size):
            task_number = random.randint(1, sys.maxsize)
            random_string = ''.join(random.choices(ascii_letters, k=10))
            row = (toggle, task_number, random_string, i + 1)
            rows.append(row)
            toggle = int(not toggle)
    with connection.cursor() as cursor:
        print('{}: updating batch number - :{}/{}'.format(
            datetime.datetime.now(), batch_number, number_of_batches))
        sql_update = ("update todoitem set done = :1,randomnumber = :2,\
                      randomstring = :3  where id = :4")
        try:
            cursor.executemany(sql_update, rows, batcherrors=True)
        except Exception as ex:
            for error in cursor.getbatcherrors():
                print("{} :Error batch number - :{}/{}".format(
            datetime.datetime.now(), batch_number, number_of_batches),
                  error.message, "at row offset", error.offset)
            print(ex)
    print('{}: committing batch number - :{}/{}'.format(
        datetime.datetime.now(),batch_number,number_of_batches))
    connection.commit()


def pump_updates(connection, batch_size, max_threads=128):
    total_rows = get_row_count(connection)
    number_of_batches = total_rows // batch_size
    future_to_batch = {}
    workers = min(max_threads, number_of_batches)
    print('number of workers - {}'.format(workers))
    with concurrent.futures.ThreadPoolExecutor(
            max_workers=workers) as executor:
        for batch_number in range(1, number_of_batches + 1):
            arg = (
            connection, batch_size, batch_number, None, number_of_batches,
            batch_number * number_of_batches - 1)
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


if __name__ == '__main__':
    result = parser.parse_args()
    connection = connect_to_oracle(result.user, result.password, result.host,
                                   result.db_name.upper())
    pump_updates(connection, result.batch_size, result.max_threads)
