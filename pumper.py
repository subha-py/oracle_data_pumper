import random
import string
import sys
import oracledb
from oracledb.exceptions import DatabaseError
import os

user = 'sys'
password = 'cohesity'
host = '10.14.69.121'
db_name = 'prod'.upper()
total_size = '100M'
datafile_size = '20M'
batch_size = 100000

def human_read_to_byte(size):
    # if no space in between retry
    size_name = ("B", "K", "M", "G", "T", "P", "E", "Z", "Y")
    i = 0
    while i < len(size):
        if size[i].isnumeric():
            i += 1
        else:
            break
    size = size[:i], size[i:]              # divide '1 GB' into ['1', 'GB']
    num, unit = int(size[0]), size[1]
    idx = size_name.index(unit)        # index in list of sizes determines power to raise it to
    factor = 1024 ** idx               # ** is the "exponent" operator - you can use it instead of math.pow()
    return num * factor

def connect_to_oracle(user, password, host, db_name):
    connection = oracledb.connect(
        user=user,
        password=password,
        dsn=f"{host}:1521/{db_name}", mode=oracledb.AUTH_MODE_SYSDBA)

    print("Successfully connected to Oracle Database")

    return connection

def delete_todoitem_table(connection, tablename = 'todoitem'):
    with connection.cursor() as cursor:
        cursor.execute(f"""
            begin
                execute immediate 'drop table {tablename}';
                exception when others then if sqlcode <> -942 then raise; end if;
            end;""")
        try:
            cursor.execute(f"drop tablespace {tablename}ts INCLUDING CONTENTS AND DATAFILES")
        except DatabaseError as e:
            if "does not exist" in str(e):
                return
        print(f'deleted table - {tablename}')
def get_datafile_dir(connection, db_name):
    print('Fetching datafile location')
    with connection.cursor() as cursor:
        cursor.execute("select value from v$parameter where name = 'db_create_file_dest'")
        result = cursor.fetchone()[0]
    result = os.path.join(result, db_name, 'datafile')
    print(f'Got datafile location - {result}')
    return result
def create_tablespace(connection, db_name, datafile_size):
    tablespace_name = 'todoitemts'
    datafile_path = os.path.join(get_datafile_dir(connection, db_name), tablespace_name)
    cmd = f"create tablespace {tablespace_name} datafile '{datafile_path}.dbf' size {datafile_size}"
    print(f'creating tablespace with name - {tablespace_name}')
    with connection.cursor() as cursor:
        cursor.execute(cmd)
    print(f'tablespace created with name - {tablespace_name}')
    return tablespace_name

def create_todo_item_table(connection, db_name, datafile_size):
    delete_todoitem_table(connection)
    tablespace_name = create_tablespace(connection, db_name, datafile_size)
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

def pump_data(connection, db_name, total_size, datafile_size, batch_size):
    ascii_letters = list(string.ascii_letters)
    rows = []
    toggle = 0
    datafile_dir = get_datafile_dir(connection,db_name )
    curr_number_of_datafile = 1
    target_number_of_datafile = human_read_to_byte(total_size) // human_read_to_byte(datafile_size)
    total_rows = 0
    print('creating rows')
    while True:
        task_number = random.randint(1, sys.maxsize)
        random_string = ''.join(random.choices(ascii_letters, k=10))
        row = (f'Task:{total_rows + 1}', toggle, task_number, random_string)
        rows.append(row)
        total_rows += 1
        toggle = int(not toggle)
        if total_rows % batch_size == 0:
            print('inserting batch')
            with connection.cursor() as cursor:
                    try:
                        cursor.executemany(
                            "insert into todoitem (description, done, randomnumber, randomstring) values(:1, :2, :3, :4)", rows)
                    except DatabaseError as e:
                        if 'unable to extend' in str(e):
                            if curr_number_of_datafile >= target_number_of_datafile:
                                print(f'Total {curr_number_of_datafile} datafiles of size - {datafile_size} created, target met, exiting...')
                                break
                            print('failed to insert data due to lack of space in tablespace')
                            print(f'extending tablespace by {datafile_size}')
                            # increase tablespace size by adding a new datafile
                            cmd = f"""ALTER TABLESPACE todoitemts ADD DATAFILE '{datafile_dir}/todoitemts_{curr_number_of_datafile}.dbf' SIZE {datafile_size}"""
                            cursor.execute(cmd)
                            curr_number_of_datafile += 1
                            print('tablespace successfully increased')
                            cursor.executemany(
                                "insert into todoitem (description, done, randomnumber, randomstring) values(:1, :2, :3, :4)",
                                rows)
            connection.commit()
            print(f'insertion completed - total rows inserted - {total_rows}')
            # check error and extend
            rows = []
    print(f"Rows Inserted - {total_rows}")


if __name__ == '__main__':
    connection = connect_to_oracle(user,password,host,db_name)
    create_todo_item_table(connection, db_name, datafile_size)
    pump_data(connection, db_name, total_size, datafile_size, batch_size)
