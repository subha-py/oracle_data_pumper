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

