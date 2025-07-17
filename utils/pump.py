import random

from utils.memory import human_read_to_byte, get_databse_size
from utils.connection import connect_to_oracle
from updater import pump_updates_sequential
from pumper import pump_data_sequential
from utils.memory import get_number_of_rows_from_file_size
import concurrent.futures
from itertools import cycle




def pump(host, db):
    # run prechecks
    # if multi table is created
    # if not created, create them
    #
    connection = connect_to_oracle(host, db)
    buffer = '10G'
    limit = '1024G'
    batch_size = 100000
    update_percentage = 10
    current_db_size = get_databse_size(connection)
    total_size = '100G'
    datafile_size = '2G'
    max_threads = 2
    if human_read_to_byte(current_db_size) + human_read_to_byte(buffer) >= human_read_to_byte(limit):
        print(f'''Conditions didnt met for new data addition, will do updates 
                      on {update_percentage}% of database - {host}:{db}, 
                      current size - {current_db_size}''')
        pump_updates_sequential(connection, batch_size, percentage=update_percentage)
    else:
        print(f'All conditions met - going to pump new data worth - '
              f'{total_size}, with datafile_size '
              f'{datafile_size}')
        pump_data_sequential(connection, db, total_size, datafile_size, batch_size, random_flag=True, multi_table=True)
def pump_data_from_hostmap(hostmap):
    batch_size = 10000
    total_number_of_dbs = 0
    workers = 128
    for host, dbs in hostmap.items():
        for _ in dbs:
            total_number_of_dbs+=1
    total_size = total_number_of_dbs*100
    total_size = f'{total_size}G'
    total_rows_required = get_number_of_rows_from_file_size(total_size)
    total_number_of_batches = total_rows_required // batch_size
    host_cycle = cycle(hostmap.keys())
    hostmap_cycle = {}
    for host, dbs in hostmap.items():
        hostmap_cycle[host] = cycle(dbs)
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        for batch_number in range(1, total_number_of_batches+1):
            next_host = next(host_cycle)
            next_db = next(hostmap_cycle[next_host])


    # for each host and db spawn the workflow
    # each workflow will check the limit of db and current size
    # if current size + buffer > limit update only
    # elif insert new rows