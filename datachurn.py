import argparse
import sys
from utils.bct import enable_bct
from utils.check_pdb import check_pdb_status
from utils.connection import connect_to_oracle
from pumper import pump_data
from utils.memory import human_read_to_byte, get_databse_size
from updater import pump_updates

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='A program to populate a db in oracle',
        usage='python3 pumper.py --host 10.14.69.121 --db_name prodsb21\
                    --user sys --password cohesity --total_size 1G \
                    --datafile_size 200M --batch_size 200000 ')
    parser._action_groups.pop()
    required = parser.add_argument_group('required arguments')
    optional = parser.add_argument_group('optional arguments')

    required.add_argument('--host', help='ip/hostname of the db',
                          type=str, required=True)
    required.add_argument('--db_name', help='name of database',
                          type=str, required=True)
    optional.add_argument('--limit',
                          help='''upper limit of database size, after that
                                   database will update old rows for data churn
                                   instead of creating new rows''',
                          type=str, default='500G')
    optional.add_argument('--user',
                          help='username of db (default:sys)', default='sys',
                          type=str)
    optional.add_argument('--password',
                          help='password of db (default:cohesity)',
                          default='cohesity', type=str)
    optional.add_argument('--total_size',
                          help='total size to be pumped (default:1G)',
                          default='25G', type=str)
    optional.add_argument('--datafile_size',
                          help='size of datafile (default:50G)',
                          default='1G',
                          type=str)
    optional.add_argument('--batch_size',
                          help='number of rows in each batch (default:200000)',
                          default=100000, type=int)
    optional.add_argument('--threads',
                          help='number of threads (default:128)',
                          default=32, type=int)
    optional.add_argument('--dest_recovery_size',
                          help='dest_recovery_size (default: 2T)',
                          default='2T', type=str)
    optional.add_argument('--percentage',
                          help='percentage of db to be updated in case of '
                               'updater',
                          default=10, type=int)
    optional.add_argument('--check_pdb',
                          help='PDB name to check for READ WRITE status',
                          type=str)
    optional.add_argument('--expected_value', type=str, help='Expected value for PDB status (e.g., RW)')
    parser.add_argument('--connect_only', nargs='?', default=False, const=True)
    parser.add_argument('--enable_bct', nargs='?', default=False, const=True)
    parser.add_argument('--random_datafile_size', nargs='?', default=False,
                    const=True, help='Enable randomization of datafile size')
    parser.add_argument('--create_table', nargs='?', default=False, const=True)

    result = parser.parse_args()
    connection = connect_to_oracle(result.user, result.password, result.host,
                                   result.db_name.upper())
    if result.connect_only:
        sys.exit(0)
    if result.enable_bct:
        enable_bct(connection)
        sys.exit(0)
    if result.check_pdb:
        status = check_pdb_status(connection, result.check_pdb)
        print(f"PDB status is: {status}")
        if result.expected_value:
            if status.strip().upper() != result.expected_value.strip().upper():
                print(f"Expected status '{result.expected_value}' does not match actual status '{status}'")
                sys.exit(1)
        sys.exit(0)
    # if a database limit is more than 100G then buffer will be 10G else 2G
    if human_read_to_byte(result.limit) > human_read_to_byte('100G'):
        buffer = '10G'
    else:
        buffer = '2G'
    current_db_size = get_databse_size(connection)
    if (human_read_to_byte(current_db_size) + human_read_to_byte(buffer) >=
            human_read_to_byte(result.limit)):
        # limit is very close only update data at this point
        print(f'''Conditions didnt met for new data addition, will do updates 
              on {result.percentage}% of database - {result.db_name}, 
              current size - {current_db_size}''')
        pump_updates(connection, result.batch_size, result.threads,
                     result.percentage)
    else:
        print(f'All conditions met - going to pump new data worth - '
              f'{result.total_size}, with datafile_size '
              f'{result.datafile_size}')

        pump_data(connection, result.db_name.upper(), result.total_size,
                  result.datafile_size, result.batch_size,
                  max_threads=result.threads, create_table=result.create_table,
                  dest_recovery_size=result.dest_recovery_size,
                  random_flag=result.random_datafile_size)
