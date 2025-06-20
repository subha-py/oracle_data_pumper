from oracledb.exceptions import DatabaseError
from utils.connection import connect_to_oracle
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
    idx = size_name.index(unit)
    # index in list of sizes determines power to raise it to
    factor = 1024 ** idx
    # ** is the "exponent" operator - you can use it instead of math.pow()
    return num * factor

def bytes_to_human_read(size_in_bytes):
    size_name = ("B", "K", "M", "G", "T", "P", "E", "Z", "Y")
    i = 0
    factor = 1024
    while size_in_bytes >= factor and i < len(size_name) - 1:
        size_in_bytes /= factor
        i += 1
    return f"{int(size_in_bytes)}{size_name[i]}"

def get_number_of_rows_from_file_size(size):
    # current todoitem schema having 11638091 rows amounts to 1G size
    return 11638091 * human_read_to_byte(size) // human_read_to_byte('1G')

def set_recovery_file_dest_size(connection, size):
    print('changing recovery file dest size')
    with connection.cursor() as cursor:
        try:
            cursor.execute(
                f"alter system set db_recovery_file_dest_size={size} scope=both")
        except DatabaseError as e:
            if 'ORA-65040' in str(e):
                print('Cannot set db_recovery_file_dest_size in pdbs')
            else:
                raise e
    print(f'db_recovery_file_dest_size changed to {size}')
    return

def get_databse_size(connection):
    print('querying database size')
    with connection.cursor() as cursor:
        try:
            cursor.execute(
                "SELECT SUM(bytes) / 1024 / 1024 / 1024 AS GB FROM dba_data_files")
        except DatabaseError as e:
            if 'ORA-01219' in str(e):
                cursor.execute(
                    'select total_size/1024/1024/1024 "PDB_SIZE_GB" from v$pdbs')
            else:
                raise e
        value = round(cursor.fetchone()[0])
    print(f'Total database size - {value}GB')
    return str(value) + 'G'
def get_number_of_rows(connection):
    print('querying table row count')
    with connection.cursor() as cursor:
        try:
            cursor.execute("SELECT COUNT(*) FROM todoitem")
        except DatabaseError as e:
            pass
        value = cursor.fetchone()[0]
    return value
if __name__ == '__main__':
    connect = connect_to_oracle('sys', 'cohesity', '10.14.24.28',
                                   'prod1',)
    result = get_databse_size(connect)
    print(f'size -> {result}')