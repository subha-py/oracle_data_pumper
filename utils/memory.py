from oracledb.exceptions import DatabaseError
from utils.connection import connect_to_oracle
from utils.ssh import execute_commands_on_host
import logging
import os
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

def get_recovery_file_dest(connection):
    value = None
    print('changing recovery file dest size')
    with connection.cursor() as cursor:
        try:
            cursor.execute("SELECT name FROM V$RECOVERY_FILE_DEST")
        except DatabaseError as e:
            print('cannot get recovery file destinatoion')
            return
        value = cursor.fetchone()[0]
    print(f'recovery file dest size - {value}')
    return value

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

def parse_size_to_gb(size_str):
    """
    Converts a size string (e.g. '512M', '1.5G', '2T') to GB as integer.
    """
    size_str = size_str.strip().upper()
    if size_str.endswith('G'):
        return int(float(size_str[:-1]))
    elif size_str.endswith('M'):
        return int(float(size_str[:-1]) / 1024)
    elif size_str.endswith('T'):
        return int(float(size_str[:-1]) * 1024)
    elif size_str.endswith('K'):
        return int(float(size_str[:-1]) / (1024 * 1024))
    else:
        try:
            return int(float(size_str))  # assume GB if no unit
        except:
            return 0

def get_remote_disk_usage_multiple_in_gbs(ip, mount_points=None):
    results = {}
    if mount_points is None:
        mount_points = ['/u02', '/']
    try:
        # Run df command
        commands = ["df -h --output=avail,target"]
        stdout, stderr = execute_commands_on_host(ip, commands)
        df_output = stdout.splitlines()

        if len(df_output) < 2:
            raise Exception("No disk data received from remote host.")

        # Parse df output
        disk_data = {}
        for line in df_output[1:]:  # Skip header
            parts = line.strip().split()
            if len(parts) == 2:
                avail, target = parts
                disk_data[target] = parse_size_to_gb(avail)

        # Fill results
        for mount in mount_points:
            results[mount] = disk_data.get(mount, 0)

    except Exception as e:
        return {mp: -1 for mp in mount_points}

    return results

def check_available_space(ips):
    logger = logging.getLogger(os.environ.get("log_file_name"))
    good_vms = []
    bad_vms = []
    mount_points = ['/u02', '/']
    for ip in ips:
        check = True
        result = get_remote_disk_usage_multiple_in_gbs(ip, mount_points)
        logger.info(f'current space usaage of {mount_points} -> {result}')
        if result['/u02'] < 50:
            check = False
        elif result['/'] < 2:
            check = False
        if check is True:
            good_vms.append(ip)
        else:
            bad_vms.append(ip)
    logger.info(f'vms which failed the space check - {bad_vms}')
    logger.info(f'vms which passed the space check - {good_vms}')
    return good_vms



if __name__ == '__main__':
    result = get_remote_disk_usage_multiple_in_gbs('10.14.69.139')
    print(result)