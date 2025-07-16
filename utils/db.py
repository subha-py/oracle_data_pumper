from utils.ssh import execute_commands_on_host
from collections import defaultdict
def get_remote_oracle_dbs(host, oratab_path='/etc/oratab'):
    oracle_sids = []
    try:

        command = f"grep -v '^#' {oratab_path} | grep -v '^$'"
        stdout, stderr = execute_commands_on_host(host,[command])

        if stderr:
            raise Exception(f"Error reading oratab: {stderr.strip()}")

        for line in stdout.strip().splitlines():
            parts = line.strip().split(":")
            if len(parts) >= 2:
                sid = parts[0].strip()
                oracle_sids.append(sid)
        return oracle_sids

    except Exception as e:
        print(f"Failed to fetch Oracle DBs: {e}")
        return []
def get_db_map_from_vms(ips):
    result = defaultdict(list)
    for ip in ips:
        result[ip] = get_remote_oracle_dbs(ip)
    return result
if __name__ == '__main__':
    dbs = get_remote_oracle_dbs('10.14.69.139')
    print(dbs)