import random
import sys
sys.path.append('/Users/subha.bera/PycharmProjects/oracle_data_pumper')
import paramiko
from pyVim.connect import SmartConnect, Disconnect
import ssl
import time
import subprocess
from utils.db import DB

from utils.log import set_logger
from utils.vmware import find_vm_by_ip, reboot_vm
from utils.memory import get_number_of_rows_from_file_size
from itertools import cycle

import concurrent.futures
class Host:
    def __init__(self, ip, vm_name=None, username='oracle', password='cohesity', root_username='root', root_password='root' ):
        self.ip = ip
        self.vm_name = vm_name
        self.username = username
        self.password = password
        self.root_username = root_username
        self.root_password = root_password
        self.log = set_logger(self.ip, 'hosts')
        self.timeout = 10*60
        self.is_healthy = True
        self.pumpable_dbs = []
        self.pump_size_in_gb = '100G'
        self.batch_size = 10000
        self.total_rows_required = get_number_of_rows_from_file_size(self.pump_size_in_gb)
        self.total_number_of_batches = self.total_rows_required // self.batch_size
        self.dbs = []
        self.curr_number_of_batch = 0
        self.failed_number_of_batch = 0
        self.scheduled_dbs = []
        self.services = ['oracle-database.service', 'oracle-listener.service']
        self.is_rac = False
        self.rac_nodes = []
    def ping(self):
        try:
            subprocess.check_output(['ping', '-c', '1', '-W', '1', self.ip], stderr=subprocess.DEVNULL)
            return True
        except subprocess.CalledProcessError:
            return False
    def wait_for_ping(self):
        start = time.time()
        while time.time() - start < self.timeout:
            if self.ping():
                self.log.info(f"{self.ip} is reachable.")
                return
            time.sleep(5)
        self.log.info('Marking host as unhealthy - it is not pingable')
        self.is_healthy = False
    def reboot(self):
        if self.is_healthy and not self.is_rac:
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            vc = 'system-test-vcfitdb.qa01.eng.cohesity.com'
            si = SmartConnect(host=vc, user='administrator@vsphere.local', pwd='Cohe$1ty', sslContext=context)
            content = si.RetrieveContent()
            vm = None
            MAX_RETRIES = 5
            RETRY_WAIT = 60  # seconds
            for attempt in range(1, MAX_RETRIES + 1):
                self.log.info(f"\n--- Attempt {attempt} - tyring to find vm {self.ip} in vc {vc}---")
                vm = find_vm_by_ip(content, self.ip)
                time.sleep(RETRY_WAIT)
                if vm is not None:
                    self.log.info(f"\n--- Attempt {attempt} - got vm {self.ip} in vc ---")
                    break
                self.log.info(f"\n--- Attempt {attempt} - failed to get vm {self.ip} in vc {vc}---")
            if vm:
                self.log.info(f"Found VM: {vm.name}")
                self.vm_name = vm.name
                reboot_vm(vm, si)
                self.wait_for_ping()
                Disconnect(si)
                self.log.info('Sleeping 15 mins before querying dbs ')
                time.sleep(15 * 60)

            else:
                self.log.warning(f"VM with IP {self.ip} not found.")
                self.log.info('Marking host as unhealthy - cannot find it in vc')
                self.is_healthy = False
        # todo: reboot rac db via srvctl command

    def exec_cmds(self, commands, username=None, password=None, key=None, timeout=60 * 60, MAX_RETRIES=5, RETRY_WAIT=60):
        if not self.is_healthy:
            self.log.info(f"Host {self.ip} is marked unhealthy. Skipping execution.")
            return None, None
        stdout_output, stderr_output = None, None
        if username is None:
            username = self.username
        if password is None:
            password = self.password
        if self.is_rac:
            ip = random.choice(self.rac_nodes)
        else:
            ip = self.ip
        for attempt in range(1, MAX_RETRIES + 1):
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.log.info(f"\n--- Attempt {attempt} - Connecting to {self.ip} ---")
            try:
                if key:
                    ssh_client.connect(hostname=ip, username=username, pkey=key, timeout=timeout)
                else:
                    ssh_client.connect(hostname=ip, username=username, password=password, timeout=timeout)

                self.log.info(f"Successfully connected to {ip}")

                for cmd in commands:
                    self.log.info(f"\n  Executing command: '{cmd}'")
                    stdin, stdout, stderr = ssh_client.exec_command(cmd, timeout=30)

                    stdout_output = stdout.read().decode().strip()
                    stderr_output = stderr.read().decode().strip()

                    if stdout_output:
                        self.log.info(f"  --- STDOUT ---\n{stdout_output}")
                    if stderr_output:
                        self.log.info(f"  --- STDERR ---\n{stderr_output}")

                    exit_status = stdout.channel.recv_exit_status()
                    self.log.info(f"  Command exited with status: {exit_status}")

                    if exit_status != 0:
                        self.log.info(f"  WARNING: Command '{cmd}' failed on {ip}")
                        raise Exception(f"Command '{cmd}' failed with exit status {exit_status}")

                    time.sleep(0.5)

                return stdout_output, stderr_output  # Success: return on first full execution

            except paramiko.AuthenticationException:
                self.log.info(f"Authentication failed for {username}@{ip}. Please check credentials.")
                self.is_healthy = False
                break
            except (paramiko.SSHException, paramiko.BadHostKeyException) as ssh_err:
                self.log.info(f"SSH-related error on {ip}: {ssh_err}")
            except Exception as e:
                self.log.info(f"Execution failed on attempt {attempt} for {ip}: {e}")
            finally:
                if ssh_client.get_transport() and ssh_client.get_transport().is_active():
                    self.log.info(f"Closing connection to {ip}")
                    ssh_client.close()

            if attempt < MAX_RETRIES:
                self.log.info(f"Retrying in {RETRY_WAIT} seconds...")
                time.sleep(RETRY_WAIT)
            else:
                self.log.info(f"All retry attempts exhausted for {ip}. Giving up.")

        return None, None  # Return None if all attempts failed
    def get_disk_usage_multiple_in_gbs(self, mount_points=None):
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

        results = {}
        if mount_points is None:
            mount_points = ['/u02', '/']
        try:
            # Run df command
            commands = ["df -h --output=avail,target"]
            stdout, stderr = self.exec_cmds(commands)
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

    def get_oracle_dbs(self, oratab_path='/etc/oratab'):
        if not self.is_healthy:
            return []
        if self.is_rac:
            self.get_rac_dbs()
            return
        oracle_dbs = []
        sid = None
        self.log.info('Trying to get dbs from hosts')
        try:
            command = f"grep -v '^#' {oratab_path} | grep -v '^$'"
            stdout, stderr = self.exec_cmds([command])

            if stderr:
                raise Exception(f"Error reading oratab: {stderr.strip()}")

            for line in stdout.strip().splitlines():
                parts = line.strip().split(":")
                if len(parts) >= 2:
                    sid = parts[0].strip().upper()
                try:
                    if 'multi' in sid: # not taking pdbs
                        continue
                    oracle_dbs.append(DB(sid, self))
                except Exception as e:
                    self.log.fatal(f'Cannot create db object for - db - {sid} due to {e}')
            self.dbs = oracle_dbs

        except Exception as e:
            self.log.fatal(f"Failed to fetch Oracle DBs: {e}")
            self.is_healthy = False

    def is_space_available(self):
        if not self.is_rac:
            mount_points = ['/u02', '/']
            result = self.get_disk_usage_multiple_in_gbs(mount_points)
            self.log.info(f'current space usage of {mount_points} -> {result}')
            u02_limit = 300
            root_limit = 2
            if result['/u02'] < u02_limit: #todo: take dynamically from self.pump_size_in_gb
                self.log.info(f"/u02 have low free memory(got {result['/u02']}), expected (> {u02_limit}) marking this host as unhealthy")
                self.is_healthy = False
            elif result['/'] < root_limit:
                self.log.info(f"/ have low free memory(got {result['/']}), expected (> {root_limit}) marking this host as unhealthy")
                self.is_healthy = False
            return
        # todo: for rac need to check size via asmcmd
    def prepare_pump_eligible_dbs(self):
        # hosts who have enough space
        self.is_space_available()
        self.get_oracle_dbs()
        if not self.is_healthy:
            self.log.info('host is not healthy, exiting')
            return
        for db in self.dbs:
            if db.is_pumpable():
                self.pumpable_dbs.append(db)

    def execute_pumper(self, executor):
        future_to_batch = {}
        db_cycle = cycle(self.pumpable_dbs)

        for batch_number in range(1, self.total_number_of_batches+1):
            db = next(db_cycle)
            if db.is_pumpable():
                future = executor.submit(db.process_batch)
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

    def is_service_running(self,service_name):
        command = f"systemctl is-active {service_name}"
        status, err = self.exec_cmds([command], username=self.root_username, password=self.root_password, MAX_RETRIES=1)
        if status == 'active':
            return True
        else:
            return False

    def set_service(self, service_name):
        if not self.is_service_running(service_name):
            commands = [f'wget https://sv4-pluto.eng.cohesity.com/bugs/sbera_backups/services/{service_name} -O /etc/systemd/system/{service_name}',
                'sudo systemctl daemon-reexec', 'sudo systemctl daemon-reload', f'sudo systemctl enable {service_name}',
                f'sudo systemctl start {service_name}',f'sudo systemctl status {service_name}']
            self.exec_cmds(commands, username=self.root_username, password=self.root_password, MAX_RETRIES=1)
    def prepare_services(self):
        if not self.is_rac:
            for service in self.services:
                self.set_service(service)
        # todo: for rac we can check the crs commands
    def change_oratab_entries(self):
        if not self.is_rac:
            oratab_commands = [r"sudo sed -i 's/^\([^#][^:]*:[^:]*:\)N/\1Y/' /etc/oratab"]
            self.exec_cmds(oratab_commands)
        # todo: need to find a way to db_start for rac
    def get_rac_dbs(self):
        cmd = "source ~/.bash_profile && srvctl config database"
        output = self.exec_cmds([cmd])[0]
        oracle_dbs = []
        if output:
            db_list = output.splitlines()
            for db in db_list:
                oracle_dbs.append(DB(db, self))
        self.dbs = oracle_dbs

    def reboot_and_prepare(self):
        self.prepare_services()
        self.change_oratab_entries()
        # self.reboot()

        self.prepare_pump_eligible_dbs()
        self.set_pumper_tasks()

    def set_pumper_tasks(self):
        if self.is_healthy:
            self.log.info(f'Got eligible dbs - {self.pumpable_dbs}')
            if self.pumpable_dbs:
                db_cycle = cycle(self.pumpable_dbs)
                for _ in range(1, self.total_number_of_batches + 1):
                    db = next(db_cycle)
                    self.scheduled_dbs.append(db)

    def __repr__(self):
        return self.ip


if __name__ == '__main__':
    host_obj = Host('10.131.37.249')
    host_obj.is_rac = True
    host_obj.rac_nodes = ['10.131.37.241', '10.131.37.242', '10.131.37.243']
    host_obj.reboot_and_prepare()