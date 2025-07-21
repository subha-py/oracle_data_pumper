import paramiko
import time
import os
from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
import ssl
import time
import subprocess
from pyVim.task import WaitForTasks
import logging
import os
from utils.db import DB

from utils.log import set_logger
from utils.vmware import get_all_vms, find_vm_by_ip, reboot_vm
from utils.memory import get_number_of_rows_from_file_size
from itertools import cycle

import concurrent.futures
class Host:
    def __init__(self, ip, vm_name=None, username='oracle', password='cohesity'):
        self.ip = ip
        self.vm_name = vm_name
        self.username = username
        self.password = password
        self.log = set_logger(self.ip, os.path.join('logs', 'hosts'))
        self.timeout = 10*60
        self.is_healthy = True
        self.pumpable_dbs = []
        self.num_workers = 10
        self.pump_size_in_gb = '100G'
        self.batch_size = 10000
        self.total_rows_required = get_number_of_rows_from_file_size(self.pump_size_in_gb)
        self.total_number_of_batches = self.total_rows_required // self.batch_size
        self.dbs = []
        self.total_dbs = 0
        self.curr_number_of_batch = 0
        self.scheduled_dbs = []
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
        self.is_healthy = False
    def reboot(self):
        if self.is_healthy:
            context = ssl.create_default_context()
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            vc = 'system-test-vc-fitdb.qa01.eng.cohesity.com'
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

            else:
                self.log.warning(f"VM with IP {self.ip} not found.")
                self.is_healthy = False

    def exec_cmds(self, commands, key=None, timeout=5 * 60):
        if not self.is_healthy:
            self.log.info(f"Host {self.ip} is marked unhealthy. Skipping execution.")
            return None, None

        MAX_RETRIES = 5
        RETRY_WAIT = 60  # seconds
        stdout_output, stderr_output = None, None

        for attempt in range(1, MAX_RETRIES + 1):
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.log.info(f"\n--- Attempt {attempt} - Connecting to {self.ip} ---")

            try:
                if key:
                    ssh_client.connect(hostname=self.ip, username=self.username, pkey=key, timeout=timeout)
                else:
                    ssh_client.connect(hostname=self.ip, username=self.username, password=self.password, timeout=timeout)

                self.log.info(f"Successfully connected to {self.ip}")

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
                        self.log.info(f"  WARNING: Command '{cmd}' failed on {self.ip}")
                        raise Exception(f"Command '{cmd}' failed with exit status {exit_status}")

                    time.sleep(0.5)

                return stdout_output, stderr_output  # Success: return on first full execution

            except paramiko.AuthenticationException:
                self.log.info(f"Authentication failed for {self.username}@{self.ip}. Please check credentials.")
                self.is_healthy = False
                break
            except (paramiko.SSHException, paramiko.BadHostKeyException) as ssh_err:
                self.log.info(f"SSH-related error on {self.ip}: {ssh_err}")
            except Exception as e:
                self.log.info(f"Execution failed on attempt {attempt} for {self.ip}: {e}")
            finally:
                if ssh_client.get_transport() and ssh_client.get_transport().is_active():
                    self.log.info(f"Closing connection to {self.ip}")
                    ssh_client.close()

            if attempt < MAX_RETRIES:
                self.log.info(f"Retrying in {RETRY_WAIT} seconds...")
                time.sleep(RETRY_WAIT)
            else:
                self.log.info(f"All retry attempts exhausted for {self.ip}. Giving up.")

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
        oracle_dbs = []
        sid = None
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
                    oracle_dbs.append(DB(sid, self))
                except Exception as e:
                    self.log.fatal(f'Cannot create db object for - db - {sid} due to {e}')
            return oracle_dbs

        except Exception as e:
            self.log.fatal(f"Failed to fetch Oracle DBs: {e}")
            self.is_healthy = False
            return []

    def is_space_available(self):
        mount_points = ['/u02', '/']
        result = self.get_disk_usage_multiple_in_gbs(mount_points)
        self.log.info(f'current space usage of {mount_points} -> {result}')
        if result['/u02'] < 300: #todo: take dynamically from self.pump_size_in_gb
            self.is_healthy = False
        elif result['/'] < 2:
            self.is_healthy = False
        return
    def prepare_pump_eligible_dbs(self):
        # hosts who have enough space
        self.is_space_available()
        if not self.is_healthy:
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
                args = (batch_number, self.total_number_of_batches)
                future = executor.submit(db.process_batch, *args)
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
    def load_dbs(self):
        self.dbs = self.get_oracle_dbs()
        self.total_dbs = len(self.dbs)
    def reboot_and_prepare(self):
        self.reboot()
        self.log.info('Sleeping 5 mins before querying dbs ')
        time.sleep(5 * 60)
        self.load_dbs()
        self.prepare_pump_eligible_dbs()
        self.set_pumper_tasks()

    def set_pumper_tasks(self):
        db_cycle = cycle(self.pumpable_dbs)
        for _ in range(1, self.total_number_of_batches + 1):
            db = next(db_cycle)
            if db.is_pumpable():
                self.scheduled_dbs.append(db)

    def __repr__(self):
        return self.ip


if __name__ == '__main__':
    host_obj = Host('10.14.70.149')

