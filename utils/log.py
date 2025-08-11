import logging
import os
import sys
import datetime
import paramiko
from scp import SCPClient
def create_log_dir():
    if not os.environ.get('log_dir'):
        script_dir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))
        script_dir = os.path.join(script_dir, 'logs')
        current_time = datetime.datetime.now().strftime("%d-%m-%y-%I-%p-%M-%S")
        folder_name = f"folder_{current_time}"
        log_dir = os.path.join(script_dir, folder_name)
        os.makedirs(log_dir, exist_ok=True)
        os.environ.setdefault('log_dir', log_dir)

def set_logger(log_file_name, dir=None):
    if not os.environ.get('log_dir'):
        create_log_dir()
    log_dir = os.environ.get('log_dir')
    if dir is not None:
        log_dir = os.path.join(log_dir, dir)
        os.makedirs(log_dir, exist_ok=True)
    log_filename = log_file_name + '.log'
    log_filepath = os.path.join(log_dir, log_filename)
    if os.path.exists(log_filepath):
        os.remove(log_filepath)
    logger = logging.getLogger(log_file_name)
    logger.setLevel(logging.INFO)
    file_handler = logging.FileHandler(log_filepath, mode='a')
    file_handler.setLevel(logging.INFO)  # Level for this handler
    format = '%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'
    formatter = logging.Formatter(format)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)  # Level for this handler
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger

def scp_to_remote(local_path, remote_host, remote_user, remote_path, password=None, port=22, key_file=None):
    try:
        ssh = paramiko.SSHClient()
        ssh.load_system_host_keys()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if key_file:
            ssh.connect(remote_host, port=port, username=remote_user, key_filename=key_file)
        else:
            ssh.connect(remote_host, port=port, username=remote_user, password=password)
        stdin, stdout, stderr = ssh.exec_command(f"mkdir -p {remote_path}")
        stdout.channel.recv_exit_status()  # Wait for command to finish
        with SCPClient(ssh.get_transport()) as scp:
            scp.put(local_path, remote_path, recursive=True)

        print(f"✅ File {local_path} copied to {remote_user}@{remote_host}:{remote_path}")

    except Exception as e:
        print(f"❌ Failed to SCP file: {e}")
    finally:
        ssh.close()


