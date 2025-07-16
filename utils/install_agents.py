import paramiko
import time
from utils.log import set_logger
# --- Configuration ---
output = '/home/oracle/master_16_05'
installer = f'https://artifactory.eng.cohesity.com/artifactory/cohesity-builds-smoke/master/20250615-200911/qa_minimal/internal_only_rpms_package/cohesity_agent_0.0.0-master_linux_x64_installer -O {output}'
# output = '/home/oracle/7.1.2_u4_installer'
# installer = f'https://sv4-pluto.eng.cohesity.com/bugs/sbera_backups/cohesity_agent_7.1.2_u4_linux_x64_installer -O {output}'
ST_05_HOSTS = [
    "10.14.69.210",
    "10.14.69.211", "10.3.63.147", "10.14.69.123", "10.14.69.212",
    "10.3.60.135", "10.14.70.98", "10.3.63.223", "10.14.69.165", "10.14.69.183",
    "10.3.63.223", "10.3.63.231", "10.3.63.232", "10.3.63.233", "10.3.63.141",
    "10.14.70.148", "10.14.70.135", "10.14.70.156", "10.14.70.147",
    "10.14.69.220", "10.14.69.221", "10.14.69.222"
]
ST_AUTO_HOSTS = [
    '10.14.69.161','10.3.63.138','10.14.70.158','10.14.70.133','10.14.70.157',
    '10.14.70.145','10.14.69.163','10.14.70.90','10.14.69.124','10.3.63.221',
    # '10.14.69.170','10.14.69.171','10.14.69.172', # -------> rac setups
    '10.3.63.224','10.14.69.121',
    '10.3.63.213','10.14.69.164','10.14.70.90','10.14.69.180','10.3.63.225',
    '10.3.63.226','10.3.63.227'
]

ST_MASTER_HOSTS = [
    '10.3.63.220', '10.14.69.215', '10.14.69.187', '10.14.69.216', '10.14.70.149',
    # '10.14.69.239', '10.14.69.240', '10.14.69.241', # ---> rac setups
    '10.3.63.185', '10.14.69.186',
    '10.14.69.139', '10.14.69.187', '10.14.69.186', '10.3.63.228', '10.3.63.229',
    '10.3.63.230', '10.3.63.139', '10.14.70.136', '10.14.70.134', '10.14.70.159',
    '10.14.70.146',
]
HOSTS = ST_AUTO_HOSTS + ST_05_HOSTS + ST_MASTER_HOSTS

USERNAME = 'root'  # SSH username

# --- IMPORTANT: CHOOSE ONE PASSWORD METHOD ---
# 1. Directly in script (NOT RECOMMENDED for production)
PASSWORD = 'root'

# COMMANDS = [
#     f'wget {installer}',
#     f'chmod +x {output}',
#     f'sudo {output} -- --full-uninstall -y',
#     f'sudo {output} -- --install -S oracle -G oinstall -c 0 -I /home/oracle -y'
# ]

COMMANDS=[
    'wget https://sv4-pluto.eng.cohesity.com/bugs/sbera_backups/services/oracle-listener.service -O /etc/systemd/system/oracle-listener.service',
    'wget https://sv4-pluto.eng.cohesity.com/bugs/sbera_backups/services/oracle-database.service -O /etc/systemd/system/oracle-database.service',
    'sudo systemctl daemon-reexec','sudo systemctl daemon-reload',
    'sudo systemctl enable oracle-listener.service', 'sudo systemctl start oracle-listener.service',
    'sudo systemctl enable oracle-database.service', 'sudo systemctl start oracle-database.service',
    'sudo systemctl status oracle-listener.service', 'sudo systemctl status oracle-database.service'
]
def execute_commands_on_host(hostname, username, password, commands, logger, ssh_client, key=None):
    logger.info(f"\n--- Connecting to {hostname} ---")
    try:
        if key:
            ssh_client.connect(hostname=hostname, username=username, pkey=key, timeout=10)
        else:
            ssh_client.connect(hostname=hostname, username=username, password=password, timeout=10)
        logger.info(f"Successfully connected to {hostname}")

        for cmd in commands:
            logger.info(f"\n  Executing command: '{cmd}'")
            stdin, stdout, stderr = ssh_client.exec_command(cmd, timeout=30)  # Add timeout for command execution

            # Read stdout and stderr to prevent hanging due to buffer limits
            stdout_output = stdout.read().decode().strip()
            stderr_output = stderr.read().decode().strip()

            if stdout_output:
                logger.info(f"  --- STDOUT ---\n{stdout_output}")
            if stderr_output:
                logger.info(f"  --- STDERR ---\n{stderr_output}")

            # Check exit status of the command
            exit_status = stdout.channel.recv_exit_status()
            logger.info(f"  Command exited with status: {exit_status}")
            if exit_status != 0:
                logger.info(f"  WARNING: Command '{cmd}' failed on {hostname}")
            time.sleep(0.5)  # Small delay between commands

    except paramiko.AuthenticationException:
        logger.info(f"Authentication failed for {username}@{hostname}. Please check your credentials.")
    except paramiko.SSHException as ssh_err:
        logger.info(f"SSH error connecting to {hostname}: {ssh_err}")
    except paramiko.BadHostKeyException as bhk_err:
        logger.info(f"Bad host key for {hostname}: {bhk_err}. Manual verification needed.")
    except Exception as e:
        logger.info(f"An unexpected error occurred while connecting or executing on {hostname}: {e}")
    finally:
        if ssh_client.get_transport() and ssh_client.get_transport().is_active():
            logger.info(f"Closing connection to {hostname}")
            ssh_client.close()


# --- Main execution ---
if __name__ == "__main__":
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    logger = set_logger('my_script_output.log')
    for host in HOSTS:
        # Pass PASSWORD or KEY based on which method you choose above
        execute_commands_on_host(host, USERNAME, PASSWORD,
            COMMANDS, logger, ssh_client)  # For password authentication  # execute_commands_on_host(host, USERNAME, None, COMMANDS, key=KEY) # For key authentication