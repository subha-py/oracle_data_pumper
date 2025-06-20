import paramiko
import os
import sys
import time
import logging

# --- Configuration ---
# output = '/home/oracle/master_16_05'
# installer = f'https://artifactory.eng.cohesity.com/artifactory/cohesity-builds-smoke/master/20250615-200911/qa_minimal/internal_only_rpms_package/cohesity_agent_0.0.0-master_linux_x64_installer -O {output}'
output = '/home/oracle/7.1.2_u4_installer'
installer = f'https://sv4-pluto.eng.cohesity.com/bugs/sbera_backups/cohesity_agent_7.1.2_u4_linux_x64_installer -O {output}'
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
    '10.14.69.170','10.14.69.171','10.14.69.172','10.3.63.224','10.14.69.121',
    '10.3.63.213','10.14.69.164','10.14.70.90','10.14.69.180','10.3.63.225',
    '10.3.63.226','10.3.63.227'
]
test_hosts = ['10.3.63.220']
HOSTS = test_hosts

USERNAME = 'root'  # SSH username

# --- IMPORTANT: CHOOSE ONE PASSWORD METHOD ---
# 1. Directly in script (NOT RECOMMENDED for production)
PASSWORD = 'root'

COMMANDS = [
    f'wget {installer}',
    f'chmod +x {output}',
    f'sudo {output} -- --full-uninstall -y',
    f'sudo {output} -- --install -S oracle -G oinstall -c 0 -I /home/oracle -y'
]

# --- SSH Client Setup ---
ssh_client = paramiko.SSHClient()
# This policy is insecure for production. For production, use AutoAddPolicy
# only after verifying the host key, or use SSHClient().load_system_host_keys().
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())  # Auto accept host keys


# --- Function to execute commands on a single host ---
# --- Configuration ---
# Get the directory where the script is located
script_dir = os.path.dirname(os.path.abspath(__file__))
log_filename = "my_script_output.log"
log_filepath = os.path.join(script_dir, log_filename)
os.remove(log_filepath)
# Configure the logger
# Create a logger object
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) # Set the minimum level for messages to be processed (e.g., INFO, DEBUG, WARNING, ERROR, CRITICAL)

# Create a file handler which logs messages to a file
# 'a' mode means append, so new runs append to the same file
file_handler = logging.FileHandler(log_filepath, mode='a')
file_handler.setLevel(logging.INFO) # Level for this handler

# Create a formatter and add it to the handler
# You can customize the log format.
# %(asctime)s: Timestamp
# %(levelname)s: Log level (INFO, DEBUG, etc.)
# %(message)s: The actual log message
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)

# Optional: Add a console handler to also logger.info to screen (like logger.info statements)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO) # Level for this handler
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)
def execute_commands_on_host(hostname, username, password, commands, key=None):
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
    for host in HOSTS:
        # Pass PASSWORD or KEY based on which method you choose above
        execute_commands_on_host(host, USERNAME, PASSWORD,
            COMMANDS)  # For password authentication  # execute_commands_on_host(host, USERNAME, None, COMMANDS, key=KEY) # For key authentication