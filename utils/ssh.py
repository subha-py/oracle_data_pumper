import paramiko
import time
import logging
import os
def execute_commands_on_host(hostname, commands, username='oracle', password='cohesity', key=None):
    ssh_client = paramiko.SSHClient()
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    logger = logging.getLogger(os.environ.get("log_file_name"))
    logger.info(f"\n--- Connecting to {hostname} ---")
    stdout_output, stderr_output = None, None
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
    return stdout_output, stderr_output