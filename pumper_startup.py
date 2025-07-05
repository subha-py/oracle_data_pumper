#!/u02/oracle_data_pumper/venv/bin/python
# /etc/systemd/system/pumper-startup.service uses this file in pumper
from utils.log import set_logger
import os
import subprocess
def pull_latest_code(logger, repo_path="."):
    """
    Pulls the latest code from the remote repository.

    Args:
        repo_path (str): The local path to the Git repository.
                         Defaults to the current directory if not specified.
    Returns:
        bool: True if pull was successful, False otherwise.
    """
    logger.info(f"Attempting to pull latest code in: {os.path.abspath(repo_path)}")

    # Check if the directory is actually a Git repository
    if not os.path.isdir(os.path.join(repo_path, '.git')):
        logger.info(f"Error: {repo_path} is not a Git repository.")
        return False

    try:
        # Use subprocess.run to execute the git pull command
        # cwd: Current Working Directory for the command
        # check=True: Raises CalledProcessError if the command returns a non-zero exit code
        # capture_output=True: Captures stdout and stderr
        # text=True: Decodes stdout/stderr as text (str)
        result = subprocess.run(
            ['git', 'pull', '-r'],
            cwd=repo_path,
        )

        logger.info("Git pull successful!")
        logger.info("STDOUT:\n", result.stdout)
        if result.stderr:
            logger.info("STDERR:\n", result.stderr)
        return True

    except subprocess.CalledProcessError as e:
        logger.info(f"Error during git pull:")
        logger.info(f"Command: {e.cmd}")
        logger.info(f"Return Code: {e.returncode}")
        logger.info(f"STDOUT:\n{e.stdout}")
        logger.info(f"STDERR:\n{e.stderr}")
        return False
    except FileNotFoundError:
        logger.info("Error: Git command not found. Make sure Git is installed and in your system's PATH.")
        return False
    except Exception as e:
        logger.info(f"An unexpected error occurred: {e}")
        return False
def startup_activities():
    logger = set_logger('pumper_startup_logger.log')
    pull_latest_code(logger)

if __name__ == '__main__':
    startup_activities()
    