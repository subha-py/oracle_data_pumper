#!/u02/oracle_data_pumper/venv/bin/python
# /etc/systemd/system/pumper-startup.service uses this file in pumper
from utils.log import set_logger
import os
import subprocess
def pull_latest_code(logger, repo_path="."):
    logger.info(f"Attempting to pull latest code in: {os.path.abspath(repo_path)}")
    if not os.path.isdir(os.path.join(repo_path, '.git')):
        logger.info(f"Error: {repo_path} is not a Git repository.")
        return False
    result = subprocess.run(
        ['git', 'pull', '-r'],
        cwd=repo_path,
    )
    logger.info("Git pull successful!")
    return result
def startup_activities():
    logger = set_logger('pumper_startup_logger.log')
    pull_latest_code(logger)

if __name__ == '__main__':
    startup_activities()
    