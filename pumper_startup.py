#!/u02/oracle_data_pumper/venv/bin/python
# /etc/systemd/system/pumper-startup.service uses this file in pumper
from utils.log import set_logger
import os
import subprocess
from utils.cohesity import get_registered_sources
from utils.vmware import reboot_vms_by_ip_list
import logging
def pull_latest_code(repo_path="."):
    logger = logging.getLogger(os.environ.get("log_file_name"))
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
    set_logger('pumper_startup_logger')
    pull_latest_code()
    sources = get_registered_sources(cluster_ip='10.14.7.1')
    # todo:
    reboot_vms_by_ip_list(sources)
    # todo:check listener status
    # todo:check space in u02 and / dir
    # todo:start db
    # todo:check connectivity via listener
    # todo:start data pumping

if __name__ == '__main__':
    startup_activities()
    