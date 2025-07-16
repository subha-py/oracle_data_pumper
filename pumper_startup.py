#!/u02/oracle_data_pumper/venv/bin/python
# /etc/systemd/system/pumper-startup.service uses this file in pumper
from utils.log import set_logger
import os
import subprocess
from utils.cohesity import get_registered_sources
from utils.vmware import reboot_vms_by_ip_list
from utils.memory import check_available_space
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
    vms = reboot_vms_by_ip_list(sources)
    # listener start and db up is handled by services in os
    vms = check_available_space(vms)
    # write a script to post the services file to all the vms
    # write a script to change all the entries of /etc/oratab to Y instead of N

if __name__ == '__main__':
    startup_activities()
    