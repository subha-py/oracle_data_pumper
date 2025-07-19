#!/u02/oracle_data_pumper/venv/bin/python
# /etc/systemd/system/pumper-startup.service uses this file in pumper
from utils.log import set_logger
import os
import subprocess
from utils.cohesity import get_registered_sources
from utils.vmware import reboot_vms_by_ip_list
from utils.memory import check_available_space
from utils.db import get_db_map_from_vms
from utils.connection import filter_host_map_by_listener_connectivity
import logging
import concurrent.futures

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
    hosts = get_registered_sources(cluster_ip='10.14.7.1')
    # todo: remove rac from this list - should have rac in its name
    # todo: datapump in pdbs - should have cdb in its name
    # todo: bigtablespace autoextend -> the db name should have big in its name
    # todo: create a new report in html after each run
    # todo: ship logs to pluto
    future_to_batch = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(hosts)) as executor:
        for host in hosts:
            future = executor.submit(host.reboot_and_pump_data)
            future_to_batch[future] = host.ip
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



if __name__ == '__main__':
    startup_activities()
    