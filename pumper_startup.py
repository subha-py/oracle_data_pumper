#!/root/oracle_data_pumper/venv/bin/python
# /etc/systemd/system/pumper-startup.service uses this file in pumper
import random
import sys
from utils.log import scp_to_remote
from utils.reports import create_report
import os
import subprocess
from utils.cohesity import get_registered_sources, get_cluster_name
import logging
import concurrent.futures
import argparse
from utils.hosts import Host
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
def dump_logs_to_pluto(cluster_ip, logdir=None):
    cluster_name = get_cluster_name(cluster_ip)
    if logdir is None:
        logdir = os.environ.get('log_dir')
    scp_to_remote(local_path=logdir, remote_host="10.130.3.10",
        remote_user="cohesity", remote_path=f"/home/cohesity/data/bugs/sbera_backups/oracle_pumper_dumps/{cluster_name}", password="fr8shst8rt"
    )

def startup_activities(cluster_ip):
    # todo revert me
    # hosts = get_registered_sources(cluster_ip=cluster_ip)
    # todo: remove rac from this list - should have rac in its name
    # todo: datapump in pdbs - should have cdb in its name
    # todo: bigtablespace autoextend -> the db name should have big in its name
    # todo: add install agent if required
    # todo: mark unhealthy if oradata is missing
    # todo: do this for windows machine
    hosts = [Host('10.14.27.120')]
    future_to_hosts = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(hosts)) as executor:
        for host in hosts:
                future = executor.submit(host.reboot_and_prepare)
                future_to_hosts[future] = host
        result = []
        for future in concurrent.futures.as_completed(future_to_hosts):
            host = future_to_hosts[future]
            try:
                res = future.result()
                if not res:
                    result.append(host)
            except Exception as exc:
                print(f"Batch {host} failed: {exc}")
    # at this point all pumpable dbs are prepared
    all_scheduled_dbs = []
    for host in hosts:
        all_scheduled_dbs.extend(host.scheduled_dbs)
    random.shuffle(all_scheduled_dbs)
    future_to_dbs = {}
    result = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
        for db in all_scheduled_dbs:
                future = executor.submit(db.process_batch)
                future_to_dbs[future] = str(db)
        for future in concurrent.futures.as_completed(future_to_dbs):
            db = future_to_dbs[future]
            try:
                res = future.result()
                if not res:
                    result.append(db)
            except Exception as exc:
                print(f"Batch {db} failed: {exc}")

    # create_report(hosts, cluster_ip)
    # dump_logs_to_pluto(cluster_ip)
    return result



if __name__ == '__main__':
    # todo revert me
    # parser = argparse.ArgumentParser(description='Program to pump data in oracle sources registered in cluster')
    # parser._action_groups.pop()
    # required = parser.add_argument_group('required arguments')
    # optional = parser.add_argument_group('optional arguments')
    #
    # required.add_argument('--clusterip', help='ip/hostname of the db', type=str, required=True)
    # result = parser.parse_args()
    # startup_activities(result.clusterip)
    startup_activities('123')