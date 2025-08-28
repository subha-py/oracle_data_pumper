import requests
import random
import os
import logging
from utils.hosts import Host
import time
def get_node_ips(cluster_ip, username="admin", password="Syst7mt7st", domain="local", access_token=None):
    headers = {'Content-Type': "application/json", 'accept': "application/json"}
    if not access_token:
        access_token = os.environ.get("accessToken")
        if not access_token and not get_access_token(cluster_ip, username=username, password=password,
                                                     domain=domain):
            raise EnvironmentError("Please provide access token")
    headers['Authorization'] = "bearer {}".format(os.environ.get('accessToken'))
    response = requests.request("GET", "https://{}/v2/clusters".format(cluster_ip), verify=False,
                                headers=headers)
    if response.status_code == 200:
        response_data = response.json()
        nope_ip_string = response_data['nodeIps']
        node_ips = nope_ip_string.split(",")
        random.shuffle(node_ips)
        node_ip_string = ','.join(node_ips)
        os.environ.setdefault("node_ips", node_ip_string)
        return node_ips

    else:
        print("could not get node - ips")
        return None
def get_access_token(cluster_ip, username="admin", password="Syst7mt7st", domain="local"):
    headers = {'Content-Type': "application/json", 'accept': "application/json"}
    data = {
        "password": password,
        "username": username
    }
    response = requests.request("POST", "https://{}/v2/access-tokens".format(cluster_ip),
                                verify=False, headers=headers, json=data)
    if response.status_code == 201:
        response_data = response.json()
        os.environ.setdefault("accessToken", response_data['accessToken'])
        return response_data['accessToken']
    else:
        print("could not get accesstoken")
        return None

def get_access_keys(cluster_ip, username="admin", password="Syst7mt7st", domain="local", access_token=None):
    headers = {'Content-Type': "application/json", 'accept': "application/json"}
    if not access_token:
        access_token = os.environ.get("accessToken")
        if not access_token and not get_access_token(cluster_ip, username=username, password=password,
                                                     domain=domain):
            raise EnvironmentError("Please provide access token")
    headers['Authorization'] = "bearer {}".format(os.environ.get('accessToken'))
    response = requests.request("GET",
                                "https://{}/irisservices/api/v1/public/users?domain={}".format(cluster_ip, domain),
                                verify=False, headers=headers)
    if response.status_code == 200:
        response_data = response.json()[0]
        os.environ.setdefault("s3AccessKeyId", response_data['s3AccessKeyId'])
        os.environ.setdefault("s3SecretKey", response_data['s3SecretKey'])
        return response_data
def get_headers():
    headers = {'Content-Type': "application/json", 'accept': "application/json"}
    if os.environ.get('accessToken'):
        headers['Authorization'] = "bearer {}".format(os.environ.get('accessToken'))
    return headers
def setup_cluster_automation_variables_in_environment(cluster_ip, username="admin", password="Syst7mt7st",
                                                      domain="local"):
    get_access_token(cluster_ip,username,password,domain)
    get_access_keys(cluster_ip, domain)
    get_node_ips(cluster_ip)

def get_registered_sources(cluster_ip,source_type='oracle'):
    setup_cluster_automation_variables_in_environment(cluster_ip)
    ips = os.environ.get("node_ips").split(",")
    ip = random.choice(ips)
    response = requests.request("GET", "https://{}//irisservices/api/v1/public/protectionSources/registrationInfo".format(ip), verify=False, headers=get_headers())
    sources = response.json().get('rootNodes')
    result = []
    for source in sources:
        registrationInfo=source['registrationInfo']
        env = registrationInfo.get('environments')
        if env is not None and source_type in env[0].lower() and 'linux' in source['rootNode']['physicalProtectionSource']['osName'].lower():
            if 'rac' in source['rootNode']['physicalProtectionSource']['type'].lower():
                continue
            host_obj = Host(ip=registrationInfo['accessInfo']['endpoint'])
            if 'rac' in source['rootNode']['physicalProtectionSource']['type'].lower():
                host_obj.is_rac = True
                host_obj.log.info('This is a rac setup!')
                for agent in source['rootNode']['physicalProtectionSource']['agents']:
                    host_obj.rac_nodes.append(agent['name'])
                host_obj.log.info(f'rac nodes in this setup - {host_obj.rac_nodes}')
            result.append(host_obj)
    print(f'cluster ip - {cluster_ip}\n \noracle sources - {result}')
    return result

def get_cluster_name(ip):
    setup_cluster_automation_variables_in_environment(ip)
    ips = os.environ.get("node_ips").split(",")
    headers = get_headers()  # Assuming this function is already defined
    name = None
    for attempt in range(5):
        cluster_ip = random.choice(ips)
        try:
            response = requests.get(f"https://{cluster_ip}/v2/clusters?fetchMetadataInfo=true", headers=headers, verify=False, timeout=5)
            if response.ok:
                name = response.json().get('name')
                if name:
                    return name
        except Exception as e:
            print(f"Attempt {attempt + 1}: Error contacting {cluster_ip} - {e}")

        print(f"Attempt {attempt + 1} failed. Retrying...")
        time.sleep(1.5 * (attempt + 1))  # Optional exponential backoff
    if name is None:
        return ip

if __name__ == '__main__':
    hosts = get_registered_sources(cluster_ip='10.14.7.1')