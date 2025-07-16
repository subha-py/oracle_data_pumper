from pyVim.connect import SmartConnect, Disconnect
from pyVmomi import vim
import ssl
import time
import subprocess
from pyVim.task import WaitForTasks
import logging
import os
def get_all_vms(content):
    obj_view = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
    vms = obj_view.view
    obj_view.Destroy()
    return vms

def find_vm_by_ip(vms, ip):
    for vm in vms:
        try:
            for net in vm.guest.net:
                for addr in net.ipAddress:
                    if addr == ip:
                        return vm
        except:
            continue
    return None

def reboot_vm(vm, si):
    logger = logging.getLogger(os.environ.get("log_file_name"))
    task = None
    try:
        if vm.runtime.powerState != vim.VirtualMachinePowerState.poweredOn:
            logger.info("VM is not powered on.")
            return
        if vm.guest.toolsStatus in [vim.vm.GuestInfo.ToolsStatus.toolsOk,
                                    vim.vm.GuestInfo.ToolsStatus.toolsOld]:
            logger.info(f"Rebooting guest OS for VM: {vm.name}")
            task = vm.RebootGuest()
        else:
            logger.info("VMware Tools not running. Doing hard reset.")
            task = vm.ResetVM_Task()
    except Exception as e:
        logger.info(f"Failed to reboot VM: {e}")
    WaitForTasks(task, si=si)
    logger.info('sleeping for 120 secs after reboot')
    time.sleep(120)


def ping_ip(ip):
    try:
        subprocess.check_output(['ping', '-c', '1', '-W', '1', ip], stderr=subprocess.DEVNULL)
        return True
    except subprocess.CalledProcessError:
        return False

def wait_for_ping(ip, timeout):
    logger = logging.getLogger(os.environ.get("log_file_name"))
    logger.info(f"Pinging {ip} to check availability...")
    start = time.time()
    while time.time() - start < timeout:
        if ping_ip(ip):
            logger.info(f"{ip} is reachable.")
            return True
        time.sleep(5)
    raise TimeoutError(f"Timed out waiting for {ip} to respond to ping.")

def reboot_vm_by_ip(vm_ip):
    logger = logging.getLogger(os.environ.get("log_file_name"))
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    si = SmartConnect(host='system-test-vc-fitdb.qa01.eng.cohesity.com', user='administrator@vsphere.local', pwd='Cohe$1ty', sslContext=context)
    content = si.RetrieveContent()
    vms = get_all_vms(content)
    vm = find_vm_by_ip(vms, vm_ip)
    if vm:
        logger.info(f"Found VM: {vm.name}")
        reboot_vm(vm, si)
        wait_for_ping(vm_ip, 10 * 60)
        Disconnect(si)
        return True
    else:
        logger.info(f"VM with IP {vm_ip} not found.")
        return False


def reboot_vms_by_ip_list(ip_list):
    logger = logging.getLogger(os.environ.get("log_file_name"))
    faulty_vm = []
    good_vms = []
    for ip in ip_list:
        result = reboot_vm_by_ip(ip)
        if not result:
            faulty_vm.append(ip)
        else:
            good_vms.append(ip)
    logger.info(f'Could not find vms with ip - {faulty_vm}\n')
    logger.info(f'Vms rebooted successfully - {good_vms}')
    return good_vms
if __name__ == '__main__':
    reboot_vms_by_ip_list(['10.131.37.91',])
