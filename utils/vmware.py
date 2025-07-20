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
