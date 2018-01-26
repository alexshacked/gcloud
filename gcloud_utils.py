import subprocess
import re
import time
import datetime
from gcloud_api import NodeMgr
from gcloud_cluster_etchosts import show_entries_internal
import threading


class MRClusterAdmin:
    def __init__(self):
        self.printLock = threading.Lock()
        self.mgr = NodeMgr(self.printLock)

    '************************************ helpers ******************************************************'
    def doShell(self, cmd):
        print("doing - {0}".format(cmd))
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        (output, err) = p.communicate()
        status = p.wait()
        return output

    def shell_cmd(self, tag, user):
        return self.base_cmd(tag, user) + " -m shell -a "

    def shell_flush_cmd(self, tag, user):
        return self.base_cmd(tag, user) + " --flush-cache  -m shell -a "

    def fetch_cmd(self, tag, user):
        return self.base_cmd(tag, user) + " -m fetch -a "

    def base_cmd(self, tag, user):
        return "~/com.infolinks.ansible/bin/ansible.sh --limit=%s   --become-user=%s " % (tag, user)

    #############################################  utilities  #####################################################
def hd_workers_disk_size(admin):
    user = "root"
    cmd = "\"df -h /data \""
    full = admin.shell_cmd(admin.mgr.HDWORKER_TAG, user) + cmd
    out = admin.doShell(full)

    lines = out.split('\n')
    box = {}
    for i in range(len(lines)):
        if i == (len(lines) - 2):
            break
        this = lines[i]
        that = lines[i + 2]
        match = re.search(r'hd-worker[0-9]+', this)
        if match == None:
            continue
        match1 = re.search(r'UNREACH', this)
        if match1 != None:
            continue
        whole = match.group()
        number = re.search(r'[0-9]+$', whole)
        hostnum = int(number.group())
        match2 = re.search(r'[0-9]+G', that)
        if match2 == None:
            continue
        size = match2.group()
        box[hostnum] = size

    for k in sorted(box.keys()):
        print 'hd-worker%s:  %s' % (k, box[k])

def hd_workers_disk_used(admin):
    user = "root"
    cmd = "\"du -hs /data \""
    full = admin.shell_cmd(admin.mgr.HDWORKER_TAG, user) + cmd
    out = admin.doShell(full)

    lines = out.split('\n')
    box = {}
    for i in range(len(lines)):
        if i == (len(lines) - 1):
            break
        this = lines[i]
        that = lines[i + 1]
        match = re.search(r'hd-worker[0-9]+', this)
        if match == None:
            continue
        match1 = re.search(r'UNREACH', this)
        if match1 != None:
            continue
        whole = match.group()
        number = re.search(r'[0-9]+$', whole)
        hostnum = int(number.group())
        match2 = re.search(r'^[0-9]+[.]?[0-9]?[GMK]', that)
        if match2 == None:
            continue
        size = match2.group()
        box[hostnum] = size

    for k in sorted(box.keys()):
        print 'hd-worker%s:  %s' % (k, box[k])

def any_ansible_cmd(admin, cmd):
    user = "root"
    full = admin.shell_cmd(admin.mgr.HDWORKER_TAG, user) + cmd
    out = admin.doShell(full)
    print(out)


if __name__ == "__main__":
    admin = MRClusterAdmin()
    hd_workers_disk_used(admin)
    exit(0)

    cmd = "\" du -hs /data \""
    #cmd = "\" grep kanji /opt/hadoop/infolinks-lib/* \""
    any_ansible_cmd(admin, cmd)