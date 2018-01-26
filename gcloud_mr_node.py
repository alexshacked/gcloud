import subprocess
import time
import datetime
from gcloud_api import NodeMgr
from gcloud_cluster_etchosts import show_entries_internal
import requests
import threading


class MRClusterAdmin:
    def __init__(self):
        self.printMutex = threading.Lock()
        self.mgr = NodeMgr(self.printMutex)
        self.in_the_cloud = self.is_this_host_in_cloud()

    '************************************ helpers ******************************************************'
    def is_this_host_in_cloud(self):
        'tests if the host on which this script is running, is located on a host in google cloud or on a private machine like my laptop'

        METADATA_URL = 'http://metadata.google.internal/computeMetadata/v1/'
        METADATA_HEADERS = {'Metadata-Flavor': 'Google'}
        url = METADATA_URL + 'instance/hostname'

        try:
            r = requests.get(
                url,
                params={'last_etag': '0', 'wait_for_change': True},
                headers=METADATA_HEADERS)
        except Exception as e:
            with self.printMutex:
                print('Script running on local machine, outside of google cloud')
            return False

        res = True if r.status_code == 200 else False
        if res:
            with self.printMutex:
                print('Script running in google cloud, status code: %d' % r.status_code)
        else:
            with self.printMutex:
                print("Script runninng on local machine outside of google cloud. error code: %d" % r.status_code)
        return res

    def get_accessible_ip(self, node):
        if self.in_the_cloud:
            ip = self.mgr.get_node_internal_ip(node)
        else:
            ip = self.mgr.get_node_public_ip(node)
        return ip



    def stop_hadoop_on_remove_node(self, node, flush_ansible_cache = True):
        ip = self.get_accessible_ip(node)
        # 1. stop tasktracker
        cmd = " ssh -o StrictHostKeyChecking=no -o ConnectTimeout=3  hadoop@%s \"/data/hadoop/bin/hadoop-daemon.sh stop tasktracker\" " % ip
        self.doShell(cmd)

    def remove_node_from_etc_hosts(self, node):
        cmd_hd_master  = " ssh  -o StrictHostKeyChecking=no  -o ConnectTimeout=3 root@hd-master \" sed \"/{0}/d\" /etc/hosts > /etc/hosts_new; mv /etc/hosts_new /etc/hosts \"".format(node)
        cmd_hd_redmap  = " ssh  -o StrictHostKeyChecking=no -o ConnectTimeout=3 root@hd-redmap \" sed \"/{0}/d\" /etc/hosts > /etc/hosts_new; mv /etc/hosts_new /etc/hosts \"".format(node)
        self.doShell(cmd_hd_master)
        self.doShell(cmd_hd_redmap)

    def remove_node_from_slaves(self, node):
        cmd = " ssh  -o StrictHostKeyChecking=no -o ConnectTimeout=3 hadoop@hd-master \" sed \"/{0}/d\" /opt/hadoop/conf/slaves > /opt/hadoop/conf/slaves_new;  \
            mv /opt/hadoop/conf/slaves_new /opt/hadoop/conf/slaves \"".format(node)
        self.doShell(cmd)
        with self.printMutex:
            print("removed %s from /opt/hadoop/conf/slaves on tag hdmaster" % (node))


    def doShell(self, cmd):
        with self.printMutex:
            print("{0}:   doing - {1}".format(str(datetime.datetime.now()), cmd))
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        (output, err) = p.communicate()
        status = p.wait()
        return output

    def update_etc_hosts_of_cluster(self, node):
        ip = self.mgr.get_node_internal_ip(node)
        cmd = "\"echo \'%-15s   %-15s   %s\' >> /etc/hosts " % (ip, node, node + '.infolinks.com')

        cmd_slaves = "echo \'%s\' >> /opt/hadoop/conf/slaves \"" % (node + '.infolinks.com')
        cmd_master = ';'.join([cmd, cmd_slaves])
        full = 'ssh -o StrictHostKeyChecking=no -o ConnectTimeout=3 root@hd-master ' + cmd_master # first time to ansible must flush_cache
        self.doShell(full)
        with self.printMutex:
            print("updated /etc/hosts on tag hdmaster")

        cmd = cmd + "\""

        full = 'ssh -o StrictHostKeyChecking=no -o ConnectTimeout=3 root@hd-redmap ' + cmd
        self.doShell(full)
        with self.printMutex:
            print("updated /etc/hosts on tag hdredmap")

    def do_update_etc_hosts_of_new_node(self, node):
        ip = self.get_accessible_ip(node)

        '1. take care of all other cluster players: hd-workerXX, hd-master, hbase-workerXX, hbase-master'
        self.fix_etchosts(node, ip)

    def update_etc_hosts_of_new_node(self, node):
        attempt = 0
        while attempt < 3:
            try:
                self.do_update_etc_hosts_of_new_node(node)
                break
            except:
                attempt = attempt + 1
                with self.printMutex:
                    print('%s could not write to its /etc/hosts. attempt %d' % (node, attempt))
                    time.sleep(3)



    def start_hadoop_on_new_node(self, node):
        ip = self.get_accessible_ip(node)
        cmd = "\"/data/hadoop/bin/hadoop-daemon.sh start tasktracker > start_tasktracker.log \""
        full = ('ssh -o StrictHostKeyChecking=no -o ConnectTimeout=3 hadoop@%s  ' % ip) + cmd
        self.doShell(full)
        with self.printMutex:
            print("started tasktracker")

    def shell_cmd(self, tag, user):
        return self.base_cmd(tag, user) + " -m shell -a "

    def shell_flush_cmd(self, tag, user):
        return self.base_cmd(tag, user) + " --flush-cache  -m shell -a "

    def fetch_cmd(self, tag, user):
        return self.base_cmd(tag, user) + " -m fetch -a "

    def base_cmd(self, tag, user):
        return "~/com.infolinks.ansible/bin/ansible.sh --limit=%s   --become-user=%s " % (tag, user)

    #############################################  interface #####################################################
    def new_node(self, node, data_disk_image='hd-worker-image-data', preemptive=True):
        '''
        2017-12-11  December 11
        data_disk_image is not used any more because we moved to calling add_node_from_script instead of
        add_node_from_image. But, we are still in pilot mode so we keep the parameter in case we need fast fallback.
        '''
        self.mgr.add_node_from_script(node, preemptive)

        # self.update_etc_hosts_of_cluster(node)
        self.update_etc_hosts_of_new_node(node)

        self.start_hadoop_on_new_node(node)
        self.mgr.set_node_tag(node, self.mgr.HDWORKER_TAG)

        self.mgr.show_node(node)

    def remove_node(self, node, flush_ansible_cache = True):
        self.stop_hadoop_on_remove_node(node, flush_ansible_cache = flush_ansible_cache)

        self.remove_node_data_from_cluster(node)

        self.mgr.remove_node(node) # remove the remove_node

    def remove_node_data_from_cluster(self, node):
        self.remove_node_from_etc_hosts(node)  # update /etc/hosts on all nodes in cluster
        self.remove_node_from_slaves(node)

    def fix_etchosts(self, node, ip):
        '''
        this function is intended for an hd-workerXX. it updates /etc/hosts with all the other players in the cluster:
        hd-workerXX, hd-master, hbase-workerXX, hbase-master
        '''
        with self.printMutex:
            print 'fix ' + node
        cmd_composite_limit = "\""

        cmds = []
        # hd-workerXX
        other_player = 'hd-worker'
        cmds.append(" sed \'/^/d\' /etc/hosts > /etc/hosts_new; mv /etc/hosts_new /etc/hosts ")
        cmds.append("echo  \'127.0.0.1  %s localhost localhost.localdomain localhost4 localhost4.localdomain4\'  >> /etc/hosts" % node)
        cmds.append("echo  \'::1 localhost localhost.localdomain localhost6 localhost6.localdomain6\'  >> /etc/hosts")

        # entries = show_entries_internal(other_player, node)
        # cmds.append("echo  \'%s\'  >> /etc/hosts" % entries)

        # hd-master
        other_player = 'hd-master'
        cmds.append(" sed \'2,1000{/%s/d;}\' /etc/hosts > /etc/hosts_new; mv /etc/hosts_new /etc/hosts " % other_player)
        entries = show_entries_internal(other_player, None)
        cmds.append("echo  \'%s\'  >> /etc/hosts" % entries)

        # hbase-workerXX
        other_player = 'hbase-worker'
        cmds.append(" sed \'2,1000{/%s/d;}\' /etc/hosts > /etc/hosts_new; mv /etc/hosts_new /etc/hosts " % other_player)
        entries = show_entries_internal(other_player, None)
        cmds.append("echo  \'%s\'  >> /etc/hosts" % entries)

        # hbase-master
        other_player = 'hbase-master'
        cmds.append(" sed \'2,1000{/%s/d;}\' /etc/hosts > /etc/hosts_new; mv /etc/hosts_new /etc/hosts " % other_player)
        entries = show_entries_internal(other_player, None)
        cmds.append("echo  \'%s\'  >> /etc/hosts" % entries)

        cmd = ";".join(cmds)
        cmd = ("ssh  -o StrictHostKeyChecking=no -o ConnectTimeout=3  root@%s " % ip) + cmd_composite_limit + cmd + cmd_composite_limit
        res = self.doShell(cmd)
        with self.printMutex:
            print res


def fix_all_etchosts(admin, last_host):
    limit = last_host + 1

    for i in range(1, limit):
        node = "hd-worker{0}".format(i)
        ip  = admin.get_accessible_ip(node)
        admin.fix_etchosts(node, ip)

def is_jobtracker_idle():
    # result =  pydoop.hadut.run_cmd(cmd='job', args=('-list'))
    idle = 0 # int(result[0])
    if idle == 0:
        return True

    print "JobTraker is busy"
    return False

def stop_rinku(admin):
    user = 'rinku'
    cmd_composite_limit = "\""

    cmds = []
    # stop rinku periodical jobs scheduling
    cmds.append(" sed \'/jobs\.running\.node/s/true/false/\' /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.cfg \
      > /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.cfg_new;  \
      mv /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.cfg_new /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.cfg ")
    cmds.append(" sed \'/batch\.jobs\.queue\.enabled/s/true/false/\' /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.cfg \
      > /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.cfg_new;  \
      mv /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.cfg_new /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.cfg ")
    # stop kanji reports processing on rinku
    cmds.append(" sed \'/is\.processing/s/true/false/\' /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.profile.cfg \
      > /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.profile.cfg_new;  \
      mv /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.profile.cfg_new /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.profile.cfg ")


    ansible_tag = admin.mgr.HDREDMAP_TAG
    cmd = ";".join(cmds)
    cmd = cmd_composite_limit + cmd + cmd_composite_limit
    full = admin.shell_cmd(ansible_tag, user) + cmd
    res = admin.doShell(full)
    print res

def start_rinku(admin):
    user = 'rinku'
    cmd_composite_limit = "\""

    cmds = []
    # start rinku periodical jobs scheduling
    cmds.append(" sed \'/jobs\.running\.node/s/false/true/\' /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.cfg \
      > /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.cfg_new;  \
      mv /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.cfg_new /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.cfg ")
    cmds.append(" sed \'/batch\.jobs\.queue\.enabled/s/false/true/\' /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.cfg \
      > /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.cfg_new;  \
      mv /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.cfg_new /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.cfg ")
    # start kanji reports processing on rinku
    cmds.append(" sed \'/is\.processing/s/false/true/\' /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.profile.cfg \
      > /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.profile.cfg_new;  \
      mv /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.profile.cfg_new /opt/infolinks/rinku/provisioning/etc/com.infolinks.rinku.profile.cfg ")


    ansible_tag = admin.mgr.HDREDMAP_TAG
    cmd = ";".join(cmds)
    cmd = cmd_composite_limit + cmd + cmd_composite_limit
    full = admin.shell_cmd(ansible_tag, user) + cmd
    res = admin.doShell(full)
    print res

def process(admin, node, mode):
    START = time.time()
    with admin.printMutex:
        print("Begun at: {0}. node: {1}".format(str(datetime.datetime.now()), node))

    if mode == 'new':
        admin.new_node(node)
    elif mode == 'remove':
        admin.remove_node(node, flush_ansible_cache = False)
    else:
        with admin.printMutex:
            print 'unknown mode %s: do nothing' %  mode

    secs_diff = int(time.time() - START)
    with admin.printMutex:
        print "ClusterAdmin finished. Execution took {0} seconds.".format(secs_diff)

class HdWorkerThread(threading.Thread):
    def __init__(self, hd_worker, admin):
        self.hd_worker  = hd_worker
        self.admin = admin
        threading.Thread.__init__(self)

    def run(self):
        process(self.admin, self.hd_worker, 'new')

if __name__ == "__main__":
    print('%s script start' % str(datetime.datetime.now()))
    admin = MRClusterAdmin()

    ####################################### remove #####################################################################
    # for i in range(51, 151):
    #     node = "hd-worker{0}".format(i)
    #     try:
    #         process(admin, node, 'remove')
    #     except Exception as e:
    #         print('!!!! %s was not removed. probably did not exist' % (node))

    ##################################### is removed ###################################################################
    for i in range(120, 151):
        node = "hd-worker{0}".format(i)
        print(node)
        if not admin.mgr.is_node_removed(node):
            print('%s  still up' % node)

    ##################################### new ##########################################################################
    # threads = []
    # for i in range(111, 151):
    #     node = "hd-worker{0}".format(i)
    #     t = HdWorkerThread(node, admin)
    #     t.start()
    #     threads.append(t)
    #
    # for t in threads:
    #     t.join()
    #     with admin.printMutex:
    #         print('%s: creation on %s finished' % (str(datetime.datetime.now()), t.hd_worker))

    print('%s script end' % str(datetime.datetime.now()))



































