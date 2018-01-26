from oauth2client.client import GoogleCredentials
from googleapiclient import discovery
import json
import subprocess
import re
import time
import datetime
import gcloud_api
from gcloud_cluster_etchosts import show_entries_internal_for_hbase
import gcloud_utils
import threading




class HbaseClusterAdmin:
    def __init__(self):
        self.printLock = threading.Lock()
        self.mgr = gcloud_api.NodeMgr(self.printLock())

    '************************************ helpers ******************************************************'
    def stop_hbase_on_remove_node(self, node):
        'stop the region server on the node'

        # 1. gracefull stop the region server
        user = "hadoop"
        newtag = self.mgr.HBASEREMOVE_TAG
        cmd = "\"/data/hbase/bin/hbase-daemon.sh stop regionserver\""
        full = self.shell_flush_cmd(newtag, user) + cmd
        self.doShell(full)
        print("stopped regionserver on node {0}".format(node))

        # 2. remove the regionserver from the master's configuration
        user = 'root'
        cmd = "\" sed \"/{0}/d\" /data/hbase/conf/regionservers > /data/hbase/conf/regionservers_new; mv /data/hbase/conf/regionservers_new /data/hbase/conf/regionservers \"".format(node)
        newtag = self.mgr.HBASEMASTER_TAG
        full = self.shell_cmd(newtag, user) + cmd
        self.doShell(full)

        # 3. restart the master
        user = "hadoop"
        newtag = self.mgr.HBASEMASTER_TAG
        cmd = "\"/data/hbase/bin/hbase-daemon.sh stop master; sleep 3; /data/hbase/bin/hbase-daemon.sh start master \""
        full = self.shell_cmd(newtag, user) + cmd
        self.doShell(full)

    def stop_hadoop_on_remove_node(self, node):
        'decomission the datanode and task tracker and than stop the processes'

        # 1. decomission 10.142.0.31:5001   - exclude files

        # a. create the entry for the exclude files
        ip = self.mgr.get_node_internal_ip(node)
        entry = "{0}:50010".format(ip)

        # b. add entry to the exclude file - so it can be decomissioned
        user = "hadoop"
        cmd = "\" cp /dev/null {1}; echo {0} >> {1}\"".format(entry, "/data/hadoop/hdfs_excludes")
        full = self.shell_cmd(self.mgr.HBASEMASTER_TAG, user) + cmd
        self.doShell(full)

        cmd = "\" cp /dev/null {1}; echo {0} >> {1}\"".format(entry, "/data/hadoop/mapred_excludes")
        full = self.shell_cmd(self.mgr.HBASEMASTER_TAG, user) + cmd
        self.doShell(full)

        # b.1 remove the temp job files that can habg decommision
        user = "hadoop"
        cmd = "\"/data/hadoop/bin/hadoop fs -rmr /tmp/mapred/staging/hadoop/.staging/* \" "
        full = self.shell_cmd(self.mgr.HBASEMASTER_TAG, user) + cmd
        self.doShell(full)
        cmd = "\"/data/hadoop/bin/hadoop fs -rmr /user/hadoop/.Trash/Current/tmp/mapred/staging/hadoop/.staging/* \" "
        full = self.shell_cmd(self.mgr.HBASEMASTER_TAG, user) + cmd
        self.doShell(full)



        # c. do the decomission command
        user = "hadoop"
        cmd = "\"/data/hadoop/bin/hadoop dfsadmin -refreshNodes; sleep 3; /data/hadoop/bin/hadoop mradmin -refreshNodes\""
        full = self.shell_cmd(self.mgr.HBASEMASTER_TAG, user) + cmd
        self.doShell(full)

        #. d wait for decommision to finish
        while not self.is_finished_decomission(entry):
            print("{0} was not decomissioned yet.".format(node))
            time.sleep(3)
        print("Hurrah! {0} was decomissioned.".format(node))

        # z. remove entry
        user = "hadoop"
        cmd = "\" cp /dev/null /data/hadoop/hdfs_excludes \""
        full = self.shell_cmd(self.mgr.HBASEMASTER_TAG, user) + cmd
        self.doShell(full)

        cmd = "\" cp /dev/null /data/hadoop/mapred_excludes \""
        full = self.shell_cmd(self.mgr.HBASEMASTER_TAG, user) + cmd
        self.doShell(full)

        # 2. stop tasktracker and datanode on the remove node
        user = "hadoop"
        tag = self.mgr.HBASEREMOVE_TAG
        cmd = "\"/data/hadoop/bin/hadoop-daemon.sh stop tasktracker; sleep 3; /data/hadoop/bin/hadoop-daemon.sh stop datanode \""
        full = self.shell_cmd(tag, user) + cmd
        self.doShell(full)

        # 3. restart namenode
        user = "hadoop"
        newtag = self.mgr.HBASEMASTER_TAG
        cmd = "\"/data/hadoop/bin/hadoop-daemon.sh stop namenode; sleep 3;  /data/hadoop/bin/hadoop-daemon.sh start namenode\""
        full = self.shell_cmd(newtag, user) + cmd
        self.doShell(full)

        # 4. restart jobtracker
        user = "hadoop"
        newtag = self.mgr.HBASEMASTER_TAG
        cmd = "\"/data/hadoop/bin/hadoop-daemon.sh stop jobtracker; sleep 3;  /data/hadoop/bin/hadoop-daemon.sh start jobtracker\""
        full = self.shell_cmd(newtag, user) + cmd
        self.doShell(full)

    def is_finished_decomission(self, entry): # entry = "{0}:50010".format(ip)
        user = "hadoop"
        report = "/tmp/hdfs_report"
        cmd = "\" /data/hadoop/bin/hadoop dfsadmin -report > {0}\"".format(report)
        full = self.shell_cmd(self.mgr.HBASEMASTER_TAG, user) + cmd
        self.doShell(full)

        cmd = "\"src={0} dest=/tmp\"".format(report)
        newtag = self.mgr.HBASEMASTER_TAG
        full = self.fetch_cmd(newtag, user) + cmd
        self.doShell(full)

        local_path = "/tmp/{0}{1}".format('hbase-master',report)
        file = open(local_path)
        lines = file.readlines()

        '''
        example of what we are looking for:
        Name: 10.142.0.30:50010
        Decommission Status : Decommissioned
        '''
        decomissioned = False
        for i in range(len(lines)):
            match = re.search(r'{0}'.format(entry), lines[i])
            if match:
                match = re.search(r'Decommissioned'.format(entry), lines[i+1])
                if match:
                    decomissioned = True
                break

        return decomissioned


    def remove_node_from_etc_hosts(self, node):
        user = "root"
        cmd = "\" sed \"/{0}/d\" /etc/hosts > /etc/hosts_new; mv /etc/hosts_new /etc/hosts \"".format(node)
        full = self.shell_cmd(self.mgr.HBASEMASTER_TAG, user) + cmd
        self.doShell(full)

        full = self.shell_cmd(self.mgr.HBASEWORKER_TAG, user) + cmd
        self.doShell(full)

        full = self.shell_cmd(self.mgr.HDREDMAP_TAG, user) + cmd
        self.doShell(full)

        full = self.shell_cmd(self.mgr.HDMASTER_TAG, user) + cmd
        self.doShell(full)

        full = self.shell_cmd(self.mgr.HDWORKER_TAG, user) + cmd
        self.doShell(full)

    def doShell(self, cmd):
        print("doing - {0}".format(cmd))
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        (output, err) = p.communicate()
        status = p.wait()

    def reset_new_node_fs(self):
        tag = self.mgr.HBASENEW_TAG
        cmd = "\"rm -rf /opt/hadoop-data/datanode-dir/*\""
        full = self.shell_flush_cmd(tag, "hadoop") + cmd
        self.doShell(full)
        print("cleaned datanode-dir")

        cmd = "\"chown -R hadoop:infolinks_app /logs\""
        full = self.shell_cmd(tag, "root") + cmd
        self.doShell(full)
        print("/logs is owned by hadoop")

        cmd = "\"chown -R hadoop:infolinks_app /opt/hadoop-data/mapred-tmp-dir\""
        full = self.shell_cmd(tag, "root") + cmd
        self.doShell(full)
        print("/opt/hadoop-data/mapred-tmp-dir is owned by hadoop")

    def update_etc_hosts_of_hbase_cluster(self, node):
        user = "root"
        ip = self.mgr.get_node_internal_ip(node)
        cmd = "\"echo \'%-15s   %-15s   %s\' >> /etc/hosts \"" % (ip, node + '.infolinks.com', node)
        full = self.shell_cmd(self.mgr.HBASEMASTER_TAG, user) + cmd
        self.doShell(full)
        print("updated /etc/hosts on tag hdmaster")

        full = self.shell_cmd(self.mgr.HBASEWORKER_TAG, user) + cmd
        self.doShell(full)
        print("updated /etc/hosts on tag hbase-worker")

    def update_etc_hosts_of_mr_cluster(self, node):
        user = "root"
        ip = self.mgr.get_node_internal_ip(node)
        cmd = "\"echo \'%-15s   %-15s   %s\' >> /etc/hosts \"" % (ip, node + '.infolinks.com', node)
        full = self.shell_cmd(self.mgr.HDMASTER_TAG, user) + cmd
        self.doShell(full)
        print("updated /etc/hosts on hd-master")

        full = self.shell_cmd(self.mgr.HDREDMAP_TAG, user) + cmd
        self.doShell(full)
        print("updated /etc/hosts on tag hd-redmap")

        full = self.shell_cmd(self.mgr.HDWORKER_TAG, user) + cmd
        self.doShell(full)
        print("updated /etc/hosts on tag hd-workers")

    def update_etc_hosts_of_new_node(self, node):
        '1. remove c.infolinks-production.internal from /etc/hosts'
        user = 'root'
        cmd = "\" sed \"/c.infolinks-production.internal/d\" /etc/hosts > /etc/hosts_new; mv /etc/hosts_new /etc/hosts \""
        newtag = self.mgr.HBASENEW_TAG
        full = self.shell_cmd(newtag, user) + cmd
        self.doShell(full)

        '2. take care of the other players: hbase-workerXX, hbase-master, hd-worker, hd-master, hd-redmap'
        self.fix_etchosts(node, do_flush=False, ansible_tag=newtag)


    def fix_etchosts(self, node, do_flush, ansible_tag):
        '''
        this function is intended for an hbase-workerXX. it updates /etc/hosts with all the other players in the cluster:
        hd-workerXX, hd-master, hbase-workerXX, hbase-master
        '''
        print 'fix ' + node
        user = 'root'
        cmd_composite_limit = "\""

        cmds = []
        # hbase-workerXX
        other_player = 'hbase-worker'
        cmds.append(" sed \'2,1000{/%s/d;}\' /etc/hosts > /etc/hosts_new; mv /etc/hosts_new /etc/hosts " % other_player)# we dont delete the first line 127.0.0.1   hd-worker<host_number>
        entries = show_entries_internal_for_hbase(other_player, None)
        cmds.append("echo  \'%s\'  >> /etc/hosts" % entries)

        # hbase-master
        other_player = 'hbase-master'
        cmds.append(" sed \'2,1000{/%s/d;}\' /etc/hosts > /etc/hosts_new; mv /etc/hosts_new /etc/hosts " % other_player)
        entries = show_entries_internal_for_hbase(other_player, None)
        cmds.append("echo  \'%s\'  >> /etc/hosts" % entries)

        # hd-workerXX
        other_player = 'hd-worker'
        cmds.append(" sed \'2,1000{/%s/d;}\' /etc/hosts > /etc/hosts_new; mv /etc/hosts_new /etc/hosts " % other_player)

        # hd-master
        other_player = 'hd-master'
        cmds.append(" sed \'2,1000{/%s/d;}\' /etc/hosts > /etc/hosts_new; mv /etc/hosts_new /etc/hosts " % other_player)

        # hd-redmap
        other_player = 'hd-redmap'
        cmds.append(" sed \'2,1000{/%s/d;}\' /etc/hosts > /etc/hosts_new; mv /etc/hosts_new /etc/hosts " % other_player)


        cmd = ";".join(cmds)
        cmd = cmd_composite_limit + cmd + cmd_composite_limit
        full = self.shell_flush_cmd(ansible_tag, user) + cmd \
            if do_flush == True \
            else self.shell_cmd(ansible_tag, user) + cmd
        res = self.doShell(full)
        print res

    def update_configuration_of_new_node(self):
        '1. update core-site.xml'
        user = 'hadoop'
        cmd = "\" sed \\\"s/gs:\/\/hdfs_main/hdfs:\/\/hbase-master.infolinks.com:8000/g\\\" /data/hadoop/conf/core-site.xml > /data/hadoop/conf/core-site.xml_new; sleep 3; mv /data/hadoop/conf/core-site.xml_new /data/hadoop/conf/core-site.xml \""
        newtag = self.mgr.HBASENEW_TAG
        full = self.shell_cmd(newtag, user) + cmd
        self.doShell(full)

        '2. update mapred-site.xml'
        user = 'hadoop'
        cmd = "\" sed \"s/hd-master/hbase-master/g\" /data/hadoop/conf/mapred-site.xml > /data/hadoop/conf/mapred-site.xml_new; mv /data/hadoop/conf/mapred-site.xml_new /data/hadoop/conf/mapred-site.xml \""
        newtag = self.mgr.HBASENEW_TAG
        full = self.shell_cmd(newtag, user) + cmd
        self.doShell(full)

    def start_hadoop_on_new_node(self, node):
        user = "hadoop"
        newtag = self.mgr.HBASENEW_TAG
        cmd = "\"/data/hadoop/bin/hadoop-daemon.sh start datanode\""
        full = self.shell_cmd(newtag, user) + cmd
        self.doShell(full)
        print("started datanode")

        cmd = "\"/data/hadoop/bin/hadoop-daemon.sh start tasktracker\""
        full = self.shell_cmd(newtag, user) + cmd
        self.doShell(full)
        print("started tasktracker")

    def start_hbase_on_new_node(self, node):
        user = "hadoop"
        newtag = self.mgr.HBASENEW_TAG
        cmd = "\"/data/hbase/bin/hbase-daemon.sh start regionserver\""
        full = self.shell_cmd(newtag, user) + cmd
        self.doShell(full)
        print("started regionserver")

        cmd = "\"echo {0}.infolinks.com >> /data/hbase/conf/regionservers\"".format(node)
        full = self.shell_cmd(self.mgr.HBASEMASTER_TAG, user) + cmd
        self.doShell(full)
        print("updated /data/hbase/conf/regionservers on hdmaster")

    def restart_hbase_regionservers(self):
        user = "hadoop"
        cmd = "\"/data/hbase/bin/hbase-daemon.sh stop regionserver; sleep 3; /data/hbase/bin/hbase-daemon.sh start regionserver\""
        full = self.shell_cmd(self.mgr.HBASEWORKER_TAG, user) + cmd
        self.doShell(full)
        print("restarted all regionservers")

        # 3. restart the master
        user = "hadoop"
        newtag = self.mgr.HBASEMASTER_TAG
        cmd = "\"/data/hbase/bin/hbase-daemon.sh stop master; sleep 3; /data/hbase/bin/hbase-daemon.sh start master \""
        full = self.shell_cmd(newtag, user) + cmd
        self.doShell(full)
        print("restarted hbase master")


    def shell_cmd(self, tag, user):
        return self.base_cmd(tag, user) + " -m shell -a "

    def shell_flush_cmd(self, tag, user):
        return self.base_cmd(tag, user) + " --flush-cache  -m shell -a "

    def fetch_cmd(self, tag, user):
        return self.base_cmd(tag, user) + " -m fetch -a "

    def base_cmd(self, tag, user):
        return "~/com.infolinks.ansible/bin/ansible.sh --limit=%s   --become-user=%s " % (tag, user)

    #############################################  interface #####################################################
    def new_node(self, node):
        self.mgr.add_node_from_image(node, data_disk_image='hbase-worker-data')
        self.mgr.set_node_tag(node, self.mgr.HBASENEW_TAG)

        self.reset_new_node_fs()

        self.update_etc_hosts_of_hbase_cluster(node)
        self.update_etc_hosts_of_mr_cluster(node)
        self.update_etc_hosts_of_new_node(node)
        self.update_configuration_of_new_node()

        self.start_hadoop_on_new_node(node)
        self.start_hbase_on_new_node(node)

        self.mgr.set_node_tag(node, self.mgr.HBASEWORKER_TAG)
        self.restart_hbase_regionservers()

        self.mgr.show_node(node)

    def remove_node(self, node):
        self.mgr.set_node_tag(node, self.mgr.HBASEREMOVE_TAG)

        self.stop_hbase_on_remove_node(node)

        self.stop_hadoop_on_remove_node(node) # decommision blocks first

        self.remove_node_from_etc_hosts(node)  # update /etc/hosts on all nodes in cluster

        self.mgr.remove_node(node) # remove the remove_node

def process(admin, node, mode):
    if not gcloud_utils.ask_user():
        return

    START = time.time()
    print("Begun at: {0}. node: {1}".format(str(datetime.datetime.now()), node))

    if mode == 'remove':
        admin.remove_node(node)
    elif mode == 'new':
        admin.new_node(node)
    else:
        print 'Unknown mode: %s. Doing nothing' % mode

    secs_diff = int(time.time() - START)
    print "ClusterAdmin finished. Execution took {0} seconds.".format(secs_diff)

def fix_all_etchosts(admin):
    if not gcloud_utils.ask_user():
        return

    for i in range(1, 6):
        node = "hbase-worker{0}".format(i)
        print 'puttig tag %s on host %s' % (node, node)
        admin.mgr.add_node_tag(node, node)

    admin.fix_etchosts("hbase-worker1", do_flush=True, ansible_tag="hbase-worker1")
    for i in range(2, 6):
        node = "hbase-worker{0}".format(i)
        admin.fix_etchosts(node, do_flush=False, ansible_tag=node)

    for i in range(1, 6):
        node = "hbase-worker{0}".format(i)
        print 'removing tag %s from host %s' % (node, node)
        admin.mgr.remove_node_tag(node, node)

if __name__ == "__main__":
    admin = HbaseClusterAdmin()
    #fix_all_etchosts(admin)

    for i in range(7, 8):
       node = "hbase-worker{0}".format(i)
       process(admin, node, mode='new')


