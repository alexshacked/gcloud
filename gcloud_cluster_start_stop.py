import subprocess
import thread



HBASEMASTER_TAG = 'hbasemaster'
HBASEWORKER_TAG = 'hbaseworker'
HBASEQUORUM_TAG = 'hbasequorum'
HDWORKER_TAG = "hdworker"
HDMASTER_TAG = "hdmaster"

NAMENODE_CMD = "\"/data/hadoop/bin/hadoop-daemon.sh %s namenode\""
DATANODE_CMD = "\"/data/hadoop/bin/hadoop-daemon.sh %s datanode\""
JOBTRACKER_CMD = "\"/data/hadoop/bin/hadoop-daemon.sh %s jobtracker\""
TASKTRACKER_CMD = "\"/data/hadoop/bin/hadoop-daemon.sh %s tasktracker\""
ZOOKEEPER_CMD = "\"/data/hbase/bin/hbase-daemon.sh %s zookeeper\""
HBASEMASTER_CMD = "\"/data/hbase/bin/hbase-daemon.sh %s master\""
REGIONSERVER_CMD = "\"/data/hbase/bin/hbase-daemon.sh %s regionserver\""

class StartStop:
    def __init__(self):
        pass


    def shell_cmd(self, tag, user):
        return self.base_cmd(tag, user) + " -m shell -a "

    def shell_flush_cmd(self, tag, user):
        return self.base_cmd(tag, user) + " --flush-cache  -m shell -a "

    def fetch_cmd(self, tag, user):
        return self.base_cmd(tag, user) + " -m fetch -a "

    def base_cmd(self, tag, user):
        return "~/com.infolinks.ansible/bin/ansible.sh --limit=%s   --become-user=%s " % (tag, user)

    def doShell(self, cmd):
        print("doing - {0}".format(cmd))
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        (output, err) = p.communicate()
        status = p.wait()
        return output

    def do_command(self, user, tag, cmd):
        full = self.shell_cmd(tag, user) + cmd
        out = self.doShell(full)
        print out

    def do_flush_command(self, user, tag, cmd):
        full = self.shell_flush_cmd(tag, user) + cmd
        out = self.doShell(full)
        print out

#########################################  API  ##################################################

    def hbase_stop(self):
        what = 'stop'

        # 1. stop regionservers
        self.do_flush_command(user='hadoop', tag=HBASEWORKER_TAG, cmd=REGIONSERVER_CMD % what)

        # 2. stop master
        self.do_command(user='hadoop', tag=HBASEMASTER_TAG, cmd=HBASEMASTER_CMD % what)

        # 3. stop zookeeper quorum
        self.do_command(user='hadoop', tag=HBASEQUORUM_TAG, cmd=ZOOKEEPER_CMD % what)

        # 4. stop tasktrackers
        self.do_command(user='hadoop', tag=HBASEWORKER_TAG, cmd=TASKTRACKER_CMD % what)

        # 5. stop jobtracker
        self.do_command(user='hadoop', tag=HBASEMASTER_TAG, cmd=JOBTRACKER_CMD % what)

        # 6. stop datanodes
        self.do_command(user='hadoop', tag=HBASEWORKER_TAG, cmd=DATANODE_CMD % what)

        # 7. stop namenode
        self.do_command(user='hadoop', tag=HBASEMASTER_TAG, cmd=NAMENODE_CMD % what)

    def hbase_start(self):
        what = 'start'

        # 1. start namenode
        self.do_flush_command(user='hadoop', tag=HBASEMASTER_TAG, cmd=NAMENODE_CMD % what)

        # 2. start datanodes
        self.do_command(user='hadoop', tag=HBASEWORKER_TAG, cmd=DATANODE_CMD % what)

        # 3. start jobtracker
        self.do_command(user='hadoop', tag=HBASEMASTER_TAG, cmd=JOBTRACKER_CMD % what)

        # 4. start tasktrackers
        self.do_command(user='hadoop', tag=HBASEWORKER_TAG, cmd=TASKTRACKER_CMD % what)

        # 5. start zookeeper quorum
        self.do_command(user='hadoop', tag=HBASEQUORUM_TAG, cmd=ZOOKEEPER_CMD % what)

        # 6. start hbase master
        self.do_command(user='hadoop', tag=HBASEMASTER_TAG, cmd=HBASEMASTER_CMD % what)

        # 6. start regionservers
        self.do_command(user='hadoop', tag=HBASEWORKER_TAG, cmd=REGIONSERVER_CMD % what)

    def mr_stop(self):
        what = 'stop'

        # 1. stop tasktrackers
        self.do_flush_command(user='hadoop', tag=HDWORKER_TAG, cmd=TASKTRACKER_CMD % what)

        # 2. stop jobtracker
        self.do_command(user='hadoop', tag=HDMASTER_TAG, cmd=JOBTRACKER_CMD % what)

    def mr_start(self):
        what = 'start'

        # 1. start jobtracker
        self.do_flush_command(user='hadoop', tag=HDMASTER_TAG, cmd=JOBTRACKER_CMD % what)

        # 2. start tasktrackers
        self.do_command(user='hadoop', tag=HDWORKER_TAG, cmd=TASKTRACKER_CMD % what)

    def all_stop(self):
        self.mr_stop()
        self.hbase_stop()

    def all_start(self):
        self.hbase_start()
        self.mr_start()

if __name__ =='__main__':
    st = StartStop()
    st.hbase_start()
    #st.mr_start()








