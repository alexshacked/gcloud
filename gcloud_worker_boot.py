#!/usr/bin/python
import subprocess
import datetime

class Maestro:
    '''
    boot script that is run by google on a new hd-worker instance. to deploy put in:
    gs://hd_worker_boot/scripts/
    '''
    def __init__(self):
        pass

    def go(self):
        self.data_disk_ready()
        self.third_party_software()
        self.install_hadoop()
        self.say_goodbye()

    def data_disk_ready(self):
        device_id = '/dev/sdb'
        data_dir = '/data'
        cmds = [
            'mkfs.ext4 -m 0 -F -E lazy_itable_init=0,lazy_journal_init=0,discard %s' % device_id,
            'mkdir %s' % data_dir,
            'mount -o discard,defaults %s %s' % (device_id, data_dir),
            'echo UUID=$(sudo blkid -s UUID -o value %s) %s ext4 discard,defaults,nofail 0 2 | sudo tee -a /etc/fstab' % (device_id, data_dir)
                ]
        flat = ';'.join(cmds)
        self.do_shell(flat)

    def third_party_software(self):
        cmds = [
            'yes | yum install java-1.7.0-openjdk.x86_64',
            'yes | yum install java-1.7.0-openjdk-devel.x86_64',

            'yes | yum install python34.x86_64',
            'ln -s /bin/python3 /usr/local/bin/python3',
            'yes | yum install python34-pip.noarch',
            'pip3 install --upgrade oauth2client',
            'pip3 install --upgrade google-api-python-client',
            'pip3 install tldextract',
            'chmod 777 /usr/lib/python3.4/site-packages/tldextract',
            'pip3 install tornado'
        ]
        flat = ';'.join(cmds)
        self.do_shell(flat)

    def install_hadoop(self):
        laptop_key = 'ssh-rsa AAAAB3NzaC1yc2EAAAABIwAAAQEA5axeQDgENbG2V1UY+szFJp8r1FSu1BQFl98v6DWud2gIn/6U04PDRBKX98P82/C0BUSYyqRcp8YHYLCW+7BGTQwmOfMmV50OG8MOvEq8VD0RMefG7Wws8Xwir5hgw+lcNNtAmlko/R2zRuifTyCqKrkdtj6jIlGJdITx7sid2fW06TsQiyoQ1VhDTJLkbAbntnUVeGS9kqAfs4IoZAf+1+8X49JA5cNVNQWfszMzi6njsuOVgWX94RAQ9VmgYj34KCh0W+AM2TobBruy7wE5JNuoqpmHDeS0fT0OTpPqGQhMAiKmsjodT7Flg1Xhhd7aAgpkb+RdvxCjw72M4nuZzw== User of alexs'
        hd_controler_key = 'ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAABAQCkq6X9p42oC1AVwE5PQE8ofJd+hv3WH5heodyZB5bGa16aoJgCL0VnHNHoqVIheKsR4LEB7iuXyNlQKfxDKitoXEmg6wgj8UGIBATSdRE21mld4UHskiRVSZt9kJe/36KmQUYnrtJjHT/cnwYpL6KOfE6BrlFH471rAeReZWKa0E6WT9Ij02w84KnbrcImmpL22kkXVtqPDJ2aIyINkOl7nmjfXDkqWyDnbuPnjB3yxzUcrq8GWb/TyJ/qfJLFb7ItIbaJSOuk1j2igaDhnhyJSn5PMWuBMHI0vvBE+oyKs2vbC+eBP5eswNaAJZ9BYFQwgEnJF3+fJcYG03Dqev0f'


        cmds = [
            # directories
            'mkdir /data/hbase-data',
            'mkdir /data/hbase-data/logs',
            'mkdir /data/zookeeper-data',
            'mkdir /logs',
            'gsutil cp gs://hd_worker_boot/hadoop/data_dirs.txt /tmp/',
            'xargs mkdir -p < /tmp/data_dirs.txt',
            'rm /tmp/data_dirs.txt',
            # hadoop dir
            'gsutil cp gs://hd_worker_boot/hadoop/datadog.jar /tmp/',
            'gsutil cp gs://hd_worker_boot/hadoop/infolinks.jar /tmp/',
            'cd /',
            'jar xvf /tmp/datadog.jar',
            'jar xvf /tmp/infolinks.jar',
            'cd -',
            'rm /tmp/datadog.jar',
            'rm /tmp/infolinks.jar',
            'gsutil cp -r gs://hd_worker_boot/hadoop/hadoop /opt/',
            'gsutil cp -r gs://hd_worker_boot/hadoop/hbase /opt/',
            'gsutil cp -r gs://hd_worker_boot/hadoop/zookeeper /opt/',
            'gsutil cp -r gs://hd_worker_boot/hadoop/etc/infolinks /etc/',
            # soft links
            'ln -s /data/hadoop-data/logs /opt/hadoop/logs',
            'ln -s /data/hadoop-data/ /opt/hadoop-data',
            'ln -s /data/hadoop-nn-mirror /opt/hadoop-nn-mirror',
            'ln -s /data/hadoop-nn-mirror2 /opt/hadoop-nn-mirror2',
            'ln -s /data/hbase-data /opt/hbase-data',
            'ln -s /data/zookeeper-data /opt/zookeeper-data',
            'ln -s /opt/hadoop /data/hadoop',
            'ln -s /opt/hbase /data/hbase',
            'ln -s /opt/zookeeper /data/zookeeper',
            # hadoop user
            'groupadd infolinks_app',
            'useradd hadoop -d/opt/hadoop -g infolinks_app',
            'chown -R hadoop:infolinks_app /data',
            'chown -R hadoop:infolinks_app /opt/hadoop',
            'chown -R hadoop:infolinks_app /opt/infolinks',
            'chown -R hadoop:infolinks_app /opt/hadoop-data',
            'chown -R hadoop:infolinks_app /opt/hadoop-nn-mirror',
            'chown -R hadoop:infolinks_app /opt/hadoop-nn-mirror2',
            'chown -R hadoop:infolinks_app /opt/hbase',
            'chown -R hadoop:infolinks_app /opt/hbase-data',
            'chown -R hadoop:infolinks_app /opt/zookeeper',
            'chown -R hadoop:infolinks_app /opt/zookeeper-data',
            'chown -R hadoop:infolinks_app /etc/infolinks',
            'chown -R hadoop:infolinks_app /logs',

            # credentials
            'chmod 777 /opt/hadoop/bin/*',
            # commands that came from new_node
            'rm -rf /opt/hadoop-data/datanode-dir/*',
            'rm -rf /opt/hadoop-data/mapred-tmp-dir/userlogs/*',

            # ssh keys
            ' echo \" # laptop_key \" >> /root/.ssh/authorized_keys',
            ' echo \" %s \" >> /root/.ssh/authorized_keys' % laptop_key,
            ' echo \" # controler_key \" >> /root/.ssh/authorized_keys',
            ' echo \" %s \" >> /root/.ssh/authorized_keys' % hd_controler_key,
            " sed \'/PermitRootLogin/s/no/yes/\' /etc/ssh/sshd_config \
      > /etc/ssh/sshd_config_new; mv /etc/ssh/sshd_config_new /etc/ssh/sshd_config ",
            'service sshd restart',
            ]
        flat = ';'.join(cmds)
        self.do_shell(flat)

##################################################### helpers ##########################################################
    def now(self):
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    def say_goodbye(self):
        print("BOOT_SCRIPT_FINISHED") # output for the startup script goes to serial port 1

    def do_shell(self, cmd):
        print("doing - {0}".format(cmd))
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
        (output, err) = p.communicate()
        status = p.wait()
        return output

##################################################### driver ###########################################################
maestro = Maestro()
maestro.go()