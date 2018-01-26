from oauth2client.client import GoogleCredentials
from googleapiclient import discovery
import json
import time
import datetime
import threading
import httplib2
import apiclient

credentials = GoogleCredentials.get_application_default()
def build_request(http, *args, **kwargs):
    new_http = credentials.authorize(httplib2.Http())
    return apiclient.http.HttpRequest(new_http, *args, **kwargs)

class NodeMgr:
    def __init__(self, printMutex):
        self.printMutex = printMutex
        self.compute = discovery.build('compute', 'beta', credentials = credentials, requestBuilder=build_request)
        self.proj = "infolinks-production"
        self.DEFALT_ZONE = "us-east1-c"
        self.zn =  self.DEFALT_ZONE

        self.HDNEW_TAG = "hdnew"
        self.HDREMOVE_TAG = "hdremove"
        self.HDWORKER_TAG = "hdworker"
        self.HDMASTER_TAG = "hdmaster"
        self.HDREDMAP_TAG = "hdredmap"

        self.HBASENEW_TAG = "hbasenew"
        self.HBASEREMOVE_TAG = "hbaseremove"
        self.HBASEWORKER_TAG = "hbaseworker"
        self.HBASEMASTER_TAG = "hbasemaster"

    def set_zone(self, zone):
        self.zn = zone

    def reset_zone(self):
        self.zn = self.DEFALT_ZONE

    def add_node_from_image(self, name, data_disk_image='hd-worker-image-data', preemptive=False):
        self.create_instance(self.compute, self.proj, self.zn, name, data_disk_image, preemptive)
        while not self.get_node_ready(name):
            with self.printMutex:
                print name + " not ready yet."
            time.sleep(3)

    def remove_node(self, name):
        resp =  self.compute.instances().delete(
            project=self.proj,
            zone=self.zn,
            instance=name).execute()
        return resp


    def get_node_public_ip(self, name):
        inst = self.get_node_info(name)
        return inst['networkInterfaces'][0]['accessConfigs'][0]['natIP']

    def get_node_internal_ip(self, name):
        inst = self.get_node_info(name)
        return inst['networkInterfaces'][0]['networkIP']

    def get_node_fingerprint(self, name):
        inst = self.get_node_info(name)
        if 'tags' in inst:
            if 'fingerprint' in inst['tags']:
                return inst['tags']['fingerprint']

        # did not find inst['tags']['fingerprint']. return inst['labelFingerprint]
        return inst['labelFingerprint']

    def get_node_ready(self, name):
        inst = self.get_node_info(name)
        return inst['status'] == 'RUNNING'

    def is_node_removed(self, name):
        try:
            self.get_node_ready(name)
        except:
            return True # if node does not exist google throws exception
        return False

    def get_node_tag_ready(self, node, tag):
        inst = self.get_node_info(node)

        # try to look first in labels
        if 'labels' in inst:
            if tag in inst['labels'].keys():
                    return True

        # not found in labels. try looking in tags
        if 'tags' in inst:
            if 'items' in inst['tags']:
                if tag in inst['tags']['items']:
                    return True

        return False

    def get_node_tags(self, node):
        inst = self.get_node_info(node)
        if not 'tags' in inst or not 'items' in inst['tags']:
            return []
        return inst['tags']['items']


    def set_node_tags(self, node_name, tags):
        jsontags = {"items": tags,
                    "fingerprint": self.get_node_fingerprint(node_name)}
        ret = self.compute.instances().setTags(project=self.proj, zone=self.zn, instance=node_name, body=jsontags).execute()

    def wait_for_tag(self, name, tag):
        while not self.get_node_tag_ready(name, tag):
            with self.printMutex:
                print tag + " tag not ready yet."
            time.sleep(3)

    def add_node_tag(self, name, tag):
        tags = self.get_node_tags(name)
        tags.append(tag)
        self.set_node_tags(node_name=name, tags=tags)
        self.wait_for_tag(name=name, tag=tag)

    def remove_node_tag(self, name, tag):
        tags = self.get_node_tags(name)
        tags = [t for t in tags if t != tag]
        self.set_node_tags(node_name=name, tags=tags)

    def set_node_nametag(self, name):
        self.set_node_tags(node_name=name, tags=[name])
        self.wait_for_tag(name=name, tag=name)

    def set_node_tag(self, name, tag):
        self.set_node_tags(node_name=name, tags=[tag])
        self.wait_for_tag(name=name, tag=tag)

    def show_node(self, name):
        inst = self.get_node_info(name)
        self.show_instance(inst)

    def get_workers(self, type):
        workers_data = self.get_workers_data(type)
        workers = [d['name'] for d in workers_data]
        return workers

    def get_terminated_workers(self, type):
        workers_data = self.get_workers_data(type)
        workers = [d['name'] for d in workers_data if d['status'] != 'RUNNING']
        return workers

    def get_workers_data(self, type):
        ret = self.compute.instances().list(project=self.proj, zone=self.zn, filter="name eq %s.*" % type).execute()
        workers_data = [one for one in ret['items']]
        return workers_data

    '************************************ helpers ******************************************************'
    def create_instance(self,compute, project, zone, name, data_disk_image='hd-worker-image-data', preemtive=False):
        config = {
            'name': name,
            'machineType': 'zones/' + zone + '/machineTypes/n1-highmem-4', # 4 CPUs 26 GB memory     # previous: custom-12-32000  12 - CPUs, 31200 - 31200 MB = 32 GB
            'disks': [
                {
                    'boot': True,
                    'autoDelete': True,
                    'initializeParams': {
                        'sourceImage': 'global/images/hd-worker-image-os',
                    }
                },
                {
                    'boot': False,
                    'autoDelete': True,
                    'initializeParams': {
                        'sourceImage': 'global/images/%s' % data_disk_image,
                        'diskName': name + '-data'
                    }
                }
            ],
             'networkInterfaces': [{
                'network': 'global/networks/default',
                'accessConfigs': [
                    {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
                ]
            }],

            'serviceAccounts': [{
                'email': 'default',
                'scopes': [
                    'https://www.googleapis.com/auth/devstorage.read_only',
                    'https://www.googleapis.com/auth/logging.write',
                    'https://www.googleapis.com/auth/monitoring.write',
                    'https://www.googleapis.com/auth/servicecontrol',
                    'https://www.googleapis.com/auth/service.management.readonly',
                    'https://www.googleapis.com/auth/trace.append',
                    'https://www.googleapis.com/auth/compute',
                    'https://www.googleapis.com/auth/cloud-platform'
                ]
            }]
        }
        if preemtive:
            config['scheduling'] = {'preemptible': True}

        return compute.instances().insert(
            project=project,
            zone=zone,
            body=config).execute()

    def get_node_info(self, name):
        inst = self.compute.instances().get(project=self.proj, zone=self.zn, instance=name).execute()
        return inst

    def show_instance(self, inst): # inst is a dictionary
        with self.printMutex:
            print json.dumps(inst, indent=4, sort_keys=True)

    ###################################### new implementation for creating node ########################################
    def create_disk(self, compute, project, zone, name, size):
        body = { "name": name,
                 "zone": "projects/infolinks-production/zones/us-east1-c",
                 "type": "projects/infolinks-production/zones/us-east1-c/diskTypes/pd-standard",
                 "sizeGb": str(size) }
        compute.disks().insert(project=project, zone=zone, body=body).execute()

        while True:
            response = compute.disks().get(project=project, zone=zone, disk=name).execute()
            if response['status'] == 'READY':
                break
            with self.printMutex:
                print 'disk ' + name + " not ready yet."
            time.sleep(3)


    def create_instance_from_script (self,compute, project, zone, name, preemtive=False):
        config = {
            'name': name,
            'machineType': 'zones/' + zone + '/machineTypes/n1-highmem-4', # 4 CPUs 26 GB memory     # previous: custom-12-32000  12 - CPUs, 31200 - 31200 MB = 32 GB


            'disks': [
                {
                    "type": "PERSISTENT",
                    "mode": "READ_WRITE",
                    'boot': True,
                    'autoDelete': True,
                    'initializeParams': {
                        'sourceImage': 'projects/centos-cloud/global/images/centos-7-v20171129',
                        "diskType": "projects/infolinks-production/zones/us-east1-c/diskTypes/pd-standard",
                        'diskSizeGb':  32,
                        'diskName': name
                    }
                },
                {
                    "type": "PERSISTENT",
                    "mode": "READ_WRITE",
                    'boot': False,
                    'autoDelete': True,
                    'source': "projects/infolinks-production/zones/us-east1-c/disks/%s-data" % name,
                    "deviceName": "%s-data" % name
                }
            ],

             'networkInterfaces': [{
                'network': 'global/networks/default',
                'accessConfigs': [
                    {'type': 'ONE_TO_ONE_NAT', 'name': 'External NAT'}
                ]
            }],

            'serviceAccounts': [{
                'email': 'default',
                'scopes': [
                    'https://www.googleapis.com/auth/devstorage.read_only',
                    'https://www.googleapis.com/auth/logging.write',
                    'https://www.googleapis.com/auth/monitoring.write',
                    'https://www.googleapis.com/auth/servicecontrol',
                    'https://www.googleapis.com/auth/service.management.readonly',
                    'https://www.googleapis.com/auth/trace.append',
                    'https://www.googleapis.com/auth/compute',
                    'https://www.googleapis.com/auth/cloud-platform'
                ]
            }],
            "metadata": {
                "items": [
                            {
                                "key": "startup-script-url",
                                "value": "gs://hd_worker_boot/scripts/gcloud_worker_boot.py"
                            },
                            {
                                "key": "serial-port-enable",
                                "value": 1
                            }
                ]
            }
        }
        if preemtive:
            config['scheduling'] = {'preemptible': True}

        return compute.instances().insert(
            project=project,
            zone=zone,
            body=config).execute()

    def add_node_from_script(self, name, preemptive=False):
        self.create_disk(self.compute, self.proj, self.zn, name + '-data', 128)
        self.create_instance_from_script(self.compute, self.proj, self.zn, name, preemptive)
        while not self.get_node_ready(name):
            with self.printMutex:
                print name + " not ready yet."
            time.sleep(3)
        '''
        all printed output of the new instance goes to serial port. this is by design. our gcloud_worker_boot.py will
        print BOOT_SCRIPT_FINISHED once it is finished. Bellow we will wait for this message in the instance port output
        as an indication that the new instance is ready.
        '''
        start = 0
        while True:
            ready_resp = self.compute.instances().getSerialPortOutput(project=self.proj, zone=self.zn, instance=name, port = 1, start = start).execute()
            ready = ready_resp['contents']
            if ready.find("BOOT_SCRIPT_FINISHED") != -1:
                break
            start = int(ready_resp['next'])
            with self.printMutex:
                print('%s --- %s: Waiting for gcloud_worker_boot.py to finish. Going to sleep for 3 seconds.'% (str(datetime.datetime.now()), name) )
            time.sleep(3)

        with self.printMutex:
            print('%s --- add_node_from_script finished' % name)

    ####################################################################################################################
if __name__ == '__main__':
    api = NodeMgr(threading.Lock())
    api.is_node_removed('hd-aaa-worker')










