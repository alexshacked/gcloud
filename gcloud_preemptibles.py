from gcloud_api import NodeMgr
from gcloud_mr_node import MRClusterAdmin
from gcloud_mr_node import fix_all_etchosts
from time import sleep
from os.path import expanduser
import os
import datetime
import threading


class Preemptor:
    def __init__(self):
        # constants
        self.MATSEVA = 75
        self.DEPENDABLES = 5
        self.ROOT_DIR = 'preempt_logs'
        self.WORKER_PREFIX = 'hd-worker'
        self.ADMIN = 'admin'
        self.LOCK = 'lock.txt'

        # members
        self.printLock = threading.Lock()
        self.nmg = NodeMgr(self.printLock)
        self.mr_admin = MRClusterAdmin()

    def who_is_missing(self):
        workers = self.nmg.get_workers(self.WORKER_PREFIX)
        present = len(workers)
        if present == self.MATSEVA:
            return []

        numbers = set([int(w.strip(self.WORKER_PREFIX)) for w in workers])
        missing = [ n for n in range(1, self.MATSEVA + 1) if n not in numbers ]
        return missing

    def who_is_terminated(self):
        term = self.nmg.get_terminated_workers(self.WORKER_PREFIX)
        numbers = [int(w.strip(self.WORKER_PREFIX)) for w in term]
        numbers_final = [n for n in numbers if n <= self.MATSEVA]
        return numbers_final

    def get_worker_dir(self, worker):
        home_dir = self.get_home_sdir()
        worker_dir = '/'.join([home_dir, worker])
        return self.get_dir(worker_dir)

    def get_home_sdir(self):
        home = expanduser("~")
        full = '/'.join([home, self.ROOT_DIR])
        return self.get_dir(full)

    def get_dir(self, dir):
        exist = os.path.isdir(dir)
        if not exist:
            os.makedirs(dir)
        return dir

    def now(self):
        return  datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S:   ")

    def log(self, worker, msg):
        file_name = 'history.log'
        worker_dir = self.get_worker_dir(worker)
        full = '/'.join([worker_dir, file_name])
        file = open(full, 'a')
        file.write(self.now() + msg + '\n')
        file.close()

    def log_2_admin(self, msg):
        print(msg)
        self.log(self.ADMIN, msg)

    def log_2_worker(self, worker, msg):
        self.log_2_admin(msg)
        self.log(worker, msg)

    def take_the_wheel(self):
        dir = self.get_worker_dir(self.ADMIN)
        full = '/'.join([dir, self.LOCK])
        exist = os.path.isfile(full)
        if exist:
            prm.log_2_admin("A previous execution has not finished yet. Will let it finish. Current cronnable will not continue.")
            return False

        f = open(full, 'w')
        f.write('locked execution for any other cronnable')
        f.close()
        return True

    def release_the_wheel(self):
        dir = self.get_worker_dir(self.ADMIN)
        full = '/'.join([dir, self.LOCK])
        os.remove(full)

class HdWorkerThread(threading.Thread):
    def __init__(self, hd_worker, prm, preempt):
        self.hd_worker  = hd_worker
        self.prm = prm
        self.preempt = preempt
        threading.Thread.__init__(self)

    def run(self):
       self.prm.mr_admin.new_node(node=self.hd_worker, data_disk_image='hd-worker-image-data', preemptive=self.preempt)

if __name__ == '__main__':
    prm = Preemptor()
    if not prm.take_the_wheel():
        exit(0)

    try:
        miss = prm.who_is_missing()
        term = prm.who_is_terminated()
        all = miss + term
        msg = 'BEGINNING CYCLE -- KEEPING-WORKERS-ALIVE --. expected number of workers: %d. up: %d. terminated: %d, will resurrect: %d' % (prm.MATSEVA, prm.MATSEVA - len(miss), len(term), len(term) + len(miss))
        prm.log_2_admin(msg)

        if len(miss) > 0:
            slist = ['%s%d' % (prm.WORKER_PREFIX, m) for m in miss]
            flat = ','.join(slist)
            msg = "The list of removed workers: " + flat
            prm.log_2_admin(msg)

        if len(term) > 0:
            slist = ['%s%d' % (prm.WORKER_PREFIX, m) for m in term]
            flat = ','.join(slist)
            msg = "The list of terminated workers: " + flat
            prm.log_2_admin(msg)

        # first remove terminated nodes
        for m in term:
            worker = '%s%d' % (prm.WORKER_PREFIX, m)
            if m > prm.DEPENDABLES:
                msg = '%s is a preemtive host and was stopped by google. now we will remove node from cluster' % worker
            else:
                msg = '%s was stopped from the google console. now we will remove node from cluster' % worker
            prm.log_2_worker(worker, msg)

            prm.mr_admin.remove_node(worker)
        # second remove redundant meta data of the deleted nodes
        for m in miss:
            worker = '%s%d' % (prm.WORKER_PREFIX, m)
            msg = '%s was removed from google console. now we will erase its presence from all cluster players' % worker
            prm.log_2_worker(worker, msg)

            prm.mr_admin.remove_node_data_from_cluster(worker)

        if len(all) > 0:
           nap = 40
           prm.log_2_admin("Going to sleep for %d seconds before recreating instances. Give google time to update removed instances state." % nap)
           sleep(nap)
           prm.log_2_admin("Woke up. Starting to recreate workers.")

        threads = []
        # now recreate the nodes
        for m in all:
            worker = '%s%d' % (prm.WORKER_PREFIX, m)
            scheduling = 'dependable' if m <= prm.DEPENDABLES else 'preemptible'
            msg = 'recreate %s node: %s' % (scheduling, worker)
            prm.log_2_worker(worker, msg)

            preempt = False if m <= prm.DEPENDABLES else True
            t = HdWorkerThread(worker, prm, preempt)
            t.start()
            threads.append(t)

        for t in threads:
            t.join()
            with prm.mr_admin.printMutex:
                prm.log_2_admin('%s: creation on %s finished' % (str(datetime.datetime.now()), t.hd_worker))


        prm.log_2_admin("END OF CYCLE")
        prm.release_the_wheel()
    except Exception as e:
        prm.release_the_wheel()
        prm.log_2_admin(repr(e))