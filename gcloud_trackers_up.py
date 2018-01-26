#!/usr/local/bin/python3

import os
import subprocess
import datetime
import re
import sys

class PollTrackers:
   def __init__(self):
      if len(sys.argv) < 2 :
         print('usage: />gcloud_trackers_up <number_of_workers>')
         exit(0)
      self.MATSEVA = int(sys.argv[1])

      print('ciao. There should be %s hd-workers' % self.MATSEVA)

   def who_is_missing(self):
      all_list = self.fetch_all()
      self.duplicates(all_list)

      missing = []
      for i in range(self.MATSEVA):
         wanted = i + 1

         wor = all_list[0] if len(all_list) > 0 else 'none'
         ind =  PollTrackers.get_index(wor) if wor !='none' else -1
         print('wanted: %d, hd_worker: %s, index: %d' % (wanted, wor, ind))

         if len(all_list) == 0:
            missing.append('hd-worker%s' % (wanted))
         elif PollTrackers.get_index(all_list[0]) == wanted:
            all_list.pop(0)
         else:
            missing.append('hd-worker%s' % (wanted))
      
      if len(missing) == 0:
         print('all tasktrackers up')
      else:
         print('The followig tasktrackers are down:')
         for m in missing: print(m)

   def show_all(self):
      all_list = self.fetch_all()
      for a in all_list: print a

   def fetch_all(self):
      cmd = 'hadoop job -list-active-trackers | awk -F: \'{print $1}\' | sort'
      all_bytes = self.doShell(cmd)
      all_flat = all_bytes.decode('utf-8')
      all_list = all_flat.split('\n')
      all_list = [l for l in all_list if len(l) > 2]
      all_list.sort(key = PollTrackers.get_index)
      return all_list

   def doShell(self, cmd):
      print("{0}:   doing - {1}".format(str(datetime.datetime.now()), cmd))
      p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True)
      (output, err) = p.communicate()
      status = p.wait()
      return output

   def get_index(worker):
      m = re.search(r'[0-9]+$', worker)
      if m == None or m.group(0) == None:
         idx = -1
      else:
         idx = int(m.group(0))
      return idx
   get_index = staticmethod(get_index)

   def duplicates(self, all_list):
      poirot = {}
      for a in all_list:
         poirot.setdefault(a, 0)
         poirot[a] += 1
      if len(all_list) != len(poirot):
         print("! THERE ARE DUPLICATES")
         for k in poirot.keys():
            if poirot[k] > 1:
               print("%s   %d" % (k, poirot[k]))
      else:
         print('JOB TRACKER OUTPUT CLEAN. NO DUPLICATES')


   def debug_make_input(self):
      input = ['tracker_hd-worker%d' % i for i in range(1, self.MATSEVA +1) ]
      return input

if __name__ == '__main__':
   hamal = PollTrackers()
   hamal.who_is_missing()

