from gcloud_api import NodeMgr
import threading

printLock = threading.Lock()
mgr = NodeMgr(printLock)

def show_entries(type, exclude):
    if type == 'hd-master':
        mgr.set_zone("us-east1-b")
    workers = mgr.get_workers_data(type)
    if type == 'hd-master':
        mgr.reset_zone()

    pairs = {}
    for w in workers:
        name =  w['name']
        if name == exclude:
            continue
        if w['status'] == 'TERMINATED':
            continue
        ip = w['networkInterfaces'][0]['accessConfigs'][0]['natIP']
        pairs[name] = ip

    def show_one(ip, name):
        return '%-15s   %-15s   %s' % (ip, name, name + '.infolinks.com')

    if len(pairs) > 1:
        lst = [ show_one(pairs[name], name) for name in sorted(pairs.keys(), key = lambda name : int(name.lstrip(type)) if name.lstrip(type).isdigit() else 0) ]
        out = '\n'.join(lst)
    else:
        names = pairs.keys()
        out = show_one(pairs[names[0]], names[0])

    return out

def show_entries_internal(type, exclude):
    if type == 'hd-master':
        mgr.set_zone("us-east1-b")
    workers = mgr.get_workers_data(type)
    if type == 'hd-master':
        mgr.reset_zone()

    pairs = {}
    for w in workers:
        name =  w['name']
        if name == exclude:
            continue
        ip = w['networkInterfaces'][0]['networkIP']
        pairs[name] = ip

    def show_one(ip, name):
        return '%-15s   %-15s   %s' % (ip, name, name + '.infolinks.com')

    if len(pairs) > 1:
        lst = [ show_one(pairs[name], name) for name in sorted(pairs.keys(), key = lambda name : int(name.lstrip(type))) ]
        out = '\n'.join(lst)
    else:
        names = pairs.keys()
        out = show_one(pairs[names[0]], names[0])

    return out

def show_entries_internal_for_hbase(type, exclude):
    if type == 'hd-master':
        mgr.set_zone("us-east1-b")
    workers = mgr.get_workers_data(type)
    if type == 'hd-master':
        mgr.reset_zone()

    pairs = {}
    for w in workers:
        name =  w['name']
        if name == exclude:
            continue
        ip = w['networkInterfaces'][0]['networkIP']
        pairs[name] = ip

    def show_one(ip, name):
        return '%-15s   %-15s   %s' % (ip, name + '.infolinks.com', name)

    if len(pairs) > 1:
        lst = [ show_one(pairs[name], name) for name in sorted(pairs.keys(), key = lambda name : int(name.lstrip(type))) ]
        out = '\n'.join(lst)
    else:
        names = pairs.keys()
        out = show_one(pairs[names[0]], names[0])

    return out



if __name__ == '__main__':
    print show_entries_internal('hd-redmap', None)
    mgr.set_zone("us-east1-b")
    print show_entries_internal('hd-master', None)
    mgr.reset_zone()
    print show_entries('hd-worker', None)
    print show_entries_internal('hbase-master', None)
    print show_entries_internal('hbase-worker', None)

