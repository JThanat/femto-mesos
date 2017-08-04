import sys
from kazoo.client import KazooClient
from compute.zk_compute import *
from dispatcher.dispathcer import Dispatcher
from dispatcher.dispathcer import Watcher

enable_create = sys.argv[1]
enable_dispatcher = sys.argv[2]
enable_slave = sys.argv[3]
enable_watcher = sys.argv[4]

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

if enable_create == '1':
    zk.create('/unowned', "unowned branch")
    zk.create('/done', "done branch")
    zk.create('/owned', "owned branch")

slave = Slave(zk, "/")
dispatcher = Dispatcher(client=zk)
watcher = Watcher(zk, dispatcher.work_queue)

if enable_dispatcher == '1':
    prefix = "entry-"
    priority = 100

    dispatcher.start()
    work_queue = dispatcher.work_queue

    for i in range(16):
        d = {}
        d["dataset"] = i % 5
        d["groupid"] = i % 5
        work_queue.put(d)

        # d["state"] = Jobstate.PENDING
        # d["worker_node"] = None
        # json_str = json.dumps(d)
        # path = '{path}/{prefix}{priority:03d}-{dataset}:{groupid}-'.format(
        #     path="/unowned",
        #     prefix=prefix,
        #     priority=priority,
        #     dataset=d["dataset"],
        #     groupid=d["groupid"]
        # )

if enable_slave == '1':
    slave.start()

if enable_watcher == '1':
    watcher.start()

if enable_dispatcher == '1':
    dispatcher.join()

if enable_slave == '1':
    slave.join()

if enable_watcher == '1':
    watcher.join()
