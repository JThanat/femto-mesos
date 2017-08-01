from kazoo.client import KazooClient
import sys

enable_create = sys.argv[1]
enable_dispatcher = sys.argv[2]
enable_slave = sys.argv[3]

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

if enable_create == '1':
    zk.create('owned', "owned branch")
    zk.create('unowned', "unowned branch")

if enable_dispatcher == '1':
    from dispatcher.dispathcer import Dispatcher

    prefix = "entry-"
    priority = 100

    dispatcher = Dispatcher(client=zk)
    dispatcher.start()

    work_queue = dispatcher.work_queue

    for i in range(8):
        d = {}
        d["dataset"] = i % 4
        d["groupid"] = i % 4
        work_queue.put(d)
        # dispatcher.join()

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
    from compute.zk_compute import *

    slave = Slave(zk,"/")
    slave.start()
    slave.join()
