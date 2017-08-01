from kazoo.client import KazooClient

zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()
# @zk.add_listener
# def watch_for_ro(state):
#     if state == KazooState.CONNECTED:
#         if zk.client_state == KeeperState.CONNECTED_RO:
#             print("Read only mode!")
#         else:
#             print("Read/Write mode!")
#
# zk.start()
# q = Queue(zk,'/zookeeper/queue')
# q.put('Hello')
#
# from zk_queue import Queue as myQueue
#
# q2 = myQueue(zk, '/zookeeper/queue')
# q.put('Hello From My Queue')

#zk.create('owned', "owned branch")
#zk.create('unowned', "unowned branch")

from zookeeper.job import Jobstate
import json

prefix = "entry-"
priority = 100

from dispatcher.dispathcer import Dispatcher

dispatcher = Dispatcher(client=zk)
dispatcher.start()

work_queue = dispatcher.work_queue

for i in range(8):
    d = {}
    d["dataset"] = i % 4
    d["groupid"] = i % 4
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

from compute.zk_compute import *

# slave = Slave(zk,"/")
# slave.start()
# slave.join()
