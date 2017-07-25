from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.client import KeeperState

zk = KazooClient(hosts='127.0.0.1:2181')

@zk.add_listener
def watch_for_ro(state):
    if state == KazooState.CONNECTED:
        if zk.client_state == KeeperState.CONNECTED_RO:
            print("Read only mode!")
        else:
            print("Read/Write mode!")

from zookeeper.queue import Queue

zk.start()
q = Queue(zk,'/zookeeper/queue')
q.put('Hello')