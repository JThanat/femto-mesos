from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.client import KeeperState
from kazoo.recipe.queue import Queue

zk = KazooClient(hosts='127.0.0.1:2181')

@zk.add_listener
def watch_for_ro(state):
    if state == KazooState.CONNECTED:
        if zk.client_state == KeeperState.CONNECTED_RO:
            print("Read only mode!")
        else:
            print("Read/Write mode!")

zk.start()
q = Queue(zk,'/zookeeper/queue')
q.put('Hello')

from zk_queue import Queue as myQueue

q2 = myQueue(zk, '/zookeeper/queue')
q.put('Hello From My Queue')