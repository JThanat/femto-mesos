from kazoo.client import KazooClient
from kazoo.client import KazooState
from kazoo.client import KeeperState

@zk.add_listener
def watch_for_ro(state):
    if state == KazooState.CONNECTED:
        if zk.client_state == KeeperState.CONNECTED_RO:
            print("Read only mode!")
        else:
            print("Read/Write mode!")


zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()
watch_for_ro(zk.state)