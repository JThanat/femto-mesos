from Slave.slave import Slave
from queue import *

if __name__ == "__main__":
    q = Queue()
    for i in range(4):
        task = {}
        task["groupid"] = str(i)
        task["dataset"] = "test"
        q.put(task)

    for i in range(5):
        task = {}
        task["groupid"] = str(i)
        task["dataset"] = "test"
        q.put(task)

    s = Slave(1,q,4,name="Slave1")

    print("=============Testing Slave=============")
    s.start()
    s.join()

