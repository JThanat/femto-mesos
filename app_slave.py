from compute.slave import Slave
from queue import *

if __name__ == "__main__":
    print("=============Testing compute=============")
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
    s.start()
    s.join()
    print("=============End of compute Testing=============")


