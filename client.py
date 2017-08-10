import sys
from kazoo.client import KazooClient
from compute.zk_compute import *
from dispatcher.dispathcer import Dispatcher
from dispatcher.dispathcer import Watcher
from storage.db import MongoInitializer


def main():
    enable_create = sys.argv[1]
    delete_all_collection = sys.argv[2]
    enable_dispatcher = sys.argv[3]
    enable_slave = sys.argv[4]
    enable_watcher = sys.argv[5]

    zk = KazooClient(hosts='127.0.0.1:2181')
    zk.start()

    if enable_create == '1':
        if zk.exists('/unowned'):
            print "deleting /unowned"
            zk.delete('/unowned', recursive=True)

        if zk.exists('/done'):
            print "deleting /done"
            zk.delete('/done', recursive=True)

        if zk.exists('/owned'):
            print "deleting /owned"
            zk.delete('/owned', recursive=True)

        zk.create('/unowned', "unowned branch")
        zk.create('/done', "done branch")
        zk.create('/owned', "owned branch")

    if delete_all_collection == '1':
        mongodb = MongoInitializer()
        results = mongodb.collection
        result = results.delete_many({})
        print "Flushing Storage"
        print "deleted: " + str(result.deleted_count) + " documents"


    slave = Slave(zk, "/")
    dispatcher = Dispatcher(client=zk)
    watcher = Watcher(zk, dispatcher.work_queue)

    if enable_dispatcher == '1':
        prefix = "entry-"
        priority = 100

        work_queue = dispatcher.work_queue
        dispatcher.start()

        number_of_queries = 4096

        for i in range(number_of_queries):
            d = {}
            d["dataset"] = i % 5
            d["groupid"] = i % 5
            work_queue.put(d)

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

if __name__ == "__main__":
    main()
