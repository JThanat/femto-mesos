# Distributed Query Engine

This project of two part. The first part is the project accoding to the Query System Layout with Zookeeper and the other part is Mesos Framework Part.

This project is part of [Toward real-time data query systems in HEP Paper](https://arxiv.org/abs/1711.01229)

## Query System Layout

![query system layout](/docs/images/query_system_layout.png)



## Folder Structure

```bash
├── client.py               # Zookeeper Client 
├── compute                 # Zookeeper Compute Node
│   ├── executor.py         # Executor Class of Compute Node
│   ├── slave.py            #
│   ├── zk_compute.py       # System Compute Node
├── dispatcher              #
│   ├── dispatcher_exec.py  # Executor of Dispatcher
│   ├── dispathcer.py       # System Dispatcher
├── docs                    # document folder
├── executor                
│   └── executor.py         # Mesos Executor
├── framework               
│   ├── framework.py        # Mesos Framework
│   └── job.py              # Mesos Job State Class
├── master.py               # Mesos Master 
├── storage                 
│   ├── db.py               # MongoDB Connector Class
├── test_results.py         # Testing Result for distributed file
├── utilities               
└── zookeeper               #
    ├── job.py              # Job Status Class for ZK 
    └── zk_queue.py         # Zookeeper Queue Class
```

## Prerequisite
* Python 2.7+
* Zookeeper 3.4.10
* MongoDB 3.4
* Python Package
    * Pymongo – For connecting to MongoDB
    * Kazoo – For connecting to Zookeeper

## Run the project
### Run Query System
* Inside the root directory of the project there is a client.py file which is the file that test the system by producing job and put inside the queue of Dispatcher and then start the rest of the system
* The command here is `python client.py [enable_create] [delete_all_collection] [enable_dispatcher] [enable_slave] [enable_watcher]` The flag can be represented by 0(disable) and 1(enable). For example python client.py 1 1 1 1 1
    * `[Enable_create]` means to create path in zookeeper `/owned`, `/unowned`, `/done`. 
    * The second flag is `delete_all_collection` flag. It is used to flush mongodb database to test the system from nothing. 
    * `enable_dispatcher` is to enable dispatcher which is the class responsible for assigning subtasks. 
    * `enable_slave` will let the compute node start running and calculating the job.
    * And watcher which is the one returning done job enabled by specifying `enable_watcher` flag.

* The running log is saved in the running.log file. It can be used to debug behavior of the system and to check the result of the data.
* To create `/owned`, `/unowned`, `/done` path, the exist path needed to be removed first. To remove the node, it is necessary to remove all children nodes. It can be done in the /zookeeper/bin by typing `./zkCli.sh rmr /done && ./zkCli.sh rmr /owned && ./zkCli.sh rmr /unowned`
* It is possible to connect to zookeeper while running the system. To connect to the zookeeper, run `./zkCli.sh -server 127.0.0.1:2181` inside `zookeeper/bin`
* The number of subtasks can be changed by changing number_of_quries in [client.py](./client.py)

### Run Test
With the result from running.log, it is possible to check the result by checking from the log file. To check the result, run `python test_results.py` command and see the result. The sum of all data return should be equal to the amount of incoming request which we specified in the range.


## System Documents
[Kazoo-Zookeeper Basic Usage](./docs/zookeeper.md)

[Kazoo API Usage in the Project](./docs/zk_usage.md)

## Zookeeper and Kazoo References
* Zookeeper Concept
    * [Official Document](https://zookeeper.apache.org/doc/trunk/zookeeperOver.html)
    * [Easy Understading Version](http://www.tutorialspoint.com/zookeeper/)
* Kazoo API Docs
    * It is good to see some further API usage in the [official docs](https://kazoo.readthedocs.io/en/latest/)
    * A better understanding of how to use Kazoo API could be found in the [Kazoo Recipe](https://github.com/python-zk/kazoo/tree/master/kazoo/recipe). It consists of barrier, cache, counter, election, lease, lock, partitioner, party, queue, watcher
