# Distributed Query Engine

This project of two part. The first part is the project accoding to the Query System Layout with Zookeeper and the other part is Mesos Framework Part.

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

## System Documents
[Zookeeper](./docs/zookeeper.md)
