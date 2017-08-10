# Zookeeper
## Zookeeper Setup
### Standalone Version
* Download Zookeeper version 3.4.10 from this the official site or this [link](http://www-eu.apache.org/dist/zookeeper/) and unzip the file 
* Create conf/zoo.cfg to define configuration file. The simple one would look like this. 
```bash
# the basic time unit in milliseconds. It is used to do heartbeats 
# and the minimum session timeout will be twice the tickTime.
tickTime=2000
# the location to store the in-memory database snapshots and, 
# unless specified otherwise, the transaction log of updates to the database.
dataDir=/var/lib/zookeeper
# the port to listen for client connections
clientPort=2181
```
* In the zookeeper folder, change directory into bin folder and start zookeeper by typing `./zkServer.sh start` to start Zookeeper server
* To check zookeeper connection, try connecting to the zookeeper using `./zkCli.sh –server 127.0.0.1:port_number_specified_in_conf_file`

## Kazoo-Zookeeper Basic Usage and Implementation
### Kazoo
From the document of Kazoo it states that Kazoo is a Python library designed to make working with Zookeeper a more hassle-free experience that is less prone to errors.

### How to connect to Zookeeper
The simplest way to connect to Zookeeper is to connect without any option. It is import tant to start `Zookeeper(zk.start())` before starting using API in the Zookeeper. Without a proper start, a `ConnectionClosedError` will be thrown. 

```python
from kazoo.client import KazooClient
zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()
```
### CRUD
#### Create (path, value(string)): return created path

The example of job owning could be found in the `own_job` function in the [zk_compute.py](../compute/zk_compute.py) To create a new node in the Zookeeper, client object is needed. Client object is the Zookeeper from `KazooClient()`. `path` object is the key to be accessed to Zookeeper. `value` is the value to store in Zookeeper. It should be in string format. `sequence=True` is to let Zookeeper append a unique sequence to the entry name according to the create order. Thus, an older node will have less sequence number. We can get the path with appended sequence to the newly created node from return value.

```python
path = '{path}/{prefix}{priority:03d}-{dataset}:{groupid}-'.format(
            path=self.owned_path, prefix=self.prefix, priority=priority,
            dataset=value_dict.get("dataset"),
            groupid=value_dict.get("groupid")
        )
final_val = json.dumps(value_dict)
self.running_job_path.put(self.client.create(path, final_val, sequence=True))
```
#### Update: client.set(path, value)
An example of update method is in the update_state function in the Slave Class in [zk_compute.py](../compute/zk_compute.py). `Client.set()` is used for updating the data to the existing node. `client.retry` function is a retry helper used for retrying the function passed into `client.retry`. Further information about retry will be described in the [Error Retry Section](#rror-retry).

```python
else:
    self.client.retry(self.client.set, current_job_path, job_updated)
```

#### Read: client.get(path) : return data, node status
`client.get()` is the most function in Zookeeper. This function return the data stored in the Zookeeper path, the value of Zookeeper key-value store.
To get the data a single line of code `client.get(path)` is enough to get the return data. 

However when getting the value, it should be done using either helper retry or custom retry function because there might be the case that Node does not exist due an creating and deleting from other client. An example can be found in the [Slave](../compute/zk_compute.py) `_inner_get` function. It is possible that we are getting the node that does not exist. If this is the case, then `NoNodeExist` Error will be thrown. 

```python
def get(client, path):  
    return client.retry(_inner_get, path)

def _inner_get(path):
    try:
        data, stat = self.client.get(path)
    except NoNodeError:
        # the first node has vanished in the meantime, try to
        # get another one
        raise ForceRetryError()
    return data
```

#### Delete: client.delete(path)

## Error Retry








