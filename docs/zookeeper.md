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




