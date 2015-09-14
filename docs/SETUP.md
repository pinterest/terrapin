# Cluster Setup

Terrapin runs on top of an existing HDFS cluster and requires a running ZooKeeper
quorum for storing cluster state. Multiple terrapin clusters can share the same
ZooKeeper quorum. A Terrapin cluster is configured through a properties file. A sample
properties file is shown below:

```
# ZooKeeper quorum for storing cluster state
zookeeper_quorum=terrapintestzk:2181

# HDFS namenode for backing HDFS cluster.
hdfs_namenode=terrapintestnn

# Name of the terrapin cluster.
helix_cluster=test
```

Please make sure that the user running the Terrapin server process has access
to perform short circuit local reads against HDFS. More details on setting up
short circuit local reads can be found [here](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/ShortCircuitLocalReads.html). 

## Customizing your setup

Terrapin comes with sample run scripts and properties files which can
be customized to your setup. Each run script ([run_controller_sample.sh],
[run_server_sample.sh],[run_thrift_sample.sh]) contains environment variables
which you may modify according to your setup. Its highly likely
that you would need to modify the following variables:
  - TERRAPIN_HOME_DIR: Directory containing extracted jars.
  - TERRAPIN_CONFIG: Your cluster properties file
  - GC_LOG_DIR: Directory for writing GC logs
  - RUN_DIR: Directory for writing PID files

Also, you will need to modify [log4j.controller.properties],
[log4j.server.properties] and [log4j.thrift.properties] to point to
the desired directories for the application logs for the controller,
server and thrift server respectively. 

## Controller Setup

The controller can be setup on any node (preferably, run on the HDFS
namenode). The sample start stop script can be found [here](../controller/src/main/scripts/run_controller_sample.sh).

```
# Copy jars to controller (typically the namenode).
rsync controller/target/terrapin-controller-0.1-SNAPSHOT-bin.tar.gz \
    $CONTROLLER_HOST:$TERRAPIN_HOME_DIR/
ssh $CONTROLLER_HOST
cd $TERRAPIN_HOME_DIR
tar -xvzf terrapin-controller-0.1-SNAPSHOT-bin.tar.gz

# Start the terrapin controller.
scripts/run_controller_sample.sh start

# Stop the terrapin controller.
scripts/run_controller_sample.sh stop 
```

## Server Setup

Start the server on all data nodes. The sample start stop
script can be found [here](../server/src/main/scripts/run_server_sample.sh).

```
# Copy jars to each datanode.
rsync server/target/terrapin-server-0.1-SNAPSHOT-bin.tar.gz \
    $SERVER_HOST:$TERRAPIN_HOME_DIR/
ssh $SERVER_HOST
cd $TERRAPIN_HOME_DIR
tar -xvzf terrapin-server-0.1-SNAPSHOT-bin.tar.gz

# Start the terrapin server.
scripts/run_server_sample.sh start

# Stop the terrapin server
scripts/run_server_sample.sh stop
```

Now that you have a running cluster, check out [USAGE.md](USAGE.md)
for instructions on how to upload and query data.

## Thrift Server Setup

If you wish to access the data using a language other than java,
you can start the terrapin thrift server. The thrift interface
exposed by the thrift server can be found at [TerrapinService.thrift](../core/src/main/thrift/TerrapinService.thrift).
A thrift server can connect to multiple terrapin clusters on a single zookeeper
quorum. The thrift server requires a properties file as below:

```
zookeeper_quorum=terrapintestnn:2181
# Comma separated list of terrapin clusters on the zookeeper quorum.
cluster_list=test
# Port number on which the terrapin servers are listening.
thrift_port=9010
```

The sample start stop script can be found [here](../client/src/main/scripts/run_thrift_sample.sh).

```
# Copy jars to thrift server node.
rsync server/target/terrapin-client-0.1-SNAPSHOT-bin.tar.gz \
    $THRIFT_SERVER_HOST:$TERRAPIN_HOME_DIR/
ssh $THRIFT_SERVER_HOST
cd $TERRAPIN_HOME_DIR
tar -xvzf terrapin-client-0.1-SNAPSHOT-bin.tar.gz

# Start the terrapin thrift server.
scripts/run_thrift_sample.sh start

# Stop the terrapin thrift server.
scripts/run_thrift_sample.sh stop
```

[log4j.controller.properties]:../core/src/main/config/log4j.controller.properties
[log4j.server.properties]:../core/src/main/config/log4j.server.properties
[log4j.thrift.properties]:../core/src/main/config/log4j.thrift.properties
[run_server_sample.sh]:../server/src/main/scripts/run_server_sample.sh
[run_controller_sample.sh]:../controller/src/main/scripts/run_controller_sample.sh
[run_thrift_sample.sh]:../client/src/main/scripts/run_thrift_sample.sh
[log4j.controller.properties]:../core/src/main/config/log4j.controller.properties
[log4j.server.properties]:../core/src/main/config/log4j.server.properties
[log4j.thrift.properties]:../core/src/main/config/log4j.thrift.properties
