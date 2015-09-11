# Cluster Setup

A minimum of three parameters are required for a terrapin cluster:
  - Cluster name (helix_cluster)
  - The HDFS namenode (hdfs_namenode)
  - Zookeeper quorum (zookeeper_quorum)

Specify the above parameters through a properties file. A sample
properties file is shown below:

```
zookeeper_quorum=terrapintestzk:2181
hdfs_namenode=terrapintestnn
helix_cluster=test
```

The cluster state is stored in the /test znode in zookeeper.

## Deploying

For each module (server/controller/client), a tar.gz file is built
in the taThe TERRAPIN_HOME_DIR variable should point to this
directory.file and the libraries under a lib/ folder.
This file is generated as part of the maven assembly. The run scripts
provided require a pointer to the directory containing the contents
of this tar file. The TERRAPIN_HOME_DIR variable should point to this
directory.

## Controller Setup

Start the controller. This can be done on any node (preferably the
namenode). A sample start stop script can be found [here](../controller/src/main/scripts/run_controller_sample.sh)
You will need to make the following changes according to your setup:
  - Modify TERRAPIN_HOME_DIR to point to the directory containing extracted jars
  - Modify TERRAPIN_CONFIG to point to your properties file 
  - Modify GC_LOG_DIR, RUN_DIR
  - Modify logging directory in log4.controller.properties

```
# Copy jars to controller (typically the namenode).
rsync controller/target/terrapin-controller-0.1-SNAPSHOT-bin.tar.gz \
    $CONTROLLER_HOST:$TERRAPIN_HOME_DIR/
ssh $CONTROLLER_HOST
cd $TERRAPIN_BASE_DIR
tar -xvzf terrapin-controller-0.1-SNAPSHOT-bin.tar.gz

# Start the terrapin controller.
scripts/run_controller_sample.sh start

# Stop the terrapin controller.
scripts/run_controller_sample.sh stop 
```

## Server Setup

Start the server on all data nodes. A sample start stop
script can be found [here](../server/src/main/scripts/run_server_sample.sh)
You may need to make the following changes according to your setup:
  - Modify TERRAPIN_HOME_DIR to point to the directory containing extracted jars
  - Modify TERRAPIN_CONFIG to point to your properties file 
  - Modify GC_LOG_DIR, RUN_DIR
  - Modify logging directory in log4.server.properties

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
you could start the terrapin thrift server. The thrift interface can
be found at [TerrapinService.thrift](../core/src/main/thrift/TerrapinService.thrift). The thrift server is located
in the client package. A thrift server can connect to multiple
terrapin clusters on a single zookeeper quorum. The thrift server requires
a properties file as below:

```
zookeeper_quorum=terrapintestnn:2181
# Comma separated list of terrapin clusters on the zookeeper quorum.
cluster_list=test
# Port number on which the terrapin servers are listening.
thrift_port=9010
```

A sample start stop script can be found [here](../client/src/main/scripts/run_thrift_sample.sh).
You may need to make the following changes according to your setup:
  - Modify TERRAPIN_HOME_DIR to point to the directory containing extracted jars
  - Modify TERRAPIN_CONFIG to point to your properties file 
  - Modify GC_LOG_DIR, RUN_DIR
  - Modify logging directory in log4.thrift.properties

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
