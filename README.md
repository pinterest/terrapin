**Note:** This project is no longer actively maintained by Pinterest.

---

# Terrapin: Serving system for Hadoop generated data

Terrapin is a low latency serving system providing random
access over large data sets, generated by Hadoop jobs and
stored on HDFS clusters.

Terrapin can ingest data from S3, HDFS or directly from a mapreduce job.
Terrapin is elastic, fault tolerant and performant enough to be used for
various web scale applications (such as serving personalized recommendations
on a website). Terrapin exposes a key-value data model.

Terrapin achieves these goals by storing the output of mapreduce
jobs on HDFS in a file format that allows fast random access. A Terrapin
server process runs on every data node and serves the files stored
on that data node. With this design, we get the scalability of HDFS and Hadoop
and also, achieve low latencies since the data is being served
from local disk. HDFS optimizations such as [short-circuit local reads],
OS page cache and possibly mmap reduce the tail latency by avoiding
round trips over a TCP socket or the network for HDFS reads. A Terrapin controller
is responsible for ensuring data locality.

If you already have an HDFS cluster running, very little needs to be done to
setup Terrapin. If you are interested in the detailed design, check out [DESIGN.md](docs/DESIGN.md)

## Key Features

  - **Filesets**: Data on Terrapin is namespaced in filesets. New data is
loaded/reloaded into a fileset. A Terrapin cluster can host multiple filesets.
  - **Live Swap and Multiple versions**: New data is loaded into an existing
fileset with a live swap and there's no disruption of service. We also
support keeping multiple versions for critical filesets. This allows quick
rollback in case of bad data load. Old versions are garbage collected.
  - **S3/HDFS/Hadoop/Hive**: A Hadoop job can directly write data to Terrapin.
Otherwise, the Hadoop job can write data to HDFS/S3 and it can be ingested by
Terrapin in a subsequent step. Terrapin can also ingest tables on Hive and
provide random access based on a certain column, marked as the key.
  - **Easy to change number of output shards**: It is easy to change the number
of output shards across different versions of data loaded for the same fileset.
This gives developers the flexibility of tuning their mapreduce job by tweaking
the number of reducers.
  - **Extensible serving/storage formats**: It is possible to plug in other
(more efficient) serving formats such as rocksdb .sst etc. Currently, Terrapin
uses HFiles as the serving file format.
  - **Monitoring**: Terrapin exports latency and value size quantiles as well as
cluster health stats through an HTTP interface. The metrics are exported through
[Ostrich].
  - **Speculative Execution**: Terrapin comes with a [client abstraction] which can
issue concurrent requests against two terrapin clusters serving the same fileset and pick the
one that is satisfied earlier. This functionality is pretty handy for increased
availability and lower latency.

## Getting Started

Java 7 is required in order to build terrapin. Currently Terrapin supports
Hadoop 2. In order to build, run the following commands from the
root of the git repository (note that hbase compiled with Hadoop 2 is not available
in the central maven repo but is required for using HFiles).

```
git clone [terrapin-repo-url]
cd terrapin

# Install HBase 0.94 artifacts compiled against Hadoop 2.
mvn install:install-file \
  -Dfile=thirdparty/hbase-hadoop2-0.94.7.jar \
  -DgroupId=org.apache.hbase \
  -DartifactId=hbase-hadoop2 \
  -Dversion=0.94.7 \
  -Dpackaging=jar

# Building against default Hadoop version - 2.7.1
mvn package

# Building against custom Hadoop version you are using (if different from 2.7.1)
mvn [-Dhadoop.version=X -Dhadoop.client.version=X] package
```

To setup a terrapin cluster, follow the instructions at [SETUP.md](docs/SETUP.md).

## Usage

Terrapin can ingest data written to S3/HDFS or it can directly ingest data
from a MapReduce job.

Once you have your cluster up and running, you can find several usage examples
at [USAGE.md](docs/USAGE.md).

## Tools

Terrapin has tools for querying filesets and performing administrative
operations such as deleting, rolling back a fileset and diagnosing cluster
health.

##### Querying a Fileset

Run the following command from the root of your git repo.

```
java -cp client/target/*:client/target/lib/*        \
    -Dterrapin.config={properties_file}             \
    com.pinterest.terrapin.client.ClientTool {fileset} {key}
```

##### Deleting a Fileset

```
ssh ${CONTROLLER_HOST}
cd ${TERRAPIN_HOME_DIR}
scripts/terrapin-admin.sh deleteFileSet {PROPERTIES_FILE} {FILE_SET}
```

Note that the deletion of a Fileset is asynchronous. The Fileset is marked for
deletion and is later garbage collected by the controller.

##### Rolling back a Fileset

```
ssh ${CONTROLLER_HOST}
cd ${TERRAPIN_HOME_DIR}
scripts/terrapin-admin.sh rollbackFileSet {PROPERTIES_FILE} {FILE_SET}
```

The tool will display the different versions (as HDFS directories), you can rollback to.
Select the appropriate version and confirm. To utilize this functionality, the fileset
must have been uploaded with multiple versions as described in [USAGE.md](docs/USAGE.md). 

##### Checking health of a cluster

``` 
ssh ${CONTROLLER_HOST}
cd ${TERRAPIN_HOME_DIR}
scripts/terrapin-admin.sh {PROPERTIES_FILE} checkClusterHealth
```

The tool will display any inconsistencies in ZooKeeper state or any filesets
not serving properly.

## Monitoring/Diagnostics

You can access the controllers web UI at **http://{controller_host}:50030/status**.
The port can be modified by setting the "status_server_binding_port" property. It
will show all the filesets on the cluster and their serving health. You can
click a fileset to get more information about it (like the current serving version
and partition assignment).

You can also retrieve detailed metrics by running **curl localhost:9999/stats.txt**
on the controller or the server. These metrics are exported using Twitter's [Ostrich]
library and are easy to parse. The port can be modified by setting the
"ostrich_metrics_port" property. The controller will export serving health across
the whole cluster (percentage of online shards for each fileset) amongst other
useful metrics. The server will export latency and value size percentiles for each fileset.

## Maintainers
  - [Jian Fang](https://github.com/fangjian601)
  - [Varun Sharma](https://github.com/varunsharmagit)

## Help

If you have any questions or comments, you can reach us at [terrapin-users@googlegroups.com](https://groups.google.com/forum/#!forum/terrapin-users)

[Ostrich]:https://github.com/twitter/ostrich
[client abstraction]:client/src/main/java/com/pinterest/terrapin/client/ReplicatedTerrapinClient.java
[short-circuit local reads]:http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/ShortCircuitLocalReads.html
