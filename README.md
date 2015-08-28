Terrapin: Serving system for batch generated data sets
------------------------------------------------------

Terrapin provides a low latency serving layer providing random
access over large data sets, stored on HDFS clusters. A terrapin
server process runs on every data node and serves the files stored
on that data node. Since the data is local, terrapin can take advantage
of HDFS optimizations such as short circuit local reads and mmap.
A controller process is responsible for managing the cluster state.

Data served by terrapin is generated through mapreduce jobs.
Currently, Terrapin uses HFiles as the file format for random access
but its possible to plug in other file formats.

Dependencies
------------

Terrapin requires an HDFS cluster (Hadoop 2) for the actual data storage and
a zookeeper quorum for cluster co-ordination.

Building
--------

Java 7 is required in order to build terrapin. In order to build,
run the following commands from the root of the git repository (note
that hbase with hadoop 2 is not available in the central maven repo
but is required for using HFiles):

```
mvn install:install-file \
  -Dfile=thirdparty/hbase-hadp2-0.94.7.jar \
  -DgroupId=org.apache.hbase \
  -DartifactId=hbase-hadoop2 \
  -Dversion=0.94.7 \
  -Dpackaging=jar

mvn package
```

For cluster setup and usage, take a look at [SETUP.md](docs/SETUP.md)
