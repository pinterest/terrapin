Cluster Setup
-------------

Three parameters are required for a terrapin cluster:
  - Cluster name (helix_cluster)
  - The HDFS namenode (hdfs_namenode)
  - Zookeeper quorum (zookeeper_quorum)

Specify the above parameters through a properties file. A sample
file is shown below:

```
zookeeper_quorum=hadoop1testnn:2181
hdfs_namenode=hadoop1testnn
helix_cluster=test
```

In the above case, the cluster state is stored in the /test znode.

Deploying
---------

For each module (server/controller/client), a tar.gz file is built
in the target folder containing the .jar file and the libraries
under a lib/ folder in the tar file. This file is generated as part
of the maven assembly. The run scripts provided require a pointer
to the directory containing the contents of this tar file. The BASE_DIR
variable should be modified to point to the appropriate directory. See
this for an example.

Controller Setup
----------------

Start the controller. This can be done on any node (preferably the
namenode). A sample start stop script can be found [here](../controller/src/main/scripts/run_controller_sample.sh)

You may need to modify it to point to your new properties file.

Server Setup
------------

Start the server on each datanode. A sample start stop script can
be found [here](../server/src/main/scripts/run_server_sample.sh)

You may need to modify it to point to your new properties file.
Also you may choose to increase the JVM heap size depending on
available memory on your servers.

Usage
-----

Once the cluster is setup. You can upload/write data into filesets
(namespaces). Existing data can be uploaded or mapreduce jobs can
directly upload data into terrapin. 

There are 3 ways to upload data into terrapin. You can upload
pre-existing HFiles on an HDFS cluster or S3. You can also upload
data directly from a mapreduce job. These approaches are described in
[DATA.md](data.md)

You can query data in a fileset, using the java client library.
A code example is available [here](../client/src/main/java/com/pinterest/terrapin/client/ClientTool.java)

If you wish to access the data using a language other than java,
you could start the terrapin thrift server. The thrift server is located
in the client package. A thrift server can connect to multiple
terrapin clusters on a single zookeeper quorum. The thrift server requires
a properties file as below:

```
zookeeper_quorum=hadoop1testnn:2181
cluster_list=test
thrift_port=9010
```

A sample start stop script can be found [here](../client/src/main/scripts/run_thrift_sample.sh)
