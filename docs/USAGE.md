# Usage examples

There are 3 sources to upload data from. You may upload pre-existing HFiles
on HDFS and S3 or directly write HFiles from a running mapreduce job into
Terrapin. Pre-existing HFiles are copied to Terrapin through a distcp job.

## Generating HFiles

To generate HFiles, use the [HFileOutputFormat](../hadoop2/src/main/java/com/pinterest/hadoop/HFileOutputFormat.java)
provided in the hadoop2 module, in your mapreduce/cascading jobs.

Note that the output key and value must be BytesWritable. You must have
the snappy native library installed and loaded on your Hadoop nodes 
if you want to generate HFiles with snappy compression.

## Setup

The required jars need to be copied to a node on your Hadoop cluster so that data
can be loaded to Terrapin. The jars are extracted to TERRAPIN_HOME_DIR.

```
# Copy terrapin hadoop2 jars to one of your Hadoop nodes.
rsync hadoop2/target/terrapin-hadoop2-0.1-SNAPSHOT-bin.tar.gz $HADOOP_NODE:${TERRAPIN_HOME_DIR}/
ssh $HADOOP_NODE
cd ${TERRAPIN_HOME_DIR}
tar -xvzf terrapin-hadoop2-0.1-SNAPSHOT-bin.tar.gz
```

The examples can then be executed, directly from the Hadoop node.

## Loading directly from Mapreduce Job

You can use the [HadoopLoaderJob](../hadoop2/src/main/java/com/pinterest/hadoop/HadoopLoaderJob.java)
to wrap around your mapreduce job. You can take a look at this example
[WordCount](../hadoop2/src/main/java/com/pinterest/hadoop/examples/WordCount.java) program. It
computes the word count and then writes the mapping from words->count(s) as hfiles
directly to terrapin. Note that we use NONE as the value for hfile.compression but if you have
native snappy properly installed and loaded in your Hadoop setup, you may use
SNAPPY to save space.

```
# Run from your Hadoop node.
HADOOP_CONF_DIR=<Path to Hadoop configs such as core-site.xml/yarn-site.xml>
HADOOP_HOME=<Path to Hadoop home directory>
CLASSPATH=$TERRAPIN_HOME_DIR/terrapin-hadoop2-0.1-SNAPSHOT.jar:$TERRAPIN_HOME_DIR/lib/*:$HADOOP_CONF_DIR:$HADOOP_HOME/share/hadoop/tools/lib/*
java -Dhfile.compression=NONE                 \
     -Dterrapin.zk_quorum=terrapintestzk:2181 \
     -Dterrapin.cluster=test                  \
     -Dterrapin.fileset=test                  \
     -cp $CLASSPATH com.pinterest.terrapin.hadoop.examples.WordCount \
     -libjars /home/varun/terrapin/lib/hbase-hadoop2-0.94.7.jar <Input Text Files>
```

## Uploading from S3

Make sure that the Hadoop cluster on which you are running the job
has the fs.s3n.awsAccessKeyId and fs.s3n.awsSecretAccessKey setup
appropriately. To upload data from S3, you need to specify the zookeeper
quorum, name of the terrapin cluster (same as helix_cluster),
name of the fileset, the s3 bucket (terrapin.s3bucket) and the key prefix
(terrapin.s3key_prefix) to locate the files.

```
# Run from your Hadoop node.
HADOOP_CONF_DIR=<Path to Hadoop configs such as core-site.xml/yarn-site.xml>
HADOOP_HOME=<Path to Hadoop home directory>
CLASSPATH=$TERRAPIN_HOME_DIR/terrapin-hadoop2-0.1-SNAPSHOT.jar:$TERRAPIN_HOME_DIR/lib/*:$HADOOP_CONF_DIR:$HADOOP_HOME/share/hadoop/tools/lib/*
java -Dterrapin.zk_quorum=terrapintestzk:2181 \
     -Dterrapin.cluster=test                  \
     -Dterrapin.fileset=test                  \
     -Dterrapin.s3bucket=my_bucket            \
     -Dterrapin.s3key_prefix=hfiles/part      \
     -cp $CLASSPATH \
    com.pinterest.terrapin.hadoop.S3Uploader
```

## Uploading from HDFS

You can also upload pre-generated HFiles from a directory on an
HDFS cluster. The process is very similar to loading data
from S3. You need to specify the HDFS directory where the HFiles reside
(terrapin.hdfs_dir)

```
# Run from your Hadoop node.
HADOOP_CONF_DIR=<Path to Hadoop configs such as core-site.xml/yarn-site.xml>
HADOOP_HOME=<Path to Hadoop home directory>
CLASSPATH=$TERRAPIN_HOME_DIR/terrapin-hadoop2-0.1-SNAPSHOT.jar:$TERRAPIN_HOME_DIR/lib/*:$HADOOP_CONF_DIR:$HADOOP_HOME/share/hadoop/tools/lib/*
java -Dterrapin.zk_quorum=terrapintestzk:2181       \
     -Dterrapin.cluster=test                        \
     -Dterrapin.fileset=test                        \
     -Dterrapin.hdfs_dir=hdfs://hdfsnamenode/hfiles \
     -cp $CLASSPATH \
    com.pinterest.terrapin.hadoop.HdfsUploader
```

## Advanced Uploading Options
  - **Versions**: For critical filesets which require rollback functionality, you can
specify the number of versions in serving to be more than one. At any point of time, only one
version is served. This is done by setting the "terrapin.num_versions" system property.
You can use the admin tool to rollback to a previous version.
  - **Partitioner**: Most mapreduce jobs write data using the HashPartitioner (called
MODULUS in Terrapin). However, cascading jobs use a slightly different hashing scheme
. The CASCADING partition conforms to this hashing scheme. The correct partitioner is needed for
terrapin to correctly hash the keys to the corresponding shards. The partitioner
can be specified using the system property "terrapin.partitioner" - accepted values are
MODULUS or CASCADING.

## Querying Data

You can use the java client library to query terrapin file sets. The
[TerrapinClient](../client/src/main/java/com/pinterest/terrapin/client/TerrapinClient.java) class
lets you connect to a terrapin cluster and issue queries against it. Check out
[ClientTool](../client/src/main/java/com/pinterest/terrapin/client/ClientTool.java) for
an example.

To access terrapin from languages other than java, you can setup thrift servers
as outlined in [SETUP.md](SETUP.md) and issue queries using the [Thrift interface](../core/src/main/thrift/TerrapinService.thrift).
