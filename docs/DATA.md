Uploading Data
--------------

There are 3 sources to upload data from. You may upload pre-existing HFiles
on HDFS/S3 or directly write HFiles from a running mapreduce job into
Terrapin. Pre-existing HFiles are copied to Terrapin through a distcp job.

Generating HFiles
-----------------

To generate HFiles, use the [HFileOutputFormat](../hadoop2/src/main/java/com/pinterest/hadoop/HFileOutputFormat.java)
provided in the hadoop2 module, in your mapreduce/cascading jobs.

Note that the output key must be BytesWritable. You must have
the snappy native library installed and loaded in your Hadoop workers
to generate HFiles with snappy compression.

Loading directly from Mapreduce Job
-----------------------------------

You can use the [HadoopLoaderJob](../hadoop2/src/main/java/com/pinterest/hadoop/HadoopLoaderJob.java)
to wrap around your mapreduce job. You can take a look at this example
[WordCount](../hadoop2/src/main/java/com/pinterest/hadoop/examples/WordCount.java) program. It
computes the word count and then writes the mapping from words->count(s) as hfiles
to terrapin. Note that we use NONE as the value for hfile.compression but if you have
native snappy properly installed and loaded in your hadoop setup, you may use
SNAPPY.

```
TERRAPIN_CLASSPATH=<Path to extracted maven assembly for hadoop2 module>
HADOOP_CONF_PATH=<Path to hadoop configs such as core-site.xml/yarn-site.xml>
HADOOP_HOME=<Path to hadoop home directory>
CLASSPATH=$TERRAPIN_CLASS_PATH/terrapin-hadoop2-0.1-SNAPSHOT.jar:$TERRAPIN_CLASS_PATH/lib/*:$HADOOP_CONF_PATH:$HADOOP_HOME/share/hadoop/tools/lib/*
TERRAPIN_CLASSPATH=<Path to extracted maven assembly for hadoop2 module>
java -Dhfile.compression=NONE                \
     -Dterrapin.zk_quorum=hadoop1testnn:2181 \
     -Dterrapin.cluster=test                 \
     -Dterrapin.fileset=count                \
     -cp $CLASSPATH com.pinterest.terrapin.hadoop.examples.WordCount \
     -libjars /home/varun/terrapin/lib/hbase-hadoop2-0.94.7.jar <Input Text Files>
```

Uploading from S3
-----------------

Make sure that the Hadoop cluster on which you are running the job
has the fs.s3n.awsAccessKeyId and fs.s3n.awsSecretAccessKey setup
appropriately. To upload data from S3, you need to specify the zookeeper
quorum, name of the terrapin cluster (same as helix_cluster),
name of the fileset, the s3 bucket and the key prefix to locate
the files.

```
TERRAPIN_CLASSPATH=<Path to extracted maven assembly for hadoop2 module>
HADOOP_CONF_PATH=<Path to hadoop configs such as core-site.xml/yarn-site.xml>
HADOOP_HOME=<Path to hadoop home directory>
CLASSPATH=$TERRAPIN_CLASS_PATH/terrapin-hadoop2-0.1-SNAPSHOT.jar:$TERRAPIN_CLASS_PATH/lib/*:$HADOOP_CONF_PATH:$HADOOP_HOME/share/hadoop/tools/lib/*
java -Dterrapin.zk_quorum=terrapintestzk:2181 \
     -Dterrapin.cluster=test                  \
     -Dterrapin.fileset=test1                 \
     -Dterrapin.s3bucket=my_bucket            \
     -Dterrapin.s3key_prefix=hfiles/part      \
     -cp $CLASSPATH
    com.pinterest.terrapin.hadoop.S3Uploader
```

Uploading from HDFS
-------------------

You can also upload pre-generated HFiles from a directory, on an
HDFS cluster. The process is very similar to the loading data
from S3. Instead of specifying the S3 bucket and key prefix,
you need to specify the HDFS directory where the HFiles reside
(terrapin.hdfs_dir)

```
TERRAPIN_CLASSPATH=<Path to extracted maven assembly for hadoop2 module>
HADOOP_CONF_PATH=<Path to hadoop configs such as core-site.xml/yarn-site.xml>
HADOOP_HOME=<Path to hadoop home directory>
CLASSPATH=$TERRAPIN_CLASS_PATH/terrapin-hadoop2-0.1-SNAPSHOT.jar:$TERRAPIN_CLASS_PATH/lib/*:$HADOOP_CONF_PATH:$HADOOP_HOME/share/hadoop/tools/lib/*
java -Dterrapin.zk_quorum=terrapintestzk:2181       \
     -Dterrapin.cluster=test                        \
     -Dterrapin.fileset=test1                       \
     -Dterrapin.hdfs_dir=hdfs://hdfsnamenode/hfiles \
     -cp $CLASSPATH
    com.pinterest.terrapin.hadoop.HdfsUploader
```
