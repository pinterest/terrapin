Uploading Data
--------------

There are 3 sources to upload data from. You may upload pre-existing HFiles
on HDFS/S3 or directly write HFiles from a running mapreduce job into
Terrapin. Pre-existing HFiles are copied to Terrapin through a distcp job.

Generating HFiles
-----------------

To generate HFiles directly use the HFileOutputFormat provided in the
hadoop2 module, in your mapreduce/cascading jobs.

Note that the output key must be BytesWritable.

Uploading from S3
-----------------

Make sure that the Hadoop cluster on which you are running the job
has the fs.s3n.awsAccessKeyId and fs.s3n.awsSecretAccessKey setup
appropriately. To upload data from S3, you need to specify the zookeeper
quorum, name of the terrapin cluster (same as helix_cluster),
name of the fileset, the s3 bucket and the key prefix to locate
the files.

```
TERRAPIN_CLASSPATH=<Path to extract maven assembly for hadoop2 module>
HADOOP_CONF_PATH=<Path to hadoop configs such as core-site.xml/yarn-site.xml>
HADOOP_HOME=<Path to hadoop home directory>
java -Dterrapin.zk_quorum=terrapintestzk:2181 \
     -Dterrapin.cluster=test                  \
     -Dterrapin.fileset=test1                 \
     -Dterrapin.s3bucket=my_bucket            \
     -Dterrapin.s3key_prefix=hfiles/part      \
     -cp $TERRAPIN_CLASS_PATH/terrapin-hadoop2-0.1-SNAPSHOT.jar:\
         $TERRAPIN_CLASS_PATH/lib/*:\
         $HADOOP_CONF_PATH:$HADOOP_HOME/share/hadoop/tools/lib/*\
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
TERRAPIN_CLASSPATH=<Path to extract maven assembly for hadoop2 module>
HADOOP_CONF_PATH=<Path to hadoop configs such as core-site.xml/yarn-site.xml>
HADOOP_HOME=<Path to hadoop home directory>
java -Dterrapin.zk_quorum=terrapintestzk:2181       \
     -Dterrapin.cluster=test                        \
     -Dterrapin.fileset=test1                       \
     -Dterrapin.hdfs_dir=hdfs://hdfsnamenode/hfiles \
     -cp $TERRAPIN_CLASS_PATH/terrapin-hadoop2-0.1-SNAPSHOT.jar:\
         $TERRAPIN_CLASS_PATH/lib/*:\
         $HADOOP_CONF_PATH:$HADOOP_HOME/share/hadoop/tools/lib/*\
    com.pinterest.terrapin.hadoop.HdfsUploader
```
