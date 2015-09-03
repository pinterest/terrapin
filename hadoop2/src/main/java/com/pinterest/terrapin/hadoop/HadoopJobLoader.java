package com.pinterest.terrapin.hadoop;

import com.google.common.collect.Lists;
import com.pinterest.terrapin.Constants;
import com.pinterest.terrapin.TerrapinUtil;
import com.pinterest.terrapin.zookeeper.ClusterInfo;
import com.pinterest.terrapin.zookeeper.FileSetInfo;
import com.pinterest.terrapin.zookeeper.ZooKeeperManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by varun on 9/1/15.
 */
public class HadoopJobLoader {
  private static Logger LOG = LoggerFactory.getLogger(HadoopJobLoader.class);

  private final TerrapinUploaderOptions options;
  private final Job job;

  public HadoopJobLoader(TerrapinUploaderOptions options, Job job) {
    this.options = options;
    this.job = job;
  }

  public boolean waitForCompletion() throws Exception {
    // Come up with a new timestamp epoch for the latest data.
    long timestampEpochMillis = System.currentTimeMillis();
    String hdfsDir = Constants.HDFS_DATA_DIR + "/" + options.terrapinFileSet +
        "/" + timestampEpochMillis;
    ZooKeeperManager zkManager = new ZooKeeperManager(
        TerrapinUtil.getZooKeeperClient(options.terrapinZkQuorum, 30), options.terrapinCluster);
    int numShards = job.getNumReduceTasks();
    FileSetInfo fileSetInfo = new FileSetInfo(options.terrapinFileSet,
        hdfsDir,
        numShards,
        (List) Lists.newArrayList(),
        options.loadOptions);

    int replicationFactor = Constants.DEFAULT_HDFS_REPLICATION;
    String terrapinNamenode = options.terrapinNamenode;
    if (terrapinNamenode == null || terrapinNamenode.isEmpty()) {
      ClusterInfo info = zkManager.getClusterInfo();
      if (info == null) {
        LOG.error("Could not cluster information for " + options.terrapinCluster);
        System.exit(1);
      }
      if (info.hdfsNameNode == null || info.hdfsNameNode.isEmpty()) {
        LOG.error("Could not find the namenode for " + options.terrapinCluster);
        System.exit(1);
      }
      terrapinNamenode = info.hdfsNameNode;
      replicationFactor = info.hdfsReplicationFactor;
    }
    job.setOutputFormatClass(HFileOutputFormat.class);
    job.setOutputKeyClass(BytesWritable.class);
    job.setOutputValueClass(BytesWritable.class);
    FileOutputFormat.setOutputPath(job, new Path("hdfs", terrapinNamenode, hdfsDir));

    zkManager.lockFileSet(options.terrapinFileSet, fileSetInfo);

    try {
      Configuration conf = job.getConfiguration();
      // Have a higher retry limit for tasks to copy data such as board joins.
      TerrapinUtil.setupConfiguration(conf,
          Constants.DEFAULT_MAX_SHARD_SIZE_BYTES, replicationFactor);

      if (!job.waitForCompletion(true)) {
        LOG.error("MR job failed.");
        return false;
      }
      TerrapinUtil.loadFileSetData(zkManager, fileSetInfo, options.loadOptions);
      // Wait for a while so that zookeeper watches have propagated before relinquishing the lock.
      try {
        LOG.info("Releasing file set lock.");
        Thread.sleep(5000);
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted.");
      }
    } finally {
      zkManager.unlockFileSet(options.terrapinFileSet);
    }
    return true;
  }
}
