/**
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.pinterest.terrapin.hadoop;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.pinterest.terrapin.Constants;
import com.pinterest.terrapin.TerrapinUtil;
import com.pinterest.terrapin.thrift.generated.Options;
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

import java.net.UnknownHostException;
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

  @VisibleForTesting
  protected ZooKeeperManager getZKManager() throws UnknownHostException {
    return new ZooKeeperManager(TerrapinUtil.getZooKeeperClient(options.terrapinZkQuorum, 30),
        options.terrapinCluster);
  }

  @VisibleForTesting
  protected void setOutputPath(String terrapinNameNode, String hdfsDir) {
    FileOutputFormat.setOutputPath(job, new Path("hdfs", terrapinNameNode, hdfsDir));
  }

  @VisibleForTesting
  protected void loadFileSetData(ZooKeeperManager zkManager, FileSetInfo fsInfo, Options options)
      throws Exception {
    TerrapinUtil.loadFileSetData(zkManager, fsInfo, options);
  }

  public boolean waitForCompletion() throws Exception {
    // Come up with a new timestamp epoch for the latest data.
    long timestampEpochMillis = System.currentTimeMillis();
    String hdfsDir = Constants.HDFS_DATA_DIR + "/" + options.terrapinFileSet +
        "/" + timestampEpochMillis;
    ZooKeeperManager zkManager = getZKManager();
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
    setOutputPath(terrapinNamenode, hdfsDir);

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
      loadFileSetData(zkManager, fileSetInfo, options.loadOptions);
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
