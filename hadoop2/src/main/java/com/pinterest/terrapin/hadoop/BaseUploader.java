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
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.pinterest.terrapin.Constants;
import com.pinterest.terrapin.PartitionerFactory;
import com.pinterest.terrapin.TerrapinUtil;
import com.pinterest.terrapin.thrift.generated.Options;
import com.pinterest.terrapin.thrift.generated.PartitionerType;
import com.pinterest.terrapin.zookeeper.ClusterInfo;
import com.pinterest.terrapin.zookeeper.FileSetInfo;
import com.pinterest.terrapin.zookeeper.ZooKeeperManager;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

/**
 * A distCp based uploader for uploading files from sources such as S3/HDFS
 * into terrapin.
 */
public abstract class BaseUploader {
  private static final Logger LOG = LoggerFactory.getLogger(BaseUploader.class);

  private final String terrapinZkQuorum;
  private String terrapinNamenode;

  protected Configuration conf;
  protected ZooKeeperManager zkManager;

  public BaseUploader(TerrapinUploaderOptions uploaderOptions) {
    this.terrapinZkQuorum = uploaderOptions.terrapinZkQuorum;
    this.terrapinNamenode = uploaderOptions.terrapinNamenode;
    this.conf = new Configuration();
    this.conf.addResource("mapred-site.xml");
    this.conf.addResource("yarn-site.xml");
  }

  /**
   * @return The list of files to be copied and their sizes.
   */
  abstract List<Pair<Path, Long>> getFileList();


  /**
   * Validates the first non-empty partition hfile has right partitioning function.
   * It reads several keys, then calculates the partition according to the partitioning function
   * client offering. If the calculated partition number is different with actual partition number
   * an exception is thrown. If all partition hfiles are empty, an exception is thrown.
   *
   * @param parts full absolute path for all partitions
   * @param partitionerType type of paritioning function
   * @param numShards total number of partitions
   * @throws IOException if something goes wrong when reading the hfiles
   * @throws IllegalArgumentException if the partitioner type is wrong or all partitions are empty
   */
  public void validate(List<Path> parts, PartitionerType partitionerType, int numShards)
      throws IOException {
    boolean hasNonEmptyPartition = false;
    HColumnDescriptor columnDescriptor = new HColumnDescriptor();
    // Disable block cache to ensure it reads the actual file content.
    columnDescriptor.setBlockCacheEnabled(false);
    for (int shardIndex = 0; shardIndex < parts.size(); shardIndex++) {
      Path fileToBeValidated = parts.get(shardIndex);
      HFile.Reader reader = null;
      try {
        FileSystem fs = FileSystem.newInstance(fileToBeValidated.toUri(), conf);
        CacheConfig cc = new CacheConfig(conf, columnDescriptor);
        reader = HFile.createReader(fs, fileToBeValidated, cc);
        Partitioner partitioner = PartitionerFactory.getPartitioner(partitionerType);
        byte[] rowKey = reader.getFirstRowKey();
        if (rowKey == null) {
          LOG.warn(String.format("empty partition %s", fileToBeValidated.toString()));
          reader.close();
          continue;
        }
        hasNonEmptyPartition = true;
        BytesWritable key = new BytesWritable(rowKey);
        int partition = partitioner.getPartition(key, null,  numShards);
        if (partition != shardIndex) {
          throw new IllegalArgumentException(
              String.format("wrong partition type %s for key %s in partition %d, expected %d",
                  partitionerType.toString(), new String(key.getBytes()), shardIndex, partition)
          );
        }
      } finally {
        if (reader != null) {
          reader.close();
        }
      }
    }
    if (!hasNonEmptyPartition) {
      throw new IllegalArgumentException("all partitions are empty");
    }
  }

  @VisibleForTesting
  protected ZooKeeperManager getZKManager(String clusterName) throws UnknownHostException {
    return new ZooKeeperManager(TerrapinUtil.getZooKeeperClient(terrapinZkQuorum, 30), clusterName);
  }

  @VisibleForTesting
  protected DistCp getDistCp(Configuration conf, DistCpOptions options) throws Exception {
    return new DistCp(conf, options);
  }

  @VisibleForTesting
  protected void loadFileSetData(ZooKeeperManager zkManager, FileSetInfo fileSetInfo,
                                 Options options) throws Exception {
    TerrapinUtil.loadFileSetData(zkManager, fileSetInfo, options);
  }

  public void upload(String clusterName, String fileSet, Options options) throws Exception {
    List<Pair<Path, Long>> fileSizePairList = getFileList();

    int numShards = fileSizePairList.size();
    LOG.info("Got " + numShards + " files.");
    if (numShards == 0) {
      LOG.warn("No files found. Exiting.");
      System.exit(1);
    }

    List<Path> parts = Lists.transform(fileSizePairList, new Function<Pair<Path, Long>, Path>() {
      @Override
      public Path apply(Pair<Path, Long> pathLongPair) {
        return pathLongPair.getKey();
      }
    });
    PartitionerType partitionerType = options.getPartitioner();

    validate(parts, partitionerType, numShards);
    long maxSize = -1;
    for (Pair<Path, Long> fileSizePair : fileSizePairList) {
      long size = fileSizePair.getRight();
      if (maxSize < size) {
        maxSize = size;
      }
    }
    // Come up with a new timestamp epoch for the latest data.
    long timestampEpochMillis = System.currentTimeMillis();
    String hdfsDir = Constants.HDFS_DATA_DIR + "/" + fileSet + "/" + timestampEpochMillis;
    ZooKeeperManager zkManager = getZKManager(clusterName);
    FileSetInfo fileSetInfo = new FileSetInfo(fileSet,
        hdfsDir,
        numShards,
        (List)Lists.newArrayList(),
        options);

    int replicationFactor = Constants.DEFAULT_HDFS_REPLICATION;
    if (terrapinNamenode == null || terrapinNamenode.isEmpty()) {
      ClusterInfo info = zkManager.getClusterInfo();
      if (info == null) {
        LOG.error("Could not find the namenode for " + clusterName);
        System.exit(1);
      }
      if (info.hdfsNameNode == null || info.hdfsNameNode.isEmpty()) {
        LOG.error("Could not find the namenode for " + clusterName);
        System.exit(1);
      }
      this.terrapinNamenode = info.hdfsNameNode;
      replicationFactor = info.hdfsReplicationFactor;
    }
    // Connect to the zookeeper and establish a lock on the fileset.
    LOG.info("Locking fileset " + fileSet);
    zkManager.lockFileSet(fileSet, fileSetInfo);

    try {
      LOG.info("Uploading " + numShards + " files through distcp to " + hdfsDir);

      // TODO: Add check for cluster disk space.
      List<Path> sourceFiles = Lists.newArrayListWithCapacity(fileSizePairList.size());
      for (Pair<Path, Long> fileSize : fileSizePairList) {
        sourceFiles.add(fileSize.getLeft());
      }
      DistCpOptions distCpOptions = new DistCpOptions(sourceFiles,
          new Path("hdfs", terrapinNamenode, hdfsDir));
      distCpOptions.setSyncFolder(true);
      distCpOptions.setSkipCRC(true);

      if (maxSize > Constants.DEFAULT_MAX_SHARD_SIZE_BYTES) {
        LOG.warn("Largest shard is " + maxSize + " bytes. This is more than 4G. " +
                 "Increase the # of shards to reduce the size.");
        System.exit(1);
      }
      TerrapinUtil.setupConfiguration(conf, maxSize, replicationFactor);

      DistCp distCp = getDistCp(conf, distCpOptions);
      Job job = distCp.execute();
      if (!job.waitForCompletion(true)) {
        throw new RuntimeException("Distributed copy failed.");
      }

      LOG.info("Successfully copied data.");

      loadFileSetData(zkManager, fileSetInfo, options);

      // Wait for a while so that zookeeper watches have propagated before relinquishing the lock.
      try {
        LOG.info("Releasing file set lock.");
        Thread.sleep(5000);
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted.");
      }
    } finally {
      zkManager.unlockFileSet(fileSet);
    }
  }
}