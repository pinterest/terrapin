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
package com.pinterest.terrapin;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.pinterest.terrapin.base.BytesUtil;
import com.pinterest.terrapin.thrift.generated.*;
import com.pinterest.terrapin.zookeeper.FileSetInfo;
import com.pinterest.terrapin.zookeeper.ZooKeeperManager;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.finagle.thrift.ThriftClientFramedCodecFactory;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.ostrich.stats.Stats;
import com.twitter.util.Duration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.helix.model.IdealState;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * General utility functions.
 */
public class TerrapinUtil {
  private static final Logger LOG = LoggerFactory.getLogger(TerrapinUtil.class);
    private static final Partitioner<BytesWritable, BytesWritable> HASH_PARTITIONER =
            new HashPartitioner<BytesWritable, BytesWritable>();

  /**
   * Get the helix instance name from the HDFS hostname.
   */
  public static String getHelixInstanceFromHDFSHost(String hdfsHostName) {
    int index = hdfsHostName.indexOf(".");
    if (index == -1) {
      return hdfsHostName;
    }
    return hdfsHostName.substring(0, index);
  }

  public static PropertiesConfiguration readPropertiesExitOnFailure(String configFile) {
    PropertiesConfiguration configuration = null;
    if (configFile.isEmpty()) {
      LOG.error("Empty configuration file name. Please specify using -Dterrapin.config.");
      System.exit(1);
    }
    try {
        configuration = new PropertiesConfiguration(configFile);
    } catch (ConfigurationException e) {
      LOG.info("Invalid configuration file " + configFile);
      System.exit(1);
    }
    return configuration;
  }

  /**
   * Extracts the partition name for a file. It expects file names with the prefix part-00000
   * etc. Currently only modulus sharding is supported.
   *
   * @param fileName
   * @param partitioner
   * @return Returns the extracted name - null if the file name does not match the expected
   *         prefix.
   */
  public static Integer extractPartitionName(String fileName, PartitionerType partitioner) {
    if (partitioner == PartitionerType.MODULUS || partitioner == PartitionerType.CASCADING) {
      // Modulus sharded files are of the format "part-00000-<hash>"
      // Retrieve 5 characters and strip leading 0's.
      if (!fileName.startsWith(Constants.FILE_PREFIX)) {
        return null;
      }
      try {
        return Integer.parseInt(fileName.substring(5, 10));
      } catch (NumberFormatException e) {
        return null;
      }
    }
    return null;
  }

  public static String formatPartitionName(int partitionNumber) {
    return String.format("%s%05d", Constants.FILE_PREFIX, partitionNumber);
  }

  public static String getPartitionName(ByteBuffer key,
                                        PartitionerType partitionerType,
                                        int numPartitions) {
    Partitioner partitioner = PartitionerFactory.getPartitioner(partitionerType);
    return Integer.toString(
        partitioner.getPartition(
            new BytesWritable(BytesUtil.readBytesFromByteBufferWithoutConsume(key)),
            null,
            numPartitions));
  }

  public static String hdfsDirToHelixResource(String hdfsDir) {
    return hdfsDir.replace('/', '$');
  }

  public static String helixResourceToHdfsDir(String helixResource) {
    return helixResource.replace('$', '/');
  }

  private static List<InetSocketAddress> getSocketAddressList(String hostPortList)
      throws UnknownHostException {
    List<InetSocketAddress> socketAddrList = Lists.newArrayListWithCapacity(7);
    String[] hostPortPairList = hostPortList.split(",");
    for (String hostPortPair : hostPortPairList) {
      String[] hostPort = hostPortPair.split(":");
      socketAddrList.add(new InetSocketAddress(InetAddress.getByName(hostPort[0]),
          Integer.parseInt(hostPort[1])));
    }
    return socketAddrList;
  }

  public static ZooKeeperClient getZooKeeperClient(String zkQuorum, int sessionTimeoutSeconds)
      throws UnknownHostException {
    return new ZooKeeperClient(Amount.of(sessionTimeoutSeconds, Time.SECONDS),
        getSocketAddressList(zkQuorum));
  }

  /**
   * IMPORTANT: Changing the logic in this function can have weird side effects since
   * the bucket size may change for an already existing resource. This is not an issue for
   * new resources but would create problems when old resources are rebalanced. Before
   * we change the logic here, we must make sure that the bucket size of pre existing
   * resources is not changed during rebalance operations.
   */
  public static int getBucketSize(int numPartitions, boolean enableZkCompression) {
    // If compression is enabled, there is no need for bucketing of resources.
    if (enableZkCompression) {
      return 0;
    }
    int numBuckets = (int)Math.ceil((double)numPartitions / 1000);
    if (numBuckets <= 1) {
      return 0;
    }
    return (int)Math.ceil((double)numPartitions / numBuckets);
  }

  /**
   * Return the fileset corresponding to a file on HDFS. If the file path is not valid,
   * then return null.
   */
  public static String extractFileSetFromPath(String resource) {
    String[] splits = resource.split("[/]");
    if (splits.length <= 3) {
      // This should really never happen.
      Stats.incr("invalid-resource");
      return null;
    }
    return splits[splits.length - 3];
  }

  public static Pair<String, Integer> getBucketizedResourceAndPartitionNum(String helixPartition) {
    int index = helixPartition.lastIndexOf("_");
    if (index == -1) {
      return null;
    }
    try {
      int partitionNum = Integer.parseInt(helixPartition.substring(index + 1));
      return new ImmutablePair(helixPartition.substring(0, index), partitionNum);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  public static Pair<String, Integer> getNonBucketizedResourceAndPartitionNum(
      String helixPartition) {
    int index = helixPartition.lastIndexOf("$");
    if (index == -1) {
      return null;
    }
    try {
      int partitionNum = Integer.parseInt(helixPartition.substring(index + 1));
      return new ImmutablePair(helixPartition.substring(0, index), partitionNum);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  /**
   * Extracts the resource name and partition number from a helix partition. Returns null
   * if the helix partition format is bad.
   */
  public static Pair<String, Integer> getResourceAndPartitionNum(String helixPartition) {
    Pair<String, Integer> nonBucketizedResourceAndPartitionNum =
        getNonBucketizedResourceAndPartitionNum(helixPartition);
    if (nonBucketizedResourceAndPartitionNum != null) {
      return nonBucketizedResourceAndPartitionNum;
    }
    return getBucketizedResourceAndPartitionNum(helixPartition);
  }

  /**
   * Get full partition name with resource prefix
   * @param resource resource name
   * @param partition partition number
   * @return full partition name
   */
  public static String getViewPartitionName(String resource, int partition) {
    return String.format("%s$%d", resource, partition);
  }

  /**
   * Parse partition number from full partition name
   * @param viewPartitionName full partition name
   * @return partition number
   */
  public static int getViewPartitionNumber(String viewPartitionName) {
    int index = viewPartitionName.lastIndexOf('$');
    if (index == -1) {
      return 0;
    }
    return Integer.parseInt(viewPartitionName.substring(index + 1));
  }

  /**
   * Sets the zk compression flag in the Helix ideal state. Compresses both the ideal
   * state and the external view.
   */
  public static void compressIdealState(IdealState is) {
    is.getRecord().setBooleanField("enableCompression", true);
  }

  /**
   * Get ZooKeeper quorum string from configuration
   *
   * @param configuration configuration instance
   * @return quorum string
   */
  public static String getZKQuorumFromConf(PropertiesConfiguration configuration) {
    String[] quorums = configuration.getStringArray(Constants.ZOOKEEPER_QUORUM);
    return Joiner.on(Constants.ZOOKEEPER_QUORUM_DELIMITER).join(quorums);
  }

  /**
   * Retrieve list of files under @hdfsDir for @hdfsClient.
   */
  public static List<HdfsFileStatus> getHdfsFileList(DFSClient hdfsClient,
                                                     String hdfsDir)
      throws IOException {
    List<HdfsFileStatus> fileList = Lists.newArrayList();
    // Build a list of files.
    DirectoryListing listing = null;
    String continuation = "";
    while (true) {
      listing = hdfsClient.listPaths(hdfsDir, continuation.getBytes());
      for (HdfsFileStatus fileStatus : listing.getPartialListing()) {
        fileList.add(fileStatus);
      }
      // Go through the listing and paginate.
      if (!listing.hasMore()) {
        break;
      } else {
        continuation = new String(listing.getLastName());
      }
    }
    return fileList;
  }

    /**
   * Attempt to load data (already in HDFS on a correct directory) into an already locked fileset.
   * The data is assumed to already have been placed in the correct directory on the terrapin
   * cluster. This is being called by the Terrapin loader jobs. The @fsInfo object is the same
   * as the locked fsInfo object.
   */
  public static void loadFileSetData(ZooKeeperManager zkManager, FileSetInfo fsInfo, Options options)
      throws Exception {
      InetSocketAddress controllerSockAddress = zkManager.getControllerLeader();
      LOG.info("Connecting to controller at " +
              controllerSockAddress.getHostName() + ":" + controllerSockAddress.getPort());
      LOG.info("Load timeout " + Constants.LOAD_TIMEOUT_SECONDS + " seconds.");

      Service<ThriftClientRequest, byte[]> service = ClientBuilder.safeBuild(ClientBuilder.get()
              .hosts(controllerSockAddress)
              .codec(new ThriftClientFramedCodecFactory(Option.<ClientId>empty()))
              .retries(1)
              .connectTimeout(Duration.fromMilliseconds(1000))
              .requestTimeout(Duration.fromSeconds(Constants.LOAD_TIMEOUT_SECONDS))
              .hostConnectionLimit(100)
              .failFast(false));
      TerrapinController.ServiceIface iface = new TerrapinController.ServiceToClient(
              service, new TBinaryProtocol.Factory());
      TerrapinLoadRequest request = new TerrapinLoadRequest();
      request.setHdfsDirectory(fsInfo.servingInfo.hdfsPath);
      request.setOptions(options);
      request.setFileSet(fsInfo.fileSetName);
      request.setExpectedNumPartitions(fsInfo.servingInfo.numPartitions);

      LOG.info("Loading file set " + fsInfo.fileSetName + " at " + fsInfo.servingInfo.hdfsPath);
      long startTimeSeconds = System.currentTimeMillis() / 1000;
      int numTriesLeft = 5;
      boolean done = false;
      Exception e = null;
      while (numTriesLeft > 0) {
          try {
              iface.loadFileSet(request).get();
              done = true;
              break;
          } catch (Throwable t) {
              LOG.error("Swap failed with exception.", t);
              e = new Exception(t);
              numTriesLeft--;
          }
          LOG.info("Retrying in 10 seconds.");
          try {
              Thread.sleep(10000);
          } catch (InterruptedException ie) {
              LOG.error("Interrupted.");
              break;
          }
      }
      if (done) {
          LOG.info("Load successful. Swap took " +
                  ((System.currentTimeMillis() / 1000) - startTimeSeconds) + " seconds.");
      } else {
          LOG.error("Load failed !!!.");
          throw new Exception(e);
      }
  }

  static public List<Pair<Path, Long>> getS3FileList(AWSCredentials credentials,
      String s3Bucket, String s3KeyPrefix) {
    List<Pair<Path, Long>> fileSizePairList = Lists.newArrayListWithCapacity(
        Constants.MAX_ALLOWED_SHARDS);
    AmazonS3Client s3Client = new AmazonS3Client(credentials);
    // List files and build the path using the s3n: prefix.
    // Note that keys > marker are retrieved where the > is by lexicographic order.
    String prefix = s3KeyPrefix;
    String marker = prefix;
    while (true) {
      boolean reachedEnd = false;
      ObjectListing listing = s3Client.listObjects(new ListObjectsRequest().
          withBucketName(s3Bucket).
          withMarker(marker));
      List<S3ObjectSummary> summaries = listing.getObjectSummaries();

      if (summaries.isEmpty()) {
        break;
      }

      for (S3ObjectSummary summary: summaries) {
        if (summary.getKey().startsWith(prefix)) {
          fileSizePairList.add(new ImmutablePair(new Path("s3n", s3Bucket, "/" + summary.getKey()),
              summary.getSize()));
          if (fileSizePairList.size() > Constants.MAX_ALLOWED_SHARDS) {
            throw new RuntimeException("Too many files " + fileSizePairList.size());
          }
        } else {
          // We found a key which does not match the prefix, stop.
          reachedEnd = true;
          break;
        }
      }
      if (reachedEnd) {
        break;
      }
      marker = summaries.get(summaries.size() - 1).getKey();
    }
    return fileSizePairList;
  }

  public static void setupConfiguration(Configuration conf,
                                        long dfsBlockSize,
                                        int dfsReplication) {
    conf.setInt("mapred.map.max.attempts", Constants.MAPRED_MAP_MAX_ATTEMPTS);
    conf.setInt("io.bytes.per.checksum", Constants.CHECKSUM_BYTES);
    long dfsBlockSizeAdjusted = dfsBlockSize;
    if (dfsBlockSize % Constants.CHECKSUM_BYTES != 0) {
      dfsBlockSizeAdjusted =
          (dfsBlockSize / Constants.CHECKSUM_BYTES + 1) * Constants.CHECKSUM_BYTES;
    }
    conf.setLong("dfs.block.size", dfsBlockSizeAdjusted);
    conf.setInt("dfs.replication", dfsReplication);
    conf.set(Constants.HFILE_COMPRESSION, System.getProperty(
            Constants.HFILE_COMPRESSION, Constants.HFILE_COMPRESSION_DEFAULT));
    conf.setInt(Constants.HFILE_BLOCKSIZE, Integer.parseInt(
        System.getProperty(Constants.HFILE_BLOCKSIZE,
            String.valueOf(Constants.HFILE_BLOCKSIZE_DEFAULT))));
  }
}
