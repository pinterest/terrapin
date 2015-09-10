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
package com.pinterest.terrapin.controller;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.pinterest.terrapin.Constants;
import com.pinterest.terrapin.TerrapinUtil;
import com.pinterest.terrapin.zookeeper.FileSetInfo;
import com.pinterest.terrapin.zookeeper.ZooKeeperManager;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.helix.ControllerChangeListener;
import org.apache.helix.HelixAdmin;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.helix.spectator.RoutingTableProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Periodically polls the list of resources for purposes of:
 * 1) Garbage collection of older versions
 * 2) Rebalancing the partitions for a resource depending on the state in HDFS.
 */
public class HdfsManager implements ControllerChangeListener {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsManager.class);

  private final PropertiesConfiguration configuration;
  private final ZooKeeperManager zkManager;
  private final String clusterName;
  private final HelixAdmin helixAdmin;
  private final DFSClient hdfsClient;
  private final RoutingTableProvider routingTableProvider;
  private volatile boolean running;
  private Rebalancer rebalancer;
  private Thread rebalancerThread;

  public HdfsManager(PropertiesConfiguration configuration,
                     ZooKeeperManager zkManager,
                     String clusterName,
                     HelixAdmin helixAdmin,
                     DFSClient hdfsClient,
                     RoutingTableProvider routingTableProvider) {
    this.configuration = configuration;
    this.zkManager = zkManager;
    this.clusterName = clusterName;
    this.helixAdmin = helixAdmin;
    this.hdfsClient = hdfsClient;
    this.routingTableProvider = routingTableProvider;
    this.running = false;
  }

  @Override
  public synchronized void onControllerChange(NotificationContext notificationContext) {
    try {
      createDataPaths();
    } catch (IOException e) {
      LOG.warn("Failed to setup data directory.", e);
    }
    if (!this.running) {
      this.running = true;
      this.rebalancer = new Rebalancer();
      this.rebalancerThread = new Thread(rebalancer);
      this.rebalancerThread.setName("rebalancer-thread");
      this.rebalancerThread.start();
    }
  }

  @VisibleForTesting
  Rebalancer createAndGetRebalancer() {
    this.rebalancer = new Rebalancer();
    return rebalancer;
  }

  static class VersionDirComparator implements Comparator<HdfsFileStatus> {
    @Override
    public int compare(HdfsFileStatus f1, HdfsFileStatus f2) {
      String ts1 = f1.getLocalName();
      String ts2 = f2.getLocalName();
      // Compare strings directly - till year 2286 they will have same # of digits.
      return -1 * (ts1.compareTo(ts2));
    }
  }

  static double calculateDeviationForResource(String resource,
                                              IdealState idealState,
                                              RoutingTableProvider routingTableProvider) {
    Set<String> partitions = idealState.getPartitionSet();
    int totalAssignments = 0, totalDeviations = 0;
    // Check if the external view has deviated from the actual view.
    for (String partition : partitions) {
      // Make a copy of the instance mapping in the ideal state.
      Set<String> idealInstances = new HashSet(idealState.getInstanceSet(partition));
      totalAssignments += idealInstances.size();
      // Now check against our real state and count the amount of deviating
      // assignments.
      List<InstanceConfig> currentInstanceConfigs = routingTableProvider.getInstances(
          resource, partition, "ONLINE");
      Set<String> currentInstances = Sets.newHashSetWithExpectedSize(
          currentInstanceConfigs.size());
      if (currentInstanceConfigs != null) {
        for (InstanceConfig instanceConfig : currentInstanceConfigs) {
          currentInstances.add(instanceConfig.getHostName());
        }
      }
      idealInstances.removeAll(currentInstances);
      totalDeviations += idealInstances.size();
    }
    return (double)totalDeviations / totalAssignments;
  }

  public void createDataPaths() throws IOException {
    if (!hdfsClient.exists(Constants.HDFS_DATA_DIR)) {
      LOG.info("Data directory " + Constants.HDFS_DATA_DIR + " does not exist. Creating...");
      hdfsClient.mkdirs(Constants.HDFS_DATA_DIR, null, true);
      LOG.info("Done.");
    }
  }

  class Rebalancer implements Runnable {
    private void rebalanceResource(String hdfsDir, String resource, FileSetInfo fileSetInfo)
        throws Exception {
      IdealState idealState = ControllerUtil.buildIdealStateForHdfsDir(
          hdfsClient,
          hdfsDir,
          resource,
          fileSetInfo.servingInfo.partitionerType,
          configuration.getInt(Constants.NUM_SERVING_REPLICAS, 3),
          configuration.getBoolean(Constants.ENABLE_ZK_COMPRESSION,
              Constants.ENABLE_ZK_COMPRESSION_DEFAULT));

      double deviation = calculateDeviationForResource(resource, idealState, routingTableProvider);
      if (deviation > configuration.getDouble(Constants.REBALANCE_DEVIATION_THRESHOLD, 0.0)) {
        // Write the new ideal state.
        LOG.info("Writing new ideal state for " + resource);
        helixAdmin.setResourceIdealState(clusterName, resource, idealState);
      } else {
        LOG.info("Resource " + resource + " is balanced. Skipping.");
      }
    }

    private boolean isVersionDir(HdfsFileStatus versionDirStatus) {
      try {
        Long.parseLong(versionDirStatus.getLocalName());
        return true;
      } catch (NumberFormatException e) {
        return false;
      }
    }

    private boolean isResourceOfflined(String resource) throws Exception {
      return routingTableProvider.getInstances(resource, "ONLINE").isEmpty();
    }

    private boolean cleanUpResource(String resource, String resourceDir, FileSetInfo fileSetInfo)
        throws Exception {
      if (isResourceOfflined(resource)) {
        LOG.info("Dropping resource " + resource);
        helixAdmin.dropResource(clusterName, resource);
        zkManager.deleteViewInfo(resource);
        // Perform a recursive delete of the version directory.
        LOG.info("Cleaning up " + resourceDir);
        hdfsClient.delete(resourceDir, true);
        return true;
      } else {
        LOG.info("Offlining " + resourceDir);
        boolean enableZkCompression = configuration.getBoolean(
            Constants.ENABLE_ZK_COMPRESSION, Constants.ENABLE_ZK_COMPRESSION_DEFAULT);
        int bucketSize = TerrapinUtil.getBucketSize(
            fileSetInfo.servingInfo.numPartitions, enableZkCompression);
        if (bucketSize > 0) {
          LOG.info("Disabling resource " + resource);
          helixAdmin.enableResource(clusterName, resource, false);
        } else {
          CustomModeISBuilder offlineStateBuilder = new CustomModeISBuilder(resource);
          offlineStateBuilder.setStateModel("OnlineOffline");
          offlineStateBuilder.setNumReplica(configuration.getInt(Constants.NUM_SERVING_REPLICAS, 3));
          offlineStateBuilder.setNumPartitions(fileSetInfo.servingInfo.numPartitions);
          IdealState offlinedState = offlineStateBuilder.build();
          offlinedState.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
          if (enableZkCompression) {
            TerrapinUtil.compressIdealState(offlinedState);
          }

          helixAdmin.setResourceIdealState(clusterName, resource, offlinedState);
        }
        return false;
      }
    }

    private void deleteFileSet(String fileSet, FileSetInfo fileSetInfo, String fileSetDir,
                               List<HdfsFileStatus> subDirs) throws Exception {
      boolean hasOnlineResource = false;
      for (HdfsFileStatus versionDirStatus : subDirs) {
        if (isVersionDir(versionDirStatus)) {
          String versionDir = versionDirStatus.getFullName(fileSetDir);
          String resource = TerrapinUtil.hdfsDirToHelixResource(versionDir);
          if (!cleanUpResource(resource, versionDir, fileSetInfo)) {
            hasOnlineResource = true;
          }
        }
      }
      if (!hasOnlineResource) {
        LOG.info(String.format("deleting file set %s from HDFS", fileSetDir));
        hdfsClient.delete(fileSetDir, true);

        LOG.info(String.format("deleting file set %s from ZooKeeper", fileSet));
        zkManager.deleteFileSetInfo(fileSet);

        LOG.info(String.format("unlocking file set %s", fileSet));
        zkManager.unlockFileSet(fileSet);

        LOG.info(String.format("successfully deleted file set %s", fileSet));
      }
    }

    void reconcileAndRebalance() throws Exception {
      // List all the filesets which are serving and get the list of current data sets
      // which are serving. List all the resources in helix and reconcile.
      List<String> resourceList = helixAdmin.getResourcesInCluster(clusterName);
      Map<String, Pair<FileSetInfo, FileSetInfo>> fileSetInfoMap =
          zkManager.getCandidateHdfsDirMap();
      List<HdfsFileStatus> fileSetDirList;
      try {
        fileSetDirList = TerrapinUtil.getHdfsFileList(hdfsClient, Constants.HDFS_DATA_DIR);
      } catch (Exception e) {
        LOG.warn("Exception while listing file set directores.", e);
        throw e;
      }
      // Loop through all the directories in HDFS. Perform the following actions,
      // 1) If the deleted bit is set, then lock the file set and perform a delete operation.
      // 2) If the directory has an associated resource or it is a serving candidate, trigger
      //    a rebalance.
      // 3) Otherwise perform cleanup.
      for (HdfsFileStatus fileSetDirStatus : fileSetDirList) {
        if (!fileSetDirStatus.isDir()) {
          continue;
        }
        if (!Character.isLetterOrDigit(fileSetDirStatus.getLocalName().charAt(0))) {
          continue;
        }
        String fileSet = fileSetDirStatus.getLocalName();
        String fileSetDir = fileSetDirStatus.getFullName(Constants.HDFS_DATA_DIR);
        try {
          Pair<FileSetInfo, FileSetInfo> fileSetInfoPair = fileSetInfoMap.get(fileSet);
          if (fileSetInfoPair == null) {
            LOG.warn("No file set found for " + fileSetDir);
            continue;
          }
          FileSetInfo currentFileSetInfo = fileSetInfoPair.getLeft();
          if (currentFileSetInfo == null) {
            LOG.warn("Current file set null for " + fileSet);
            continue;
          }
          FileSetInfo lockedFileSetInfo = fileSetInfoPair.getRight();
          List<HdfsFileStatus> versionDirList = null;
          try {
            versionDirList = TerrapinUtil.getHdfsFileList(hdfsClient, fileSetDir);
          } catch (Exception e) {
            LOG.warn("Exception listing directories for fileset in " + fileSetDir);
            continue;
          }

          if (currentFileSetInfo.deleted) {
            deleteFileSet(fileSet, currentFileSetInfo, fileSetDir, versionDirList);
            continue;
          }

          // We sort the version directories in reverse order by timestamp. For safety,
          // we do not clean up the latest version since it is most likely to be either
          // data being copied by an in flight mapreduce job or it is the current serving
          // version.
          Collections.sort(versionDirList, new VersionDirComparator());
          long currentEpochMillis = System.currentTimeMillis();
          boolean isLatest = true;
          for (HdfsFileStatus versionDirStatus : versionDirList) {
            String versionDir = versionDirStatus.getFullName(fileSetDir);
            if (!isVersionDir(versionDirStatus)) {
              LOG.warn("Invalid directory for fileset " + fileSet + " : " + versionDir);
              LOG.warn("This could be a temporary directory from a mapreduce job.");
              continue;
            }
            boolean canDelete = !isLatest;
            isLatest = false;
            String resource = TerrapinUtil.hdfsDirToHelixResource(versionDir);
            List<String> oldVersionsHdfsPathList = Lists.newArrayListWithCapacity(
                currentFileSetInfo.oldServingInfoList.size());
            for (FileSetInfo.ServingInfo info : currentFileSetInfo.oldServingInfoList) {
              oldVersionsHdfsPathList.add(info.hdfsPath);
            }
            boolean rebalanceResource = oldVersionsHdfsPathList.contains(versionDir) ||
                currentFileSetInfo.servingInfo.hdfsPath.equals(versionDir);
            if (rebalanceResource) {
              // Only rebalance resources which are servable.
              rebalanceResource(versionDir, resource, currentFileSetInfo);
            }
            if (rebalanceResource ||
                (lockedFileSetInfo != null &&
                 lockedFileSetInfo.servingInfo.hdfsPath.equals(versionDir))) {
              LOG.info("Skipping " + versionDir + " due to lock/rebalance.");
              continue;
            }
            // Only clean up directories which are older than 2 hours.
            if (currentEpochMillis - versionDirStatus.getModificationTime() < 2 * 60 * 60 * 1000) {
              LOG.info("Skipping " + versionDir + " due to recency.");
              continue;
            }
            if (!canDelete) {
              LOG.info("Skipping " + versionDir + " since its latest.");
              continue;
            }
            // Check if a resource already exists for this file set. If yes, then drop the
            // resource.
            if (resourceList.contains(resource)) {
              cleanUpResource(resource, versionDir, currentFileSetInfo);
            } else {
              // An old enough directory with no links (resource/file set info) can be deleted.
              LOG.info("Removing orphaned directory " + versionDir);
              hdfsClient.delete(versionDir, true);
            }
          }
        } catch (Throwable t) {
          LOG.info("Exception while processing file set " + fileSet, t);
        }
      }
    }

    @Override
    public void run() {
      LOG.info("Starting balancer...");
      while (running) {
        try {
          reconcileAndRebalance();
        } catch (Exception e) {
          LOG.warn("Exception in rebalancer loop.", e);
        }
        try {
          Thread.sleep(configuration.getInt(Constants.REBALANCE_INTERVAL_SECONDS, 600) * 1000);
        } catch (InterruptedException e) {
          LOG.info("Interrupted shutting down...");
          running = false;
        }
      }
    }
  }

  public void shutdown() {
    this.running = false;
    rebalancerThread.interrupt();
    try {
      rebalancerThread.join();
    } catch (InterruptedException e) {
      LOG.warn("Interrupted while waiting for rebalancer thread to exit.");
    }
    try {
      this.hdfsClient.close();
    } catch (IOException e) {
      LOG.error("Exception while closing HDFS client.", e);
    }
  }
}
