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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.pinterest.terrapin.TerrapinUtil;
import com.pinterest.terrapin.thrift.generated.ControllerErrorCode;
import com.pinterest.terrapin.thrift.generated.ControllerException;
import com.pinterest.terrapin.thrift.generated.PartitionerType;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility functions for the controller.
 */
public class ControllerUtil {
  private static final Logger LOG = LoggerFactory.getLogger(ControllerUtil.class);

  /**
   * Builds the helix ideal state for HDFS directory by finding the locations of HDFS blocks and
   * creating an ideal state assignment based on those.
   *
   * @param hdfsClient The HDFS client object.
   * @param hdfsDir The HDFS directory containing the various files.
   * @param resourceName The name of the Helix resource for which the ideal state is being created.
   * @param partitioner The partitioner type, used for extracting helix partition names from
   *                    HDFS files.
   * @param numReplicas The number of replicas for each partition.
   * @param enableZkCompression Whether data in zk is kept compressed.
   * @return The ideal state as computed based on HDFS block placement.
   * @throws ControllerException
   */
  public static IdealState buildIdealStateForHdfsDir(DFSClient hdfsClient,
                                                     String hdfsDir,
                                                     String resourceName,
                                                     PartitionerType partitioner,
                                                     int numReplicas,
                                                     boolean enableZkCompression)
      throws ControllerException {
    List<HdfsFileStatus> fileList;
    try {
      fileList = TerrapinUtil.getHdfsFileList(hdfsClient, hdfsDir);
    } catch (IOException e) {
      throw new ControllerException("Exception while listing files in " + hdfsDir,
          ControllerErrorCode.HDFS_ERROR);
    }
    // Mapping from file to HDFS block locations.
    Map<Integer, Set<String>> hdfsBlockMapping = Maps.newHashMapWithExpectedSize(fileList.size());
    for (HdfsFileStatus fileStatus : fileList) {
      Integer partitionName = TerrapinUtil.extractPartitionName(fileStatus.getLocalName(), partitioner);
      if (partitionName == null) {
        LOG.info("Skipping " + fileStatus.getLocalName() + " for " + hdfsDir);
        continue;
      }
      String fullName = fileStatus.getFullName(hdfsDir);
      BlockLocation[] locations = null;
      try {
        locations = hdfsClient.getBlockLocations(fullName, 0, fileStatus.getLen());
      } catch (Exception e) {
        throw new ControllerException("Exception while getting block locations " + e.getMessage(),
            ControllerErrorCode.HDFS_ERROR);
      }
      Set<String> instanceSet = Sets.newHashSetWithExpectedSize(3);
      BlockLocation firstLocation = locations[0];
      String[] hosts = null;
      try {
        hosts = firstLocation.getHosts();
      } catch (IOException e) {
        throw new ControllerException("Exception while getting hosts " + e.getMessage(),
            ControllerErrorCode.HDFS_ERROR);
      }
      for (String host : hosts) {
        instanceSet.add(host);
      }
      hdfsBlockMapping.put(partitionName, instanceSet);
    }
    // Assign helix partitions for the resource - which is the HDFS directory.
    int bucketSize = TerrapinUtil.getBucketSize(hdfsBlockMapping.size(), enableZkCompression);
    CustomModeISBuilder idealStateBuilder = new CustomModeISBuilder(resourceName);
    for (Map.Entry<Integer, Set<String>> mapping : hdfsBlockMapping.entrySet()) {
      // Make partitions globally unique
      String partitionName = null;
      // This is needed because of the way helix parses partition numbers for buckets.
      if (bucketSize > 0) {
        partitionName = resourceName + "_" + mapping.getKey();
      } else {
        partitionName = resourceName + "$" + mapping.getKey();
      }
      Set<String> instanceSet = mapping.getValue();
      for (String instance : instanceSet) {
        idealStateBuilder.assignInstanceAndState(
            partitionName, TerrapinUtil.getHelixInstanceFromHDFSHost(instance), "ONLINE");
      }
    }
    idealStateBuilder.setStateModel("OnlineOffline");
    idealStateBuilder.setNumReplica(numReplicas);
    idealStateBuilder.setNumPartitions(hdfsBlockMapping.size());
    IdealState is = idealStateBuilder.build();
    if (bucketSize > 0) {
      is.setBucketSize(bucketSize);
    }
    is.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    if (enableZkCompression) {
      TerrapinUtil.compressIdealState(is);
    }
    return is;
  }
}
