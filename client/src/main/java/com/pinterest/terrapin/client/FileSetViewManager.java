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
package com.pinterest.terrapin.client;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.pinterest.terrapin.thrift.generated.TerrapinGetErrorCode;
import com.pinterest.terrapin.thrift.generated.TerrapinGetException;
import com.pinterest.terrapin.zookeeper.FileSetInfo;
import com.pinterest.terrapin.zookeeper.ViewInfo;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.ZooKeeperMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * Manages views against all file sets for a cluster.
 */
public class FileSetViewManager {
  private static final Logger LOG = LoggerFactory.getLogger(FileSetViewManager.class);
  private final String clusterName;

  // The following map data structures manage a mapping from each file set and the corresponding
  // external views to appropriate data structures. We store the view for each helix resource and
  // we store the current serving resource for each fileset.
  final Map<String, FileSetInfo> zkBackedFileSetInfoMap;
  final Map<String, ViewInfo> zkBackedViewInfoMap;

  // We also populate a backup map in case the watch trigger for the external view is delayed.
  private final ConcurrentMap<String, FileSetInfo> fileSetInfoBackupMap;

  // These functions are package private for testing. They convert the data in zookeeper into
  // the corresponding zookeeper map data structure.
  Function<byte[], FileSetInfo> fileSetInfoFunction = new Function<byte[], FileSetInfo>() {
    @Override
    public FileSetInfo apply(byte[] jsonBytes) {
      try {
        // When a fileSet is initially created and locked, we would have a zero length string.
        if (jsonBytes.length == 0) {
          return new FileSetInfo();
        }
        FileSetInfo info = FileSetInfo.fromJson(jsonBytes);
        LOG.info("Got new file set information for " + info.fileSetName);
        if (zkBackedFileSetInfoMap != null) {
          FileSetInfo backupInfo = zkBackedFileSetInfoMap.get(info.fileSetName);
          if (backupInfo != null) {
            fileSetInfoBackupMap.put(info.fileSetName, backupInfo);
          }
        }
        return info;
      } catch (Exception e) {
        LOG.warn("Error while decoding file set information.", e);
        throw new RuntimeException(e);
      }
    }
  };

  Function<byte[], ViewInfo> viewInfoFunction = new Function<byte[], ViewInfo>() {
    @Override
    public ViewInfo apply(byte[] compressedJsonBytes) {
      try {
        ViewInfo viewInfo = ViewInfo.fromCompressedJson(compressedJsonBytes);
        LOG.info("Got view for " + viewInfo.getResource());
        return ViewInfo.fromCompressedJson(compressedJsonBytes);
      } catch (Exception e) {
        LOG.info("Error while processing new view.", e);
        return new ViewInfo();
      }
    }
  };

  public FileSetViewManager(ZooKeeperClient zkClient,
                            String clusterName) throws Exception {
    this.clusterName = clusterName;
    this.fileSetInfoBackupMap = Maps.newConcurrentMap();
    this.zkBackedFileSetInfoMap = ZooKeeperMap.create(zkClient,
        "/" + this.clusterName + "/filesets",
        fileSetInfoFunction);
    this.zkBackedViewInfoMap = ZooKeeperMap.create(zkClient,
        "/" + this.clusterName + "/views",
        viewInfoFunction);
  }

  // For testing.
  FileSetViewManager(String clusterName) {
    this.clusterName = clusterName;
    this.fileSetInfoBackupMap = Maps.newConcurrentMap();
    this.zkBackedFileSetInfoMap = Maps.newHashMap();
    this.zkBackedViewInfoMap = Maps.newHashMap();
  }

  public Pair<FileSetInfo, ViewInfo> getFileSetViewInfo(String fileSet) throws TerrapinGetException {
    FileSetInfo fileSetInfo = zkBackedFileSetInfoMap.get(fileSet);
    if (fileSetInfo == null) {
      throw new TerrapinGetException("File set " + fileSet + " not found.",
          TerrapinGetErrorCode.FILE_SET_NOT_FOUND);
    }
    if (!fileSetInfo.valid) {
      throw new TerrapinGetException("Invalid info for fileset " + fileSet,
          TerrapinGetErrorCode.FILE_SET_NOT_FOUND);
    }
    ViewInfo viewInfo = zkBackedViewInfoMap.get(fileSetInfo.servingInfo.helixResource);
    if (viewInfo == null) {
      LOG.warn("View not found for " + fileSet + ", trying backup resource");
      fileSetInfo = fileSetInfoBackupMap.get(fileSet);
      if (fileSetInfo == null) {
        throw new TerrapinGetException("View for file set " + fileSet + " not found.",
            TerrapinGetErrorCode.INVALID_FILE_SET_VIEW);
      }
      viewInfo = zkBackedViewInfoMap.get(fileSetInfo.servingInfo.helixResource);
      if (viewInfo == null) {
        throw new TerrapinGetException("View not file set " + fileSet + " not found.",
            TerrapinGetErrorCode.INVALID_FILE_SET_VIEW);
      }
    }
    return new ImmutablePair(fileSetInfo, viewInfo);
  }
}