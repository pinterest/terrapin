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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.pinterest.terrapin.TerrapinUtil;
import com.pinterest.terrapin.zookeeper.FileSetInfo;
import com.pinterest.terrapin.zookeeper.ViewInfo;
import com.pinterest.terrapin.zookeeper.ZooKeeperManager;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Collections;

/**
 * This class implements a servlet to check status for a specific cluster.
 */
public class ClusterStatusServlet extends HttpServlet {

  private static final Logger LOG = LoggerFactory.getLogger(ClusterStatusServlet.class);
  public static final String BASE_URI = "/status";

  /**
   * This class contains summary information for a file set
   */
  public static class FileSetStatusRow implements Comparable<FileSetStatusRow> {
    public String fileSet;
    public int numOfPartitions;
    public int numOfOnlinePartitions;
    public String hdfsPath;
    public Date createdAt;

    public FileSetStatusRow(String fileSet, int numOfPartitions, int numOfOnlinePartitions,
                            String hdfsPath) {
      this.fileSet = fileSet;
      this.numOfPartitions = numOfPartitions;
      this.numOfOnlinePartitions = numOfOnlinePartitions;
      this.hdfsPath = hdfsPath;
      this.createdAt = new Date(parseTimestampFromHdfsPath(hdfsPath));
    }

    @Override
    public int compareTo(FileSetStatusRow other) {
      return fileSet.toLowerCase().compareTo(other.fileSet.toLowerCase());
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof FileSetStatusRow)) {
        return false;
      }
      FileSetStatusRow otherRow = (FileSetStatusRow) other;
      return fileSet.equals(otherRow.fileSet) &&
          (numOfPartitions == otherRow.numOfPartitions) &&
          (numOfOnlinePartitions == otherRow.numOfOnlinePartitions) &&
          hdfsPath.equals(otherRow.hdfsPath);
    }
  }

  /**
   * This class contains information for file sets are currently loading
   */
  public static class LoadingFileSetRow implements Comparable<LoadingFileSetRow> {
    public String fileSet;
    public Date createdAt;

    public LoadingFileSetRow(String fileSet, String hdfsPath) {
      this.fileSet = fileSet;
      this.createdAt = new Date(parseTimestampFromHdfsPath(hdfsPath));
    }

    @Override
    public int compareTo(LoadingFileSetRow other) {
      return fileSet.toLowerCase().compareTo(other.fileSet.toLowerCase());
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof LoadingFileSetRow)) {
        return false;
      }
      LoadingFileSetRow otherRow = (LoadingFileSetRow) other;
      return fileSet.equals(otherRow.fileSet) && createdAt.equals(otherRow.createdAt);
    }
  }

  /**
   * Enumeration for all kinds of file set errors
   */
  public static enum FileSetError {
    NO_SERVING_INFO("no serving info"),
    NO_VIEW_INFO("no view info");

    private String reason;
    private FileSetError(String reason) {
      this.reason = reason;
    }

    @Override
    public String toString() {
      return reason;
    }
  }

  /**
   * This class contains information for file sets have errors
   */
  public static class FileSetWithErrorRow implements Comparable<LoadingFileSetRow> {
    public String fileSet;
    public FileSetError reason;

    public FileSetWithErrorRow(String fileSet, FileSetError reason) {
      this.fileSet = fileSet;
      this.reason = reason;
    }

    @Override
    public int compareTo(LoadingFileSetRow other) {
      return fileSet.toLowerCase().compareTo(other.fileSet.toLowerCase());
    }

    @Override
    public boolean equals(Object other) {
      if (other == null || !(other instanceof FileSetWithErrorRow)) {
        return false;
      }
      FileSetWithErrorRow otherRow = (FileSetWithErrorRow) other;
      return fileSet.equals(otherRow.fileSet) && reason.equals(otherRow.reason);
    }
  }

  /**
   * Parse timestamp from a hdfs path
   * E.g. Giving '/a/b/c/1234567890000', it returns 1234567890000
   * @param hdfsPath hdfs path
   * @return timestamp
   */
  public static long parseTimestampFromHdfsPath(String hdfsPath) {
    int index = hdfsPath.lastIndexOf('/');
    if (index >= 0) {
      return Long.parseLong(hdfsPath.substring(index + 1));
    } else {
      return Long.parseLong(hdfsPath);
    }
  }

  /**
   * Get a list contains summary information for each file set
   * @param zkManager ZookeeperManager instance
   * @param fileSetsWithError list contains file sets have errors
   * @return file set summary information list
   */
  public static List<FileSetStatusRow> getFileSetStatusTable(
      ZooKeeperManager zkManager, List<FileSetWithErrorRow> fileSetsWithError) {
    List<FileSetStatusRow> rows = new ArrayList<FileSetStatusRow>();
    Map<String, FileSetInfo> fileSetMap = zkManager.getFileSetInfoMap();
    for (Map.Entry<String, FileSetInfo> entry : fileSetMap.entrySet()) {
      String fileSet = entry.getKey();
      FileSetInfo fileSetInfo = entry.getValue();
      if (fileSetInfo.servingInfo == null) {
        fileSetsWithError.add(new FileSetWithErrorRow(fileSet, FileSetError.NO_SERVING_INFO));
        continue;
      }
      ViewInfo viewInfo = zkManager.getViewInfo(fileSetInfo.servingInfo.helixResource);
      if (viewInfo == null) {
        fileSetsWithError.add(new FileSetWithErrorRow(fileSet, FileSetError.NO_VIEW_INFO));
        continue;
      }
      rows.add(new FileSetStatusRow(
        fileSet,
        fileSetInfo.servingInfo.numPartitions,
        viewInfo.getNumOnlinePartitions(),
        fileSetInfo.servingInfo.hdfsPath
      ));
    }
    Collections.sort(rows);
    return rows;
  }

  /**
   * Get loading file sets from zookeeper
   * @param zkManager zookeeper manager
   * @param fileSetsWithError list contains file sets have errors
   * @return loading file sets
   * @throws Exception if communication with zookeeper servers raises exceptions
   */
  public static List<LoadingFileSetRow> getLoadingFileSetTable(
      ZooKeeperManager zkManager, List<FileSetWithErrorRow> fileSetsWithError)
      throws Exception {
    List<LoadingFileSetRow> rows = new ArrayList<LoadingFileSetRow>();
    List<String> loadingFileSets = zkManager.getLockedFileSets();
    for (String fileSet : loadingFileSets) {
      FileSetInfo fileSetInfo =  zkManager.getLockedFileSetInfo(fileSet);
      if (fileSetInfo.servingInfo == null) {
        fileSetsWithError.add(new FileSetWithErrorRow(fileSet, FileSetError.NO_SERVING_INFO));
        continue;
      }
      rows.add(new LoadingFileSetRow(fileSet, fileSetInfo.servingInfo.hdfsPath));
    }
    return rows;
  }

  /**
   * Get all data nodes
   * @param hdfsClient client instance for HDFS
   * @return live data nodes
   * @throws IOException if client goes wrong when communicating with server
   */
  public static List<String> getAllNodeNames(DFSClient hdfsClient) throws IOException {
    DatanodeInfo[] allNodes = hdfsClient.datanodeReport(HdfsConstants.DatanodeReportType.LIVE);
    List<String> allNodeNames = new ArrayList<String>(allNodes.length);
    for (DatanodeInfo nodeInfo : allNodes) {
      allNodeNames.add(TerrapinUtil.getHelixInstanceFromHDFSHost(nodeInfo.getHostName()));
    }
    return allNodeNames;
  }

  @Override
  public void doGet(HttpServletRequest request, HttpServletResponse response)
    throws IOException {
    response.setContentType("text/html");
    ServletContext context = getServletContext();

    ZooKeeperManager zkManager = (ZooKeeperManager) context.getAttribute("zookeeper-manager");
    String clusterName = (String) context.getAttribute("cluster_name");
    DFSClient hdfsClient = (DFSClient) context.getAttribute("hdfs-client");

    List<FileSetWithErrorRow> fileSetsWithError = new ArrayList<FileSetWithErrorRow>();
    List<FileSetStatusRow> fileSets = getFileSetStatusTable(zkManager, fileSetsWithError);
    List<LoadingFileSetRow> loadingFileSets = null;
    List<String> liveInstances = null;
    List<String> allInstances = null;

    try {
      liveInstances = zkManager.getLiveInstances();
    } catch (Exception e) {
      LOG.warn("failed to get live instances from zookeeper", e);
      liveInstances = Lists.newArrayListWithCapacity(0);
    }

    try {
      allInstances = getAllNodeNames(hdfsClient);
    } catch (Exception e) {
      LOG.warn("failed to get all instances from HDFS name node", e);
      allInstances = Lists.newArrayListWithCapacity(0);
      liveInstances.clear();
    }

    try {
      loadingFileSets = getLoadingFileSetTable(zkManager, fileSetsWithError);
    } catch (Exception e) {
      LOG.warn("failed to get loading file sets from zookeeper", e);
      loadingFileSets = Lists.newArrayListWithCapacity(0);
    }

    ClusterStatusTmpl clusterStatusTmpl = new ClusterStatusTmpl();
    clusterStatusTmpl.render(
        response.getWriter(),
        clusterName,
        fileSets,
        loadingFileSets,
        fileSetsWithError,
        allInstances,
        Sets.newHashSet(liveInstances)
    );
  }
}
