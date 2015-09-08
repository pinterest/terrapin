package com.pinterest.terrapin.zookeeper;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.ZooKeeperMap;
import net.minidev.json.JSONObject;
import net.minidev.json.parser.JSONParser;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;

/**
 * Responsible for maintaining the zookeeper client for performing operations on
 * top of terrapin's filesets znode. This znode contains information such as
 * locks on a fileset, maintaining a map of file sets to their current schema/info
 * and doing CRUD operations on filesets.
 *
 * TODO(varun): Handle missed zookeeper watches. Make this a singleton.
 */
public class ZooKeeperManager {
  private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperManager.class);

  private final String clusterName;
  private final String clusterPath;
  private final String clusterLocksPath;
  private final String clusterViewsPath;
  private final ZooKeeperClient zkClient;

  // A zookeeper backed map containing the mapping from file set name -> FileSetInfo.
  private Map<String, FileSetInfo> fileSetInfoMap;
  // A zookeeper backed map containing the mapping from file set name -> ViewInfo.
  private Map<String, ViewInfo> viewInfoMap;

  private static final Function<byte[], FileSetInfo> BYTES_TO_FILE_SET_INFO =
      new Function<byte[], FileSetInfo>() {
        @Override
        public FileSetInfo apply(byte[] jsonBytes) {
          try {
            // When a fileSet is initially created and locked, we would have a zero length string.
            if (jsonBytes.length == 0) {
              return new FileSetInfo();
            }
            return FileSetInfo.fromJson(jsonBytes);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };

  private static final Function<byte[], ViewInfo> BYTES_TO_VIEW_INFO =
      new Function<byte[], ViewInfo>() {
        @Override
        public ViewInfo apply(byte[] jsonBytes) {
          try {
            // When a fileSet is initially created and locked, we would have a zero length string.
            if (jsonBytes.length == 0) {
              return new ViewInfo();
            }
            return ViewInfo.fromCompressedJson(jsonBytes);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        }
      };

  private static final List<ACL> ZK_ACL_LIST = ZooDefs.Ids.OPEN_ACL_UNSAFE;

  public ZooKeeperManager(ZooKeeperClient zkClient, String clusterName) throws UnknownHostException {
    this.clusterName = clusterName;
    this.clusterPath = "/" + this.clusterName + "/filesets";
    this.clusterLocksPath = "/" + this.clusterName + "/locks";
    this.clusterViewsPath = "/" + this.clusterName + "/views";
    this.zkClient = zkClient;
    this.fileSetInfoMap = null;
  }

  /**
   * Create cluster fileset, lock, view paths
   *
   * @throws Exception if communication with ZooKeeper goes wrong
   */
  public void createClusterPaths() throws Exception {
    if (zkClient.get().exists(clusterPath, false) == null) {
      zkClient.get().create(clusterPath, null, ZK_ACL_LIST, CreateMode.PERSISTENT);
    }
    if (zkClient.get().exists(clusterLocksPath, false) == null) {
      zkClient.get().create(clusterLocksPath, null, ZK_ACL_LIST, CreateMode.PERSISTENT);
    }
    if (zkClient.get().exists(clusterViewsPath, false) == null) {
      zkClient.get().create(clusterViewsPath, null, ZK_ACL_LIST, CreateMode.PERSISTENT);
    }
  }

  public void registerWatchAllFileSets() throws ZooKeeperClient.ZooKeeperConnectionException,
      KeeperException, InterruptedException {
    this.fileSetInfoMap = ZooKeeperMap.create(
        this.zkClient,
        this.clusterPath,
        BYTES_TO_FILE_SET_INFO);
    this.viewInfoMap = ZooKeeperMap.create(
        this.zkClient,
        this.clusterViewsPath,
        BYTES_TO_VIEW_INFO);
  }

  private String getFileSetPath(String fileSet) {
    return this.clusterPath + "/" + fileSet;
  }

  private String getFileSetLockPath(String fileSet) {
    return this.clusterLocksPath + "/" + fileSet;
  }

  /**
   * Creates a lock on a file set. Used for operations such as loading new data into a file
   * set, deleting a file set etc. Since these are coarse grained operations, the lock qps
   * is extremely low.
   *
   * @param fileSet The name of the file set to lock.
   * @param fileSetInfo The file set info to be written as part of the lock. This is especially
   *                    useful while uploading data since this info can be used by the controller
   *                    to ensure that it does not delete data as it is being uploaded.
   * @param mode        create mode for the lock
   */
  public void lockFileSet(String fileSet,
                          FileSetInfo fileSetInfo,
                          CreateMode mode)
      throws Exception {
    try {
      zkClient.get().create(getFileSetLockPath(fileSet),
          fileSetInfo.toJson(),
          ZK_ACL_LIST,
          mode);
    } catch (KeeperException e) {
      if (e instanceof KeeperException.NodeExistsException) {
        LOG.error("File set " + fileSet + " is already locked.");
      }
      throw e;
    }
  }

  /**
   * Creates a lock on a file set. Used for operations such as loading new data into a file
   * set, deleting a file set etc. Since these are coarse grained operations, the lock qps
   * is extremely low.
   *
   * @param fileSet The name of the file set to lock.
   * @param fileSetInfo The file set info to be written as part of the lock. This is especially
   *                    useful while uploading data since this info can be used by the controller
   *                    to ensure that it does not delete data as it is being uploaded.
   */
  public void lockFileSet(String fileSet,
                          FileSetInfo fileSetInfo)
      throws Exception {

    lockFileSet(fileSet, fileSetInfo, CreateMode.EPHEMERAL);
  }

  /**
   * Unlocks a file set. See @lockFileSet above.
   */
  public void unlockFileSet(String fileSet) throws ZooKeeperClient.ZooKeeperConnectionException,
      KeeperException, InterruptedException {
    zkClient.get().delete(getFileSetLockPath(fileSet), -1);
  }

  /**
   * Sets the FileSetInfo for a fileset. If the file set node, does not, it creates the node.
   */
  public void setFileSetInfo(String fileSet,
                             FileSetInfo fileSetInfo) throws Exception {
    // Check if the fileSet exists. If not, then create it.
    String fileSetPath = getFileSetPath(fileSet);
    Stat stat = zkClient.get().exists(fileSetPath, false);
    if (stat == null) {
      try {
        zkClient.get().create(
            fileSetPath, fileSetInfo.toJson(), ZK_ACL_LIST, CreateMode.PERSISTENT);
      } catch (KeeperException e) {
        if (e instanceof KeeperException.NodeExistsException) {
          LOG.error("File set " + fileSet + " was created after we checked that it does not " +
                    "exist. We are racing with another client.");
        }
        throw e;
      }
    } else {
      zkClient.get().setData(getFileSetPath(fileSet), fileSetInfo.toJson(), -1);
    }
  }

  /**
   * Delete file set info from ZooKeeper
   * @param fileSet name of file set
   * @throws Exception if file set does not exist or communication with ZooKeeper goes wrong
   */
  public void deleteFileSetInfo(String fileSet)
      throws Exception {
    zkClient.get().delete(getFileSetPath(fileSet), -1);
  }

  public FileSetInfo getFileSetInfo(String fileSet) {
    if (fileSetInfoMap == null) {
      throw new UnsupportedOperationException("Watch is not established on " + clusterPath);
    }
    FileSetInfo fileSetInfo = fileSetInfoMap.get(fileSet);
    if (fileSetInfo != null && fileSetInfo.valid) {
      return fileSetInfo;
    }
    return null;
  }

  public ViewInfo getViewInfo(String resource) {
    if (viewInfoMap == null) {
      throw new UnsupportedOperationException("Watch is not established on " + clusterViewsPath);
    }
    return viewInfoMap.get(resource);
  }

  public void setViewInfo(ViewInfo viewInfo) throws Exception {
    String resource = viewInfo.getResource();
    byte[] json = viewInfo.toCompressedJson();
    LOG.info("Writtng compressed json for resource : " + resource + " - total bytes " +
             json.length);
    if (viewInfo.hasOnlinePartitions()) {
      String viewPath = this.clusterViewsPath + "/" + resource;
      if (zkClient.get().exists(viewPath, false) != null) {
        zkClient.get().setData(viewPath, json, -1);
      } else {
        zkClient.get().create(viewPath, json, ZK_ACL_LIST, CreateMode.PERSISTENT);
      }
    } else {
      LOG.info("No online partitions for " + resource + " - skipped writing.");
    }
  }

  public void deleteViewInfo(String resource) throws Exception {
    LOG.info("Deleting compressed view for " + resource);
    String viewPath = this.clusterViewsPath + "/" + resource;
    if (zkClient.get().exists(viewPath, false) != null) {
      zkClient.get().delete(viewPath, -1);
    }
  }

  /**
   * Performs a forced read against zookeeper instead of relying on our watch to get
   * the information for each node.
   */
  public FileSetInfo getFileSetInfoForced(String fileSet) throws
      ZooKeeperClient.ZooKeeperConnectionException,
      KeeperException,
      InterruptedException {
    return BYTES_TO_FILE_SET_INFO.apply(zkClient.get().getData(getFileSetPath(fileSet),
        false,
        new Stat()));
  }
    
  public FileSetInfo getLockedFileSetInfo(String fileSet) throws
      ZooKeeperClient.ZooKeeperConnectionException,
      KeeperException,
      InterruptedException {
    return BYTES_TO_FILE_SET_INFO.apply(zkClient.get().getData(getFileSetLockPath(fileSet),
        false,
        new Stat()));
  }

  /**
   * Returns a mapping of all file sets and their file set info(s) including the ones stored
   * as part of locks.
   */
  public Map<String, Pair<FileSetInfo, FileSetInfo>> getCandidateHdfsDirMap() throws
      ZooKeeperClient.ZooKeeperConnectionException,
      KeeperException,
      InterruptedException {
    Map<String, Pair<FileSetInfo, FileSetInfo>> map = Maps.newHashMapWithExpectedSize(
        fileSetInfoMap.size());
    for (String fileSet : fileSetInfoMap.keySet()) {
      FileSetInfo currentFileSetInfo = null;
      FileSetInfo lockedFileSetInfo = null;
      // We do a forced zookeeper read to avoid any delays in propagation of watches.
      // This API is only used by the controller inside a single thread, so it should
      // not be heavy on zookeeper.
      try {
        currentFileSetInfo = getFileSetInfoForced(fileSet);
      } catch (KeeperException e) {
        // If we get an exception other than NoNodeException, simply rethrow.
        if (!(e instanceof KeeperException.NoNodeException)) {
          throw e;
        }
      }
      try {
        lockedFileSetInfo = getLockedFileSetInfo(fileSet);
      } catch (KeeperException e) {
        // If we get an exception other than NoNodeException, simply rethrow.
        if (!(e instanceof KeeperException.NoNodeException)) {
          throw e;
        }
      }
      if (currentFileSetInfo != null && !currentFileSetInfo.valid) {
        currentFileSetInfo = null;
      }
      if (lockedFileSetInfo != null || currentFileSetInfo != null) {
        map.put(fileSet, new ImmutablePair(currentFileSetInfo, lockedFileSetInfo));
      }
    }
    return map;
  }

  /**
   * Returns the address of the currently active controller.
   */
  public InetSocketAddress getControllerLeader() throws Exception {
    String json = new String(zkClient.get().getData(
        "/" + this.clusterName + "/CONTROLLER/LEADER",
        false,
        new Stat()));
    // Parse this Json.
    JSONObject object = (JSONObject)new JSONParser(
        JSONParser.MODE_JSON_SIMPLE|JSONParser.ACCEPT_SIMPLE_QUOTE).parse(json);
    String[] leaderHostPort = ((String)object.get("id")).split("_");
    String host = leaderHostPort[0];
    int port = Integer.parseInt(leaderHostPort[1]);
    return new InetSocketAddress(host, port);
  }

  /**
   * Retrieves and returns the cluster info for the terrapin cluster.
  */
  public ClusterInfo getClusterInfo() throws Exception {
    byte[] data = zkClient.get().getData("/" + this.clusterName, false, new Stat());
    if (data == null) {
      return null;
    }
    String json = new String(data);
    ClusterInfo info;
    try {
      info = ClusterInfo.fromJson(json.getBytes());
    } catch (Exception e) {
      LOG.info("Invalid cluster info " + json);
      return null;
    }
    return info;
  }

  /**
   * Sets/overwrites the cluster info for the terrapin cluster.
   */
  public void setClusterInfo(ClusterInfo info) throws Exception {
    byte[] json = info.toJson();
    LOG.info("Setting cluster info to \n" + new String(json));
    zkClient.get().setData("/" + this.clusterName, json, -1);
  }

  /**
   * WARNING: The returning map is read-only. If the client code tries to modify it, it will
   * raise a UnsupportedOperationException
   * @return A zookeeper backed map containing the mapping from file set name -> FileSetInfo
   */
  public Map<String, FileSetInfo> getFileSetInfoMap(){
    return fileSetInfoMap;
  }

  /**
   * Get currently loading file sets
   * @return file sets
   * @throws Exception if zookeeper client has problems for communicating with servers
   */
  public List<String> getLockedFileSets() throws Exception {
    return zkClient.get().getChildren(this.clusterLocksPath, false);
  }

  /**
   * Get live instances from ZooKeeper
   * @return List of live instances
   */
  public List<String> getLiveInstances()
      throws InterruptedException, ZooKeeperClient.ZooKeeperConnectionException, KeeperException {
    return zkClient.get().getChildren(String.format("/%s/LIVEINSTANCES", this.clusterName), false);
  }
}
