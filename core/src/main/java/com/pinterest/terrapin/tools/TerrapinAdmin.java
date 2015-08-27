package com.pinterest.terrapin.tools;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.pinterest.terrapin.Constants;
import com.pinterest.terrapin.TerrapinUtil;
import com.pinterest.terrapin.zookeeper.ClusterInfo;
import com.pinterest.terrapin.zookeeper.FileSetInfo;
import com.pinterest.terrapin.zookeeper.ViewInfo;
import com.pinterest.terrapin.zookeeper.ZooKeeperManager;
import com.twitter.common.zookeeper.ZooKeeperClient;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.HelixAdmin;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/**
 * Admin tool for running administrative actions against a terrapin
 * cluster.
 */
public class TerrapinAdmin {
  private static final Logger LOG = LoggerFactory.getLogger(TerrapinAdmin.class);

  private final PropertiesConfiguration configuration;
  private final String zkQuorum;
  private final ZooKeeperManager zkManager;

  public TerrapinAdmin(PropertiesConfiguration configuration) throws Exception {
    this.configuration = configuration;

    String zkQuorum = TerrapinUtil.getZKQuorumFromConf(configuration);
    ZooKeeperClient zkClient = TerrapinUtil.getZooKeeperClient(zkQuorum, 30);
    ZooKeeperManager zkManager = new ZooKeeperManager(zkClient,
        configuration.getString(Constants.HELIX_CLUSTER));
    zkManager.registerWatchAllFileSets();
    this.zkQuorum = zkQuorum;
    this.zkManager = zkManager;
  }

  // Set the namenode address for the terrapin cluster.
  public void setNamenode(String[] args) throws Exception {
    ClusterInfo clusterInfo = zkManager.getClusterInfo();
    if (clusterInfo == null) {
      clusterInfo = new ClusterInfo("", Constants.DEFAULT_HDFS_REPLICATION);
    }
    clusterInfo.hdfsNameNode = args[1];
    zkManager.setClusterInfo(clusterInfo);
  }

  // Override the HDFS replication number.
  public void setDfsReplication(String[] args) throws Exception {
    ClusterInfo clusterInfo = zkManager.getClusterInfo();
    if (clusterInfo == null) {
      clusterInfo = new ClusterInfo("", Constants.DEFAULT_HDFS_REPLICATION);
    }
    clusterInfo.hdfsReplicationFactor = Integer.parseInt(args[1]);
    zkManager.setClusterInfo(clusterInfo);
  }

  // Check the cluster health.
  public void checkClusterHealth(String[] args) throws Exception {
    this.zkManager.registerWatchAllFileSets();
    Thread.sleep(10000);
    int totalMissing = 0, totalPartitions = 0;
    List<String> fileSetsNotFullyServing = Lists.newArrayList();
    List<String> fileSetsWithInconsistentView = Lists.newArrayList();

    Map<String, Pair<FileSetInfo, FileSetInfo>> fileSetInfoMap =
        this.zkManager.getCandidateHdfsDirMap();
    HelixAdmin helixAdmin = new ZKHelixAdmin(zkQuorum);
    for (Map.Entry<String, Pair<FileSetInfo, FileSetInfo>> entry : fileSetInfoMap.entrySet()) {
      String fileSet = "\"" + entry.getKey() + "\"";
      LOG.info("");
      LOG.info("Checking file set " + fileSet);
      if (entry.getValue().getRight() != null) {
        LOG.info(fileSet + " is locked (either UPLOADING or a maintenance operation).");
      }
      FileSetInfo fileSetInfo = entry.getValue().getLeft();
      if (fileSetInfo == null) {
        LOG.info(fileSet + " is not serving.");
      } else {
        int numPartitions = fileSetInfo.servingInfo.numPartitions;
        totalPartitions += numPartitions;
        ExternalView externalView = helixAdmin.getResourceExternalView(
            configuration.getString(Constants.HELIX_CLUSTER),
            fileSetInfo.servingInfo.helixResource);
        int numServing = 0;
        for (String partition : externalView.getPartitionSet()) {
          Map<String, String> stateMap = externalView.getStateMap(partition);
          if (stateMap == null) {
            continue;
          }
          for (Map.Entry<String, String> stateEntry : stateMap.entrySet()) {
            if (stateEntry.getValue().equals("ONLINE")) {
              numServing++;
              break;
            }
          }
        }
        LOG.info(fileSet + " serving at " + numServing + "/" + numPartitions);
        totalMissing += (numPartitions - numServing);
        if (numServing < numPartitions) {
          fileSetsNotFullyServing.add(fileSet);
        }
        ViewInfo viewInfo = new ViewInfo(externalView);
        ViewInfo viewInfoInZk = zkManager.getViewInfo(fileSetInfo.servingInfo.helixResource);
        if (viewInfoInZk == null || !viewInfoInZk.equals(viewInfo)) {
          LOG.info("Compressed view inconsistent for " + fileSet);
          fileSetsWithInconsistentView.add(fileSet);
        } else {
          LOG.info("Compressed view consistent with helix external view.");
        }
      }
    }
    if (totalMissing > 0) {
      LOG.info("");
      LOG.info("TOTAL MISSING PARTITIONS " + totalMissing + " out of " + totalPartitions);
      LOG.info("Unhealthy filesets.");
      for (String fileSet : fileSetsNotFullyServing) {
        LOG.info(fileSet);
      }
    } else {
      LOG.info("");
      LOG.info("ALL SHARDS SERVING :)");
    }
    if (!fileSetsWithInconsistentView.isEmpty()) {
      LOG.info("");
      LOG.info("Filesets with inconsistent compressed/external view.");
      for (String fileSet : fileSetsWithInconsistentView) {
        LOG.info(fileSet);
      }
    }
  }

  @VisibleForTesting
  protected static int selectFileSetRollbackVersion(FileSetInfo fileSetInfo,
                                                    InputStream inputStream) {
    int size = fileSetInfo.oldServingInfoList.size();
    if (fileSetInfo.numVersionsToKeep < 2 || fileSetInfo.oldServingInfoList.size() < 1) {
      throw new IllegalArgumentException("no available version for rollback");
    }
    System.out.println("available versions for rollback:");
    for (int i = 0; i < size; i++) {
      FileSetInfo.ServingInfo servingInfo = fileSetInfo.oldServingInfoList.get(i);
      System.out.println(String.format("[%d] %s", i, servingInfo.hdfsPath));
    }
    if (size > 1) {
      System.out.print(String.format("choose a version [0-%d]: ", size - 1));
    } else {
      System.out.print("choose a version [0]: ");
    }
    Scanner scanner = new Scanner(inputStream);
    if (scanner.hasNextLine()) {
      String input = scanner.nextLine();
      if (input.length() > 0) {
        int index = Integer.valueOf(input);
        if (index >= 0 && index < size) {
          return index;
        }
        throw new IllegalArgumentException(
            String.format("version index should between 0 and %d", size - 1)
        );
      }
    }
    throw new IllegalArgumentException("version index not specified");
  }

  @VisibleForTesting
  protected static boolean confirmFileSetRollbackVersion(String fileSet, FileSetInfo fileSetInfo,
                                                         int versionIndex,
                                                         InputStream inputStream) {
    String rollbackHdfs = fileSetInfo.oldServingInfoList.get(versionIndex).hdfsPath;
    System.out.print(String.format("are you sure to rollback %s from %s to %s? [y/n]: ",
        fileSet, fileSetInfo.servingInfo.hdfsPath, rollbackHdfs));
    Scanner scanner = new Scanner(inputStream);
    if (scanner.hasNextLine()) {
      String input = scanner.nextLine();
      if (input.length() > 0 && input.equalsIgnoreCase("y")) {
        return true;
      }
    }
    return false;
  }

  @VisibleForTesting
  protected static boolean confirmFileSetDeletion(String fileSet,
                                                  InputStream inputStream) {
    System.out.print(String.format("are you sure to delete %s? [y/n]: ", fileSet));
    Scanner scanner = new Scanner(inputStream);
    if (scanner.hasNextLine()) {
      String input = scanner.nextLine();
      if (input.length() > 0 && input.equalsIgnoreCase("y")) {
        return true;
      }
    }
    return false;
  }

  /**
   * Rollback specific file set to last kept version.
   * This function is for command line tool usage.
   *
   * @param args command line arguments containing file set
   * @throws Exception if communication with ZooKeeper fail
   */
  public void rollbackFileSet(String[] args) throws Exception {
    if (args.length > 1) {
      final String fileSet = args[1];
      FileSetInfo fileSetInfo = lockFileSet(zkManager, fileSet);

      // Add shutdown hook to gracefully catch SIGKILL signal
      Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            unlockFileSet(zkManager, fileSet);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }));

      try {
        int versionIndex = selectFileSetRollbackVersion(fileSetInfo, System.in);
        if (confirmFileSetRollbackVersion(fileSet, fileSetInfo, versionIndex, System.in)) {
          rollbackFileSet(zkManager, fileSet, fileSetInfo, versionIndex);
        } else {
          System.out.println("exiting rollback...");
        }
      } catch (IllegalArgumentException exception) {
        LOG.error(exception.getMessage());
      }
    } else {
      System.err.println("missing file set argument");
    }
  }

  /**
   * Delete file set from command line
   *
   * @param args command line arguments containing file set
   * @throws Exception if communication with ZooKeeper fail
   */
  public void deleteFileSet(String[] args) throws Exception {
    if (args.length > 1) {
      String fileSet = args[1];
      if (confirmFileSetDeletion(fileSet, System.in)) {
        deleteFileSet(zkManager, fileSet);
        System.out.println("file set is successfully marked to be deleted. It will be " +
            "physically deleted after short period of time.");
      }
    } else {
      System.err.println("missing file set argument");
    }
  }

  /**
   * Lock file set
   *
   * @param zkManager ZooKeeperManager instance
   * @param fileSet name of file set
   * @param mode create mode for lock
   * @return file set information
   * @throws Exception if communication with ZooKeeper goes wrong
   */
  public static FileSetInfo lockFileSet(ZooKeeperManager zkManager,
                                        String fileSet,
                                        CreateMode mode)
      throws Exception {
    FileSetInfo fileSetInfo = zkManager.getFileSetInfo(fileSet);
    if (fileSetInfo == null) {
      throw new IllegalArgumentException("no such file set or it is invalid");
    }
    zkManager.lockFileSet(fileSet, fileSetInfo, mode);

    //re-read the file set information in case it is changed during the above process
    return zkManager.getFileSetInfo(fileSet);
  }

  /**
   * Lock file set
   *
   * @param zkManager ZooKeeperManager instance
   * @param fileSet name of file set
   * @return file set information
   * @throws Exception if communication with ZooKeeper goes wrong
   */
  public static FileSetInfo lockFileSet(ZooKeeperManager zkManager, String fileSet)
      throws Exception {
    return lockFileSet(zkManager, fileSet, CreateMode.EPHEMERAL);
  }

  /**
   * Unlock file set
   *
   * @param zkManager ZooKeeperManager instance
   * @param fileSet name of file set
   * @throws Exception if communication with ZooKeeper goes wrong
   */
  public static void unlockFileSet(ZooKeeperManager zkManager, String fileSet)
      throws Exception {
    zkManager.unlockFileSet(fileSet);
    LOG.info(String.format("release %s lock", fileSet));
  }

  /**
   * Rollback file set to a specific version
   *
   * @param zkManager ZooKeeperManager instance
   * @param fileSet name of file set
   * @param fileSetInfo file set information
   * @param versionIndex index of version to be rolled back to
   * @throws IllegalArgumentException if version index is out of bound
   * @throws Exception if communication with ZooKeeper goes wrong
   */
  public static void rollbackFileSet(ZooKeeperManager zkManager, String fileSet,
                                     FileSetInfo fileSetInfo, int versionIndex)
      throws Exception {
    List<FileSetInfo.ServingInfo> oldServingInfoList = fileSetInfo.oldServingInfoList;
    if (versionIndex >= 0 && versionIndex < oldServingInfoList.size()) {
      String currentHdfsPath = fileSetInfo.servingInfo.hdfsPath;
      String rollbackHdfsPath = oldServingInfoList.get(versionIndex).hdfsPath;
      fileSetInfo.servingInfo = oldServingInfoList.get(versionIndex);
      fileSetInfo.oldServingInfoList = oldServingInfoList.subList(versionIndex + 1,
          oldServingInfoList.size());
      zkManager.setFileSetInfo(fileSet, fileSetInfo);
      LOG.info(String.format("successfully rollback %s from %s to %s",
          fileSet, currentHdfsPath, rollbackHdfsPath));
    } else {
      throw new IllegalArgumentException("version index is out of bound");
    }
  }

  /**
   * Delete file set
   *
   * @param zkManager ZooKeeperManager instance
   * @param fileSet name of file set
   * @throws Exception if communication with ZooKeeper goes wrong
   */
  public static void deleteFileSet(ZooKeeperManager zkManager, String fileSet)
      throws Exception {
    LOG.info(String.format("deleting file set %s", fileSet));
    FileSetInfo fileSetInfo = lockFileSet(zkManager, fileSet, CreateMode.PERSISTENT);
    fileSetInfo.deleted = true;
    zkManager.setFileSetInfo(fileSet, fileSetInfo);
  }

  public static void main(String[] args) throws Exception {
    PropertiesConfiguration configuration = TerrapinUtil.readPropertiesExitOnFailure(
            System.getProperties().getProperty("terrapin.config"));
    TerrapinAdmin admin = new TerrapinAdmin(configuration);
    String action = args[0];
    Method[] methods = admin.getClass().getMethods();
    boolean done = false;
    for (Method method : methods) {
      if (method.getName().equals(action) && !Modifier.isStatic(method.getModifiers())) {
        method.invoke(admin, (Object)args);
        done = true;
      }
    }
    if (!done) {
      LOG.error("Could not find a function call for " + args[0]);
      System.exit(1);
    }
  }
}