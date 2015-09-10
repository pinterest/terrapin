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

import com.google.common.collect.*;
import com.pinterest.terrapin.Constants;
import com.pinterest.terrapin.TerrapinUtil;
import com.pinterest.terrapin.thrift.generated.Options;
import com.pinterest.terrapin.thrift.generated.PartitionerType;
import com.pinterest.terrapin.zookeeper.FileSetInfo;
import com.pinterest.terrapin.zookeeper.ZooKeeperManager;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.UnresolvedLinkException;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.helix.spectator.RoutingTableProvider;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.*;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test for HdfsManager.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(HdfsFileStatus.class)
public class HdfsManagerTest {
  private static final String CLUSTER_NAME = "test";
  private static final String FILESET = "fileset";

  @MockitoAnnotations.Mock
  private ZooKeeperManager mockZkManager;

  @MockitoAnnotations.Mock
  private HelixAdmin mockHelixAdmin;

  @MockitoAnnotations.Mock
  private DFSClient mockDfsClient;

  private TestRoutingTableProvider testRoutingTableProvider;
  private HdfsManager hdfsManager;

  private InstanceConfig hostConfig1;
  private InstanceConfig hostConfig2;
  private InstanceConfig hostConfig3;
  private InstanceConfig hostConfig4;

  class TestRoutingTableProvider extends RoutingTableProvider {
    // Mapping from a resource to its partition->host mapping.
    private Map<String, Map<String, List<InstanceConfig>>> onlinePartitionMap;

    public TestRoutingTableProvider(Map<String, Map<String, List<InstanceConfig>>> onlinePartitionMap) {
      this.onlinePartitionMap = onlinePartitionMap;
    }

    public void setOnlinePartitionMap(Map<String, Map<String, List<InstanceConfig>>> onlinePartitionMap) {
      this.onlinePartitionMap = onlinePartitionMap;
    }

    @Override
    public List<InstanceConfig> getInstances(String resource, String partition, String state) {
      if (!state.equals("ONLINE")) {
        throw new RuntimeException("Invalid state.");
      }
      if (!onlinePartitionMap.containsKey(resource)) {
        throw new RuntimeException("resource not found.");
      }
      Map<String, List<InstanceConfig>> partitionMap = onlinePartitionMap.get(resource);
      List<InstanceConfig> instanceConfigList = Lists.newArrayList();
      if (partitionMap.containsKey(partition)) {
        return partitionMap.get(partition);
      }
      return instanceConfigList;
    }

    @Override
    public Set<InstanceConfig> getInstances(String resource, String state) {
      Set<InstanceConfig> instanceSet = Sets.newHashSet();
      if (!state.equals("ONLINE")) {
        throw new RuntimeException("Invalid state.");
      }
      if (!onlinePartitionMap.containsKey(resource)) {
        return instanceSet;
      }
      for (List<InstanceConfig> instances : onlinePartitionMap.get(resource).values()) {
        instanceSet.addAll(instances);
      }
      return instanceSet;
    }
  }

  private HdfsFileStatus buildHdfsStatus(String path) {
    HdfsFileStatus status = PowerMockito.mock(HdfsFileStatus.class);
    when(status.getLocalName()).thenReturn(new Path(path).getName());
    when(status.getFullName(any(String.class))).thenReturn(path);
    when(status.getLen()).thenReturn(1000L);
    return status;
  }

  private HdfsFileStatus buildHdfsStatus(String path, Boolean isDir,
      Long modificationTime) {
    HdfsFileStatus status = buildHdfsStatus(path);
    if (isDir != null) {
      when(status.isDir()).thenReturn(isDir);
    }
    if (modificationTime != null) {
      when(status.getModificationTime()).thenReturn(modificationTime);
    }
    return status;
  }

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);
    this.testRoutingTableProvider = new TestRoutingTableProvider(null);
    this.hdfsManager = new HdfsManager(new PropertiesConfiguration(),
        mockZkManager,
        CLUSTER_NAME,
        mockHelixAdmin,
        mockDfsClient,
        testRoutingTableProvider);
    hostConfig1 = new InstanceConfig("host1");
    hostConfig1.setHostName("host1");
    hostConfig2 = new InstanceConfig("host2");
    hostConfig2.setHostName("host2");
    hostConfig3 = new InstanceConfig("host3");
    hostConfig3.setHostName("host3");
    hostConfig4 = new InstanceConfig("host4");
    hostConfig4.setHostName("host4");
  }

  @Test
  public void testVersionDirComparator() {
    HdfsManager.VersionDirComparator comparator = new HdfsManager.VersionDirComparator();

    long versionDir1 = System.currentTimeMillis();
    long versionDir2 = versionDir1 + (long)(Math.random() * 100000L);
    long versionDir3 = versionDir1 - (long)(Math.random() * 100000L);
    // Dir2 comes above Dir1.
    assertTrue(comparator.compare(buildHdfsStatus("/terrapin/data/fileset1/" + versionDir2),
        buildHdfsStatus("/terrapin/data/fileset1/" + versionDir1)) < 0);
    // Dir3 comes above Dir1.
    assertTrue(comparator.compare(buildHdfsStatus("/terrapin/data/fileset1/" + versionDir1),
        buildHdfsStatus("/terrapin/data/fileset1/" + versionDir3)) < 0);
    // Dir3 comes below Dir2.
    assertTrue(comparator.compare(buildHdfsStatus("/terrapin/data/fileset1/" + versionDir3),
        buildHdfsStatus("/terrapin/data/fileset1/" + versionDir2)) > 0);
  }

  @Test
  public void testCalculateDeviationForResource() {
    String resource = "resource";
    CustomModeISBuilder isBuilder = new CustomModeISBuilder(resource);
    isBuilder.assignInstanceAndState("0", "host1", "ONLINE");
    isBuilder.assignInstanceAndState("0", "host2", "ONLINE");
    isBuilder.assignInstanceAndState("1", "host2", "ONLINE");
    isBuilder.assignInstanceAndState("1", "host3", "ONLINE");
    isBuilder.setNumPartitions(2);
    isBuilder.setNumReplica(3);
    isBuilder.setRebalancerMode(IdealState.RebalanceMode.CUSTOMIZED);
    isBuilder.setStateModel("OnlineOffline");
    IdealState idealState = isBuilder.build();

    // Check with deviation.
    Map<String, List<InstanceConfig>> onlinePartitionMap1 = (Map)ImmutableMap.of(
        "0", ImmutableList.of(hostConfig1, hostConfig2),
        "1", ImmutableList.of(hostConfig3, hostConfig4));
    TestRoutingTableProvider routingTableProvider =
        new TestRoutingTableProvider(ImmutableMap.of(resource, onlinePartitionMap1));
    assertEquals(0.25, HdfsManager.calculateDeviationForResource(
        resource, idealState, routingTableProvider), 0.01);

    // Check perfect match.
    Map<String, List<InstanceConfig>> onlinePartitionMap2 = (Map)ImmutableMap.of(
        "0", ImmutableList.of(hostConfig1, hostConfig2),
        "1", ImmutableList.of(hostConfig2, hostConfig3));
    routingTableProvider.setOnlinePartitionMap(ImmutableMap.of(resource, onlinePartitionMap2));
    assertEquals(0.0, HdfsManager.calculateDeviationForResource(
        resource, idealState, routingTableProvider), 0.01);
  }

  private String getResource(String fileSet, long timeSecondsBeforeCurrent) {
    return TerrapinUtil.hdfsDirToHelixResource(Constants.HDFS_DATA_DIR + "/" + fileSet + "/" +
        (System.currentTimeMillis() - (timeSecondsBeforeCurrent * 1000)));
  }

  private void setupBaseDirListing(List<String> fileSets) throws IOException {
    HdfsFileStatus[] fsStatusList = new HdfsFileStatus[fileSets.size() + 1];
    fsStatusList[0] = buildHdfsStatus(Constants.HDFS_DATA_DIR + "/_distcp_XcndjkA", true, null);
    int i = 1;
    for (String fileSet : fileSets) {
      fsStatusList[i++] = buildHdfsStatus(Constants.HDFS_DATA_DIR + "/" + fileSet, true, null);
    }
    when(mockDfsClient.listPaths(eq(Constants.HDFS_DATA_DIR), any(byte[].class))).thenReturn(
            new DirectoryListing(fsStatusList, 0));
  }

  private Long getTimestampFromResource(String resource) {
    return Long.parseLong(resource.split("\\$")[resource.split("\\$").length - 1]);
  }

  private void setupListingForFileSet(String fileSet, List<String> resources, int partitions)
      throws NumberFormatException, IOException {
    HdfsFileStatus[] fsStatusList = new HdfsFileStatus[resources.size() + 1];
    fsStatusList[0] = buildHdfsStatus(Constants.HDFS_DATA_DIR + "/_distcp_XcnddseA", true, null);
    int i = 1;
    for (String resource : resources) {
      fsStatusList[i++] = buildHdfsStatus(TerrapinUtil.helixResourceToHdfsDir(resource), true,
          getTimestampFromResource(resource));
    }
    for (String resource : resources) {
      HdfsFileStatus[] resourceFsStatusList = new HdfsFileStatus[partitions + 1];
      resourceFsStatusList[0] = buildHdfsStatus(TerrapinUtil.helixResourceToHdfsDir(resource) +
          "/_SUCCESS", false, null);
      for (int j = 1 ; j <= partitions; ++j) {
        resourceFsStatusList[j] = buildHdfsStatus(
                TerrapinUtil.helixResourceToHdfsDir(resource) +
            String.format("/part-%05d", j - 1), false, null);
      }
      when(mockDfsClient.listPaths(eq(TerrapinUtil.helixResourceToHdfsDir(resource)),
          any(byte[].class))). thenReturn(new DirectoryListing(resourceFsStatusList, 0));
    }
    when(mockDfsClient.listPaths(eq(Constants.HDFS_DATA_DIR + "/" + fileSet), any(byte[].class))).
        thenReturn(new DirectoryListing(fsStatusList, 0));
  }

  private void setupBlockLocations(String resource, Map<Integer, List<String>> locationMap)
      throws IOException {
    for (Map.Entry<Integer, List<String>> entry : locationMap.entrySet()) {
      String path = TerrapinUtil.helixResourceToHdfsDir(resource) +
          String.format("/part-%05d", entry.getKey());
      BlockLocation[] locations = new BlockLocation[1];
      String[] hostsArray = new String[entry.getValue().size()];
      entry.getValue().toArray(hostsArray);
      locations[0] = new BlockLocation(hostsArray, hostsArray, 0, 1000);
      when(mockDfsClient.getBlockLocations(eq(path), eq(0L), eq(1000L))).thenReturn(
          locations);
    }
  }

  private class IdealStateMatcher extends ArgumentMatcher<IdealState> {
    private final String resource;
    private final Map<Integer, List<String>> partitionHostMap;
    private final int numPartitions;

    public IdealStateMatcher(String resource,
        Map<Integer, List<String>> partitionHostMap, int numPartitions) {
      this.resource = resource;
      this.partitionHostMap = partitionHostMap;
      this.numPartitions = numPartitions;
    }

    @Override
    public boolean matches(Object o) {
      if (o == null || !(o instanceof IdealState)) {
        return false;
      }
      IdealState is = (IdealState)o;
      if (is.getRebalanceMode() != IdealState.RebalanceMode.CUSTOMIZED ||
          !is.getReplicas().equals("3") ||
          is.getNumPartitions() != this.numPartitions ||
          !is.getStateModelDefRef().equals("OnlineOffline")) {
        return false;
      }
      for (Map.Entry<Integer, List<String>> entry : partitionHostMap.entrySet()) {
        Map<String, String> stateMap = is.getInstanceStateMap(
                this.resource + "$" + entry.getKey());
        if (stateMap.size() != entry.getValue().size()) {
          return false;
        }
        for (String host : entry.getValue()) {
          if (!(stateMap.containsKey(host) && stateMap.get(host).equals("ONLINE"))) {
            return false;
          }
        }
      }
      return true;
    }
  }

  private void checkIdealStateModified(String resource,
                                       Map<Integer, List<String>> servingHdfsMapping,
                                       int numPartitions) {
    verify(mockHelixAdmin, times(1)).setResourceIdealState(
        eq(CLUSTER_NAME), eq(resource), argThat(new IdealStateMatcher(
            resource, servingHdfsMapping, numPartitions)));
  }

  private void checkIdealStateNotModified(String resource) {
    verify(mockHelixAdmin, times(0)).setResourceIdealState(
        eq(CLUSTER_NAME), eq(resource), any(IdealState.class));

  }

  private void checkHdfsBlocksNotRetrieved(String resource, int numPartitions) throws Exception {
    for (int i = 0; i < numPartitions; ++i) {
      String path = TerrapinUtil.helixResourceToHdfsDir(resource) + String.format("/part-%05d", i);
      verify(mockDfsClient, times(0)).getBlockLocations(eq(path), eq(0L), anyLong());
    }
  }

  private void checkHdfsBlocksRetrieved(String resource, int numPartitions) throws Exception {
    for (int i = 0; i < numPartitions; ++i) {
      String path = TerrapinUtil.helixResourceToHdfsDir(resource) + String.format("/part-%05d", i);
      verify(mockDfsClient, times(1)).getBlockLocations(eq(path), eq(0L), anyLong());
    }
  }

  private void checkHdfsDataNotDeleted(String resource) throws IOException {
    verify(mockDfsClient, times(0)).delete(
        eq(TerrapinUtil.helixResourceToHdfsDir(resource)), eq(true));
  }

  private void checkHdfsDataDeleted(String resource) throws IOException {
    verify(mockDfsClient, times(1)).delete(
        eq(TerrapinUtil.helixResourceToHdfsDir(resource)), eq(true));
  }

  private void checkResourceDeleted(String resource) {
    verify(mockHelixAdmin, times(1)).dropResource(eq(CLUSTER_NAME), eq(resource));
  }

  private void checkResourceNotDeleted(String resource) {
    verify(mockHelixAdmin, times(0)).dropResource(eq(CLUSTER_NAME), eq(resource));
  }

  private void checkViewInfoDeleted(String resource) throws Exception {
    verify(mockZkManager, times(1)).deleteViewInfo(eq(resource));
  }

  private void checkViewInfoNotDeleted(String resource) throws Exception {
    verify(mockZkManager, times(0)).deleteViewInfo(eq(resource));
  }

  public void testRebalanceLoop(boolean lockLatestFileSet) throws Exception {
    List<String> resources = Lists.newArrayList();
    // This data set is locked if @lockLatestFileSet is true, otherwise the dataset is
    // from an orphaned job.
    resources.add(getResource(FILESET, 100));
    // This data set is from an orphaned job. This will not be deleted (since its new).
    resources.add(getResource(FILESET, 3600));
    // This is the current serving version. This will be rebalanced.
    resources.add(getResource(FILESET, 5400));
    // This is an older version, also in serving. This will be rebalanced.
    resources.add(getResource(FILESET, 6000));
    // This is an older version, in serving. This will be offlined.
    resources.add(getResource(FILESET, 7500));
    // This is an older version, not in serving/offlined. This will be deleted
    // but data on HDFS will remain.
    resources.add(getResource(FILESET, 8000));
    // No corresponding helix resource exists for this directly. It will be deleted.
    resources.add(getResource(FILESET, 8100));
    when(mockHelixAdmin.getResourcesInCluster(CLUSTER_NAME)).thenReturn(
        resources.subList(0, resources.size() - 1));

    FileSetInfo lockedFsInfo = null;
    if (lockLatestFileSet) {
      lockedFsInfo = new FileSetInfo(FILESET,
          TerrapinUtil.helixResourceToHdfsDir(resources.get(0)),
          2,
          Lists.<FileSetInfo.ServingInfo>newArrayList(),
          new Options().setNumVersionsToKeep(2));
    }
    FileSetInfo.ServingInfo oldVersionFsServingInfo = new FileSetInfo.ServingInfo(
        TerrapinUtil.helixResourceToHdfsDir(resources.get(3)),
        resources.get(2),
        2,
        PartitionerType.MODULUS);
    FileSetInfo fsInfo = new FileSetInfo(FILESET,
        TerrapinUtil.helixResourceToHdfsDir(resources.get(2)),
        2,
        Lists.newArrayList(oldVersionFsServingInfo),
        new Options().setNumVersionsToKeep(2));
    when(mockZkManager.getCandidateHdfsDirMap()).thenReturn((Map)ImmutableMap.of(
        FILESET, new ImmutablePair(fsInfo, lockedFsInfo)));
    setupBaseDirListing(Lists.newArrayList(FILESET));
    setupListingForFileSet(FILESET, resources, 2);

    // For the second resource, set up the block mapping.
    String servingResource = resources.get(2);
    String oldVersionResource = resources.get(3);
    String toBeOfflinedResource = resources.get(4);
    Map<Integer, List<String>> servingHdfsMapping = (Map)ImmutableMap.of(
        0, ImmutableList.of("host1", "host2"),
        1, ImmutableList.of("host2", "host3"));
    Map<Integer, List<String>> oldVersionHdfsMapping = (Map)ImmutableMap.of(
        0, ImmutableList.of("host1", "host2"),
        1, ImmutableList.of("host2", "host3"));
    setupBlockLocations(servingResource, servingHdfsMapping);
    setupBlockLocations(oldVersionResource, oldVersionHdfsMapping);
    testRoutingTableProvider.setOnlinePartitionMap(
        (Map) ImmutableMap.of(
            servingResource,
            ImmutableMap.of(
                TerrapinUtil.getViewPartitionName(servingResource, 0),
                ImmutableList.of(hostConfig1, hostConfig2),
                TerrapinUtil.getViewPartitionName(servingResource, 1),
                ImmutableList.of(hostConfig1, hostConfig2)),
            oldVersionResource,
            ImmutableMap.of(
                TerrapinUtil.getViewPartitionName(oldVersionResource, 0),
                ImmutableList.of(hostConfig1, hostConfig2),
                TerrapinUtil.getViewPartitionName(oldVersionResource, 1),
                ImmutableList.of(hostConfig2, hostConfig3)),
            toBeOfflinedResource,
            ImmutableMap.of(
                TerrapinUtil.getViewPartitionName(toBeOfflinedResource, 0),
                ImmutableList.of(hostConfig1, hostConfig2),
                TerrapinUtil.getViewPartitionName(toBeOfflinedResource, 1),
                ImmutableList.of(hostConfig1, hostConfig2))));
    hdfsManager.createAndGetRebalancer().reconcileAndRebalance();

    checkIdealStateModified(servingResource, servingHdfsMapping, 2);
    checkIdealStateModified(toBeOfflinedResource, (Map)Maps.newHashMap(), 2);
    for (String resource : resources) {
      // For other resources, confirm that we do not modify the ideal state.
      if (!resource.equals(toBeOfflinedResource) && !resource.equals(servingResource)) {
        checkIdealStateNotModified(resource);
      }
      // Confirm that we only retrieve HDFS block locations for serving versions.
      if (!resource.equals(servingResource) && !resource.equals(oldVersionResource)) {
        checkHdfsBlocksNotRetrieved(resource, 2);
      } else {
        checkHdfsBlocksRetrieved(resource, 2);
      }
    }
    // This resource has already been offlined, so data as well as the resource are cleaned up.
    String toBeDeletedResource = resources.get(5);
    // The corresponding resource does not exist but there is data on HDFS, this must be deleted.
    String orphanedDataResource = resources.get(6);
    for (String resource : resources) {
      if (resource.equals(toBeDeletedResource)) {
        checkResourceDeleted(resource);
        checkHdfsDataDeleted(resource);
        checkViewInfoDeleted(resource);
      } else if (resource.equals(orphanedDataResource)) {
        checkHdfsDataDeleted(resource);
        checkResourceNotDeleted(resource);
        checkViewInfoNotDeleted(resource);
      } else {
        checkResourceNotDeleted(resource);
        checkHdfsDataNotDeleted(resource);
        checkViewInfoNotDeleted(resource);
      }
    }
  }

  @Test
  public void testRebalanceLoopWithLock() throws Exception {
    testRebalanceLoop(true);
  }

  @Test
  public void testRebalanceLoopWithoutLock() throws Exception {
    testRebalanceLoop(false);
  }

  @Test
  public void testFileSetDelete() throws Exception {
    List<String> resources = Lists.newArrayList();
    // This version is currently serving.
    resources.add(getResource(FILESET, 100));
    // This version if offlined but the helix resource and HDFS data is still there.
    resources.add(getResource(FILESET, 7400));
    when(mockHelixAdmin.getResourcesInCluster(CLUSTER_NAME)).thenReturn(resources);

    FileSetInfo fsInfo = new FileSetInfo(FILESET,
        TerrapinUtil.helixResourceToHdfsDir(resources.get(0)),
        2,
        (List)Lists.newArrayList(),
        new Options());
    fsInfo.deleted = true;
    when(mockZkManager.getCandidateHdfsDirMap()).thenReturn((Map)ImmutableMap.of(
        FILESET, new ImmutablePair(fsInfo, null)));
    setupBaseDirListing(Lists.newArrayList(FILESET));
    setupListingForFileSet(FILESET, resources, 2);

    String servingResource = resources.get(0);
    String oldResource = resources.get(1);
    testRoutingTableProvider.setOnlinePartitionMap((Map)ImmutableMap.of(
        servingResource, ImmutableMap.of(
            TerrapinUtil.getViewPartitionName(servingResource, 0),
            ImmutableList.of(hostConfig1, hostConfig2)),
            TerrapinUtil.getViewPartitionName(servingResource, 1),
            ImmutableList.of(hostConfig1, hostConfig3)));

    HdfsManager.Rebalancer rebalancer = hdfsManager.createAndGetRebalancer();
    rebalancer.reconcileAndRebalance();

    // In the first rebalance, the serving resource will get offlined while the older
    // resource will be deleted (since its already offlined.
    checkIdealStateModified(servingResource, (Map) Maps.newHashMap(), 2);
    checkIdealStateModified(servingResource, (Map) Maps.newHashMap(), 2);

    checkHdfsBlocksNotRetrieved(servingResource, 2);
    checkHdfsDataNotDeleted(servingResource);
    checkViewInfoNotDeleted(servingResource);
    checkResourceNotDeleted(servingResource);

    checkIdealStateNotModified(oldResource);
    checkHdfsBlocksNotRetrieved(oldResource, 2);
    checkHdfsDataDeleted(oldResource);
    checkViewInfoDeleted(oldResource);
    checkResourceDeleted(oldResource);

    // Make sure that the fileset is not deleted yet.
    verify(mockDfsClient, times(0)).delete(eq(Constants.HDFS_DATA_DIR + "/" + FILESET), eq(true));
    verify(mockZkManager, times(0)).deleteFileSetInfo(eq(FILESET));
    verify(mockZkManager, times(0)).unlockFileSet(eq(FILESET));

    // In the 2nd rebalance, the fileset will be deleted.
    // Change HDFS listing & helix listing since one of the resources was deleted.
    when(mockHelixAdmin.getResourcesInCluster(eq(CLUSTER_NAME))).thenReturn(
        resources.subList(0, 1));
    setupListingForFileSet(FILESET, resources.subList(0, 1), 2);
    // The serving resource has been offlined.
    testRoutingTableProvider.setOnlinePartitionMap((Map)Maps.newHashMap());

    rebalancer.reconcileAndRebalance();

    // Note that this check means that the ideal state was NOT modified since these
    // mock calls from the previous reconcileAndRebalance call.
    checkIdealStateModified(servingResource, (Map) Maps.newHashMap(), 2);
    checkHdfsBlocksNotRetrieved(servingResource, 2);
    checkViewInfoDeleted(servingResource);
    checkResourceDeleted(servingResource);
    checkHdfsDataDeleted(servingResource);

    verify(mockDfsClient, times(1)).delete(eq(Constants.HDFS_DATA_DIR + "/" + FILESET), eq(true));
    verify(mockZkManager, times(1)).deleteFileSetInfo(eq(FILESET));
    verify(mockZkManager, times(1)).unlockFileSet(eq(FILESET));
  }
}