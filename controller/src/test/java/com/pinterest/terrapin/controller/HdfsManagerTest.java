package com.pinterest.terrapin.controller;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.apache.helix.spectator.RoutingTableProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for HdfsManager.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(HdfsFileStatus.class)
public class HdfsManagerTest {
  class TestRoutingTableProvider extends RoutingTableProvider {
    private Map<String, List<InstanceConfig>> onlinePartitionMap;

    public TestRoutingTableProvider(Map<String, List<InstanceConfig>> onlinePartitionMap) {
      this.onlinePartitionMap = onlinePartitionMap;
    }

    public void setOnlinePartitionMap(Map<String, List<InstanceConfig>> onlinePartitionMap) {
      this.onlinePartitionMap = onlinePartitionMap;
    }

    @Override
    public List<InstanceConfig> getInstances(String resource, String partition, String state) {
      if (!state.equals("ONLINE")) {
        throw new RuntimeException("Invalid state.");
      }
      List<InstanceConfig> instanceConfigList = Lists.newArrayList();
      if (onlinePartitionMap.containsKey(partition)) {
        return onlinePartitionMap.get(partition);
      }
      return instanceConfigList;
    }
  }

  private HdfsFileStatus buildHdfsStatus(String path) {
    HdfsFileStatus status = PowerMockito.mock(HdfsFileStatus.class);
    Mockito.when(status.getLocalName()).thenReturn(new Path(path).getName());
    return status;
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
    InstanceConfig hostConfig1 = new InstanceConfig("host1");
    hostConfig1.setHostName("host1");
    InstanceConfig hostConfig2 = new InstanceConfig("host2");
    hostConfig2.setHostName("host2");
    InstanceConfig hostConfig3 = new InstanceConfig("host3");
    hostConfig3.setHostName("host3");
    InstanceConfig hostConfig4 = new InstanceConfig("host4");
    hostConfig4.setHostName("host4");

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
        new TestRoutingTableProvider(onlinePartitionMap1);
    assertEquals(0.25, HdfsManager.calculateDeviationForResource(
        resource, idealState, routingTableProvider), 0.01);

    // Check perfect match.
    Map<String, List<InstanceConfig>> onlinePartitionMap2 = (Map)ImmutableMap.of(
        "0", ImmutableList.of(hostConfig1, hostConfig2),
        "1", ImmutableList.of(hostConfig2, hostConfig3));
    routingTableProvider.setOnlinePartitionMap(onlinePartitionMap2);
    assertEquals(0.0, HdfsManager.calculateDeviationForResource(
        resource, idealState, routingTableProvider), 0.01);
  }
}
