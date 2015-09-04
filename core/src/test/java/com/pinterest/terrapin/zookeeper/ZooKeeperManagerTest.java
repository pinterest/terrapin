package com.pinterest.terrapin.zookeeper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyListOf;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.verifyStatic;

import com.pinterest.terrapin.thrift.generated.Options;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.ZooKeeperMap;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.api.support.membermodification.MemberModifier;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;

@RunWith(PowerMockRunner.class)
public class ZooKeeperManagerTest {
  private static final String CLUSTER_NAME = "test_cluster";
  private static final String FILE_SET = "test_fileset";
  private static final FileSetInfo FILE_SET_INFO = new FileSetInfo();
  private static final String FILE_SET_DIR = "/" + CLUSTER_NAME + "/filesets";
  private static final String VIEWS_DIR = "/" + CLUSTER_NAME + "/views";
  private static final String LOCKS_DIR = "/" + CLUSTER_NAME + "/locks";
  private static final String FILE_SET_PATH = FILE_SET_DIR + "/" + FILE_SET;
  private static final String FILE_SET_LOCK_PATH = LOCKS_DIR + "/" + FILE_SET;

  private ZooKeeperManager zkManager;
  private ZooKeeperClient zkClient;
  private ZooKeeper zk;

  @Before
  public void setUp() throws Exception {
    zkClient = mock(ZooKeeperClient.class);
    zk = mock(ZooKeeper.class);
    zkManager = new ZooKeeperManager(zkClient, CLUSTER_NAME);
    when(zkClient.get()).thenReturn(zk);
  }

  @Test
  public void testCreateClusterPaths() throws Exception {
    when(zk.create(anyString(), any(byte[].class), anyListOf(ACL.class), any(CreateMode.class)))
      .thenReturn("");
    ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
    zkManager.createClusterPaths();

    verify(zk, times(3)).create(pathCaptor.capture(), any(byte[].class), anyListOf(ACL.class),
        any(CreateMode.class));
    Set<String> allPaths = Sets.newHashSet(pathCaptor.getAllValues());
    assertTrue(allPaths.contains(FILE_SET_DIR));
    assertTrue(allPaths.contains(VIEWS_DIR));
    assertTrue(allPaths.contains(LOCKS_DIR));
  }

  @Test
  @PrepareForTest(ZooKeeperMap.class)
  public void testRegisterWatchAllFileSets() throws Exception {
    PowerMockito.mockStatic(ZooKeeperMap.class);
    when(ZooKeeperMap.create(any(ZooKeeperClient.class), anyString(), any(Function.class)))
      .thenReturn(null);
    ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
    zkManager.registerWatchAllFileSets();
    verifyStatic(times(2));
    ZooKeeperMap.create(any(ZooKeeperClient.class), pathCaptor.capture(), any(Function.class));
    Set<String> allPaths = Sets.newHashSet(pathCaptor.getAllValues());
    assertTrue(allPaths.contains(FILE_SET_DIR));
    assertTrue(allPaths.contains(VIEWS_DIR));
  }

  @Test
  public void testLockFileSet() throws Exception {
    when(zk.create(anyString(), any(byte[].class), anyListOf(ACL.class), any(CreateMode.class)))
        .thenReturn("");
    CreateMode mode = CreateMode.EPHEMERAL;
    zkManager.lockFileSet(FILE_SET, FILE_SET_INFO, mode);
    verify(zk).create(eq(FILE_SET_LOCK_PATH), any(byte[].class),
        anyListOf(ACL.class), eq(mode));
  }

  @Test
  public void testUnlockFileSet() throws Exception {
    doNothing().when(zk).delete(anyString(), anyInt());
    ArgumentCaptor<String> pathCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<Integer> versionCaptor = ArgumentCaptor.forClass(Integer.class);
    zkManager.unlockFileSet(FILE_SET);
    verify(zk).delete(pathCaptor.capture(), versionCaptor.capture());
    assertEquals(FILE_SET_LOCK_PATH, pathCaptor.getValue());
    assertEquals(new Integer(-1), versionCaptor.getValue());
  }

  @Test
  public void testSetNonExistingFileSetInfo() throws Exception {
    when(zk.exists(anyString(), anyBoolean())).thenReturn(null);
    when(zk.create(anyString(), any(byte[].class), anyListOf(ACL.class), any(CreateMode.class)))
        .thenReturn("");
    zkManager.setFileSetInfo(FILE_SET, FILE_SET_INFO);

    verify(zk).exists(eq(FILE_SET_PATH), eq(false));
    verify(zk).create(eq(FILE_SET_PATH), eq(FILE_SET_INFO.toJson()), anyListOf(ACL.class),
        any(CreateMode.class));
  }

  @Test
  public void testSetExistingFileSetInfo() throws Exception {
    when(zk.exists(anyString(), anyBoolean())).thenReturn(mock(Stat.class));
    when(zk.setData(anyString(), any(byte[].class), anyInt())).thenReturn(mock(Stat.class));
    zkManager.setFileSetInfo(FILE_SET, FILE_SET_INFO);
    verify(zk).exists(eq(FILE_SET_PATH), eq(false));
    verify(zk).setData(eq(FILE_SET_PATH), eq(FILE_SET_INFO.toJson()), eq(-1));
  }

  @Test
  public void testDeleteFileSetInfo() throws Exception {
    doNothing().when(zk).delete(anyString(), anyInt());
    zkManager.deleteFileSetInfo(FILE_SET);
    verify(zk).delete(eq(FILE_SET_PATH), eq(-1));
  }

  @Test
  public void testGetViewInfo() throws Exception {
    PowerMockito.mockStatic(ZooKeeperMap.class);
    ViewInfo viewInfo = new ViewInfo();
    String resource1 = "test_view";
    MemberModifier.field(ZooKeeperManager.class, "viewInfoMap")
        .set(zkManager, ImmutableMap.of(resource1, viewInfo));
    assertNotNull(zkManager.getViewInfo(resource1));
    assertNull(zkManager.getViewInfo("non_existing"));
  }

  @Test
  public void testSetExistingViewInfo() throws Exception {
    ViewInfo viewInfo = mock(ViewInfo.class);
    String resource = "test_resource";
    String resourcePath = VIEWS_DIR + "/" + resource;
    byte[] resourceData = "hello".getBytes();
    when(viewInfo.toCompressedJson()).thenReturn(resourceData);
    when(viewInfo.hasOnlinePartitions()).thenReturn(true);
    when(viewInfo.getResource()).thenReturn(resource);
    when(zk.exists(eq(resourcePath), anyBoolean())).thenReturn(new Stat());
    when(zk.setData(eq(resourcePath), eq(resourceData), eq(-1))).thenReturn(new Stat());
    zkManager.setViewInfo(viewInfo);
    verify(zk).setData(eq(resourcePath), eq(resourceData), eq(-1));
  }

  @Test
  public void testSetNonExistingViewInfo() throws Exception {
    ViewInfo viewInfo = mock(ViewInfo.class);
    String resource = "test_resource";
    String resourcePath = VIEWS_DIR + "/" + resource;
    byte[] resourceData = "hello".getBytes();
    when(viewInfo.toCompressedJson()).thenReturn(resourceData);
    when(viewInfo.hasOnlinePartitions()).thenReturn(true);
    when(viewInfo.getResource()).thenReturn(resource);
    when(zk.exists(eq(resourcePath), anyBoolean())).thenReturn(null);
    when(zk.create(eq(resourcePath), eq(resourceData), anyListOf(ACL.class), any(CreateMode.class)))
        .thenReturn(null);
    zkManager.setViewInfo(viewInfo);
    verify(zk).create(eq(resourcePath), eq(resourceData),
        anyListOf(ACL.class), any(CreateMode.class));
  }

  @Test
  public void testSetViewInfoNoOnlinePart() throws Exception {
    ViewInfo viewInfo = mock(ViewInfo.class);
    byte[] resourceData = "hello".getBytes();
    when(viewInfo.toCompressedJson()).thenReturn(resourceData);
    when(viewInfo.hasOnlinePartitions()).thenReturn(false);
    zkManager.setViewInfo(viewInfo);
  }

  @Test
  public void testGetCandidateHdfsDirMap() throws Exception {
    FileSetInfo fileSetInfo1 =
        new FileSetInfo("abc", "123", 0,
            ImmutableList.copyOf(new FileSetInfo.ServingInfo[]{}), new Options());
    FileSetInfo fileSetInfo2 =
        new FileSetInfo("def", "456", 0,
            ImmutableList.copyOf(new FileSetInfo.ServingInfo[]{}), new Options());
    FileSetInfo fileSetInfo3 =
        new FileSetInfo("ghi", "789", 0,
            ImmutableList.copyOf(new FileSetInfo.ServingInfo[]{}), new Options());
    MemberModifier.field(ZooKeeperManager.class, "fileSetInfoMap")
        .set(zkManager, ImmutableMap.builder()
            .put(fileSetInfo1.fileSetName, fileSetInfo1)
            .put(fileSetInfo2.fileSetName, fileSetInfo2)
            .put(fileSetInfo3.fileSetName, fileSetInfo3)
            .build());

    when(zk.getData(eq(FILE_SET_DIR + "/" + fileSetInfo1.fileSetName),
        anyBoolean(), any(Stat.class))).thenReturn(fileSetInfo1.toJson());
    when(zk.getData(eq(LOCKS_DIR + "/" + fileSetInfo1.fileSetName),
        anyBoolean(), any(Stat.class))).thenReturn(fileSetInfo1.toJson());


    when(zk.getData(eq(FILE_SET_DIR + "/" + fileSetInfo2.fileSetName),
        anyBoolean(), any(Stat.class))).thenReturn(fileSetInfo2.toJson());
    when(zk.getData(eq(LOCKS_DIR + "/" + fileSetInfo2.fileSetName),
        anyBoolean(), any(Stat.class))).thenThrow(new KeeperException.NoNodeException());

    when(zk.getData(eq(FILE_SET_DIR + "/" + fileSetInfo3.fileSetName),
        anyBoolean(), any(Stat.class))).thenThrow(new KeeperException.NoNodeException());
    when(zk.getData(eq(LOCKS_DIR + "/" + fileSetInfo3.fileSetName),
        anyBoolean(), any(Stat.class))).thenReturn(fileSetInfo3.toJson());

    Map<String, Pair<FileSetInfo, FileSetInfo>> results = zkManager.getCandidateHdfsDirMap();
    assertEquals(3, results.size());
    assertEquals(fileSetInfo1, results.get(fileSetInfo1.fileSetName).getLeft());
    assertEquals(fileSetInfo1, results.get(fileSetInfo1.fileSetName).getRight());
    assertEquals(fileSetInfo2, results.get(fileSetInfo2.fileSetName).getLeft());
    assertNull(results.get(fileSetInfo2.fileSetName).getRight());
    assertNull(results.get(fileSetInfo3.fileSetName).getLeft());
    assertEquals(fileSetInfo3, results.get(fileSetInfo3.fileSetName).getRight());
  }

  @Test
  public void testGetControllerLeader() throws Exception {
    String host = "somehost";
    int port = 9090;
    String json = String.format("{\"id\": \"%s_%d\"}", host, port);
    when(zk.getData(eq("/" + CLUSTER_NAME + "/CONTROLLER/LEADER"),
        anyBoolean(), any(Stat.class))).thenReturn(json.getBytes());
    InetSocketAddress address = zkManager.getControllerLeader();
    assertEquals(host, address.getHostName());
    assertEquals(port, address.getPort());
  }

  @Test
  public void testGetClusterInfo() throws Exception {
    String hdfsNameNode = "namenode001";
    int replicaFactor = 3;
    ClusterInfo clusterInfo = new ClusterInfo(hdfsNameNode, replicaFactor);
    when(zk.getData(eq("/" + CLUSTER_NAME), anyBoolean(), any(Stat.class)))
        .thenReturn(clusterInfo.toJson());
    ClusterInfo returnedInfo = zkManager.getClusterInfo();
    assertEquals(hdfsNameNode, returnedInfo.hdfsNameNode);
    assertEquals(replicaFactor, returnedInfo.hdfsReplicationFactor);
  }

  @Test
  public void testSetClusterInfo() throws Exception {
    String hdfsNameNode = "namenode001";
    int replicaFactor = 3;
    ClusterInfo clusterInfo = new ClusterInfo(hdfsNameNode, replicaFactor);
    when(zk.setData(eq("/" + CLUSTER_NAME), eq(clusterInfo.toJson()), anyInt()))
        .thenReturn(new Stat());
    zkManager.setClusterInfo(clusterInfo);
  }
}
