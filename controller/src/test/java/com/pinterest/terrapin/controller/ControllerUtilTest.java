package com.pinterest.terrapin.controller;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.pinterest.terrapin.TerrapinUtil;
import com.pinterest.terrapin.thrift.generated.PartitionerType;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.helix.model.IdealState;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
public class ControllerUtilTest {

  @Test
  @PrepareForTest({ControllerUtil.class, TerrapinUtil.class})
  public void testBuildIdealStateForHdfsDir() throws Exception {
    PowerMockito.mockStatic(TerrapinUtil.class);
    String hdfsDir = "/terrapin/data/fileset";

    DFSClient dfsClient = mock(DFSClient.class);
    HdfsFileStatus fileStatus1 = new HdfsFileStatus(1234l, false, 0, 0, 0, 0, null, null, null,
        null, "part-00000".getBytes(), 0, 0, null, (byte)0);
    HdfsFileStatus fileStatus2 = new HdfsFileStatus(5678l, false, 0, 0, 0, 0, null, null, null,
        null, "part-00001".getBytes(), 0, 0, null, (byte)0);
    BlockLocation blockLocation1 = new BlockLocation(new String[]{"host1"},
        new String[]{"host1"}, 0, 0);
    BlockLocation blockLocation2 = new BlockLocation(new String[]{"host2"},
        new String[]{"host2"}, 0, 0);
    BlockLocation blockLocation3 = new BlockLocation(new String[]{"host1"},
        new String[]{"host3"}, 0, 0);
    BlockLocation blockLocation4 = new BlockLocation(new String[]{"host3"},
        new String[]{"host1"}, 0, 0);

    when(TerrapinUtil.getHdfsFileList(any(DFSClient.class), eq(hdfsDir))).thenReturn(
        Lists.newArrayList(fileStatus1, fileStatus2)
    );
    when(dfsClient.getBlockLocations(eq(hdfsDir + "/part-00000"), anyInt(), eq(1234l)))
        .thenReturn(new BlockLocation[]{blockLocation1, blockLocation2});
    when(dfsClient.getBlockLocations(eq(hdfsDir + "/part-00001"), anyInt(), eq(5678l)))
        .thenReturn(new BlockLocation[]{blockLocation3, blockLocation4});
    when(TerrapinUtil.extractPartitionName(eq(fileStatus1.getLocalName()),
        eq(PartitionerType.CASCADING))).thenReturn(0);
    when(TerrapinUtil.extractPartitionName(eq(fileStatus2.getLocalName()),
        eq(PartitionerType.CASCADING))).thenReturn(1);
    when(TerrapinUtil.getHelixInstanceFromHDFSHost(eq("host1"))).thenReturn("host1");
    when(TerrapinUtil.getHelixInstanceFromHDFSHost(eq("host2"))).thenReturn("host2");
    when(TerrapinUtil.getHelixInstanceFromHDFSHost(eq("host3"))).thenReturn("host3");
    when(TerrapinUtil.getBucketSize(eq(2))).thenReturn(1);

    IdealState is = ControllerUtil.buildIdealStateForHdfsDir(dfsClient, hdfsDir, "resource",
        PartitionerType.CASCADING, 100);

    assertEquals(2, is.getNumPartitions());
    assertEquals("resource", is.getResourceName());
    assertEquals(Sets.newHashSet("host1"), is.getInstanceSet("resource_0"));
    assertEquals(Sets.newHashSet("host3"), is.getInstanceSet("resource_1"));
  }
}
