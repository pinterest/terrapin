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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.pinterest.terrapin.Constants;
import com.pinterest.terrapin.TerrapinUtil;
import com.pinterest.terrapin.thrift.generated.PartitionerType;

import com.google.common.collect.Sets;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.DirectoryListing;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.helix.model.IdealState;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.List;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HdfsFileStatus.class})
public class ControllerUtilTest {

  public void testBuildIdealStateForHdfsDirHelper(boolean zkCompression,
                                                  int numPartitions) throws Exception {
    String hdfsDir = Constants.HDFS_DATA_DIR + "/fileset";
    DFSClient dfsClient = mock(DFSClient.class);

    // Create three hosts in the clusters.
    List<BlockLocation> locations = ImmutableList.of(
            new BlockLocation(new String[]{"host1", "host2"},
                              new String[]{"host1", "host2"}, 0, 0));
    HdfsFileStatus[] fileStatuses = new HdfsFileStatus[numPartitions];
    for (int i = 0; i < numPartitions; ++i) {
      fileStatuses[i] = PowerMockito.mock(HdfsFileStatus.class);
      String localName = TerrapinUtil.formatPartitionName(i);
      when(fileStatuses[i].getLocalName()).thenReturn(localName);
      when(fileStatuses[i].getFullName(eq(hdfsDir))).thenReturn(hdfsDir + "/" + localName);
      when(fileStatuses[i].getLen()).thenReturn(1000L);
      BlockLocation[] locationArray = new BlockLocation[1];
      locations.subList(0, 1).toArray(locationArray);
      when(dfsClient.getBlockLocations(eq(fileStatuses[i].getFullName(hdfsDir)),
          anyLong(), anyLong())).thenReturn(locationArray);
    }


    when(dfsClient.listPaths(eq(hdfsDir), any(byte[].class))).thenReturn(new DirectoryListing(
        fileStatuses, 0));

    IdealState is = ControllerUtil.buildIdealStateForHdfsDir(dfsClient, hdfsDir, "resource",
        PartitionerType.CASCADING, 2, zkCompression);

    assertEquals(numPartitions, is.getNumPartitions());
    assertEquals("resource", is.getResourceName());
    for (int i = 0; i < numPartitions; ++i) {
      String partition;
      if (numPartitions > 1000 && !zkCompression) {
        partition = "resource_" + i;
      } else {
        partition = "resource$" + i;
      }
      assertEquals(Sets.newHashSet("host1", "host2"), is.getInstanceSet(partition));
    }
    assertEquals("OnlineOffline", is.getStateModelDefRef());
    if (zkCompression) {
      assertTrue(is.getRecord().getBooleanField("enableCompression", false));
    }
    assertEquals(IdealState.RebalanceMode.CUSTOMIZED, is.getRebalanceMode());
  }

  @Test
  public void testBuildIdealStateForHdfsDirNoCompressionNoBucketing() throws Exception {
    testBuildIdealStateForHdfsDirHelper(false, 500);
  }

  @Test
  public void testBuildIdealStateForHdfsDirNoCompressionWithBucketing() throws Exception {
    testBuildIdealStateForHdfsDirHelper(false, 1500);
  }

  @Test
  public void testBuildIdealStateForHdfsDirWithCompression() throws Exception {
    testBuildIdealStateForHdfsDirHelper(true, 1100);
  }
}