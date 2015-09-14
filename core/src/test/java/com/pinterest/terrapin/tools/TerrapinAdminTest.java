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
package com.pinterest.terrapin.tools;

import com.google.common.collect.ImmutableList;
import com.pinterest.terrapin.thrift.generated.PartitionerType;
import com.pinterest.terrapin.zookeeper.FileSetInfo;
import com.pinterest.terrapin.zookeeper.ZooKeeperManager;
import org.apache.zookeeper.CreateMode;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.ByteArrayInputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TerrapinAdminTest {

  @Test
  public void testLockFileSet() throws Exception {
    ZooKeeperManager mockZKManager = mock(ZooKeeperManager.class);
    FileSetInfo fileSetInfo = new FileSetInfo();
    String fileSet = "test_fileset";
    when(mockZKManager.getFileSetInfo(eq(fileSet))).thenReturn(fileSetInfo);
    doNothing().when(mockZKManager).lockFileSet(anyString(), any(FileSetInfo.class),
        eq(CreateMode.EPHEMERAL));
    TerrapinAdmin.lockFileSet(mockZKManager, fileSet);
    verify(mockZKManager, times(1)).lockFileSet(eq(fileSet), eq(fileSetInfo),
        eq(CreateMode.EPHEMERAL));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testLockNonExistingFileSet() throws Exception {
    ZooKeeperManager mockZKManager = mock(ZooKeeperManager.class);
    String fileSet = "test_fileset";
    when(mockZKManager.getFileSetInfo(eq(fileSet))).thenReturn(null);
    TerrapinAdmin.lockFileSet(mockZKManager, fileSet);
  }

  @Test
  public void testUnlockFileSet() throws Exception {
    ZooKeeperManager mockZKManager = mock(ZooKeeperManager.class);
    String fileSet = "test_fileset";
    doNothing().when(mockZKManager).unlockFileSet(anyString());
    TerrapinAdmin.unlockFileSet(mockZKManager, fileSet);
    verify(mockZKManager, times(1)).unlockFileSet(eq(fileSet));
  }

  @Test
  public void testRollbackFileSet() throws Exception {
    ZooKeeperManager mockZKManager = mock(ZooKeeperManager.class);
    FileSetInfo fileSetInfo = new FileSetInfo();
    fileSetInfo.numVersionsToKeep = 4;
    fileSetInfo.oldServingInfoList = ImmutableList.of(
        new FileSetInfo.ServingInfo("/hdfs/path/1", "resource", 1000, PartitionerType.MODULUS),
        new FileSetInfo.ServingInfo("/hdfs/path/2", "resource", 1000, PartitionerType.MODULUS),
        new FileSetInfo.ServingInfo("/hdfs/path/3", "resource", 1000, PartitionerType.MODULUS)
    );
    fileSetInfo.servingInfo =
        new FileSetInfo.ServingInfo("/hdfs/path/4", "resource", 1000, PartitionerType.MODULUS);
    String fileSet = "test_fileset";

    when(mockZKManager.getFileSetInfo(eq(fileSet))).thenReturn(fileSetInfo);
    doNothing().when(mockZKManager).setFileSetInfo(anyString(), any(FileSetInfo.class));

    TerrapinAdmin.rollbackFileSet(mockZKManager, fileSet, fileSetInfo, 1);

    ArgumentCaptor<String> fileSetCaptor = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<FileSetInfo> fileSetInfoCaptor = ArgumentCaptor.forClass(FileSetInfo.class);

    verify(mockZKManager).setFileSetInfo(fileSetCaptor.capture(), fileSetInfoCaptor.capture());

    assertEquals(fileSet, fileSetCaptor.getValue());
    assertEquals("/hdfs/path/2", fileSetInfoCaptor.getValue().servingInfo.hdfsPath);
    assertEquals(1, fileSetInfoCaptor.getValue().oldServingInfoList.size());
    assertEquals("/hdfs/path/3", fileSetInfoCaptor.getValue().oldServingInfoList.get(0).hdfsPath);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRollbackFileSetWithInvalidVersion() throws Exception {
    ZooKeeperManager mockZKManager = mock(ZooKeeperManager.class);
    FileSetInfo fileSetInfo = new FileSetInfo();
    fileSetInfo.numVersionsToKeep = 4;
    fileSetInfo.oldServingInfoList = ImmutableList.of(
        new FileSetInfo.ServingInfo("/hdfs/path/1", "resource", 1000, PartitionerType.MODULUS),
        new FileSetInfo.ServingInfo("/hdfs/path/2", "resource", 1000, PartitionerType.MODULUS),
        new FileSetInfo.ServingInfo("/hdfs/path/3", "resource", 1000, PartitionerType.MODULUS)
    );
    fileSetInfo.servingInfo =
        new FileSetInfo.ServingInfo("/hdfs/path/4", "resource", 1000, PartitionerType.MODULUS);
    String fileSet = "test_fileset";

    when(mockZKManager.getFileSetInfo(eq(fileSet))).thenReturn(fileSetInfo);
    doNothing().when(mockZKManager).setFileSetInfo(anyString(), any(FileSetInfo.class));

    TerrapinAdmin.rollbackFileSet(mockZKManager, fileSet, fileSetInfo, 3);
  }

  @Test
  public void testSelectFileSetRollbackVersion() {
    FileSetInfo fileSetInfo = new FileSetInfo();
    fileSetInfo.numVersionsToKeep = 4;
    fileSetInfo.oldServingInfoList = ImmutableList.of(
        new FileSetInfo.ServingInfo("/hdfs/path/1", "resource", 1000, PartitionerType.MODULUS),
        new FileSetInfo.ServingInfo("/hdfs/path/2", "resource", 1000, PartitionerType.MODULUS),
        new FileSetInfo.ServingInfo("/hdfs/path/3", "resource", 1000, PartitionerType.MODULUS)
    );
    int expectedVersionIndex = 1;
    ByteArrayInputStream inputStream =
        new ByteArrayInputStream((String.valueOf(expectedVersionIndex) + "\n").getBytes());
    int versionIndex = TerrapinAdmin.selectFileSetRollbackVersion(fileSetInfo, inputStream);
    assertEquals(expectedVersionIndex, versionIndex);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSelectFileSetRollbackVersionWithEmptyInput() {
    FileSetInfo fileSetInfo = new FileSetInfo();
    fileSetInfo.numVersionsToKeep = 4;
    fileSetInfo.oldServingInfoList = ImmutableList.of(
        new FileSetInfo.ServingInfo("/hdfs/path/1", "resource", 1000, PartitionerType.MODULUS),
        new FileSetInfo.ServingInfo("/hdfs/path/2", "resource", 1000, PartitionerType.MODULUS),
        new FileSetInfo.ServingInfo("/hdfs/path/3", "resource", 1000, PartitionerType.MODULUS)
    );
    ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0]);
    TerrapinAdmin.selectFileSetRollbackVersion(fileSetInfo, inputStream);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSelectFileSetRollbackVersionWhenNoAvailableVersions() {
    FileSetInfo fileSetInfo = new FileSetInfo();
    fileSetInfo.numVersionsToKeep = 2;
    fileSetInfo.oldServingInfoList = ImmutableList.of();
    ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0]);
    TerrapinAdmin.selectFileSetRollbackVersion(fileSetInfo, inputStream);
  }

  @Test
  public void testConfirmFileSetRollbackVersion() {
    FileSetInfo fileSetInfo = new FileSetInfo();
    fileSetInfo.fileSetName = "test_fileset";
    fileSetInfo.numVersionsToKeep = 4;
    fileSetInfo.oldServingInfoList = ImmutableList.of(
        new FileSetInfo.ServingInfo("/hdfs/path/1", "resource", 1000, PartitionerType.MODULUS),
        new FileSetInfo.ServingInfo("/hdfs/path/2", "resource", 1000, PartitionerType.MODULUS),
        new FileSetInfo.ServingInfo("/hdfs/path/3", "resource", 1000, PartitionerType.MODULUS)
    );
    fileSetInfo.servingInfo =
        new FileSetInfo.ServingInfo("/hdfs/path/4", "resource", 1000, PartitionerType.MODULUS);
    ByteArrayInputStream inputStream = new ByteArrayInputStream("y\n".getBytes());
    assertTrue(TerrapinAdmin.confirmFileSetRollbackVersion(fileSetInfo.fileSetName,
        fileSetInfo, 1, inputStream));
  }

  @Test
  public void testConfirmFileSetRollbackVersionWithEmptyInput() {
    FileSetInfo fileSetInfo = new FileSetInfo();
    fileSetInfo.fileSetName = "test_fileset";
    fileSetInfo.numVersionsToKeep = 4;
    fileSetInfo.oldServingInfoList = ImmutableList.of(
        new FileSetInfo.ServingInfo("/hdfs/path/1", "resource", 1000, PartitionerType.MODULUS),
        new FileSetInfo.ServingInfo("/hdfs/path/2", "resource", 1000, PartitionerType.MODULUS),
        new FileSetInfo.ServingInfo("/hdfs/path/3", "resource", 1000, PartitionerType.MODULUS)
    );
    fileSetInfo.servingInfo =
        new FileSetInfo.ServingInfo("/hdfs/path/4", "resource", 1000, PartitionerType.MODULUS);
    ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0]);
    assertFalse(TerrapinAdmin.confirmFileSetRollbackVersion(fileSetInfo.fileSetName,
        fileSetInfo, 1, inputStream));
  }

  @Test
  public void testConfirmFileSetRollbackVersionWithOtherInput() {
    FileSetInfo fileSetInfo = new FileSetInfo();
    fileSetInfo.fileSetName = "test_fileset";
    fileSetInfo.numVersionsToKeep = 4;
    fileSetInfo.oldServingInfoList = ImmutableList.of(
        new FileSetInfo.ServingInfo("/hdfs/path/1", "resource", 1000, PartitionerType.MODULUS),
        new FileSetInfo.ServingInfo("/hdfs/path/2", "resource", 1000, PartitionerType.MODULUS),
        new FileSetInfo.ServingInfo("/hdfs/path/3", "resource", 1000, PartitionerType.MODULUS)
    );
    fileSetInfo.servingInfo =
        new FileSetInfo.ServingInfo("/hdfs/path/4", "resource", 1000, PartitionerType.MODULUS);
    ByteArrayInputStream inputStream = new ByteArrayInputStream("other input".getBytes());
    assertFalse(TerrapinAdmin.confirmFileSetRollbackVersion(fileSetInfo.fileSetName,
        fileSetInfo, 1, inputStream));
  }

  @Test
  public void testConfirmFileSetDeletion() {
    ByteArrayInputStream inputStream = new ByteArrayInputStream("y\n".getBytes());
    assertTrue(TerrapinAdmin.confirmFileSetDeletion("test_fileset", inputStream));

    inputStream = new ByteArrayInputStream(new byte[0]);
    assertFalse(TerrapinAdmin.confirmFileSetDeletion("test_fileset", inputStream));

    inputStream = new ByteArrayInputStream("other input".getBytes());
    assertFalse(TerrapinAdmin.confirmFileSetDeletion("test_fileset", inputStream));
  }

  @Test
  public void testDeleteFileSet() throws Exception {
    ZooKeeperManager mockZKManager = mock(ZooKeeperManager.class);
    FileSetInfo fileSetInfo = new FileSetInfo();
    fileSetInfo.fileSetName = "test_fileset";
    when(mockZKManager.getFileSetInfo(eq(fileSetInfo.fileSetName))).thenReturn(fileSetInfo);
    doNothing().when(mockZKManager).lockFileSet(anyString(), any(FileSetInfo.class));
    doNothing().when(mockZKManager).setFileSetInfo(anyString(), any(FileSetInfo.class));

    TerrapinAdmin.deleteFileSet(mockZKManager, fileSetInfo.fileSetName);
    verify(mockZKManager, times(1))
        .setFileSetInfo(eq(fileSetInfo.fileSetName), any(FileSetInfo.class));

    assertTrue(fileSetInfo.deleted);
  }
}
