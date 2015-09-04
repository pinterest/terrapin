package com.pinterest.terrapin.controller;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.pinterest.terrapin.Constants;
import com.pinterest.terrapin.TerrapinUtil;
import com.pinterest.terrapin.thrift.generated.Options;
import com.pinterest.terrapin.thrift.generated.PartitionerType;
import com.pinterest.terrapin.thrift.generated.TerrapinLoadRequest;
import com.pinterest.terrapin.zookeeper.FileSetInfo;
import com.pinterest.terrapin.zookeeper.ViewInfo;
import com.pinterest.terrapin.zookeeper.ZooKeeperManager;

import com.google.common.collect.Lists;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.builder.CustomModeISBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;

@RunWith(PowerMockRunner.class)
public class TerrapinControllerServiceImplTest {
  private static final String CLUSTER = "cluster";

  @Mock
  private PropertiesConfiguration configuration;
  @Mock
  private ZooKeeperManager zkManager;
  @Mock
  private DFSClient hdfsClient;
  @Mock
  private HelixAdmin helixAdmin;
  private TerrapinControllerServiceImpl serviceImpl;

  @Before
  public void setUp() {
    serviceImpl = new TerrapinControllerServiceImpl(configuration, zkManager, hdfsClient,
        helixAdmin, CLUSTER);
  }

  @Test
  @PrepareForTest({ControllerUtil.class})
  public void testLoadFileSet() throws Exception {
    PowerMockito.mockStatic(ControllerUtil.class);
    TerrapinLoadRequest request =
        new TerrapinLoadRequest("fileset", "/terrapin/data/fileset/12345564534", 200);
    Options requestOptions = new Options();
    requestOptions.setNumVersionsToKeep(2);
    request.setOptions(requestOptions);
    FileSetInfo fileSetInfo = new FileSetInfo(
        request.getFileSet(), request.getHdfsDirectory(), request.getExpectedNumPartitions(),
        Lists.newArrayList(mock(FileSetInfo.ServingInfo.class)), new Options()
    );
    String resourceName = TerrapinUtil.hdfsDirToHelixResource(request.getHdfsDirectory());
    CustomModeISBuilder idealStateBuilder = new CustomModeISBuilder(resourceName);
    idealStateBuilder.assignInstanceAndState(resourceName + "$0", "host0", "ONLINE");
    idealStateBuilder.assignInstanceAndState(resourceName + "$1", "host1", "ONLINE");
    idealStateBuilder.setStateModel("OnlineOffline");
    idealStateBuilder.setNumReplica(2);
    idealStateBuilder.setNumPartitions(request.getExpectedNumPartitions());
    IdealState is = idealStateBuilder.build();
    is.setBucketSize(2);
    is.setRebalanceMode(IdealState.RebalanceMode.CUSTOMIZED);
    ViewInfo viewInfo1 = mock(ViewInfo.class);
    ViewInfo viewInfo2 = mock(ViewInfo.class);

    when(configuration.getInt(eq(Constants.NUM_SERVING_REPLICAS), eq(3))).thenReturn(3);
    when(zkManager.getFileSetInfo(eq(request.getFileSet()))).thenReturn(fileSetInfo);
    when(zkManager.getViewInfo(eq(resourceName))).thenReturn(viewInfo1).thenReturn(viewInfo2);
    doNothing().when(zkManager).setFileSetInfo(eq(request.getFileSet()), any(FileSetInfo.class));
    when(viewInfo1.getNumOnlinePartitions()).thenReturn(request.getExpectedNumPartitions() / 2);
    when(viewInfo2.getNumOnlinePartitions()).thenReturn(request.getExpectedNumPartitions());
    when(helixAdmin.getResourcesInCluster(CLUSTER)).thenReturn(new ArrayList<String>());
    when(ControllerUtil.buildIdealStateForHdfsDir(any(DFSClient.class),
        anyString(), anyString(), any(PartitionerType.class), anyInt())).thenReturn(is);
    doNothing().when(helixAdmin).addResource(eq(CLUSTER), eq(resourceName), eq(is));
    doNothing().when(helixAdmin).addResource(eq(CLUSTER), eq(resourceName),
        eq(is.getNumPartitions()), eq("OnlineOffline"), eq("CUSTOMIZED"), eq(is.getBucketSize()));
    doNothing().when(helixAdmin).setResourceIdealState(eq(CLUSTER), eq(resourceName), eq(is));

    serviceImpl.loadFileSet(request).apply();

    ArgumentCaptor<FileSetInfo> fileSetInfoCaptor = ArgumentCaptor.forClass(FileSetInfo.class);
    verify(zkManager).setFileSetInfo(eq(request.getFileSet()), fileSetInfoCaptor.capture());
    FileSetInfo capturedInfo = fileSetInfoCaptor.getValue();
    assertEquals(request.getFileSet(), capturedInfo.fileSetName);
    assertEquals(request.getExpectedNumPartitions(), capturedInfo.servingInfo.numPartitions);
    assertEquals(Lists.newArrayList(fileSetInfo.servingInfo),
        (ArrayList)capturedInfo.oldServingInfoList);
  }

}
