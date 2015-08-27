package com.pinterest.terrapin.controller;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.pinterest.terrapin.zookeeper.FileSetInfo;
import com.pinterest.terrapin.zookeeper.ViewInfo;
import com.pinterest.terrapin.zookeeper.ZooKeeperManager;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.eq;

public class ClusterStatusServletTest {
  @MockitoAnnotations.Mock
  private ZooKeeperManager mockZKManager;

  @MockitoAnnotations.Mock
  private FileSetInfo mockFileSetInfo1;

  @MockitoAnnotations.Mock
  private FileSetInfo mockFileSetInfo2;

  @MockitoAnnotations.Mock
  private FileSetInfo mockFileSetInfo3;


  @MockitoAnnotations.Mock
  private FileSetInfo mockFileSetInfo4;

  @MockitoAnnotations.Mock
  private FileSetInfo.ServingInfo mockServingInfo1;

  @MockitoAnnotations.Mock
  private FileSetInfo.ServingInfo mockServingInfo2;

  @MockitoAnnotations.Mock
  private FileSetInfo.ServingInfo mockServingInfo4;

  @MockitoAnnotations.Mock
  private ViewInfo mockViewInfo1;

  @MockitoAnnotations.Mock
  private ViewInfo mockViewInfo2;


  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    this.mockFileSetInfo1.servingInfo = this.mockServingInfo1;
    this.mockFileSetInfo2.servingInfo = this.mockServingInfo2;
    this.mockFileSetInfo4.servingInfo = this.mockServingInfo4;

    this.mockServingInfo1.helixResource = "$fileset1$1234423233333";
    this.mockServingInfo1.numPartitions = 32;
    this.mockServingInfo1.hdfsPath = "/fileset1/1234423233333";

    this.mockServingInfo2.helixResource = "$fileset2$1344423233333";
    this.mockServingInfo2.numPartitions = 16;
    this.mockServingInfo2.hdfsPath = "/fileset2/1344423233333";

    this.mockServingInfo4.helixResource = "$fileset4$1344423233333";
    this.mockServingInfo4.numPartitions = 16;
    this.mockServingInfo4.hdfsPath = "/fileset4/1344423233333";
  }

  @Test
  public void testGetFileSetStatusTable() {
    when(mockZKManager.getFileSetInfoMap()).thenReturn(
        ImmutableMap.of(
            "fileset1", mockFileSetInfo1,
            "fileset2", mockFileSetInfo2,
            "fileset3", mockFileSetInfo3,
            "fileset4", mockFileSetInfo4
        )
    );
    when(mockZKManager.getViewInfo(eq("$fileset1$1234423233333"))).thenReturn(mockViewInfo1);
    when(mockZKManager.getViewInfo(eq("$fileset2$1344423233333"))).thenReturn(mockViewInfo2);
    when(mockZKManager.getViewInfo(eq("$fileset4$1344423233333"))).thenReturn(null);

    when(mockViewInfo1.getNumOnlinePartitions()).thenReturn(23);
    when(mockViewInfo2.getNumOnlinePartitions()).thenReturn(16);

    List<ClusterStatusServlet.FileSetWithErrorRow> fileSetsWithError =
        new ArrayList<ClusterStatusServlet.FileSetWithErrorRow>();
    List<ClusterStatusServlet.FileSetStatusRow> fileSets =
        ClusterStatusServlet.getFileSetStatusTable(mockZKManager, fileSetsWithError);
    assertEquals(2, fileSets.size());
    assertEquals(
        new ClusterStatusServlet.FileSetStatusRow("fileset1", 32, 23, "/fileset1/1234423233333"),
        fileSets.get(0)
    );
    assertEquals(
        new ClusterStatusServlet.FileSetStatusRow("fileset2", 16, 16, "/fileset2/1344423233333"),
        fileSets.get(1)
    );

    assertEquals(2, fileSetsWithError.size());
    assertEquals(
        new ClusterStatusServlet.FileSetWithErrorRow("fileset3",
            ClusterStatusServlet.FileSetError.NO_SERVING_INFO),
        fileSetsWithError.get(0)
    );
    assertEquals(
        new ClusterStatusServlet.FileSetWithErrorRow("fileset4",
            ClusterStatusServlet.FileSetError.NO_VIEW_INFO),
        fileSetsWithError.get(1)
    );
  }

  @Test
  public void testGetLoadingFileSetTable() {
    try{
      when(mockZKManager.getLockedFileSets()).thenReturn(ImmutableList.of(
          "fileset1", "fileset2", "fileset3"
      ));
      when(mockZKManager.getLockedFileSetInfo(eq("fileset1"))).thenReturn(mockFileSetInfo1);
      when(mockZKManager.getLockedFileSetInfo(eq("fileset2"))).thenReturn(mockFileSetInfo2);
      when(mockZKManager.getLockedFileSetInfo(eq("fileset3"))).thenReturn(mockFileSetInfo3);
      List<ClusterStatusServlet.FileSetWithErrorRow> fileSetsWithError =
          new ArrayList<ClusterStatusServlet.FileSetWithErrorRow>();
      List<ClusterStatusServlet.LoadingFileSetRow> loadingFileSets =
          ClusterStatusServlet.getLoadingFileSetTable(mockZKManager, fileSetsWithError);

      assertEquals(2, loadingFileSets.size());
      assertEquals(
          new ClusterStatusServlet.LoadingFileSetRow("fileset1", "/fileset1/1234423233333"),
          loadingFileSets.get(0)
      );
      assertEquals(
          new ClusterStatusServlet.LoadingFileSetRow("fileset2", "/fileset2/1344423233333"),
          loadingFileSets.get(1)
      );

      assertEquals(1, fileSetsWithError.size());
      assertEquals(
          new ClusterStatusServlet.FileSetWithErrorRow("fileset3",
              ClusterStatusServlet.FileSetError.NO_SERVING_INFO),
          fileSetsWithError.get(0)
      );
    } catch (Exception e) {
      fail(e.getMessage());
    }

  }

  @Test
  public void testParseTimestampFromHdfsPath() {
    assertEquals(1234567890123l,
        ClusterStatusServlet.parseTimestampFromHdfsPath("/1234567890123"));
    assertEquals(1234567890123l,
        ClusterStatusServlet.parseTimestampFromHdfsPath("1234567890123"));
    assertEquals(1234567890123l,
        ClusterStatusServlet.parseTimestampFromHdfsPath("/a/b/1234567890123"));
  }
}
