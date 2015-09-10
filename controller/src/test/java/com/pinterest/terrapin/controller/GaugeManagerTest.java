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


import com.google.common.collect.ImmutableMap;
import com.pinterest.terrapin.zookeeper.FileSetInfo;
import com.pinterest.terrapin.zookeeper.ViewInfo;
import com.pinterest.terrapin.zookeeper.ZooKeeperManager;
import com.twitter.ostrich.stats.Stats;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import scala.Option;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.eq;

public class GaugeManagerTest {
  @MockitoAnnotations.Mock
  private ZooKeeperManager mockZKManager;
  
  @MockitoAnnotations.Mock
  private FileSetInfo mockFileSetInfo;
  
  @MockitoAnnotations.Mock
  private FileSetInfo.ServingInfo mockServingInfo;
  
  @MockitoAnnotations.Mock
  private ViewInfo mockViewInfo;

  private GaugeManager.FileSetGaugeCalculator fileSetGaugeCalculator;
  private GaugeManager.OnlinePercentageGaugeCalculator onlinePercentageGaugeCalculator;
  
  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    this.mockFileSetInfo.servingInfo = mockServingInfo;
    this.fileSetGaugeCalculator = new GaugeManager.FileSetGaugeCalculator("test-gauge") {
      @Override
      public double calcValue(String fileSet) {
        return 0;
      }
    };
    this.onlinePercentageGaugeCalculator = 
        new GaugeManager.OnlinePercentageGaugeCalculator(this.mockZKManager, "online-pct");
  }
  
  @Test
  public void testFormatGaugeName() {
    String fileset = "file_set";
    assertEquals("terrapin-controller-fileset-file_set-test-gauge", 
        fileSetGaugeCalculator.formatGaugeName(fileset));
  }
  
  @Test
  public void testParseFileSet(){
    assertEquals("file-set", 
        fileSetGaugeCalculator.parseFileSet("terrapin-controller-fileset-file-set-test-gauge"));
    assertEquals("file#$%&*set",
        fileSetGaugeCalculator.parseFileSet("terrapin-controller-fileset-file#$%&*set-test-gauge"));
    assertEquals(null, 
        fileSetGaugeCalculator.parseFileSet("terrapin-controller-file-set-test-gauge"));
  }
  
  @Test
  public void testPercentageCalculation() {
    String fileSet = "file_set";
    when(mockZKManager.getFileSetInfo(eq(fileSet))).thenReturn(mockFileSetInfo);
    when(mockZKManager.getViewInfo(eq(fileSet))).thenReturn(mockViewInfo);
    when(mockViewInfo.getNumOnlinePartitions()).thenReturn(3);
    mockServingInfo.helixResource = fileSet;
    mockServingInfo.numPartitions = 12;
    assertEquals(0.25, onlinePercentageGaugeCalculator.calcValue(fileSet), 0);
  }
  
  @Test
  public void testPercentageCalculationWithZero() {
    String fileSet = "file_set";
    when(mockZKManager.getFileSetInfo(eq(fileSet))).thenReturn(mockFileSetInfo);
    when(mockZKManager.getViewInfo(eq(fileSet))).thenReturn(mockViewInfo);
    when(mockViewInfo.getNumOnlinePartitions()).thenReturn(0);
    mockServingInfo.helixResource = fileSet;
    mockServingInfo.numPartitions = 0;
    assertEquals(0, onlinePercentageGaugeCalculator.calcValue(fileSet), 0);
  }

  @Test
  public void testGaugeManagerRunnable() {
    String fileSet = "file-set";
    when(mockZKManager.getFileSetInfoMap()).thenReturn(ImmutableMap.of(fileSet, mockFileSetInfo));
    when(mockZKManager.getFileSetInfo(eq(fileSet))).thenReturn(mockFileSetInfo);
    when(mockZKManager.getViewInfo(eq(fileSet))).thenReturn(mockViewInfo);
    when(mockViewInfo.getNumOnlinePartitions()).thenReturn(3);
    mockServingInfo.helixResource = fileSet;
    mockServingInfo.numPartitions = 12;
    GaugeManager gaugeManager = new GaugeManager(mockZKManager, 1);
    try {
      Thread.sleep(1500);
      Option<Object> value = Stats.getGauge("terrapin-controller-fileset-file-set-online-pct");
      assertEquals(0.25, value.get());
    } catch (InterruptedException e) {
      gaugeManager.shutdown();
      fail("test failed for interruption");
    }
    gaugeManager.shutdown();
  }
  
  @Test
  public void testCalcOverallOnlinePercentageGauge() {
    FileSetInfo mockFileSetInfo2 = mock(FileSetInfo.class);
    FileSetInfo.ServingInfo mockServingInfo2 = mock(FileSetInfo.ServingInfo.class);
    ViewInfo mockViewInfo2 = mock(ViewInfo.class);
    mockFileSetInfo2.servingInfo = mockServingInfo2;
    when(mockZKManager.getFileSetInfoMap())
        .thenReturn(ImmutableMap.of("file_set1", mockFileSetInfo, "file_set2", mockFileSetInfo2));
    when(mockZKManager.getFileSetInfo(eq("file_set1"))).thenReturn(mockFileSetInfo);
    when(mockZKManager.getFileSetInfo(eq("file_set2"))).thenReturn(mockFileSetInfo2);
    when(mockZKManager.getViewInfo("file_set1")).thenReturn(mockViewInfo);
    when(mockZKManager.getViewInfo("file_set2")).thenReturn(mockViewInfo2);
    when(mockViewInfo.getNumOnlinePartitions()).thenReturn(4);
    when(mockViewInfo2.getNumOnlinePartitions()).thenReturn(5);
    mockServingInfo.helixResource = "file_set1";
    mockServingInfo2.helixResource = "file_set2";
    mockServingInfo.numPartitions = 20;
    mockServingInfo2.numPartitions = 16;
    assertEquals(0.25, GaugeManager.calcOverallOnlinePercentageGauge(mockZKManager), 0);
  }
}
