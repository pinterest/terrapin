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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.pinterest.terrapin.zookeeper.FileSetInfo;
import com.pinterest.terrapin.zookeeper.ViewInfo;
import com.pinterest.terrapin.zookeeper.ZooKeeperManager;
import org.apache.helix.model.ExternalView;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.eq;

public class FileStatusServletTest {
  private ExternalView externalView;
  private ViewInfo viewInfo;

  @Before
  public void setUp() {
    externalView = new ExternalView("resource");
    externalView.setStateMap("resource_1", new ImmutableMap.Builder()
        .put("host1", "OFFLINE")
        .put("host2", "ONLINE")
        .put("host3", "ONLINE").build());
    externalView.setStateMap("resource$2", new ImmutableMap.Builder()
        .put("host1", "OFFLINE")
        .put("host2", "OFFLINE").build());
    externalView.setStateMap("resource$3", new ImmutableMap.Builder()
        .put("host1", "ONLINE")
        .put("host2", "OFFLINE").build());
    viewInfo = new ViewInfo(externalView);
  }

  @Test
  public void testParseFileSetFromURI() {
    assertEquals("",
        FileSetStatusServlet.parseFileSetFromURI(FileSetStatusServlet.BASE_URI + "/"));
    assertEquals("file_set",
        FileSetStatusServlet.parseFileSetFromURI(FileSetStatusServlet.BASE_URI + "/file_set"));
  }

  @Test
  public void testGetMissingPartitions() {
    ZooKeeperManager zkManager = mock(ZooKeeperManager.class);
    FileSetInfo mockFileSetInfo = mock(FileSetInfo.class);
    FileSetInfo.ServingInfo mockServingInfo = mock(FileSetInfo.ServingInfo.class);
    mockFileSetInfo.servingInfo = mockServingInfo;
    mockFileSetInfo.servingInfo.numPartitions = 3;
    mockFileSetInfo.servingInfo.helixResource = "resource";

    when(zkManager.getViewInfo(eq("resource"))).thenReturn(viewInfo);

    assertEquals(ImmutableList.of(0, 2),
        FileSetStatusServlet.getMissingPartitions(zkManager, mockFileSetInfo));

  }
}
