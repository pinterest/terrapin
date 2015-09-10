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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.pinterest.terrapin.zookeeper.ViewInfo;
import com.pinterest.terrapin.zookeeper.ZooKeeperManager;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.helix.model.ExternalView;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.util.Map;

public class TerrapinRoutingTableProviderTest {

  @Test
  public void testUpdateExternalView() {
    ZooKeeperManager zkManager = mock(ZooKeeperManager.class);
    ExternalView ev1 = mock(ExternalView.class);
    ExternalView ev2 = mock(ExternalView.class);
    ExternalView ev3 = mock(ExternalView.class);

    when(ev1.getPartitionSet()).thenReturn(Sets.newHashSet("p$0", "p$1"));
    when(ev2.getPartitionSet()).thenReturn(Sets.newHashSet("p$1", "p$2"));
    when(ev3.getPartitionSet()).thenReturn(Sets.newHashSet("p$0", "p$2"));
    when(ev1.getStateMap(eq("p$0"))).thenReturn(ImmutableMap.of("p$0", "ONLINE"));
    when(ev1.getStateMap(eq("p$1"))).thenReturn(ImmutableMap.of("p$1", "ONLINE"));
    when(ev2.getStateMap(eq("p$1"))).thenReturn(ImmutableMap.of("p$1", "ONLINE"));
    when(ev2.getStateMap(eq("p$2"))).thenReturn(ImmutableMap.of("p$2", "ONLINE"));
    when(ev3.getStateMap(eq("p$0"))).thenReturn(ImmutableMap.of("p$0", "ONLINE"));
    when(ev3.getStateMap(eq("p$2"))).thenReturn(ImmutableMap.of("p$2", "ONLINE"));
    when(ev1.getResourceName()).thenReturn("r1");
    when(ev2.getResourceName()).thenReturn("r2");
    when(ev3.getResourceName()).thenReturn("r3");

    ViewInfo v1 = new ViewInfo(ev1);
    ViewInfo v2 = new ViewInfo(ev2);

    when(zkManager.getViewInfo(eq("r1"))).thenReturn(v1);
    when(zkManager.getViewInfo(eq("r2"))).thenReturn(v2);

    TerrapinRoutingTableProvider provider =
        new TerrapinRoutingTableProvider(zkManager, ImmutableList.of("r1", "r2"));

    Map<String, TerrapinRoutingTableProvider.ViewInfoRecord> viewInfoRecordMap =
        Whitebox.getInternalState(provider, "viewInfoRecordMap");
    viewInfoRecordMap.get("r1").drained = true;
    viewInfoRecordMap.get("r2").drained = true;

    provider.updateExternalView(ImmutableList.of(ev2, ev3));

    viewInfoRecordMap = Whitebox.getInternalState(provider, "viewInfoRecordMap");

    assertEquals(2, viewInfoRecordMap.size());
    assertTrue(viewInfoRecordMap.containsKey("r2"));
    assertTrue(viewInfoRecordMap.containsKey("r3"));
    assertTrue(viewInfoRecordMap.get("r2").drained);
    assertFalse(viewInfoRecordMap.get("r3").drained);
    assertEquals(v2, viewInfoRecordMap.get("r2").viewInfo);
    assertEquals(new ViewInfo(ev3), viewInfoRecordMap.get("r3").viewInfo);
  }
}