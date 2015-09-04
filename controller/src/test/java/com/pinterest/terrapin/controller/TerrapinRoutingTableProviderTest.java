package com.pinterest.terrapin.controller;

import static org.junit.Assert.assertEquals;
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

    when(ev1.getPartitionSet()).thenReturn(Sets.newHashSet("p1", "p2"));
    when(ev2.getPartitionSet()).thenReturn(Sets.newHashSet("p2", "p3"));
    when(ev3.getPartitionSet()).thenReturn(Sets.newHashSet("p1", "p3"));
    when(ev1.getStateMap(eq("p1"))).thenReturn(ImmutableMap.of("p1", "ONLINE"));
    when(ev1.getStateMap(eq("p2"))).thenReturn(ImmutableMap.of("p2", "ONLINE"));
    when(ev2.getStateMap(eq("p2"))).thenReturn(ImmutableMap.of("p2", "ONLINE"));
    when(ev2.getStateMap(eq("p3"))).thenReturn(ImmutableMap.of("p3", "ONLINE"));
    when(ev3.getStateMap(eq("p1"))).thenReturn(ImmutableMap.of("p1", "ONLINE"));
    when(ev3.getStateMap(eq("p3"))).thenReturn(ImmutableMap.of("p3", "ONLINE"));
    when(ev1.getResourceName()).thenReturn("r1");
    when(ev2.getResourceName()).thenReturn("r2");
    when(ev3.getResourceName()).thenReturn("r3");

    ViewInfo v1 = new ViewInfo(ev1);
    ViewInfo v2 = new ViewInfo(ev2);

    when(zkManager.getViewInfo(eq("r1"))).thenReturn(v1);
    when(zkManager.getViewInfo(eq("r2"))).thenReturn(v2);

    TerrapinRoutingTableProvider provider =
        new TerrapinRoutingTableProvider(zkManager, ImmutableList.of("r1", "r2"));
    provider.updateExternalView(ImmutableList.of(ev2, ev3));

    Map<String, TerrapinRoutingTableProvider.ViewInfoRecord> viewInfoRecordMap =
        Whitebox.getInternalState(provider, "viewInfoRecordMap");

    assertEquals(2, viewInfoRecordMap.size());
    assertTrue(viewInfoRecordMap.containsKey("r2"));
    assertTrue(viewInfoRecordMap.containsKey("r3"));

    provider.shutdown();
  }
}
