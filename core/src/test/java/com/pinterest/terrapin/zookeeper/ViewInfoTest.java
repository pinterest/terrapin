package com.pinterest.terrapin.zookeeper;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.IOUtils;
import org.apache.helix.model.ExternalView;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Unit tests for ViewInfo.
 */
public class ViewInfoTest {
  private static final String JSON =
      "{\"resource\":\"resource\"," +
       "\"partitionMap\":{\"resource$1\":[\"host2\",\"host3\"]," +
                         "\"resource$3\":[\"host1\"]}}";

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
  public void testViewInfoMethods() {
    assertTrue(viewInfo.hasOnlinePartitions());
    assertEquals(2, viewInfo.getNumOnlinePartitions());

    assertEquals(ImmutableList.of("host2", "host3"), viewInfo.getInstancesForPartition("resource$1"));
    assertEquals(ImmutableList.of("host1"), viewInfo.getInstancesForPartition("resource$3"));
    assertTrue(viewInfo.getInstancesForPartition("resource$2").isEmpty());
  }

  @Test
  public void testSerialization() throws Exception {
    assertEquals(JSON, new String(viewInfo.toJson()));
    assertEquals(viewInfo, ViewInfo.fromJson(JSON.getBytes()));
  }

  @Test
  public void testCompressedSerialization() throws Exception {
    // Check that compressed json serialization works correctly.
    byte[] compressedJson = viewInfo.toCompressedJson();
    GZIPInputStream zipIn = new GZIPInputStream(new ByteArrayInputStream(compressedJson));
    assertEquals(JSON, new String(IOUtils.toByteArray(zipIn)));

    // Check that compressed json deserialization works correctly.
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GZIPOutputStream zipOut = new GZIPOutputStream(out);
    zipOut.write(JSON.getBytes());
    zipOut.close();
    assertEquals(viewInfo, ViewInfo.fromCompressedJson(out.toByteArray()));
  }

  @Test
  public void testToPrettyPrintingJson() {
    try {
      String expected = "{\n" +
          "  \"1\" : [ \"host2\", \"host3\" ],\n" +
          "  \"3\" : [ \"host1\" ]\n" +
          "}";
      assertEquals(expected, viewInfo.toPrettyPrintingJson());
    } catch (IOException e) {
      e.printStackTrace();
      fail("translating to pretty printing json raises exception: " + e.getMessage());
    }
  }
}
