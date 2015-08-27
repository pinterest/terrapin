package com.pinterest.terrapin;

import com.pinterest.terrapin.thrift.generated.PartitionerType;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Test;

import org.junit.Assert;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for TerrapinUtil.
 */
public class TerrapinUtilTest {
  @Test
  public void testgetHelixInstanceFromHDFSHost() {
    Assert.assertEquals("hdfs-instance",
        TerrapinUtil.getHelixInstanceFromHDFSHost("hdfs-instance.my-domain.abc.com"));
    Assert.assertEquals("hdfs-instance",
        TerrapinUtil.getHelixInstanceFromHDFSHost("hdfs-instance"));
  }

  @Test
  public void testExtractPartitionName() {
    Assert.assertEquals(new Integer(10), TerrapinUtil.extractPartitionName(
        "part-00010-ad2", PartitionerType.MODULUS));
    Assert.assertEquals(new Integer(10), TerrapinUtil.extractPartitionName(
        "part-00010", PartitionerType.MODULUS));
    Assert.assertNull(TerrapinUtil.extractPartitionName("part-929-ad2", PartitionerType.MODULUS));

    Assert.assertEquals(new Integer(10), TerrapinUtil.extractPartitionName(
        "part-00010-ad2", PartitionerType.CASCADING));
    Assert.assertEquals(new Integer(10), TerrapinUtil.extractPartitionName(
        "part-00010", PartitionerType.CASCADING));
    Assert.assertNull(TerrapinUtil.extractPartitionName("part-929-ad2", PartitionerType.CASCADING));
  }

  @Test
  public void testHelixResourceToHdfsDir() {
    Assert.assertEquals("/terrapin/data/123",
        TerrapinUtil.helixResourceToHdfsDir("$terrapin$data$123"));
  }

  @Test
  public void testHdfsDirToHelixResource() {
    Assert.assertEquals("$terrapin$data$123",
        TerrapinUtil.hdfsDirToHelixResource("/terrapin/data/123"));
  }

  @Test
  public void testGetBucketSize() {
    Assert.assertEquals(0, TerrapinUtil.getBucketSize(600));
    Assert.assertEquals(701, TerrapinUtil.getBucketSize(1401));
    Assert.assertEquals(1000, TerrapinUtil.getBucketSize(3000));
    Assert.assertEquals(751, TerrapinUtil.getBucketSize(3001));
  }

  @Test
  public void testExtractFileSetFromResource() {
    Assert.assertEquals("file_set", TerrapinUtil.extractFileSetFromPath(
        "/terrapin/data/file_set/1343443323/part-00000"));
    Assert.assertEquals("file_set", TerrapinUtil.extractFileSetFromPath(
        "/file_set/1343443323/part-00000"));
    Assert.assertNull(TerrapinUtil.extractFileSetFromPath("/file_set/1343443323"));
    Assert.assertNull(TerrapinUtil.extractFileSetFromPath("/file_set"));
    Assert.assertNull(TerrapinUtil.extractFileSetFromPath("/file_set"));
  }

  @Test
  public void testGetResourceAndPartitionNum() {
    Assert.assertEquals(new ImmutablePair("$terrapin$data$file_set$1343443323", 100),
        TerrapinUtil.getResourceAndPartitionNum("$terrapin$data$file_set$1343443323$100"));
    Assert.assertEquals(new ImmutablePair("$terrapin$data$file_set$1343443323", 100),
        TerrapinUtil.getResourceAndPartitionNum("$terrapin$data$file_set$1343443323_100"));

    // Invalid partition number.
    Assert.assertNull(
            TerrapinUtil.getResourceAndPartitionNum("$terrapin$data$file_set$1343443323_1r0"));
    Assert.assertNull(
        TerrapinUtil.getResourceAndPartitionNum("$terrapin$data$file_set$1343443323$1r0"));

    // Invalid separator - neither $ nor _ are present.
    Assert.assertNull(TerrapinUtil.getResourceAndPartitionNum("WrongPartition"));
  }

  @Test
  public void testGetViewPartitionName() {
    Assert.assertEquals("resource$1", TerrapinUtil.getViewPartitionName("resource", 1));
    Assert.assertEquals("$1", TerrapinUtil.getViewPartitionName("", 1));
    Assert.assertEquals("null$1", TerrapinUtil.getViewPartitionName(null, 1));
  }

  @Test
  public void testGetViewPartitionNumber() {
    Assert.assertEquals(0, TerrapinUtil.getViewPartitionNumber("resource"));
    Assert.assertEquals(1, TerrapinUtil.getViewPartitionNumber("resource$1"));
  }

  @Test
  public void testGetZKQuorumFromConf() {
    PropertiesConfiguration conf = mock(PropertiesConfiguration.class);
    when(conf.getStringArray(eq(Constants.ZOOKEEPER_QUORUM))).thenReturn(
        new String[]{"a", "b", "c"}
    );
    String expected = String.format("a%sb%sc", Constants.ZOOKEEPER_QUORUM_DELIMITER,
        Constants.ZOOKEEPER_QUORUM_DELIMITER);
    Assert.assertEquals(expected, TerrapinUtil.getZKQuorumFromConf(conf));
  }
}
