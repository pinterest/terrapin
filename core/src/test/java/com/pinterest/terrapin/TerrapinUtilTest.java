package com.pinterest.terrapin;

import com.pinterest.terrapin.thrift.generated.Options;
import com.pinterest.terrapin.thrift.generated.PartitionerType;
import com.pinterest.terrapin.thrift.generated.TerrapinController;
import com.pinterest.terrapin.thrift.generated.TerrapinLoadRequest;
import com.pinterest.terrapin.zookeeper.FileSetInfo;
import com.pinterest.terrapin.zookeeper.ZooKeeperManager;

import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableList;
import com.twitter.util.Future;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import org.junit.Assert;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.whenNew;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for TerrapinUtil.
 */
@RunWith(PowerMockRunner.class)
public class TerrapinUtilTest {
  @Test
  public void testgetHelixInstanceFromHDFSHost() {
    assertEquals("hdfs-instance",
        TerrapinUtil.getHelixInstanceFromHDFSHost("hdfs-instance.my-domain.abc.com"));
    assertEquals("hdfs-instance",
        TerrapinUtil.getHelixInstanceFromHDFSHost("hdfs-instance"));
  }

  @Test
  public void testExtractPartitionName() {
    assertEquals(new Integer(10), TerrapinUtil.extractPartitionName(
        "part-00010-ad2", PartitionerType.MODULUS));
    assertEquals(new Integer(10), TerrapinUtil.extractPartitionName(
        "part-00010", PartitionerType.MODULUS));
    Assert.assertNull(TerrapinUtil.extractPartitionName("part-929-ad2", PartitionerType.MODULUS));

    assertEquals(new Integer(10), TerrapinUtil.extractPartitionName(
        "part-00010-ad2", PartitionerType.CASCADING));
    assertEquals(new Integer(10), TerrapinUtil.extractPartitionName(
        "part-00010", PartitionerType.CASCADING));
    Assert.assertNull(TerrapinUtil.extractPartitionName("part-929-ad2", PartitionerType.CASCADING));
  }

  @Test
  public void testHelixResourceToHdfsDir() {
    assertEquals("/terrapin/data/123",
        TerrapinUtil.helixResourceToHdfsDir("$terrapin$data$123"));
  }

  @Test
  public void testHdfsDirToHelixResource() {
    assertEquals("$terrapin$data$123",
        TerrapinUtil.hdfsDirToHelixResource("/terrapin/data/123"));
  }

  @Test
  public void testGetBucketSize() {
    assertEquals(0, TerrapinUtil.getBucketSize(600));
    assertEquals(701, TerrapinUtil.getBucketSize(1401));
    assertEquals(1000, TerrapinUtil.getBucketSize(3000));
    assertEquals(751, TerrapinUtil.getBucketSize(3001));
  }

  @Test
  public void testExtractFileSetFromResource() {
    assertEquals("file_set", TerrapinUtil.extractFileSetFromPath(
        "/terrapin/data/file_set/1343443323/part-00000"));
    assertEquals("file_set", TerrapinUtil.extractFileSetFromPath(
        "/file_set/1343443323/part-00000"));
    Assert.assertNull(TerrapinUtil.extractFileSetFromPath("/file_set/1343443323"));
    Assert.assertNull(TerrapinUtil.extractFileSetFromPath("/file_set"));
    Assert.assertNull(TerrapinUtil.extractFileSetFromPath("/file_set"));
  }

  @Test
  public void testGetResourceAndPartitionNum() {
    assertEquals(new ImmutablePair("$terrapin$data$file_set$1343443323", 100),
        TerrapinUtil.getResourceAndPartitionNum("$terrapin$data$file_set$1343443323$100"));
    assertEquals(new ImmutablePair("$terrapin$data$file_set$1343443323", 100),
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
    assertEquals("resource$1", TerrapinUtil.getViewPartitionName("resource", 1));
    assertEquals("$1", TerrapinUtil.getViewPartitionName("", 1));
    assertEquals("null$1", TerrapinUtil.getViewPartitionName(null, 1));
  }

  @Test
  public void testGetViewPartitionNumber() {
    assertEquals(0, TerrapinUtil.getViewPartitionNumber("resource"));
    assertEquals(1, TerrapinUtil.getViewPartitionNumber("resource$1"));
  }

  @Test
  public void testGetZKQuorumFromConf() {
    PropertiesConfiguration conf = mock(PropertiesConfiguration.class);
    when(conf.getStringArray(eq(Constants.ZOOKEEPER_QUORUM))).thenReturn(
        new String[]{"a", "b", "c"}
    );
    String expected = String.format("a%sb%sc", Constants.ZOOKEEPER_QUORUM_DELIMITER,
        Constants.ZOOKEEPER_QUORUM_DELIMITER);
    assertEquals(expected, TerrapinUtil.getZKQuorumFromConf(conf));
  }

  @Test
  @PrepareForTest(TerrapinUtil.class)
  public void testLoadFileSetData() throws Exception {
    TerrapinController.ServiceToClient serviceImpl = mock(TerrapinController.ServiceToClient.class);
    ZooKeeperManager zkManager = mock(ZooKeeperManager.class);

    FileSetInfo fileSetInfo = new FileSetInfo("fileset", "dir", 100,
        new ArrayList<FileSetInfo.ServingInfo>(), new Options());
    InetSocketAddress address = new InetSocketAddress("namenode001", 9090);
    Options requestOptions = new Options();

    when(zkManager.getControllerLeader()).thenReturn(address);
    whenNew(TerrapinController.ServiceToClient.class).withAnyArguments().thenReturn(serviceImpl);
    when(serviceImpl.loadFileSet(any(TerrapinLoadRequest.class))).thenReturn(Future.Void());

    ArgumentCaptor<TerrapinLoadRequest> requestCaptor =
        ArgumentCaptor.forClass(TerrapinLoadRequest.class);
    TerrapinUtil.loadFileSetData(zkManager, fileSetInfo, requestOptions);

    verify(serviceImpl).loadFileSet(requestCaptor.capture());
    TerrapinLoadRequest request = requestCaptor.getValue();

    assertEquals(fileSetInfo.servingInfo.hdfsPath, request.getHdfsDirectory());
    assertEquals(requestOptions, request.getOptions());
    assertEquals(fileSetInfo.fileSetName, request.getFileSet());
    assertEquals(fileSetInfo.servingInfo.numPartitions, request.getExpectedNumPartitions());
  }

  @Test
  @PrepareForTest(TerrapinUtil.class)
  public void testGetS3FileList() throws Exception {
    AmazonS3Client s3Client = mock(AmazonS3Client.class);
    ObjectListing objectListing = mock(ObjectListing.class);
    S3ObjectSummary summary1 = new S3ObjectSummary();
    S3ObjectSummary summary2 = new S3ObjectSummary();
    S3ObjectSummary summary3 = new S3ObjectSummary();
    summary1.setKey("/abc/123");
    summary2.setKey("/abc/456");
    summary3.setKey("/def/123");
    summary1.setSize(32432);
    summary2.setSize(213423);
    summary3.setSize(2334);
    List<S3ObjectSummary> summaries = ImmutableList.of(summary1, summary2, summary3);
    whenNew(AmazonS3Client.class).withAnyArguments().thenReturn(s3Client);
    when(s3Client.listObjects(any(ListObjectsRequest.class))).thenReturn(objectListing);
    when(objectListing.getObjectSummaries()).thenReturn(summaries);

    List<Pair<Path, Long>> results = TerrapinUtil.getS3FileList(new AnonymousAWSCredentials(),
        "bucket", "/abc");

    assertEquals(2, results.size());
    assertTrue(results.get(0).getLeft().toString().endsWith(summary1.getKey()));
    assertEquals(new Long(summary1.getSize()), results.get(0).getRight());
    assertTrue(results.get(1).getLeft().toString().endsWith(summary2.getKey()));
    assertEquals(new Long(summary2.getSize()), results.get(1).getRight());
  }
}