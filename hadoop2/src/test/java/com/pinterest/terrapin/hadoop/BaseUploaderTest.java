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
package com.pinterest.terrapin.hadoop;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.pinterest.terrapin.Constants;
import com.pinterest.terrapin.thrift.generated.Options;
import com.pinterest.terrapin.thrift.generated.PartitionerType;
import com.pinterest.terrapin.tools.HFileGenerator;
import com.pinterest.terrapin.zookeeper.ClusterInfo;
import com.pinterest.terrapin.zookeeper.FileSetInfo;
import com.pinterest.terrapin.zookeeper.ZooKeeperManager;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.tools.DistCpOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.List;

public class BaseUploaderTest {

  private final static int NUM_PART = 100;
  private final static String CLUSTER = "test_cluster";
  private final static String NAME_NODE = "test_node";
  private final static int REPLICA_FACTOR = 2;
  private final static String FILE_SET = "test_fileset";
  private final static String HDFS_DIR = Constants.HDFS_DATA_DIR + "/" + FILE_SET;

  public class TestUploader extends BaseUploader {
    private List<Path> sourceFiles;
    private long blockSize;
    private ZooKeeperManager zkManager;
    private DistCp distCp;
    private Job job;

    public TestUploader(TerrapinUploaderOptions options) {
      super(options);
    }

    public void init() throws Exception {
      sourceFiles = HFileGenerator.generateHFiles(fs, conf, tempFolder,
          options.loadOptions.getPartitioner(), NUM_PART, NUM_PART * 1000);
      blockSize = 0;
      for (Path path : sourceFiles) {
        long fileSize = new File(path.toString()).length();
        if (fileSize > blockSize) {
          blockSize = fileSize;
        }
      }
      zkManager = mock(ZooKeeperManager.class);
      distCp = mock(DistCp.class);
      job = mock(Job.class);
      when(zkManager.getClusterInfo()).thenReturn(new ClusterInfo(NAME_NODE, REPLICA_FACTOR));
      when(distCp.execute()).thenReturn(job);
      when(job.waitForCompletion(anyBoolean())).then(new Answer<Object>() {
        @Override
        public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
          Thread.sleep(1000);
          return true;
        }
      });
      doNothing().when(zkManager).lockFileSet(anyString(), any(FileSetInfo.class));
      doNothing().when(zkManager).unlockFileSet(anyString());
    }

    public void verifyTest() throws Exception {
      verify(zkManager).getClusterInfo();
      verify(zkManager).lockFileSet(eq(FILE_SET), any(FileSetInfo.class));
      verify(zkManager).unlockFileSet(eq(FILE_SET));
      verify(distCp).execute();
      verify(job).waitForCompletion(eq(true));
    }

    @Override
    List<Pair<Path, Long>> getFileList() {
      return Lists.transform(sourceFiles, new Function<Path, Pair<Path, Long>>() {
        @Override
        public Pair<Path, Long> apply(Path path) {
          return ImmutablePair.of(path, new File(path.toString()).length());
        }
      });
    }

    @Override
    protected ZooKeeperManager getZKManager(String clusterName)
        throws UnknownHostException {
      assertEquals(clusterName, CLUSTER);
      return zkManager;
    }

    @Override
    protected DistCp getDistCp(Configuration conf, DistCpOptions options) {
      assertEquals(Constants.MAPRED_MAP_MAX_ATTEMPTS,
          Integer.parseInt(conf.get("mapred.map.max.attempts")));
      assertEquals(Constants.CHECKSUM_BYTES,
          Integer.parseInt(conf.get("io.bytes.per.checksum")));
      long blockSizeExpected = blockSize;
      if (blockSizeExpected % Constants.CHECKSUM_BYTES != 0) {
        blockSizeExpected = (blockSize / Constants.CHECKSUM_BYTES + 1) * Constants.CHECKSUM_BYTES;
      }
      assertEquals(blockSizeExpected, Long.parseLong(conf.get("dfs.block.size")));
      assertEquals(REPLICA_FACTOR, Integer.parseInt(conf.get("dfs.replication")));
      assertEquals(sourceFiles, options.getSourcePaths());
      assertTrue(options.shouldSkipCRC());
      assertTrue(options.shouldSyncFolder());
      assertTrue(options.getTargetPath().toString()
          .startsWith("hdfs://" + NAME_NODE + HDFS_DIR));
      return distCp;
    }

    @Override
    protected void loadFileSetData(ZooKeeperManager zkManager, FileSetInfo fileSetInfo,
                                   Options options) {
      assertEquals(FILE_SET, fileSetInfo.fileSetName);
      assertEquals(NUM_PART, fileSetInfo.servingInfo.numPartitions);
    }
  }

  private Configuration conf;
  private FileSystem fs;
  private TestUploader uploader;
  private TerrapinUploaderOptions options;
  private File tempFolder;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration();
    fs = FileSystem.get(conf);
    tempFolder = Files.createTempDir();
    options = new TerrapinUploaderOptions();
    options.terrapinZkQuorum = "terrapinzk";
    uploader = new TestUploader(options);
  }

  @After
  public void cleanUp() throws IOException {
    FileUtils.deleteDirectory(tempFolder);
  }

  @Test
  public void testValidateCascadingPartition() throws IOException {
    List<Path> parts = HFileGenerator.generateHFiles(fs, conf, tempFolder,
        PartitionerType.CASCADING, 10, 1000);
    uploader.validate(parts, PartitionerType.CASCADING, 10);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateCascadingPartitionWithException() throws IOException {
    List<Path> parts = HFileGenerator.generateHFiles(fs, conf, tempFolder,
        PartitionerType.MODULUS, 10, 1000);
    uploader.validate(parts, PartitionerType.CASCADING, 10);
  }

  @Test
  public void testValidateModulusPartition() throws IOException {
    List<Path> parts = HFileGenerator.generateHFiles(fs, conf, tempFolder,
        PartitionerType.MODULUS, 10, 1000);
    uploader.validate(parts, PartitionerType.MODULUS, 10);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testValidateModulusPartitionWithException() throws IOException {
    List<Path> parts = HFileGenerator.generateHFiles(fs, conf, tempFolder,
        PartitionerType.CASCADING, 10, 1000);
    uploader.validate(parts, PartitionerType.MODULUS, 10);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAllEmptyPartitions() throws IOException {
    List<Path> parts = HFileGenerator.generateHFiles(fs, conf, tempFolder,
        PartitionerType.CASCADING, 10, 0);
    uploader.validate(parts, PartitionerType.MODULUS, 10);
  }

  @Test
  public void testUpload() throws Exception {
    uploader.init();
    uploader.upload(CLUSTER, FILE_SET, new Options());
    uploader.verifyTest();
  }
}
