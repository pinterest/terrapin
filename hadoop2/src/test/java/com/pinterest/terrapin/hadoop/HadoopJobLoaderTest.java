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
import com.pinterest.terrapin.zookeeper.ClusterInfo;
import com.pinterest.terrapin.zookeeper.FileSetInfo;
import com.pinterest.terrapin.zookeeper.ZooKeeperManager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.net.UnknownHostException;

public class HadoopJobLoaderTest {
  private final static int NUM_PART = 100;
  private final static String NAME_NODE = "test_node";
  private final static String FILE_SET = "test_fileset";
  private final static int REPLICA_FACTOR = 2;

  public class TestHadoopJobLoader extends HadoopJobLoader {

    private ZooKeeperManager zkManager;
    public TestHadoopJobLoader(ZooKeeperManager zkManager, TerrapinUploaderOptions options,
                               Job job) {
      super(options, job);
      this.zkManager = zkManager;
    }

    @Override
    protected ZooKeeperManager getZKManager() throws UnknownHostException {
      return zkManager;
    }

    @Override
    protected void setOutputPath(String terrapinNameNode, String hdfsDir) {
      assertEquals(NAME_NODE, terrapinNameNode);
      assertTrue(hdfsDir.startsWith(Constants.HDFS_DATA_DIR + "/" + FILE_SET));
    }

    @Override
    protected void loadFileSetData(ZooKeeperManager zkManager, FileSetInfo fsInfo, Options options)
        throws Exception {
      assertEquals(FILE_SET, fsInfo.fileSetName);
      assertEquals(NUM_PART, fsInfo.servingInfo.numPartitions);
    }
  }

  @Test
  public void testUpload() throws Exception {
    ZooKeeperManager zkManager = mock(ZooKeeperManager.class);
    Job job = mock(Job.class);
    Configuration conf = new Configuration();
    TerrapinUploaderOptions options = new TerrapinUploaderOptions();
    options.terrapinFileSet = FILE_SET;

    when(zkManager.getClusterInfo()).thenReturn(new ClusterInfo(NAME_NODE, REPLICA_FACTOR));
    doNothing().when(zkManager).lockFileSet(anyString(), any(FileSetInfo.class));
    doNothing().when(zkManager).unlockFileSet(anyString());
    when(job.waitForCompletion(anyBoolean())).then(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        Thread.sleep(1000);
        return true;
      }
    });
    when(job.getNumReduceTasks()).thenReturn(NUM_PART);
    when(job.getConfiguration()).thenReturn(conf);
    doNothing().when(job).setOutputFormatClass(Matchers.<Class<? extends OutputFormat>>any());
    doNothing().when(job).setOutputKeyClass(Matchers.<Class<?>>any());
    doNothing().when(job).setOutputValueClass(Matchers.<Class<?>>any());

    TestHadoopJobLoader uploader = new TestHadoopJobLoader(zkManager, options, job);
    assertTrue(uploader.waitForCompletion());

    assertEquals(Constants.MAPRED_MAP_MAX_ATTEMPTS,
        Integer.parseInt(conf.get("mapred.map.max.attempts")));
    assertEquals(Constants.CHECKSUM_BYTES,
        Integer.parseInt(conf.get("io.bytes.per.checksum")));
    assertEquals(Constants.DEFAULT_MAX_SHARD_SIZE_BYTES,
        Long.parseLong(conf.get("dfs.block.size")));
    assertEquals(REPLICA_FACTOR, Integer.parseInt(conf.get("dfs.replication")));

    verify(zkManager).getClusterInfo();
    verify(zkManager).lockFileSet(eq(FILE_SET), any(FileSetInfo.class));
    verify(zkManager).unlockFileSet(eq(FILE_SET));
    verify(job).waitForCompletion(eq(true));
    verify(job).setOutputFormatClass(eq(HFileOutputFormat.class));
    verify(job).setOutputValueClass(eq(BytesWritable.class));
    verify(job).setOutputKeyClass(eq(BytesWritable.class));
  }
}
