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
package com.pinterest.terrapin.storage;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.twitter.ostrich.stats.Stats;
import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.FuturePool;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.pinterest.terrapin.TerrapinUtil;

/**
 * Unit test for HFileReader. The test creates an HFile with upto 10K keys on the local file system.
 * It then instantiates an HFileReader over the local file system to issue HFile lookups and
 * ensure that the lookups are executed accurately.
 */
public class HFileReaderTest {
  private static HFileReader hfileReader;
  private static Map<ByteBuffer, ByteBuffer> keyValueMap;
  private static Set<ByteBuffer> errorKeys;
  private static String hfilePath;

  // Class to override HFileReader so we can simulate failed lookups for part of the batch
  // and return an exception.
  static class TestHFileReader extends HFileReader {
    private static Set<ByteBuffer> errorKeys;
    public TestHFileReader(FileSystem fs,
                           String hfilePath,
                           CacheConfig cacheConfig,
                           FuturePool futurePool,
                           Set<ByteBuffer> errorKeys) throws IOException {
      super(fs, hfilePath, cacheConfig, futurePool);
      this.errorKeys = errorKeys;
    }

    @Override
    protected Pair<ByteBuffer, Pair<ByteBuffer, Throwable>> getValueFromHFile(ByteBuffer key) {
      if (errorKeys.contains(key)) {
        return new ImmutablePair(key, new ImmutablePair(null, new IOException()));
      } else {
        return super.getValueFromHFile(key);
      }
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    int randomNum = (int) (Math.random() * Integer.MAX_VALUE);
    hfilePath = "/tmp/hfile-" + randomNum;
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    keyValueMap = Maps.newHashMapWithExpectedSize(10000);
    errorKeys = Sets.newHashSetWithExpectedSize(2000);
    StoreFile.Writer writer = new StoreFile.WriterBuilder(conf, new CacheConfig(conf),
        fs, 4096).
        withFilePath(new Path(hfilePath)).
        withCompression(Compression.Algorithm.NONE).
        build();
    // Add upto 10K values.
    for (int i = 0; i < 10000; i++) {
      byte[] key = String.format("%04d", i).getBytes();
      byte[] value = null;
      // Add a couple of empty values for testing and making sure we return them.
      if (i <= 1) {
        value = "".getBytes();
      } else {
        value = ("v" + (i + 1)).getBytes();
      }
      KeyValue kv = new KeyValue(key,
          Bytes.toBytes("cf"),
          Bytes.toBytes(""),
          value);
      writer.append(kv);
      keyValueMap.put(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
      if (i >= 4000 && i < 6000) {
        errorKeys.add(ByteBuffer.wrap(key));
      }
    }
    writer.close();
    hfileReader = new TestHFileReader(fs,
        hfilePath,
        new CacheConfig(conf),
        new ExecutorServiceFuturePool(Executors.newFixedThreadPool(1)),
        errorKeys);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    hfileReader.close();
    FileUtils.forceDelete(new File(hfilePath));
  }

  private void checkKeyValues(List<ByteBuffer> keyList,
                              int expectedTotalCount,
                              int expectedErrorKeyCount,
                              int expectedNotFoundKeyCount)
      throws Throwable {
    Map<ByteBuffer, Pair<ByteBuffer, Throwable>> valuesMap =
        hfileReader.getValues(keyList).get();
    int errorKeyCount = 0, notFoundKeyCount = 0;
    for (ByteBuffer key : keyList) {
      if (!valuesMap.containsKey(key)) {
        notFoundKeyCount++;
        continue;
      }
      ByteBuffer value = valuesMap.get(key).getLeft();
      Throwable exception = valuesMap.get(key).getRight();
      if (errorKeys.contains(key)) {
        assertNull(value);
        assertTrue(exception instanceof IOException);
        errorKeyCount++;
      } else {
        assertNull(exception);
        assertEquals(keyValueMap.get(key), value);
      }
    }
    assertEquals(expectedTotalCount, valuesMap.size());
    assertEquals(expectedErrorKeyCount, errorKeyCount);
    assertEquals(expectedNotFoundKeyCount, notFoundKeyCount);
  }

  @Test
  public void testGetValues() throws Throwable {
    // Set up a null file set to start with.
    hfileReader.setFileSet(null);
    // Test a batch of all even values along with some lookups on non existent keys.
    List<ByteBuffer> keyList = Lists.newArrayListWithCapacity(5000);
    for (int i = 0; i < 10000; i += 2) {
      keyList.add(ByteBuffer.wrap(String.format("%04d", i).getBytes()));
    }
    // Add one not found key.
    keyList.add(ByteBuffer.wrap(String.format("%04d", 15000).getBytes()));

    checkKeyValues(keyList, 5000, 1000, 1);
    assertEquals(5001, Stats.getMetric("lookup-latency-ms").apply().count());
    assertEquals(1000, Stats.getCounter("lookup-errors").apply());
    assertEquals(1, Stats.getCounter("not-found").apply());

    // Test a batch of all odd values along with some lookups on non existent keys.
    // This time, we use "test" as the file set to check if the metrics are recorded
    // correctly.
    hfileReader.setFileSet("test");
    keyList = Lists.newArrayListWithCapacity(5000);
    for (int i = 1; i < 10000; i += 2) {
      keyList.add(ByteBuffer.wrap(String.format("%04d", i).getBytes()));
    }
    // Add one not found key.
    keyList.add(ByteBuffer.wrap(String.format("%04d", 16000).getBytes()));
    checkKeyValues(keyList, 5000, 1000, 1);

    assertEquals(10002, Stats.getMetric("lookup-latency-ms").apply().count());
    assertEquals(5001, Stats.getMetric("test-lookup-latency-ms").apply().count());
    assertEquals(8000, Stats.getMetric("value-size").apply().count());
    assertEquals(4000, Stats.getMetric("test-value-size").apply().count());
    assertEquals(2000, Stats.getCounter("lookup-errors").apply());
    assertEquals(1000, Stats.getCounter("test-lookup-errors").apply());
    assertEquals(2, Stats.getCounter("not-found").apply());
    assertEquals(1, Stats.getCounter("test-not-found").apply());
  }

  @Test
  public void testTerrapinPath() {
    String partName = TerrapinUtil.formatPartitionName(0);
    Path path = new HFileReader.TerrapinPath("/terrapin/data/meta_user_join/1234/" + partName);
    assertEquals(partName + "_meta_user_join_1234", path.getName());
  }
}
