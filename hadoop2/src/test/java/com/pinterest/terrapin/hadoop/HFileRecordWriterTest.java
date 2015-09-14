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

import com.pinterest.terrapin.Constants;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class HFileRecordWriterTest {
  private File tempFile;

  @Before
  public void setUp() throws IOException {
    tempFile = File.createTempFile("hfile", Integer.toString((int) Math.random()));
  }

  @Test
  public void testWrite() throws Exception {
    Configuration conf = new Configuration();
    HColumnDescriptor columnDescriptor = new HColumnDescriptor();
    // Disable block cache to ensure it reads the actual file content.
    columnDescriptor.setBlockCacheEnabled(false);
    FileSystem fs = FileSystem.get(conf);
    int blockSize = conf.getInt(Constants.HFILE_BLOCKSIZE, 16384);
    final StoreFile.Writer writer =
        new StoreFile.WriterBuilder(conf, new CacheConfig(conf, columnDescriptor), fs, blockSize)
            .withFilePath(new Path(tempFile.toURI()))
            .build();
    /* Create our RecordWriter */
    RecordWriter<BytesWritable, BytesWritable> hfileWriter =
        new HFileRecordWriter(writer);

    List<String> keys = Lists.newArrayList();
    List<String> values = Lists.newArrayList();
    for (int i = 0; i < 100; ++i) {
      String key = String.format("%03d", i);
      String val = "value " + i;
      keys.add(key);
      values.add(val);
      hfileWriter.write(new BytesWritable(key.getBytes()), new BytesWritable(val.getBytes()));
    }
    /* This internally closes the StoreFile.Writer */
    hfileWriter.close(null);

    HFile.Reader reader = HFile.createReader(fs, new Path(tempFile.toURI()),
        new CacheConfig(conf, columnDescriptor));
    HFileScanner scanner = reader.getScanner(false, false, false);
    boolean valid = scanner.seekTo();
    List<String> gotKeys = Lists.newArrayListWithCapacity(keys.size());
    List<String> gotValues = Lists.newArrayListWithCapacity(values.size());
    while(valid) {
      KeyValue keyValue = scanner.getKeyValue();
      gotKeys.add(new String(keyValue.getRow()));
      gotValues.add(new String(keyValue.getValue()));
      valid = scanner.next();
    }
    assertEquals(keys, gotKeys);
    assertEquals(values, gotValues);
    reader.close();
  }

  @After
  public void cleanUp() {
    if (tempFile != null) {
      tempFile.delete();
    }
  }
}
