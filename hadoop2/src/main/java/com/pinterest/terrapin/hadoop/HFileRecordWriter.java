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

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * HFileRecordWriter for write key value pairs to HFile
 */
public class HFileRecordWriter extends RecordWriter<BytesWritable, BytesWritable> {
  private StoreFile.Writer writer;

  public HFileRecordWriter(StoreFile.Writer writer) {
    this.writer = writer;
  }

  @Override
  public void write(BytesWritable key, BytesWritable value)
      throws IOException, InterruptedException {
    // Mapreduce reuses the same Text objects and hence sometimes they have some
    // additional left over garbage at the end from previous keys. So we need to
    // retrieve the String objects which do not have the additional garbage. getBytes()
    // call does not work well.
    byte[] row = new byte[key.getLength()];
    byte[] val = new byte[value.getLength()];
    for (int i = 0; i < row.length; i++) {
      row[i] = key.getBytes()[i];
    }
    for (int i = 0; i < val.length; i++) {
      val[i] = value.getBytes()[i];
    }
    writer.append(new KeyValue(row,
        Bytes.toBytes("cf"),
        Bytes.toBytes(""),
        val));
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException, InterruptedException {
    writer.close();
  }
}
