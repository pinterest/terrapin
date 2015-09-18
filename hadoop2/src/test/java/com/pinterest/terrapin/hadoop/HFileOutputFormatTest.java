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

import com.pinterest.terrapin.TerrapinUtil;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.junit.Test;

public class HFileOutputFormatTest {
  @Test
  public void testGetCompression() {
    assertEquals(Compression.Algorithm.SNAPPY, HFileOutputFormat.getAlgorithm(null));
    assertEquals(Compression.Algorithm.SNAPPY, HFileOutputFormat.getAlgorithm(""));
    assertEquals(Compression.Algorithm.SNAPPY, HFileOutputFormat.getAlgorithm("WRONG_ALGO"));
    assertEquals(Compression.Algorithm.SNAPPY, HFileOutputFormat.getAlgorithm("SNAPPY"));
    assertEquals(Compression.Algorithm.NONE, HFileOutputFormat.getAlgorithm("NONE"));
  }

  @Test
  public void testHFilePath() {
    Path outputDir = new Path("/abc/def");
    int partitionIndex = 233;
    Path expectedDir = new Path("/abc/def/" + TerrapinUtil.formatPartitionName(233));
    assertEquals(expectedDir, HFileOutputFormat.hfilePath(outputDir, partitionIndex));
  }
}
