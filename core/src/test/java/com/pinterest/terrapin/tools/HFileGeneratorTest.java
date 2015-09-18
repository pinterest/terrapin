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
package com.pinterest.terrapin.tools;

import static org.junit.Assert.assertEquals;

import com.pinterest.terrapin.Constants;
import com.pinterest.terrapin.thrift.generated.PartitionerType;

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

public class HFileGeneratorTest {
  private File outputDir;

  @Before
  public void setUp() {
    outputDir = Files.createTempDir();
  }

  @Test
  public void testGenerateHFiles() throws IOException {
    Configuration conf = new Configuration();
    FileSystem fs = FileSystem.get(conf);
    int numOfPart = 10;
    int numOfKeys = 1000;
    HFileGenerator.generateHFiles(fs, conf, outputDir,
        PartitionerType.CASCADING, numOfPart, numOfKeys);
    FilenameFilter hfileFilter = new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.startsWith(Constants.FILE_PREFIX);
      }
    };
    File[] hfiles = outputDir.listFiles(hfileFilter);
    assertEquals(numOfPart, hfiles.length);

    int count = 0;
    for(File hfile : hfiles) {
      HColumnDescriptor columnDescriptor = new HColumnDescriptor();
      columnDescriptor.setBlockCacheEnabled(false);
      HFile.Reader reader =
          HFile.createReader(fs, new Path(hfile.toURI()), new CacheConfig(conf, columnDescriptor));
      count += reader.getEntries();
      reader.close();
    }
    assertEquals(numOfKeys, count);
  }

  @After
  public void cleanUp() throws IOException {
    FileUtils.deleteDirectory(outputDir);
  }
}
