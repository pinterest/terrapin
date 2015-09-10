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

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.pinterest.terrapin.Constants;
import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.FuturePool;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Factory class for creating different kinds of readers.
 */
public class ReaderFactory {
  private final PropertiesConfiguration configuration;
  private final FileSystem hadoopFs;
  private final FuturePool readerFuturePool;

  public ReaderFactory(PropertiesConfiguration configuration, FileSystem hadoopFs) {
    this.configuration = configuration;
    this.hadoopFs = hadoopFs;
    int numReaderThreads = this.configuration.getInt(Constants.READER_THREAD_POOL_SIZE, 200);
    ExecutorService threadPool = new ThreadPoolExecutor(numReaderThreads,
                               numReaderThreads,
                               0,
                               TimeUnit.SECONDS,
                               new LinkedBlockingDeque<Runnable>(10000),
                               new ThreadFactoryBuilder().setDaemon(false)
                                   .setNameFormat("reader-pool-%d")
                                   .build());
    this.readerFuturePool = new ExecutorServiceFuturePool(threadPool);
  }

  public Reader createHFileReader(String hdfsPath, CacheConfig cacheConfig) throws IOException {
    return new HFileReader(hadoopFs, hdfsPath, cacheConfig, readerFuturePool);
  }
}
