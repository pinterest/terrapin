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
