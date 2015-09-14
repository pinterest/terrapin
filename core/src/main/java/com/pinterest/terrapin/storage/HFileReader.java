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
import com.pinterest.terrapin.Constants;
import com.pinterest.terrapin.TerrapinUtil;
import com.pinterest.terrapin.base.BytesUtil;
import com.twitter.ostrich.stats.Stats;
import com.twitter.util.Function;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import com.twitter.util.FuturePool;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * A reader which can perform lookups against HFiles.
 */
public class HFileReader implements Reader {
  private static final byte[] EMPTY_COLUMN = Bytes.toBytes("");

  private final HFile.Reader reader;
  private final FuturePool futurePool;

  private String fileSet;

  private String notFoundStatsKey;
  private String metricsStatsKey;
  private String errorStatsKey;
  private String sizeStatsKey;

  // The HBase Block cache uses only the last part of the path as the block cache key.
  // This does not play nicely with terrapin since different file sets can have the
  // same files such as "part-00000". We override the getName() function of the Path
  // class since that goes into the BlockCacheKey to include the file set name
  // and the version.
  static class TerrapinPath extends Path {
    public TerrapinPath(String path) {
      super(path);
    }

    @Override
    public String getName() {
      Path versionPath = getParent();
      Path fileSetPath = versionPath.getParent();
      return new StringBuilder(super.getName()).append("_").append(
          fileSetPath.getName()).append("_").append(versionPath.getName()).toString();
    }
  }

  public HFileReader(FileSystem fs,
                     String path,
                     CacheConfig cacheConf,
                     FuturePool futurePool) throws IOException {
    this.reader = HFile.createReader(fs, new TerrapinPath(path), cacheConf);
    this.futurePool = futurePool;
    this.fileSet = TerrapinUtil.extractFileSetFromPath(path);
    setUpStatsKeys();
  }

  private void setUpStatsKeys() {
    if (this.fileSet != null && !this.fileSet.isEmpty()) {
      this.notFoundStatsKey = this.fileSet + "-not-found";
      this.metricsStatsKey = this.fileSet + "-lookup-latency-ms";
      this.errorStatsKey = this.fileSet + "-lookup-errors";
      this.sizeStatsKey = this.fileSet + "-value-size";
    } else {
      this.notFoundStatsKey = null;
      this.metricsStatsKey = null;
      this.errorStatsKey = null;
      this.sizeStatsKey = null;
    }
  }

  static KeyValue buildKeyValueForLookup(byte[] key) {
    return new KeyValue(key,
                        Constants.HFILE_COLUMN_FAMILY,
                        EMPTY_COLUMN,
                        HConstants.LATEST_TIMESTAMP,
                        KeyValue.Type.Put);
  }

  /**
   * For testing.
   */
  void setFileSet(String fileSet) {
    this.fileSet = fileSet;
    setUpStatsKeys();
  }

  /**
   * Issues an HFile lookup on the underlying HFile.Reader. This is protected
   * for testing.
   */
  protected Pair<ByteBuffer, Pair<ByteBuffer, Throwable>> getValueFromHFile(ByteBuffer key) {
    try {
      HFileScanner scanner = reader.getScanner(true, true, false);
      KeyValue kv = buildKeyValueForLookup(
          BytesUtil.readBytesFromByteBufferWithoutConsume(key));
      int code = scanner.seekTo(kv.getKey());
      ByteBuffer value = null;
      if (code == 0) {
        value = ByteBuffer.wrap(scanner.getKeyValue().getValue());
        if (this.sizeStatsKey != null) {
          Stats.addMetric(this.sizeStatsKey, value.remaining());
        }
        Stats.addMetric("value-size", value.remaining());
      } else {
        Stats.incr("not-found");
        if (this.notFoundStatsKey != null) {
          Stats.incr(this.notFoundStatsKey);
        }
      }
      return new ImmutablePair(key, new ImmutablePair(value, null));
    } catch (Throwable t) {
      return new ImmutablePair(key, new ImmutablePair(null, t));
    }
  }

  @Override
  public Future<Map<ByteBuffer, Pair<ByteBuffer, Throwable>>> getValues(
      List<ByteBuffer> keyList) throws Throwable {
    // Build a Future by executing a call against the future pool for every key.
    List<Future<Pair<ByteBuffer, Pair<ByteBuffer, Throwable>>>> futureList =
        Lists.newArrayListWithCapacity(keyList.size());
    for (final ByteBuffer key : keyList) {
      Future<Pair<ByteBuffer, Pair<ByteBuffer, Throwable>>> future = futurePool.apply(
          new Function0<Pair<ByteBuffer, Pair<ByteBuffer, Throwable>>>() {
            @Override
            public Pair<ByteBuffer, Pair<ByteBuffer, Throwable>> apply() {
              long startTimeMillis = System.currentTimeMillis();
              Pair<ByteBuffer, Pair<ByteBuffer, Throwable>> keyExceptionPair = getValueFromHFile(key);
              int timeMs = (int)(System.currentTimeMillis() - startTimeMillis);
              if (metricsStatsKey != null) {
                Stats.addMetric(metricsStatsKey, timeMs);
              }
              Stats.addMetric("lookup-latency-ms", timeMs);
              if (keyExceptionPair.getRight().getRight() != null) {
                if (errorStatsKey != null) {
                  Stats.incr(errorStatsKey);
                }
                Stats.incr("lookup-errors");
              }
              return keyExceptionPair;
            }
          });
      futureList.add(future);
    }
    return Future.collect(futureList).map(
        new Function<List<Pair<ByteBuffer, Pair<ByteBuffer, Throwable>>>,
                     Map<ByteBuffer, Pair<ByteBuffer, Throwable>>>() {
          @Override
          public Map<ByteBuffer, Pair<ByteBuffer, Throwable>> apply(
              List<Pair<ByteBuffer, Pair<ByteBuffer, Throwable>>> keyValuePairList) {
            Map<ByteBuffer, Pair<ByteBuffer, Throwable>> returnMap =
                Maps.newHashMapWithExpectedSize(keyValuePairList.size());
            for (Pair<ByteBuffer, Pair<ByteBuffer, Throwable>> pair : keyValuePairList) {
              Pair<ByteBuffer, Throwable> value = pair.getRight();
              if (value.getLeft() != null || value.getRight() != null) {
                returnMap.put(pair.getLeft(), pair.getRight());
              }
            }
            return returnMap;
          }
        });
  }

  @Override
  public void close() throws IOException {
    reader.close(true);
  }
}