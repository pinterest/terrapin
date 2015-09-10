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
package com.pinterest.terrapin.server;

import com.google.common.collect.Maps;
import com.pinterest.terrapin.storage.Reader;
import com.pinterest.terrapin.thrift.generated.TerrapinGetErrorCode;
import com.pinterest.terrapin.thrift.generated.TerrapinGetException;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Maintains a mapping of resources and partitions to their corresponding readers.
 */
public class ResourcePartitionMap {
  private static final Logger LOG = LoggerFactory.getLogger(ResourcePartitionMap.class);

  // Map from (resource,partition) -> reader.
  private final Map<Pair<String, String>, Reader> readerMap;
  // Map from resource->number of partitions for a resource.
  private final Map<String, Integer> partitionCountMap;

  public ResourcePartitionMap() {
    // We use concurrent maps since we require the ability to issue lock free reads. Writes
    // are still synchronized so that we can update both maps together.
    this.readerMap = Maps.newConcurrentMap();
    this.partitionCountMap = Maps.newConcurrentMap();
  }

  private void incrementPartitionCount(String resource) {
    Integer resourceCount = partitionCountMap.get(resource);
    if (resourceCount == null) {
      partitionCountMap.put(resource, 1);
    } else {
      partitionCountMap.put(resource, resourceCount + 1);
    }
  }

  private void decrementPartitionCount(String resource) {
    Integer resourceCount = partitionCountMap.get(resource);
    if (resourceCount == null || resourceCount <= 1) {
      partitionCountMap.remove(resource);
    } else {
      partitionCountMap.put(resource, resourceCount - 1);
    }
  }

  public void addReader(String resource,
                        String partition,
                        Reader r) throws UnsupportedOperationException {
    Pair<String, String> readerMapKey = new ImmutablePair(resource, partition);
    synchronized (this) {
      if (readerMap.containsKey(readerMapKey)) {
        // If we already have a reader available, log as an error and return.
        LOG.warn("Resource : " + resource + " Partition : " + partition +
                " already exists.");
        return;
      }
      readerMap.put(readerMapKey, r);
      incrementPartitionCount(resource);
    }
  }

  public Reader removeReader(String resource, String partition)
      throws UnsupportedOperationException {
    Pair<String, String> readerMapKey = new ImmutablePair(resource, partition);
    synchronized (this) {
      Reader r = this.readerMap.get(readerMapKey);
      if (r != null) {
        readerMap.remove(readerMapKey);
        decrementPartitionCount(resource);
        return r;
      } else {
        throw new UnsupportedOperationException("Partition " + partition +
            " not found for Resource " + resource);
      }
    }
  }

  public Reader getReader(String resource, String partition) throws TerrapinGetException {
    Pair<String, String> readerMapKey = new ImmutablePair(resource, partition);
    Reader r = readerMap.get(readerMapKey);
    if (r == null) {
      Integer partitionCount = partitionCountMap.get(resource);
      if (partitionCount == null || partitionCount < 1) {
        throw new TerrapinGetException("Resource " + resource + " not found.",
            TerrapinGetErrorCode.NOT_SERVING_RESOURCE);
      } else {
        throw new TerrapinGetException("Resource " + resource + " Partition " + partition +
            " not found.", TerrapinGetErrorCode.NOT_SERVING_PARTITION);
      }
    }
    return r;
  }

  public void close() {
    // TODO(varun): stop accepting any more open/close commands and close all readers/release
    // file descriptors.
  }
}
