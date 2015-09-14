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
package com.pinterest.terrapin;

import com.pinterest.terrapin.thrift.generated.PartitionerType;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * Get a suitable partitioner. We currently have two ways of generating hfiles: raw mapreduce jobs
 * and cascading jobs. Raw mapreduce jobs use HashPartitioner, but cascading jobs use cascading's
 * internal partitioners. This class returns the suitable partitioners.
 */
public class PartitionerFactory {
  private static class CascadingPartitioner extends Partitioner<BytesWritable, BytesWritable> {

    @Override
    public int getPartition(BytesWritable key, BytesWritable value, int totalShards) {
      int hash = 1;
      hash = 31 * hash + key.hashCode();
      return (hash & Integer.MAX_VALUE) % totalShards;
    }
  }

  private static final Partitioner<BytesWritable, BytesWritable> HASH_PARTITIONER =
      new HashPartitioner<BytesWritable, BytesWritable>();

  private static final Partitioner CASCADING_PARTITIONER = new CascadingPartitioner();

  /**
   * Get the partitioner. If shardFunction is "ShardFunction.CASCADING", return
   * CascadingPartitioner. Otherwise, return HashPartitioner.
   */
  public static Partitioner getPartitioner(PartitionerType type) {
    if (type.equals(PartitionerType.CASCADING)) {
      return CASCADING_PARTITIONER;
    } else if (type.equals(PartitionerType.MODULUS)) {
      return HASH_PARTITIONER;
    } else {
      throw new RuntimeException("Unsupported ShardFunction." + type);
    }
  }
}
