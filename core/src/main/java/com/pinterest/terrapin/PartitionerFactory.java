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
