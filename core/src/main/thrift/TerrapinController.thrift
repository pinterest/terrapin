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

/**
 * Thrift file for communicating with the terrapin controller. This enables
 * loading of new file sets.
 */

namespace java com.pinterest.terrapin.thrift.generated

enum ControllerErrorCode {
  OTHER = 1,
  HDFS_ERROR = 2,
  SHARD_LOAD_TIMEOUT = 3,
  // Thrown when the data set being loaded has issues such as the number of
  // partitions at the time of loading does not match the number of partitions
  // actually copied over.
  INVALID_DATA_SET = 4,
  INVALID_REQUEST = 5,
  HELIX_ERROR = 6
}

exception ControllerException {
  1: required string message,
  2: required ControllerErrorCode errorCode
}

/**
 * The scheme used for partitioning the (h)files.
 */
enum PartitionerType {
  // The MODULUS scheme is the default scheme used by Hadoop for splitting
  // the key space across reduce shards. This uses the HashPartitioner as
  // provided by Hadoop.
  MODULUS = 1,
  // Sharding scheme used by cascading mapreduce jobs.
  CASCADING = 2
}

/**
 * The schema for a file set - includes information such as partitioning
 * scheme and garbage collection policy.
 */
struct Options {
  // For certain critical data sets, we need to keep multiple versions of
  // (h)files around. This number must not be less than 1.
  1: optional i32 numVersionsToKeep = 1,
  2: optional PartitionerType partitioner = PartitionerType.MODULUS
}

struct TerrapinLoadRequest {
  1: required string fileSet,
  2: required string hdfsDirectory,
  // Expected number of partitions for the file set, if the two don't match up,
  // something has gone terribly wrong.
  3: required i32 expectedNumPartitions,

  4: optional Options options
}

struct TerrapinDeleteRequest {
  1: required string fileSet
}

service TerrapinController {
  /**
   * Loads data already existing in an HDFS directory into serving.
   */
  void loadFileSet(1:TerrapinLoadRequest request)
      throws (1:ControllerException e)

  /**
   * Removes a fileset from serving - this deletes all the versions associated
   * with the file set.
   */
  void deleteFileSet(1:TerrapinDeleteRequest request)
      throws (1:ControllerException e)
}
