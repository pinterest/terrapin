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
 * Thrift definitions for communication between the terrapin client library
 * and the terrapin server. These are for internal use by the client library
 * only which knows about the sharding of the data and can appropriately
 * route the requests.
 */

namespace java com.pinterest.terrapin.thrift.generated

include "TerrapinCommon.thrift"

exception TerrapinGetException {
  1: required string message,
  2: required TerrapinCommon.TerrapinGetErrorCode errorCode
}

/**
 * A batch of keys belonging to the same resource and partition. These
 * are grouped by the client library before being sent to the server.
 */
struct MultiKey {
  1: required list<binary> key,
  2: required string resource,
  3: required string partition
}

/**
 * The request struct representing multiple key batches which could
 * span multiple resources and partitions.
 */
struct TerrapinInternalGetRequest {
  1: required list<MultiKey> keyList
}

service TerrapinServerInternal {
  /**
   * Retrieve the response for multiple keys within a *single* resource. Keys
   * for which no data is found are not included in the returned response. If
   * the entire batch fails, an exception is thrown. Individual errors are
   * reported by setting the errorCode in the corresponding
   * TerrapinSingleResponse objects.
   */
  TerrapinCommon.TerrapinResponse get(1:TerrapinInternalGetRequest request)
      throws (1:TerrapinCommon.TerrapinGetException e)
}
