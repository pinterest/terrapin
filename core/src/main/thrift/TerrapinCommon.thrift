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
 * Common thrift definitions such as thrift structures/error codes used
 * for both internal communication and by the terrapin thrift server.
 */

namespace java com.pinterest.terrapin.thrift.generated
namespace py services.terrapin.common

enum TerrapinGetErrorCode {
  OTHER = 1,
  // Thrown by the server - errors include requests being routed to servers
  // not serving the corresponding HFiles or other kinds of hfile lookup
  // errors.
  NOT_SERVING_RESOURCE = 101,
  NOT_SERVING_PARTITION = 102,
  READ_ERROR = 103,
  // Thrown by the client library. Errors include requests for a file set
  // which does not exist etc.
  PARTITION_OFFLINE = 201,
  FILE_SET_NOT_FOUND = 202,
  // Thrown by the thrift server if it can't find a configuration for a
  // particular terrapin cluster.
  CLUSTER_NOT_FOUND = 203,
  INVALID_REQUEST = 204,
  INVALID_FILE_SET_VIEW = 205
}

exception TerrapinGetException {
  1: required string message,
  2: required TerrapinGetErrorCode errorCode
}

/**
 * The response for a single key lookup.
 */
struct TerrapinSingleResponse {
  1: optional binary value,
  // If the lookup failed, the errorCode will be appropriately set and the
  // value will not be set.
  2: optional TerrapinGetErrorCode errorCode
}

/**
 * The response for multiple key lookups spanning many keys within a single
 * resource.
 */
struct TerrapinResponse {
  1: required map<binary, TerrapinSingleResponse> responseMap
}
