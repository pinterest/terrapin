/**
 * Thrift definitions for communication between the terrapin client library
 * and the terrapin server. These are for internal use by the client library
 * only which knows about the sharding of the data and can appropriately
 * route the requests.
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
