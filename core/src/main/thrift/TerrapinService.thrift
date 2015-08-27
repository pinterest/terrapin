/**
 * Thrift definitions for exposing terrapin as a thrift service. The
 * service will wrap the client library and services in other languages
 * will be able to talk to the underlying terrapin cluster through the
 * thrift service defined in this spec.
 */

namespace java com.pinterest.terrapin.thrift.generated
namespace py services.terrapin.thrift_libs

include "TerrapinCommon.thrift"

enum SelectionPolicy {
  // The RANDOM selection policy indicates the Terrapin service will 
  // randomly pick a cluster as primary cluster
  RANDOM = 1,
  // The PRIMARY_FIRST selection policy indicates the Terrapin service will
  // pick first cluster as primary cluster
  PRIMARY_FIRST = 2
}

struct RequestOptions {
  1: optional i64 speculativeTimeoutMillis = 100,
  2: optional SelectionPolicy selectionPolicy = SelectionPolicy.RANDOM
}


struct TerrapinGetRequest {
  // The name of the clusters we are talking to. We allow multiple clusters
  // if we have data replicated across multiple clusters. The failover
  // functionality is not implemented yet.
  1: required list<string> clusterList,
  2: required string fileSet,
  3: required binary key,
  4: optional RequestOptions options
}

struct TerrapinMultiGetRequest {
  // The name of the clusters we are talking to. We allow multiple clusters
  // if we have data replicated across multiple clusters. The failover
  // functionality is not implemented yet.
  1: required list<string> clusterList,
  2: required string fileSet,
  3: required list<binary> keyList,
  4: optional RequestOptions options
}

service TerrapinService {
  /**
   * If the look is successful, the TerrapinSingleResponse object will have
   * a value set. If the key was not found, the value will not be set. If
   * an error is raised, we would throw an exception and propagate it
   * back to the client.
   */
  TerrapinCommon.TerrapinSingleResponse get(1:TerrapinGetRequest request)
      throws (1:TerrapinCommon.TerrapinGetException e)

  /**
   * Retrieve the response for multiple keys within a *single* fileset. Keys
   * for which no data is found are not included in the returned response. If
   * the entire batch fails, an exception is thrown. Individual errors are
   * reported by setting the errorCode in the corresponding
   * TerrapinSingleResponse objects.
   */
  TerrapinCommon.TerrapinResponse multiGet(1:TerrapinMultiGetRequest request)
      throws (1:TerrapinCommon.TerrapinGetException e)
}
