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
package com.pinterest.terrapin.client;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.pinterest.terrapin.Constants;
import com.pinterest.terrapin.TerrapinUtil;
import com.pinterest.terrapin.base.BytesUtil;
import com.pinterest.terrapin.thrift.generated.MultiKey;
import com.pinterest.terrapin.thrift.generated.PartitionerType;
import com.pinterest.terrapin.thrift.generated.TerrapinGetErrorCode;
import com.pinterest.terrapin.thrift.generated.TerrapinGetException;
import com.pinterest.terrapin.thrift.generated.TerrapinInternalGetRequest;
import com.pinterest.terrapin.thrift.generated.TerrapinResponse;
import com.pinterest.terrapin.thrift.generated.TerrapinServerInternal;
import com.pinterest.terrapin.thrift.generated.TerrapinSingleResponse;
import com.pinterest.terrapin.zookeeper.FileSetInfo;
import com.pinterest.terrapin.zookeeper.ViewInfo;
import com.twitter.finagle.Service;
import com.twitter.finagle.builder.ClientBuilder;
import com.twitter.finagle.thrift.ClientId;
import com.twitter.finagle.thrift.ThriftClientFramedCodecFactory;
import com.twitter.finagle.thrift.ThriftClientRequest;
import com.twitter.ostrich.stats.Stats;
import com.twitter.util.Duration;
import com.twitter.util.ExceptionalFunction;
import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.Function;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import com.twitter.util.FuturePool;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A client for communicating with the terrapin cluster. The client's job is to batch keys
 * by partitions within a resource and determining the right set of instances to issue the
 * requests against. It then collates the responses and returns back to the client.
 *
 * TODO(varun): Add speculative execution for full shard failures. This would give us lower
 * latency.
 */
public class TerrapinClient {
  private static final Logger LOG = LoggerFactory.getLogger(TerrapinClient.class);

  // Of the format, terrapin-client-<cluster_name>-
  private String statsPrefix;
  private FileSetViewManager fileSetViewManager;

  private Cache<String, Pair<Service<ThriftClientRequest, byte[]>,
                             TerrapinServerInternal.ServiceIface>>
      thriftClientCache;
  private int targetPort;
  private int connectTimeoutMs;
  private int timeoutMs;

  // Future pool establishing connections.
  private FuturePool connectionfuturePool;

  public TerrapinClient(PropertiesConfiguration configuration,
                        int targetPort,
                        int connectTimeoutMs,
                        int timeoutMs) throws Exception {
    String zkQuorum = configuration.getString(Constants.ZOOKEEPER_QUORUM);
    String clusterName = configuration.getString(Constants.HELIX_CLUSTER);
    FileSetViewManager fileSetViewManager = new FileSetViewManager(
        TerrapinUtil.getZooKeeperClient(zkQuorum, 30), clusterName);
    init(fileSetViewManager, clusterName, targetPort, connectTimeoutMs, timeoutMs);
  }

  public TerrapinClient(FileSetViewManager fileSetViewManager,
                        String clusterName,
                        int targetPort,
                        int connectTimeoutMs,
                        int timeoutMs) throws Exception {
    init(fileSetViewManager, clusterName, targetPort, connectTimeoutMs, timeoutMs);
  }

  private void init(FileSetViewManager fileSetViewManager,
                    String clusterName,
                    int targetPort,
                    int connectTimeoutMs,
                    int timeoutMs) throws Exception {
    this.statsPrefix = "terrapin-client-" + clusterName + "-";
    this.fileSetViewManager = fileSetViewManager;
    this.thriftClientCache = CacheBuilder.newBuilder()
        .maximumSize(5000)
        .expireAfterAccess(60, TimeUnit.MINUTES)
        .removalListener(new RemovalListener<String,
            Pair<Service<ThriftClientRequest, byte[]>,
                 TerrapinServerInternal.ServiceIface>>() {
          @Override
          public void onRemoval(RemovalNotification<String,
              Pair<Service<ThriftClientRequest, byte[]>, TerrapinServerInternal.ServiceIface>>
                  removalNotification) {
            removalNotification.getValue().getLeft().release();
            LOG.info("Closing client connections to " + removalNotification.getKey());
          }
        }).build();
    this.targetPort = targetPort;
    this.connectTimeoutMs = connectTimeoutMs;
    this.timeoutMs = timeoutMs;
    this.connectionfuturePool = new ExecutorServiceFuturePool(
        Executors.newFixedThreadPool(10));
  }

  protected Future<TerrapinServerInternal.ServiceIface> getClientFuture(final String hostName) {
    return connectionfuturePool.apply(new Function0<TerrapinServerInternal.ServiceIface>() {
      @Override
      public TerrapinServerInternal.ServiceIface apply() {
        Pair<Service<ThriftClientRequest, byte[]>, TerrapinServerInternal.ServiceIface> client =
            thriftClientCache.getIfPresent(hostName);
        if (client == null) {
          Service<ThriftClientRequest, byte[]> service = ClientBuilder.safeBuild(
              ClientBuilder.get()
                  .hosts(new InetSocketAddress(hostName, targetPort))
                  .codec(new ThriftClientFramedCodecFactory(Option.<ClientId>empty()))
                  .retries(1)
                  .connectTimeout(Duration.fromMilliseconds(connectTimeoutMs))
                  .requestTimeout(Duration.fromMilliseconds(timeoutMs))
                  .hostConnectionLimit(100)
                  .failFast(false));

          client = new ImmutablePair(service, new TerrapinServerInternal.ServiceToClient(
              service, new TBinaryProtocol.Factory()));
          // A release is automatically called when an element is kicked out as part of the
          // removal listener. Doing a double release causes other issues.
          thriftClientCache.asMap().putIfAbsent(hostName, client);
        }
        return client.getRight();
      }
    });
  }

  public Future<TerrapinResponse> getManyNoRetries(String fileSet, Set<ByteBuffer> keyList) {
    return getManyHelper(fileSet, keyList, false);
  }

  public Future<TerrapinResponse> getMany(String fileSet, Set<ByteBuffer> keyList) {
    return getManyHelper(fileSet, keyList, true);
  }

  private ExceptionalFunction<TerrapinResponse, TerrapinSingleResponse> getManyToGetOneFunction(
      final ByteBuffer key) {
    return new ExceptionalFunction<TerrapinResponse, TerrapinSingleResponse>() {
        @Override
        public TerrapinSingleResponse applyE(TerrapinResponse response) throws Throwable {
          TerrapinSingleResponse singleResponse = response.getResponseMap().get(key);
          if (singleResponse == null) {
            return new TerrapinSingleResponse();
          } else {
            if (singleResponse.isSetErrorCode()) {
              throw new TerrapinGetException().setErrorCode(singleResponse.getErrorCode());
            } else {
              return singleResponse;
            }
          }
        }
      };
  }

  /**
   * Get one API without any retries on a terrapin cluster. This will throw an exception if
   * an error occurs. If the value is not found, the value field in the response will not be
   * set.
   *
   * @param fileSet The fileset to use query against.
   * @param key The key to query against.
   * @return The TerrapinSingleResponse object.
   */
  public Future<TerrapinSingleResponse> getOneNoRetries(String fileSet, ByteBuffer key) {
    return getManyNoRetries(fileSet, Sets.newHashSet(key)).map(getManyToGetOneFunction(key));
  }

  /**
   * Get one API with retries.
   */
  public Future<TerrapinSingleResponse> getOne(String fileSet, ByteBuffer key) {
    return getMany(fileSet, Sets.newHashSet(key)).map(getManyToGetOneFunction(key));
  }

  /**
   * Issues a call for retrieving a response for multiple keys. Does appropriate batching.
   * Uses a 2 pass approach:
   * 1) Pass 1 - Batches the keys by partition, issues the requests to each relevant replica
   *             and tracks failed keys and replicas with failures.
   * 2) Pass 2 - Batches the failed keys from pass 1 by host, issues the request to
   *             relevant replicas, excluding the replicas with failures in pass 1.
   *
   * TODO(varun): Think about using speculative execution for the single key lookup use case.
   *              It could provide better latency.
   *
   * @param fileSet The file set against which the request should be issued.
   * @param keyList The list of keys.
   * @param retry Whether to perform a second try on the cluster.
   * @return A future wrapping a TerrapinResponse object.
   */
  protected Future<TerrapinResponse> getManyHelper(final String fileSet,
                                                final Set<ByteBuffer> keyList,
                                                final boolean retry) {
    Pair<FileSetInfo, ViewInfo> pair = null;
    try {
      pair = fileSetViewManager.getFileSetViewInfo(fileSet);
    } catch (TerrapinGetException e) {
      return Future.exception(e);
    }
    final FileSetInfo info = pair.getLeft();
    final ViewInfo viewInfo = pair.getRight();

    // This runs in two passes. In the first pass, we send a query to all the hosts
    // containing the keys in @keyList. We collect the list of failed keys in the first
    // pass and also the set of hosts which had errors. We send out a second query
    // with the failed keys to the respective set of hosts and attempt to exclude
    // the initial set of failed hosts.
    final Set<String> failedHostsFirstPass = Collections.synchronizedSet(
        Sets.<String>newHashSet());
    final Map<ByteBuffer, TerrapinSingleResponse> failedKeysFirstPass = Collections.synchronizedMap(
        Maps.<ByteBuffer, TerrapinSingleResponse>newHashMap());
    Map<String, Future<TerrapinResponse>> responseFutureMapFirstPass = getManyHelper(
        fileSet,
        info.servingInfo.helixResource,
        info.servingInfo.numPartitions,
        viewInfo,
        keyList,
        info.servingInfo.partitionerType,
        failedHostsFirstPass,
        failedKeysFirstPass,
        (Set)Sets.newHashSet(),
        1);
    List<Future<TerrapinResponse>> responseFutureListFirstPass = Lists.newArrayListWithCapacity(
        responseFutureMapFirstPass.size());
    for (Map.Entry<String, Future<TerrapinResponse>> entry :
         responseFutureMapFirstPass.entrySet()) {
      responseFutureListFirstPass.add(entry.getValue());
    }
    // For the failed keys.
    return Stats.timeFutureMillis(statsPrefix + fileSet + "-latency",
        Future.<TerrapinResponse>collect(responseFutureListFirstPass).flatMap(
            new Function<List<TerrapinResponse>, Future<TerrapinResponse>>() {
                @Override
                public Future<TerrapinResponse> apply(final List<TerrapinResponse> responseListPass1) {
                  // At this point, we have a black list of hosts and we also have a list of keys
                  // which did not succeed in the first run.
                  // If the first pass fully succeeded or we have explicitly disabled retries,
                  // then don't perform a retry.
                  if (failedKeysFirstPass.isEmpty() || !retry) {
                    TerrapinResponse aggResponse = new TerrapinResponse();
                    aggResponse.setResponseMap((Map) Maps.newHashMapWithExpectedSize(
                        keyList.size()));
                    for (TerrapinResponse response : responseListPass1) {
                      aggResponse.getResponseMap().putAll(response.getResponseMap());
                    }
                    aggResponse.getResponseMap().putAll(failedKeysFirstPass);
                    return Future.value(aggResponse);
                  }
                  // Otherwise, we fire off a second set of futures.
                  Map<String, Future<TerrapinResponse>> responseFutureMapSecondPass =
                      getManyHelper(fileSet,
                                    info.servingInfo.helixResource,
                                    info.servingInfo.numPartitions,
                                    viewInfo,
                                    failedKeysFirstPass.keySet(),
                                    info.servingInfo.partitionerType,
                                    null,
                                    null,
                                    failedHostsFirstPass,
                                    2);
                  List<Future<TerrapinResponse>> responseFutureListSecondPass =
                      Lists.newArrayListWithCapacity(responseFutureMapSecondPass.size());
                  responseFutureListSecondPass.addAll(responseFutureMapSecondPass.values());
                  return Future.collect(responseFutureListSecondPass).map(
                      new Function<List<TerrapinResponse>, TerrapinResponse>() {
                        @Override
                        public TerrapinResponse apply(List<TerrapinResponse> responseListPass2) {
                          // The two responses (first pass and second pass) should be disjoint
                          // in the set of keys they return, so we can safely merge them.
                          TerrapinResponse aggResponse = new TerrapinResponse();
                          aggResponse.setResponseMap((Map) Maps.newHashMapWithExpectedSize(
                              keyList.size()));
                          for (TerrapinResponse response : responseListPass1) {
                            aggResponse.getResponseMap().putAll(response.getResponseMap());
                          }
                          for (TerrapinResponse response : responseListPass2) {
                            aggResponse.getResponseMap().putAll(response.getResponseMap());
                          }
                          return aggResponse;
                        }
                      });
                }
            }));
  }

    /**
     * Helper function for the getMany call above. Responsible for batching the request
     * by host, tracking hosts/keys with failures and excluding any blacklisted hosts.
     *
     * @param fileSet The original fileset useful for metrics tracking.
     * @param resource The helix resource against which the lookups are being issued.
     * @param numPartitions The number of partitions in the resource.
     * @param keyList The list of keys to be looked up.
     * @param partitionerType The partitioner type (MODULUS etc.)
     * @param failedHostsContainer The container in which failed hosts need to be accumulated.
     * @param failedKeysContainer The container in which failed keys need to be accumulated.
     *                            If null, then we return the failed keys are part of the
     *                            response futures.
     * @param blacklistedHosts The list of hosts which should be excluded from issuing
     *                         rpc(s) against.
     * @param rpcPassNum The rpc pass we are on. The first pass is 1, second is 2 and so on.
     * @return A mapping from host name to a future wrapping a TerrapinResponse object.
     */
  protected Map<String, Future<TerrapinResponse>> getManyHelper(
      final String fileSet,
      String resource,
      int numPartitions,
      final ViewInfo viewInfo,
      Set<ByteBuffer> keyList,
      PartitionerType partitionerType,
      final Set<String> failedHostsContainer,
      final Map<ByteBuffer, TerrapinSingleResponse> failedKeysContainer,
      Set<String> blacklistedHosts,
      final int rpcPassNum) {
    Map<String, Future<TerrapinResponse>> hostResponseMap = Maps.newHashMap();
    TerrapinResponse offlinePartitionResponse = new TerrapinResponse();
    offlinePartitionResponse.setResponseMap((Map)Maps.newHashMap());
    Map<String, MultiKey> partitionKeyMap = Maps.newHashMap();
    for (ByteBuffer key : keyList) {
      String partitionName = TerrapinUtil.getPartitionName(key, partitionerType, numPartitions);
      MultiKey existingMultiKey = partitionKeyMap.get(partitionName);
      if (existingMultiKey != null) {
        existingMultiKey.addToKey(key);
      } else {
        MultiKey multiKey = new MultiKey();
        multiKey.setKey(Lists.newArrayList(key));
        multiKey.setResource(resource);
        multiKey.setPartition(partitionName);
        partitionKeyMap.put(partitionName, multiKey);
      }
    }
    // Batch the requests by hostname.
    Map<String,TerrapinInternalGetRequest> hostRequestMap = Maps.newHashMap();
    for (Map.Entry<String, MultiKey> entry : partitionKeyMap.entrySet()) {
      List<String> instanceList = viewInfo.getInstancesForPartition(resource + "$" + entry.getKey());
      // Do a deterministic hash using the partitionName. The partitionName acts as the randomizing
      // function so that we don't always choose the alphabetically smallest instance otherwise
      // we will have some replicas (alphabetically largest) which never get any traffic.
      if (instanceList == null || instanceList.size() == 0) {
        for (ByteBuffer key : entry.getValue().getKey()) {
          offlinePartitionResponse.getResponseMap().put(key,
              new TerrapinSingleResponse().setErrorCode(TerrapinGetErrorCode.PARTITION_OFFLINE));
        }
        continue;
      }
      int index = entry.getKey().hashCode() % instanceList.size();
      int oldIndex = index;
      while (blacklistedHosts.contains(instanceList.get(index))) {
        if ((index + 1) % instanceList.size() == oldIndex) {
          break;
        } else {
          index = (index + 1) % instanceList.size();
        }
      }
      String hostName = instanceList.get(index);
      TerrapinInternalGetRequest request = hostRequestMap.get(hostName);
      if (request != null) {
        request.getKeyList().add(entry.getValue());
      } else {
        request = new TerrapinInternalGetRequest().setKeyList(Lists.newArrayList(
            entry.getValue()));
        hostRequestMap.put(hostName, request);
      }
    }
    // Grab the clients for each host and issue requests.
    for (final Map.Entry<String, TerrapinInternalGetRequest> entry : hostRequestMap.entrySet()) {
      final String hostName = entry.getKey();
      String latencyStatsPrefix = statsPrefix + fileSet + "-host-" + rpcPassNum + "-latency";
      hostResponseMap.put(hostName, Stats.timeFutureMillis(latencyStatsPrefix,
        getClientFuture(entry.getKey()).flatMap(
          new Function<TerrapinServerInternal.ServiceIface,
                      Future<TerrapinResponse>>() {
            @Override
            public Future<TerrapinResponse> apply(TerrapinServerInternal.ServiceIface c) {
              return c.get(entry.getValue());
            }
          }).map(new Function<TerrapinResponse, TerrapinResponse>() {
            @Override
            public TerrapinResponse apply(TerrapinResponse response) {
              if (failedKeysContainer == null || failedHostsContainer == null) {
                return response;
              }
              Set<ByteBuffer> keysToRemove = Sets.newHashSet();
              for (Map.Entry<ByteBuffer, TerrapinSingleResponse> entry :
                   response.getResponseMap().entrySet()) {
                if (entry.getValue().isSetErrorCode()) {
                  TerrapinGetErrorCode errorCode = entry.getValue().getErrorCode();
                  if (errorCode == TerrapinGetErrorCode.OTHER ||
                      errorCode == TerrapinGetErrorCode.READ_ERROR ||
                      errorCode == TerrapinGetErrorCode.NOT_SERVING_PARTITION ||
                      errorCode == TerrapinGetErrorCode.NOT_SERVING_RESOURCE) {
                    // Do not add the keys to the response since we will follow up
                    // with a retry on the failed keys.
                    keysToRemove.add(entry.getKey());
                    failedKeysContainer.put(entry.getKey(), entry.getValue());
                    // This host did have a partial failure.
                    failedHostsContainer.add(hostName);
                  }
                }
              }
              for (ByteBuffer key : keysToRemove) {
                response.getResponseMap().remove(key);
              }
              return response;
            }
          }).rescue(new Function<Throwable, Future<TerrapinResponse>>() {
            @Override
            public Future<TerrapinResponse> apply(Throwable t) {
              // Track the number of failing rpc(s) for each fileset.
              Stats.incr(statsPrefix + fileSet + "-host-" + rpcPassNum + "-failures");
              Stats.incr(statsPrefix + hostName + "-" + rpcPassNum + "-failures");
              TerrapinGetErrorCode errorCode = TerrapinGetErrorCode.READ_ERROR;
              if (t instanceof TerrapinGetException) {
                errorCode = ((TerrapinGetException) t).getErrorCode();
              }
              TerrapinResponse response = new TerrapinResponse();
              response.setResponseMap((Map) Maps.newHashMap());
              if (failedHostsContainer == null || failedKeysContainer == null) {
                // Populate the error code for the keys in the request.
                for (MultiKey multiKey : entry.getValue().getKeyList()) {
                  for (ByteBuffer key : multiKey.getKey()) {
                    response.getResponseMap().put(key,
                        new TerrapinSingleResponse().setErrorCode(errorCode));
                  }
                }
                return Future.value(response);
              } else {
                failedHostsContainer.add(hostName);
                for (MultiKey multiKey : entry.getValue().getKeyList()) {
                  for (ByteBuffer key : multiKey.getKey()) {
                    failedKeysContainer.put(key, new TerrapinSingleResponse().setErrorCode(
                            errorCode));
                  }
                }
                // Return an empty response since the keys failed and we will
                // do second retry.
                return Future.value(response);
              }
            }
        })));
    }
    hostResponseMap.put("", Future.value(offlinePartitionResponse));
    return hostResponseMap;
  }
}
