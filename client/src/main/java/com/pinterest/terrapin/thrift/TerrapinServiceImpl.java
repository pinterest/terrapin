package com.pinterest.terrapin.thrift;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.pinterest.terrapin.Constants;
import com.pinterest.terrapin.TerrapinUtil;
import com.pinterest.terrapin.client.FileSetViewManager;
import com.pinterest.terrapin.client.ReplicatedTerrapinClient;
import com.pinterest.terrapin.client.TerrapinClient;
import com.pinterest.terrapin.thrift.generated.*;
import com.pinterest.terrapin.zookeeper.ZooKeeperManager;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.ostrich.stats.Stats;
import com.twitter.util.ExceptionalFunction;
import com.twitter.util.Function0;
import com.twitter.util.Function;
import com.twitter.util.Future;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.runtime.BoxedUnit;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Implements a thrift server on top of the terrapin client.
 */
public class TerrapinServiceImpl implements TerrapinService.ServiceIface {
  private static final Logger LOG = LoggerFactory.getLogger(TerrapinServiceImpl.class);

  // Since each TerrapinClient is per cluster, the thrift server will instantiate
  // TerrapinClients at startup. The list will remain static throughout the life
  // time of the thrift server.
  private final Map<String, TerrapinClient> clusterClientMap;

  /**
   * @param configuration Config settings containing settings such
   * @throws Exception
   */
  public TerrapinServiceImpl(PropertiesConfiguration configuration,
                             List<String> clusterList) throws Exception {
    String zkQuorum = TerrapinUtil.getZKQuorumFromConf(configuration);
    Preconditions.checkNotNull(zkQuorum, "Zookeeper quorum should not be empty/null.");
    ZooKeeperClient zkClient = TerrapinUtil.getZooKeeperClient(zkQuorum, 30);
    this.clusterClientMap = Maps.newHashMapWithExpectedSize(clusterList.size());
    for (String clusterName : clusterList) {
      try {
        FileSetViewManager fsViewManager = new FileSetViewManager(zkClient, clusterName);
        LOG.info("Connecting to cluster " + clusterName + " on " + zkQuorum);
        TerrapinClient terrapinClient = new TerrapinClient(
            fsViewManager,
            clusterName,
            configuration.getInt(Constants.TERRAPIN_SERVER_TARGET_PORT, 9090),
            configuration.getInt(Constants.CLIENT_CONNECT_TIMEOUT_MILLIS, 300),
            configuration.getInt(Constants.CLIENT_RPC_TIMEOUT_MILLIS, 500));
        clusterClientMap.put(clusterName, terrapinClient);
        LOG.info("Done.");
      } catch (Exception e) {
        LOG.warn("Could not connect to cluster " + clusterName, e);
        throw e;
      }
    }
  }

  TerrapinServiceImpl(Map<String, TerrapinClient> clusterClientMap) {
    this.clusterClientMap = clusterClientMap;
  }

  private <T> Future<T> getExceptionFuture(Throwable t) {
    if (t instanceof TerrapinGetException) {
      return Future.exception(t);
    }
    return Future.exception(new TerrapinGetException("Failed due to " + t.toString(),
        TerrapinGetErrorCode.OTHER));
  }

  
  private ReplicatedTerrapinClient getReplicatedTerrapinClient(List<String> clusterList) {
    String firstCluster = clusterList.get(0);
    String secondCluster = null;
    if (clusterList.size() > 1) {
      secondCluster = clusterList.get(1);
    }
    TerrapinClient firstClient = clusterClientMap.get(firstCluster);
    TerrapinClient secondClient = null;
    if (secondCluster != null) {
      secondClient = clusterClientMap.get(secondCluster);
    }
    if(firstClient == null && secondClient == null) {
      return null;  
    }
    return new ReplicatedTerrapinClient(firstClient, secondClient);
  }
  
  
  @Override
  public Future<TerrapinSingleResponse> get(final TerrapinGetRequest request) {
    final long startTimeMillis = System.currentTimeMillis();
    if (request.getClusterList().isEmpty()) {
      return Future.exception(new TerrapinGetException("Cluster list is empty", TerrapinGetErrorCode.INVALID_REQUEST));
    }
    ReplicatedTerrapinClient terrapinClient = getReplicatedTerrapinClient(request.getClusterList());
    if (terrapinClient == null) {
      return Future.exception(new TerrapinGetException(
          "Clusters [" + Joiner.on(", ").join(request.getClusterList()) + "] not found.",
          TerrapinGetErrorCode.CLUSTER_NOT_FOUND));
    }
    RequestOptions options;
    if (request.isSetOptions()) {
      options = request.getOptions();
    } else {
      options = new RequestOptions();
    }
    try {
      return terrapinClient.getMany(request.getFileSet(),
          Sets.newHashSet(ByteBuffer.wrap(request.getKey())), options).map(
              new ExceptionalFunction<TerrapinResponse, TerrapinSingleResponse>() {
                @Override
                public TerrapinSingleResponse applyE(TerrapinResponse response)
                    throws TerrapinGetException {
                  ByteBuffer keyBuf = ByteBuffer.wrap(request.getKey());
                  if (response.getResponseMap().containsKey(keyBuf)) {
                    TerrapinSingleResponse returnResponse = response.getResponseMap().get(keyBuf);
                    if (returnResponse.isSetErrorCode()) {
                      throw new TerrapinGetException("Read failed.", returnResponse.getErrorCode());
                    } else {
                      Stats.addMetric(request.getFileSet() + "-value-size", returnResponse.getValue().length);
                      Stats.addMetric("value-size", returnResponse.getValue().length);
                      return returnResponse;
                    }
                  } else {
                    return new TerrapinSingleResponse();
                  }
                }
              }).rescue(new Function<Throwable, Future<TerrapinSingleResponse>>() {
                @Override
                public Future<TerrapinSingleResponse> apply(Throwable t) {
                  return getExceptionFuture(t);
                }
              }).ensure(new Function0<BoxedUnit>() {
                @Override
                public BoxedUnit apply() {
                  int timeMillis = (int)(System.currentTimeMillis() - startTimeMillis);
                  Stats.addMetric(request.getFileSet() + "-lookup-latency-ms", timeMillis);
                  Stats.addMetric("lookup-latency-ms", timeMillis);
                  return BoxedUnit.UNIT;
                }
            });
    } catch (Exception e) {
      return getExceptionFuture(e);
    }
  }

  @Override
  public Future<TerrapinResponse> multiGet(final TerrapinMultiGetRequest request) {
    final long startTimeMillis = System.currentTimeMillis();
    if (request.getClusterList().isEmpty()) {
      return Future.exception(new TerrapinGetException("Cluster list is empty", TerrapinGetErrorCode.INVALID_REQUEST));
    }
    ReplicatedTerrapinClient terrapinClient = getReplicatedTerrapinClient(request.getClusterList());
    if (terrapinClient == null) {
      return Future.exception(new TerrapinGetException(
          "Clusters [" + Joiner.on(", ").join(request.getClusterList()) + "] not found.",
          TerrapinGetErrorCode.CLUSTER_NOT_FOUND));
    }
    RequestOptions options;
    if (request.isSetOptions()) {
      options = request.getOptions();
    } else {
      options = new RequestOptions();
    }
    try {
      return terrapinClient.getMany(request.getFileSet(), Sets.newHashSet(request.getKeyList()), options)
          .onSuccess(new Function<TerrapinResponse, BoxedUnit>() {
            @Override
            public BoxedUnit apply(TerrapinResponse terrapinResponse) {
              int responseSize = 0;
              for (Map.Entry<ByteBuffer, TerrapinSingleResponse> response : terrapinResponse.getResponseMap().entrySet()) {
                if (!response.getValue().isSetErrorCode()) {
                  responseSize += response.getValue().getValue().length;
                }
              }
              Stats.addMetric(request.getFileSet() + "-multi-value-size", responseSize);
              Stats.addMetric("multi-value-size", responseSize);
              return BoxedUnit.UNIT;
            }
          })
          .rescue(new Function<Throwable, Future<TerrapinResponse>>() {
            @Override
            public Future<TerrapinResponse> apply(Throwable t) {
              return getExceptionFuture(t);
            }
          })
          .ensure(new Function0<BoxedUnit>() {
            @Override
            public BoxedUnit apply() {
              int timeMillis = (int) (System.currentTimeMillis() - startTimeMillis);
              Stats.addMetric(request.getFileSet() + "-multi-lookup-latency-ms", timeMillis);
              Stats.addMetric("multi-lookup-latency-ms", timeMillis);
              return BoxedUnit.UNIT;
            }
          });
    } catch (Exception e) {
      return getExceptionFuture(e);
    }
  }
}
