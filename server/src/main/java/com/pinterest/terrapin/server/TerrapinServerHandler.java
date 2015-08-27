package com.pinterest.terrapin.server;

import com.pinterest.terrapin.Constants;
import com.pinterest.terrapin.TerrapinUtil;
import com.pinterest.terrapin.base.OstrichAdminService;
import com.pinterest.terrapin.storage.ReaderFactory;
import com.pinterest.terrapin.thrift.generated.TerrapinServerInternal;
import com.twitter.finagle.builder.Server;
import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.stats.OstrichStatsReceiver;
import com.twitter.finagle.thrift.ThriftServerFramedCodec;
import com.twitter.ostrich.stats.Stats;
import com.twitter.util.Duration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.participant.StateMachineEngine;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

/**
 * This class is responsible for connecting to Helix and starting the thrift server.
 */
public class TerrapinServerHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TerrapinServerHandler.class);

  private final PropertiesConfiguration configuration;
  private HelixManager helixManager;
  private StateModelFactory stateModelFactory;
  private Server server;

  // A mapping from resource name + partition name to the corresponding Reader.
  private final ResourcePartitionMap resourcePartitionMap;

  public TerrapinServerHandler(PropertiesConfiguration configuration) {
    this.configuration = configuration;
    this.resourcePartitionMap = new ResourcePartitionMap();
  }

  private void startThriftServer(int thriftPort) {
    TerrapinServerInternal.ServiceIface serviceImpl = new TerrapinServerInternalImpl(configuration,
        resourcePartitionMap);
    TerrapinServerInternal.Service service =
        new TerrapinServerInternal.Service(serviceImpl, new TBinaryProtocol.Factory());

    this.server = ServerBuilder.safeBuild(
        service,
        ServerBuilder.get()
            .name("TerrapinServer")
            .codec(ThriftServerFramedCodec.get())
            .hostConnectionMaxIdleTime(Duration.fromTimeUnit(
                    configuration.getInt(Constants.THRIFT_CONN_MAX_IDLE_TIME, 1), TimeUnit.MINUTES))
            .maxConcurrentRequests(configuration.getInt(Constants.THRIFT_MAX_CONCURRENT_REQUESTS,
                    100))
            .reportTo(new OstrichStatsReceiver(Stats.get("")))
            .bindTo(new InetSocketAddress(thriftPort)));
      new OstrichAdminService(configuration.getInt(Constants.OSTRICH_METRICS_PORT, 9999)).start();  
  }

  public void start() throws Exception {
    String zookeeperQuorum = TerrapinUtil.getZKQuorumFromConf(configuration);
    int thriftPort = configuration.getInt(Constants.THRIFT_PORT, 9090);

    // Connect to Helix.
    this.helixManager = HelixManagerFactory.getZKHelixManager(
        configuration.getString(Constants.HELIX_CLUSTER, Constants.HELIX_CLUSTER_NAME_DEFAULT),
        TerrapinUtil.getHelixInstanceFromHDFSHost(InetAddress.getLocalHost().getHostName()),
        InstanceType.PARTICIPANT,
        zookeeperQuorum);
    StateMachineEngine stateMach = this.helixManager.getStateMachineEngine();

    // Create state model factory for HDFS.
    Configuration conf = new Configuration();
    conf.set("fs.default.name", configuration.getString(Constants.HDFS_NAMENODE));
    // Setup HDFS short circuit parameters.
    conf.setBoolean("dfs.client.read.shortcircuit", true);
    conf.setInt("dfs.client.read.shortcircuit.streams.cache.size", 5000);
    conf.setInt("dfs.client.read.shortcircuit.buffer.size", 131072);
    conf.set("dfs.domain.socket.path", "/var/run/hadoop-hdfs/dn._PORT");

    FileSystem fs = FileSystem.get(conf);
    this.stateModelFactory = new OnlineOfflineStateModelFactory(
        this.configuration,
        resourcePartitionMap,
        new ReaderFactory(configuration, new HFileSystem(fs)));
    stateMach.registerStateModelFactory("OnlineOffline", this.stateModelFactory);
    this.helixManager.connect();

    // Start up the thrift server for serving.
    startThriftServer(thriftPort);
  }

  public void shutdown() {
    this.server.close(Duration.fromSeconds(10));
    this.helixManager.disconnect();
    // Sleep a little to make sure that the zk node truly disappears.
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      LOG.error("Interrupted in shutdown. This should not happen.");
    }
    this.resourcePartitionMap.close();
  }
}
