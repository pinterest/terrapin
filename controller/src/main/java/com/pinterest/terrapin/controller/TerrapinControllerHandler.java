package com.pinterest.terrapin.controller;

import com.google.common.collect.ImmutableSet;
import com.pinterest.terrapin.Constants;
import com.pinterest.terrapin.TerrapinUtil;
import com.pinterest.terrapin.base.OstrichAdminService;
import com.pinterest.terrapin.client.FileSetViewManager;
import com.pinterest.terrapin.client.TerrapinClient;
import com.pinterest.terrapin.thrift.generated.TerrapinController;
import com.pinterest.terrapin.zookeeper.ClusterInfo;
import com.pinterest.terrapin.zookeeper.ViewInfo;
import com.pinterest.terrapin.zookeeper.ZooKeeperManager;

import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.finagle.builder.Server;
import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.stats.OstrichStatsReceiver;
import com.twitter.finagle.thrift.ThriftServerFramedCodec;
import com.twitter.ostrich.stats.Stats;
import com.twitter.util.Duration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.manager.zk.ZkClient;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.HelixConfigScope;
import org.apache.helix.tools.ClusterSetup;
import org.apache.helix.util.ZKClientPool;
import org.apache.helix.webapp.HelixAdminWebApp;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Starts the terrapin thrift server.
 */
public class TerrapinControllerHandler {
  private static final Logger LOG = LoggerFactory.getLogger(TerrapinControllerHandler.class);

  private PropertiesConfiguration configuration;
  private HelixAdmin helixAdmin;
  private HelixManager helixManager;
  private HelixManager spectatorManager;
  private String clusterName;
  private DFSClient hdfsClient;
  private ZooKeeperManager zkManager;
  private HdfsManager hdfsManager;
  private Server server;
  private TerrapinRoutingTableProvider routingTableProvider;
  private GaugeManager gaugeManager;
  private HelixAdminWebApp webApp;
  private StatusServer statusServer;
  // for querying a fileset from admin UI
  private TerrapinClient sampleClient;

  public TerrapinControllerHandler(PropertiesConfiguration configuration) {
    this.configuration = configuration;
  }

  private void startThriftServer(int thriftPort) throws UnknownHostException {
    TerrapinController.ServiceIface serviceImpl = new TerrapinControllerServiceImpl(
        this.configuration,
        this.zkManager,
        this.hdfsClient,
        this.helixAdmin,
        this.clusterName);
    TerrapinController.Service service =
        new TerrapinController.Service(serviceImpl, new TBinaryProtocol.Factory());

    this.server = ServerBuilder.safeBuild(
        service,
        ServerBuilder.get()
        .name("TerrapinController")
        .codec(ThriftServerFramedCodec.get())
        .hostConnectionMaxIdleTime(Duration.fromTimeUnit(
            configuration.getInt(Constants.THRIFT_CONN_MAX_IDLE_TIME, 1), TimeUnit.MINUTES))
        .maxConcurrentRequests(configuration.getInt(Constants.THRIFT_MAX_CONCURRENT_REQUESTS,
            100))
        .reportTo(new OstrichStatsReceiver(Stats.get("")))
        .bindTo(new InetSocketAddress(thriftPort)));
    new OstrichAdminService(configuration.getInt(Constants.OSTRICH_METRICS_PORT, 9999)).start();
  }

  private void reconcileViews(List<String> resourceList) throws Exception {
    for (String resource : resourceList) {
      LOG.info("Reconciling resource " + resource);
      ExternalView externalView = helixAdmin.getResourceExternalView(this.clusterName,
          resource);
      if (externalView == null) {
        LOG.warn("External view null for " + resource + " - Skipping");
        continue;
      }
      ViewInfo viewInfo = this.zkManager.getViewInfo(resource);
      if (viewInfo == null) {
        LOG.info("Compressed view does not exist for " + resource);
        this.zkManager.setViewInfo(new ViewInfo(externalView));
      } else {
        ViewInfo currentViewInfo = new ViewInfo(externalView);
        if (!currentViewInfo.equals(viewInfo)) {
          LOG.info("Mismatch b/w external view & compressed view for " + resource);
          this.zkManager.setViewInfo(currentViewInfo);
        } else {
          LOG.info("External view & compressed view match for " + resource);
        }
      }
    }
  }

  private void setUpHelixCluster(String zookeeperQuorum, String clusterName) {
    ZkClient zkClient = ZKClientPool.getZkClient(zookeeperQuorum);
    HelixAdmin helixAdmin = new ZKHelixAdmin(zkClient);
    try {
      if(!ImmutableSet.copyOf(helixAdmin.getClusters()).contains(clusterName)) {
        ClusterSetup helixClusterSetUp = new ClusterSetup(zkClient);
        helixClusterSetUp.addCluster(clusterName, false);
        helixClusterSetUp.setConfig(HelixConfigScope.ConfigScopeProperty.CLUSTER, clusterName,
            "allowParticipantAutoJoin=true");
      }
    } finally {
      zkClient.close();
    }
  }

  public void start() throws Exception {
    String zookeeperQuorum = TerrapinUtil.getZKQuorumFromConf(configuration);
    // Ensure that the cluster exists and connect through helix.
    this.helixAdmin = new ZKHelixAdmin(zookeeperQuorum);
    this.clusterName = configuration.getString(Constants.HELIX_CLUSTER,
        Constants.HELIX_CLUSTER_NAME_DEFAULT);
    setUpHelixCluster(zookeeperQuorum, this.clusterName);

    String namenode = configuration.getString(Constants.HDFS_NAMENODE);
    int hdfsReplicationFactor = configuration.getInt(Constants.HDFS_REPLICATION,
        Constants.DEFAULT_HDFS_REPLICATION);

    // Start the zookeeper manager and watch filesets/compressed views.
    ZooKeeperClient zkClient = TerrapinUtil.getZooKeeperClient(zookeeperQuorum, 30);
    this.zkManager = new ZooKeeperManager(zkClient, this.clusterName);
    this.zkManager.createClusterPaths();
    this.zkManager.registerWatchAllFileSets();
    ClusterInfo clusterInfo = new ClusterInfo(namenode, hdfsReplicationFactor);
    // Set the HDFS replication factor and namenode address.
    this.zkManager.setClusterInfo(clusterInfo);

    // Initialize a sample client instance
    this.sampleClient = new TerrapinClient(new FileSetViewManager(zkClient, clusterName),
        clusterName, configuration.getInt(Constants.THRIFT_PORT, Constants.DEFAULT_THRIFT_PORT),
        1000, 1000);

    List<String> resourceList = this.helixAdmin.getResourcesInCluster(this.clusterName);
    reconcileViews(resourceList);

    // Instantiate HDFS client.
    LOG.info("Connecting to HDFS: " + namenode);
    Configuration conf = new Configuration();
    conf.set("fs.default.name", namenode);
    this.hdfsClient = new DFSClient(conf);

    LOG.info("Starting Helix against " + zookeeperQuorum);
    int thriftPort = configuration.getInt(Constants.THRIFT_PORT, Constants.DEFAULT_THRIFT_PORT);
    String instanceName = InetAddress.getLocalHost().getHostName() + "_" + thriftPort;

    // Connect as spectator.
    this.spectatorManager = HelixManagerFactory.getZKHelixManager(this.clusterName,
        instanceName,
        InstanceType.SPECTATOR,
        zookeeperQuorum);
    this.spectatorManager.connect();
    this.routingTableProvider = new TerrapinRoutingTableProvider(zkManager, resourceList);
    this.gaugeManager = new GaugeManager(zkManager, 
        Constants.GAUGE_MANAGER_EXEC_INTERVAL_SECONDS_DEFAULT);
    this.spectatorManager.addExternalViewChangeListener(this.routingTableProvider);

    this.hdfsManager = new HdfsManager(configuration,
        zkManager,
        this.clusterName,
        helixAdmin,
        this.hdfsClient,
        this.routingTableProvider);

    // Connect as controller.
    this.helixManager = HelixManagerFactory.getZKHelixManager(this.clusterName,
        InetAddress.getLocalHost().getHostName() + "_" + thriftPort,
        InstanceType.CONTROLLER,
        zookeeperQuorum);
    this.helixManager.connect();
    this.helixManager.addControllerListener(this.hdfsManager);

    LOG.info("Starting thrift server on " + thriftPort);
    // Start up the thrift server.
    startThriftServer(thriftPort);

    // Start the Helix Web App.
    this.webApp = new HelixAdminWebApp(zookeeperQuorum,
        configuration.getInt(Constants.HELIX_WEBAPP_PORT, 60000));
    this.webApp.start();

    // Start the status server to serve servlets
    this.statusServer = new StatusServer(
        "status",
        configuration.getString(Constants.STATUS_SERVER_BINDING_ADDRESS, "0.0.0.0"),
        configuration.getInt(Constants.STATUS_SERVER_BINDING_PORT, 50030),
        false, this.clusterName, this.zkManager, this.hdfsClient, this.sampleClient
    );
    this.statusServer.start();
  }

  public void shutdown() {
    this.server.close(Duration.fromSeconds(60));
    webApp.stop();
    this.helixAdmin.close();
    this.helixManager.disconnect();
    this.routingTableProvider.shutdown();
    // Sleep a little to make sure that the zk node truly disappears.
    try {
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      LOG.error("Interrupted in shutdown. This should not happen.");
    }
    this.hdfsManager.shutdown();
    this.gaugeManager.shutdown();
    try {
      this.statusServer.stop();
    } catch (Exception e) {
      e.printStackTrace();
      LOG.warn("failed to stop status server", e);
    }
  }
}