package com.pinterest.terrapin.thrift;

import com.pinterest.terrapin.Constants;
import com.pinterest.terrapin.TerrapinUtil;
import com.pinterest.terrapin.base.OstrichAdminService;
import com.pinterest.terrapin.thrift.generated.TerrapinService;
import com.twitter.finagle.Service;
import com.twitter.finagle.builder.Server;
import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.stats.OstrichStatsReceiver;
import com.twitter.finagle.thrift.ThriftServerFramedCodec;
import com.twitter.ostrich.stats.Stats;
import com.twitter.util.Duration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Java class for starting a terrapin thrift server. See sample.thrift.properties
 * for an example terrapin.config file.
 */
public class TerrapinThriftMain {
  private static final Logger LOG = LoggerFactory.getLogger(TerrapinThriftMain.class);

  public static void main(String[] args) throws Exception {
    final PropertiesConfiguration config = TerrapinUtil.readPropertiesExitOnFailure(
        System.getProperties().getProperty("terrapin.config", "thrift.properties"));

    OstrichStatsReceiver statsReceiver = new OstrichStatsReceiver(Stats.get(""));
    int listenPort = config.getInt("thrift_port", 9090);
    TerrapinServiceImpl serviceImpl = new TerrapinServiceImpl(config,
        (List) config.getList("cluster_list"));
    Service<byte[], byte[]> service = new TerrapinService.Service(serviceImpl,
        new TBinaryProtocol.Factory());
    Server server = ServerBuilder.safeBuild(
        service,
        ServerBuilder.get()
            .name("TERRAPIN_THRIFT")
            .codec(ThriftServerFramedCodec.get())
            .hostConnectionMaxIdleTime(Duration.apply(1, TimeUnit.MINUTES))
            .maxConcurrentRequests(3000)
            .reportTo(statsReceiver)
            .bindTo(new InetSocketAddress(listenPort)));
    new OstrichAdminService(config.getInt(Constants.OSTRICH_METRICS_PORT, 9999)).start();
    LOG.info("\n#######################################"
            + "\n#      Ready To Serve Requests.       #"
            + "\n#######################################");
  }
}
