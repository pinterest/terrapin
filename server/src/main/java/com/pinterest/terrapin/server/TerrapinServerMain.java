package com.pinterest.terrapin.server;

import com.pinterest.terrapin.Constants;
import com.pinterest.terrapin.TerrapinUtil;
import com.pinterest.terrapin.base.OstrichAdminService;
import com.pinterest.terrapin.thrift.generated.TerrapinServerInternal;
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
import java.util.concurrent.TimeUnit;

/**
 * Main java class for firing up the Terrapin server.
 */
public class TerrapinServerMain {
  private static final Logger LOG = LoggerFactory.getLogger(TerrapinServerMain.class);

  public static void main(String[] args) {
    PropertiesConfiguration configuration = TerrapinUtil.readPropertiesExitOnFailure(
        System.getProperties().getProperty("terrapin.config", "server.properties"));

    try {
      final TerrapinServerHandler handler = new TerrapinServerHandler(configuration);
      handler.start();
      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          handler.shutdown();
        }
      });
    } catch (Throwable t) {
      LOG.error("Could not start up server.", t);
      System.exit(1);
    }
  }
}