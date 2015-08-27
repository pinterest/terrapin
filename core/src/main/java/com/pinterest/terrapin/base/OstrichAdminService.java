package com.pinterest.terrapin.base;

import com.google.common.collect.Maps;
import com.twitter.ostrich.admin.*;
import com.twitter.util.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.Map$;
import scala.collection.immutable.List;
import scala.collection.immutable.List$;
import scala.util.matching.Regex;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class OstrichAdminService {
  private static final Logger LOG = LoggerFactory.getLogger(OstrichAdminService.class);
  private final int mPort;
  private final Map<String, CustomHttpHandler> mCustomHttpHandlerMap = Maps.newHashMap();

  public OstrichAdminService(int port) {
    this.mPort = port;
  }

  public void addHandler(String path, CustomHttpHandler handler) {
    this.mCustomHttpHandlerMap.put(path, handler);
  }

  public void start() {
    Duration[] defaultLatchIntervals = {Duration.apply(1, TimeUnit.MINUTES)};
    @SuppressWarnings("deprecation")
    AdminServiceFactory adminServiceFactory = new AdminServiceFactory(
        this.mPort,
        20,
        List$.MODULE$.<StatsFactory>empty(),
        Option.<String>empty(),
        List$.MODULE$.<Regex>empty(),
        Map$.MODULE$.<String, CustomHttpHandler>empty(),
        List.<Duration>fromArray(defaultLatchIntervals));
    RuntimeEnvironment runtimeEnvironment = new RuntimeEnvironment(this);
    AdminHttpService service = adminServiceFactory.apply(runtimeEnvironment);
    for (Map.Entry<String, CustomHttpHandler> entry: this.mCustomHttpHandlerMap.entrySet()) {
      service.httpServer().createContext(entry.getKey(), entry.getValue());
    }
  }
}
