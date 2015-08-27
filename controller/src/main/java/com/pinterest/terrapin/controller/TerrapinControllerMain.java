package com.pinterest.terrapin.controller;

import com.pinterest.terrapin.TerrapinUtil;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main java class for firing up the Terrapin controller.
 */
public class TerrapinControllerMain {
  private static final Logger LOG = LoggerFactory.getLogger(TerrapinControllerMain.class);

  public static void main(String[] args) {
    PropertiesConfiguration configuration = TerrapinUtil.readPropertiesExitOnFailure(
        System.getProperties().getProperty("terrapin.config", "controller.properties"));
    try {
      LOG.info("Starting controller.");
      final TerrapinControllerHandler handler = new TerrapinControllerHandler(configuration);
      handler.start();

      Runtime.getRuntime().addShutdownHook(new Thread() {
        @Override
        public void run() {
          handler.shutdown();
        }
      });
    } catch (Throwable t) {
      LOG.error("Could not start the controller.", t);
    }
  }
}
