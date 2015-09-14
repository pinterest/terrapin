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
package com.pinterest.terrapin.server;

import com.pinterest.terrapin.TerrapinUtil;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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