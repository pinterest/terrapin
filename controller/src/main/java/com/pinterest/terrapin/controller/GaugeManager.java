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
package com.pinterest.terrapin.controller;


import com.pinterest.terrapin.Constants;
import com.pinterest.terrapin.zookeeper.FileSetInfo;
import com.pinterest.terrapin.zookeeper.ViewInfo;
import com.pinterest.terrapin.zookeeper.ZooKeeperManager;
import com.twitter.ostrich.stats.Stats;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Responsible for maintaining a separate thread which is used for updating gauges.
 */
public class GaugeManager {
  /**
   * This class takes care of gauge name formatting/parsing and gauge calculation
   */
  abstract static class FileSetGaugeCalculator {
    private static final String GAUGE_NAME_PREFIX = "terrapin-controller-fileset";
    private Pattern pattern;
    private String gaugeNameSuffix;

    public FileSetGaugeCalculator(String gaugeNameSuffix) {
      this.gaugeNameSuffix = gaugeNameSuffix;
      this.pattern = Pattern.compile(String.format("%s-(.*)-%s", 
          GAUGE_NAME_PREFIX, this.gaugeNameSuffix));
    }

    public String formatGaugeName(String fileSet) {
      return String.format("%s-%s-%s", GAUGE_NAME_PREFIX, fileSet, this.gaugeNameSuffix);
    }

    /**
     *
     * @param gaugeName the name of gauge to be parsed
     * @return the file set if the input gauge name is valid, otherwise null is returned
     */
    public String parseFileSet(String gaugeName) {
      Matcher matcher = pattern.matcher(gaugeName);
      if (matcher.matches()) {
        return matcher.group(1);
      } else {
        return null;
      }
    }

    public abstract double calcValue(String fileSet);
  }

  static class OnlinePercentageGaugeCalculator extends FileSetGaugeCalculator {
    private ZooKeeperManager zkManager;
    public OnlinePercentageGaugeCalculator(ZooKeeperManager zkManager, String gaugeNameSuffix) {
      super(gaugeNameSuffix);
      this.zkManager = zkManager;
    }

    /**
     * @param fileSet name of the fileset
     * @return the percentage of online partitions for the specific fileset
     */
    @Override
    public double calcValue(String fileSet) {
      FileSetInfo fileSetInfo = zkManager.getFileSetInfo(fileSet);
      if (fileSetInfo == null || fileSetInfo.servingInfo == null) {
        LOG.warn(String.format("FileSetInfo for %s is not available", fileSet));
        return 0;
      }
      ViewInfo viewInfo = zkManager.getViewInfo(fileSetInfo.servingInfo.helixResource);
      if (viewInfo == null) {
        LOG.warn(String.format("ViewInfo for %s is not available", 
            fileSetInfo.servingInfo.helixResource));
        return 0;
      }
      int totalPartitions = fileSetInfo.servingInfo.numPartitions;
      int onlinePartitions = viewInfo.getNumOnlinePartitions();
      if (totalPartitions == 0) {
        return 0.0;
      }
      return (double) onlinePartitions / totalPartitions;
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(GaugeManager.class);
  private FileSetGaugeCalculator[] fileSetGauges;
  
  private final ZooKeeperManager zkManager;
  private Thread gaugeManager;
  private int sleepSeconds;
  private volatile boolean isRunning;
  
  public GaugeManager(ZooKeeperManager zkManager, int sleepSeconds) {
    this.zkManager = zkManager;
    this.sleepSeconds = sleepSeconds;
    this.isRunning = false;
    this.fileSetGauges = new FileSetGaugeCalculator[] {
      new OnlinePercentageGaugeCalculator(this.zkManager, "online-pct")
    };
    this.gaugeManager = new Thread(new GaugeManagerRunnable());
    this.gaugeManager.setName("gauge-manager");
    this.gaugeManager.start();
  }

  
  class GaugeManagerRunnable implements Runnable {
    @Override
    public void run() {
      isRunning = true;
      while (isRunning) {
        try {
          // Update overall online percentage gauge
          Stats.setGauge("terrapin-controller-online-pct", 
              calcOverallOnlinePercentageGauge(zkManager));
          
          // Removing non-existing fileset gauges
          Set<String> fileSets = zkManager.getFileSetInfoMap().keySet();
          Iterator<String> existingGaugeIterator = Stats.getGauges().keySet().iterator();
          while (existingGaugeIterator.hasNext()) {
            String gaugeName = existingGaugeIterator.next();
            for (FileSetGaugeCalculator gauge : fileSetGauges) {
              String fileSet = gauge.parseFileSet(gaugeName);
              if (fileSet != null && !fileSets.contains(fileSet)) {
                Stats.clearGauge(gaugeName);
              }
            }
          }
          
          // Updating existing fileset gauges
          for(String fileSet : fileSets) {
            for (FileSetGaugeCalculator gauge : fileSetGauges) {
              String gaugeName = gauge.formatGaugeName(fileSet);
              Stats.setGauge(gaugeName, gauge.calcValue(fileSet));
            }
          }
        } catch (Throwable e) {
          LOG.warn("Gauge manager got exception", e);
        }
        try {
          Thread.sleep(sleepSeconds * 1000);
        } catch (InterruptedException e) {
          LOG.warn("Gauge manager is interrupted", e);
        }
      }
    }
  }
  
  public void shutdown() {
    this.isRunning = false;
    this.gaugeManager.interrupt();
  }

  /**
   * Calculate the overall percentage of online partitions
   * @param zkManager ZooKeeperManager object to obtain useful information
   * @return overall percentage
   */
  public static double calcOverallOnlinePercentageGauge(ZooKeeperManager zkManager) {
    Set<String> fileSets = zkManager.getFileSetInfoMap().keySet();
    int totalPartitions = 0, onlinePartitions = 0;
    for (String fileSet : fileSets) {
      FileSetInfo fileSetInfo = zkManager.getFileSetInfo(fileSet);
      if (fileSetInfo == null || fileSetInfo.servingInfo == null) {
        LOG.warn(String.format("FileSetInfo for %s is not available", fileSet));
        continue;
      }
      ViewInfo viewInfo = zkManager.getViewInfo(fileSetInfo.servingInfo.helixResource);
      if (viewInfo == null) {
        LOG.warn(String.format("ViewInfo for %s is not available",
            fileSetInfo.servingInfo.helixResource));
        continue;
      }
      totalPartitions += fileSetInfo.servingInfo.numPartitions;
      onlinePartitions += viewInfo.getNumOnlinePartitions();
    }
    if (totalPartitions == 0) {
      return 0;
    }
    return (double)onlinePartitions / totalPartitions;
  }

}
