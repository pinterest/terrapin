package com.pinterest.terrapin.controller;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.pinterest.terrapin.Constants;
import com.pinterest.terrapin.zookeeper.ViewInfo;
import com.pinterest.terrapin.zookeeper.ZooKeeperManager;
import com.twitter.ostrich.stats.Stats;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.ExternalView;
import org.apache.helix.spectator.RoutingTableProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This class adds a batching abstraction on top of the RoutingTable provider to batch updates
 * to the client view by time. It writes out external views every 15 seconds.
 */
public class TerrapinRoutingTableProvider extends RoutingTableProvider {
  private static final Logger LOG = LoggerFactory.getLogger(TerrapinRoutingTableProvider.class);

  class ViewInfoRecord {
    public ViewInfo viewInfo;
    public boolean drained;
  }

  private final ZooKeeperManager zkManager;
  private Map<String, ViewInfoRecord> viewInfoRecordMap;
  private volatile boolean isRunning;
  private Thread thread;

  class ViewSyncRunnable implements Runnable {
    @Override
    public void run() {
      isRunning = true;
      while (isRunning) {
        try {
          List<ViewInfo> viewInfoList = Lists.newArrayList();
          synchronized (viewInfoRecordMap) {
            for (Map.Entry<String, ViewInfoRecord> entry : viewInfoRecordMap.entrySet()) {
              if (!entry.getValue().drained) {
                viewInfoList.add(entry.getValue().viewInfo);
                entry.getValue().drained = true;
              }
            }
          }
          for (ViewInfo viewInfo : viewInfoList) {
            try {
              zkManager.setViewInfo(viewInfo);
            } catch (Exception e) {
              LOG.error("Exception while syncing " + viewInfo.getResource(), e);
              Stats.incr("compressed-view-sync-errors");
            }
          }
          try {
            Thread.sleep(Constants.VIEW_INFO_REFRESH_INTERVAL_SECONDS_DEFAULT * 1000);
          } catch (InterruptedException e) {
            LOG.info("Interrupted.");
          }
        } catch (Throwable t) {
          LOG.info("Got exception in view syncer thread", t);
        }
      }
    }
  }

  public TerrapinRoutingTableProvider(ZooKeeperManager zkManager,
                                      List<String> resourceList) {
    this.zkManager = zkManager;
    this.viewInfoRecordMap = Maps.newHashMapWithExpectedSize(resourceList.size());
    // Initialize the view info with what we have in zookeeper to avoid double writes.
    for (String resource : resourceList) {
      ViewInfo viewInfo = this.zkManager.getViewInfo(resource);
      if (viewInfo != null) {
        ViewInfoRecord viewInfoRecord = new ViewInfoRecord();
        viewInfoRecord.drained = true;
        viewInfoRecord.viewInfo = viewInfo;
        this.viewInfoRecordMap.put(resource, viewInfoRecord);
      } else {
        LOG.error("Compressed view is null on startup for " + resource);
        Stats.incr("compressed-view-init-errors");
      }
    }
    this.thread = new Thread(new ViewSyncRunnable());
    this.thread.setName("sync-external-view-thread");
    this.thread.start();
  }


  @Override
  public void onExternalViewChange(List<ExternalView> externalViewList,
                                   NotificationContext context) {
    super.onExternalViewChange(externalViewList, context);

    Set<String> resourceSet = Sets.newHashSetWithExpectedSize(externalViewList.size());
    synchronized (viewInfoRecordMap) {
      for (ExternalView externalView : externalViewList) {
        if (externalView == null) {
          continue;
        }
        String resource = externalView.getResourceName();
        resourceSet.add(resource);
        ViewInfoRecord record = viewInfoRecordMap.get(resource);
        ViewInfoRecord newRecord = new ViewInfoRecord();
        newRecord.viewInfo = new ViewInfo(externalView);

        if (record != null) {
          // Only put stuff to be drained if the new external view does not equal the
          // previous external view.
          if (newRecord.viewInfo.equals(record.viewInfo))
            continue;
        }
        newRecord.drained = false;

        viewInfoRecordMap.put(resource, newRecord);
      }
      // Finally remove any deleted external views from the map.
      Set<String> resourcesToRemove = Sets.newHashSetWithExpectedSize(viewInfoRecordMap.size());
      for (String resource : viewInfoRecordMap.keySet()) {
        if (!resourceSet.contains(resource)) {
          resourcesToRemove.add(resource);
        }
      }
      for (String resource : resourcesToRemove) {
        this.viewInfoRecordMap.remove(resource);
      }
    }
  }

  public void shutdown() {
    this.isRunning = false;
    this.thread.interrupt();
  }
}
