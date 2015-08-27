package com.pinterest.terrapin.server;

import com.pinterest.terrapin.Constants;
import com.pinterest.terrapin.TerrapinUtil;
import com.pinterest.terrapin.storage.Reader;
import com.pinterest.terrapin.storage.ReaderFactory;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.helix.NotificationContext;
import org.apache.helix.model.Message;
import org.apache.helix.participant.statemachine.StateModel;
import org.apache.helix.participant.statemachine.StateModelFactory;
import org.apache.helix.participant.statemachine.Transition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Implements a state model factory with only two states ONLINE and OFFLINE.
 */
public class OnlineOfflineStateModelFactory extends StateModelFactory<StateModel> {
  private static final Logger LOG = LoggerFactory.getLogger(OnlineOfflineStateModelFactory.class);

  private final Configuration conf;
  private final boolean isBlockCacheEnabled;
  private final ResourcePartitionMap resourcePartitionMap;
  private final ReaderFactory readerFactory;

  public OnlineOfflineStateModelFactory(PropertiesConfiguration configuration,
                                        ResourcePartitionMap resourcePartitionMap,
                                        ReaderFactory readerFactory) {
    this.conf = new Configuration();
    float cacheHeapPct = configuration.getFloat(Constants.HFILE_BLOCK_CACHE_HEAP_PCT, 0.7f);
    if (cacheHeapPct > 0.0f) {
      conf.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, cacheHeapPct);
      this.isBlockCacheEnabled = true;
    } else {
      this.isBlockCacheEnabled = false;
    }
    this.resourcePartitionMap = resourcePartitionMap;
    this.readerFactory = readerFactory;
  }

  @Override
  public StateModel createNewStateModel(String resource, String stateModelUnit) {
    OnlineOfflineStateModel stateModel = new OnlineOfflineStateModel();
    return stateModel;
  }

  static Pair<String, String> getHdfsPathAndPartitionNum(Message message) {
    // Since the partition names in helix are of the form resource$120 etc., we need to
    // strip out the resource from partition name.
    String partitionNum = message.getPartitionName().substring(
        message.getResourceName().length() + 1);
    String partitionName = "part-" + String.format("%05d",
        Integer.parseInt(partitionNum));
    String hdfsPath = TerrapinUtil.helixResourceToHdfsDir(message.getResourceName()) +
        "/" + partitionName;

    return new ImmutablePair(hdfsPath, partitionNum);
  }

  public class OnlineOfflineStateModel extends StateModel {
    @Transition(from = "OFFLINE", to = "ONLINE")
    public void onBecomeOnlineFromOffline(Message message,
                                          NotificationContext context) {
      Pair<String, String> hdfsPathAndPartition = getHdfsPathAndPartitionNum(message);
      String hdfsPath = hdfsPathAndPartition.getLeft();
      LOG.info("Opening " + hdfsPath);
      try {
        // TODO(varun): Maybe retry here.
        HColumnDescriptor family = new HColumnDescriptor(Constants.HFILE_COLUMN_FAMILY);
        family.setBlockCacheEnabled(isBlockCacheEnabled);
        Reader r = readerFactory.createHFileReader(hdfsPath, new CacheConfig(conf, family));
        resourcePartitionMap.addReader(
            message.getResourceName(), hdfsPathAndPartition.getRight(), r);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Transition(from = "ONLINE", to = "OFFLINE")
    public void onBecomeOfflineFromOnline(Message message,
                                          NotificationContext context) {
      Pair<String, String> hdfsPathAndPartition = getHdfsPathAndPartitionNum(message);
      String hdfsPath = hdfsPathAndPartition.getLeft();
      LOG.info("Closing " + hdfsPath);
      try {
        Reader r = resourcePartitionMap.removeReader(message.getResourceName(),
            hdfsPathAndPartition.getRight());
        r.close();
      } catch (Exception e) {
        LOG.warn("Could not close reader for " + hdfsPath, e);
      }
    }

    @Transition(from = "OFFLINE", to = "DROPPED")
    public void onBecomeDroppedFromOffline(Message message,
                                           NotificationContext context) {
      // This is just an extra notification when a resource is dropped, we don't do anything here.
    }
  }
}
