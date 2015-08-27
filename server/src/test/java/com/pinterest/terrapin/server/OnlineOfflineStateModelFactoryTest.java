package com.pinterest.terrapin.server;

import com.pinterest.terrapin.storage.Reader;
import com.pinterest.terrapin.storage.ReaderFactory;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.helix.model.Message;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.MockitoAnnotations;

import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit test for OnlineOfflineStateModelFactory.
 */
public class OnlineOfflineStateModelFactoryTest {
  @MockitoAnnotations.Mock
  Message mockHelixMessage;

  @MockitoAnnotations.Mock
  ResourcePartitionMap mockResourcePartitionMap;

  @MockitoAnnotations.Mock
  ReaderFactory mockReaderFactory;

  @MockitoAnnotations.Mock
  Reader mockReader;

  private OnlineOfflineStateModelFactory.OnlineOfflineStateModel stateModel;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    this.stateModel = (OnlineOfflineStateModelFactory.OnlineOfflineStateModel)
        new OnlineOfflineStateModelFactory(new PropertiesConfiguration(),
                                           mockResourcePartitionMap,
                                           mockReaderFactory).createNewStateModel(
                                               "resource", "OnlineOffline");
  }

  @Test
  public void testOnlineToOffline() throws Exception {
    when(mockHelixMessage.getResourceName()).thenReturn("$terrapin$data$file_set$1393");
    when(mockHelixMessage.getPartitionName()).thenReturn("$terrapin$data$file_set$1393$100");

    this.stateModel.onBecomeOnlineFromOffline(mockHelixMessage, null);
    verify(mockResourcePartitionMap).addReader(
        eq("$terrapin$data$file_set$1393"), eq("100"), Matchers.<Reader>anyObject());
    verify(mockReaderFactory).createHFileReader(eq("/terrapin/data/file_set/1393/part-00100"),
        Matchers.<CacheConfig>anyObject());
  }

  @Test
  public void testOnlineToOfflineBucketized() throws Exception {
    when(mockHelixMessage.getResourceName()).thenReturn("$terrapin$data$file_set$1393");
    when(mockHelixMessage.getPartitionName()).thenReturn("$terrapin$data$file_set$1393_100");

    this.stateModel.onBecomeOnlineFromOffline(mockHelixMessage, null);
    verify(mockResourcePartitionMap).addReader(
        eq("$terrapin$data$file_set$1393"), eq("100"), Matchers.<Reader>anyObject());
    verify(mockReaderFactory).createHFileReader(eq("/terrapin/data/file_set/1393/part-00100"),
        Matchers.<CacheConfig>anyObject());
  }

  @Test
  public void testOfflineToOnline() throws Exception {
    when(mockHelixMessage.getResourceName()).thenReturn("$terrapin$data$file_set$1393");
    // Bucketized resources use a slightly different partition naming due to helix partition
    // parsing issues.
    when(mockHelixMessage.getPartitionName()).thenReturn("$terrapin$data$file_set$1393$100");
    when(mockResourcePartitionMap.removeReader(
        eq("$terrapin$data$file_set$1393"), eq("100"))).thenReturn(mockReader);

    this.stateModel.onBecomeOfflineFromOnline(mockHelixMessage, null);
    verify(mockReader, times(1)).close();
  }

  @Test
  public void testOfflineToOnlineBucketized() throws Exception {
    when(mockHelixMessage.getResourceName()).thenReturn("$terrapin$data$file_set$1393");
    // Bucketized resources use a slightly different partition naming due to helix partition
    // parsing issues.
    when(mockHelixMessage.getPartitionName()).thenReturn("$terrapin$data$file_set$1393_100");
    when(mockResourcePartitionMap.removeReader(
        eq("$terrapin$data$file_set$1393"), eq("100"))).thenReturn(mockReader);

    this.stateModel.onBecomeOfflineFromOnline(mockHelixMessage, null);
    verify(mockReader, times(1)).close();
  }
}
