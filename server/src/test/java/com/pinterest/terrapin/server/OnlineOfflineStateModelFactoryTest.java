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
    verify(mockReaderFactory).createHFileReader(eq("/terrapin/data/file_set/1393/" +
            TerrapinUtil.formatPartitionName(100)),
        Matchers.<CacheConfig>anyObject());
  }

  @Test
  public void testOnlineToOfflineBucketized() throws Exception {
    when(mockHelixMessage.getResourceName()).thenReturn("$terrapin$data$file_set$1393");
    when(mockHelixMessage.getPartitionName()).thenReturn("$terrapin$data$file_set$1393_100");

    this.stateModel.onBecomeOnlineFromOffline(mockHelixMessage, null);
    verify(mockResourcePartitionMap).addReader(
        eq("$terrapin$data$file_set$1393"), eq("100"), Matchers.<Reader>anyObject());
    verify(mockReaderFactory).createHFileReader(eq("/terrapin/data/file_set/1393/" +
        TerrapinUtil.formatPartitionName(100)),
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
