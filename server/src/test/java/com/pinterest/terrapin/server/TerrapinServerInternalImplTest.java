package com.pinterest.terrapin.server;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.pinterest.terrapin.storage.Reader;
import com.pinterest.terrapin.thrift.generated.MultiKey;
import com.pinterest.terrapin.thrift.generated.TerrapinGetErrorCode;
import com.pinterest.terrapin.thrift.generated.TerrapinGetException;
import com.pinterest.terrapin.thrift.generated.TerrapinInternalGetRequest;
import com.pinterest.terrapin.thrift.generated.TerrapinResponse;
import com.pinterest.terrapin.thrift.generated.TerrapinSingleResponse;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import com.twitter.util.Throw;
import com.twitter.util.Try;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit test for TerrapinServerInternalImpl.
 */
public class TerrapinServerInternalImplTest {
  @MockitoAnnotations.Mock
  private ResourcePartitionMap mockResourcePartitionMap;

  private TerrapinServerInternalImpl  serverImpl;

  private static final String RESOURCE = "resource";

  // Map of keys to the values returned by the corresponding.
  private static final Map<ByteBuffer, Pair<ByteBuffer, Throwable>> KEY_VALUES_MAP =
      new ImmutableMap.Builder().
          put(ByteBuffer.wrap("k1".getBytes()),
              new ImmutablePair<ByteBuffer, Throwable>(ByteBuffer.wrap("v1".getBytes()), null)).
          put(ByteBuffer.wrap("k2".getBytes()),
              new ImmutablePair<ByteBuffer, Throwable>(ByteBuffer.wrap("v2".getBytes()), null)).
          // Key with error.
          put(ByteBuffer.wrap("k3".getBytes()),
              new ImmutablePair<ByteBuffer, Throwable>(null, new IOException())).
          put(ByteBuffer.wrap("k4".getBytes()),
                  new ImmutablePair<ByteBuffer, Throwable>(ByteBuffer.wrap("v4".getBytes()), null)).
          put(ByteBuffer.wrap("k5".getBytes()),
              new ImmutablePair<ByteBuffer, Throwable>(ByteBuffer.wrap("v5".getBytes()), null)).
          put(ByteBuffer.wrap("k6".getBytes()),
              new ImmutablePair<ByteBuffer, Throwable>(ByteBuffer.wrap("v6".getBytes()), null)).
          put(ByteBuffer.wrap("k7".getBytes()),
              new ImmutablePair<ByteBuffer, Throwable>(ByteBuffer.wrap("v7".getBytes()), null)).
          // Key with error.
          put(ByteBuffer.wrap("k8".getBytes()),
              new ImmutablePair<ByteBuffer, Throwable>(null, new IOException())).
          build();

  // Batches of keys - first 4 keys go to partition 1 while the last 4 go to partition 2.
  private static final List<ByteBuffer> KEYS_PART_1 = ImmutableList.of(ByteBuffer.wrap("k1".getBytes()),
      ByteBuffer.wrap("k2".getBytes()),    // Non existent key.
      ByteBuffer.wrap("k3".getBytes()),
      ByteBuffer.wrap("k31".getBytes()),
      ByteBuffer.wrap("k4".getBytes()));
  private static final List<ByteBuffer> KEYS_PART_2 = ImmutableList.of(ByteBuffer.wrap("k5".getBytes()),
      ByteBuffer.wrap("k6".getBytes()),
      ByteBuffer.wrap("k61".getBytes()),   // Non existent key.
      ByteBuffer.wrap("k7".getBytes()),
      ByteBuffer.wrap("k8".getBytes()));

    @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    this.serverImpl = new TerrapinServerInternalImpl(new PropertiesConfiguration(),
        mockResourcePartitionMap);
  }

  private TerrapinInternalGetRequest prepareRequest() {
    TerrapinInternalGetRequest request = new TerrapinInternalGetRequest();

    MultiKey multiKey1 = new MultiKey();
    multiKey1.setKey(KEYS_PART_1);
    multiKey1.setResource(RESOURCE);
    multiKey1.setPartition("1");

    MultiKey multiKey2 = new MultiKey();
    multiKey2.setKey(KEYS_PART_2);
    multiKey2.setResource(RESOURCE);
    multiKey2.setPartition("2");

    request.addToKeyList(multiKey1);
    request.addToKeyList(multiKey2);

    return request;
  }

  @Test
  public void testGetErrorMultipleResources() throws Exception {
    TerrapinInternalGetRequest request = new TerrapinInternalGetRequest();
    MultiKey multiKey1 = new MultiKey().setResource("resource1").setPartition("1");
    multiKey1.addToKey(ByteBuffer.wrap("k1".getBytes()));
    MultiKey multiKey2 = new MultiKey().setResource("resource2").setPartition("1");
    multiKey2.addToKey(ByteBuffer.wrap("k2".getBytes()));
    request.addToKeyList(multiKey1);
    request.addToKeyList(multiKey2);

    Reader mockReader = mock(Reader.class);
    when(mockResourcePartitionMap.getReader(eq("resource1"), eq("1"))).thenReturn(mockReader);
    Try<TerrapinResponse> responseTry = serverImpl.get(request).get(Duration.forever());
    TerrapinGetException e = (TerrapinGetException)((Throw)responseTry).e();
    assertEquals(TerrapinGetErrorCode.INVALID_REQUEST, e.getErrorCode());
  }

  private static final Answer GET_VALUES_ANSWER = new Answer<Future<Map<ByteBuffer,
      Pair<ByteBuffer, Throwable>>>>() {
    @Override
    public Future<Map<ByteBuffer, Pair<ByteBuffer, Throwable>>> answer(
        InvocationOnMock invocationOnMock) {
      List<ByteBuffer> keyList = (List)invocationOnMock.getArguments()[0];
      Map<ByteBuffer, Pair<ByteBuffer, Throwable>> returnMap = Maps.newHashMap();
      for (ByteBuffer key : keyList) {
        // The reader should only put keys for which values exist.
        if (KEY_VALUES_MAP.containsKey(key)) {
          returnMap.put(key, KEY_VALUES_MAP.get(key));
        }
      }
      return Future.value(returnMap);
    }
  };

  private void checkLookup(List<ByteBuffer> keyList,
                           Map<ByteBuffer, TerrapinSingleResponse> responseMap) {
    for (ByteBuffer key : keyList) {
      TerrapinSingleResponse response = responseMap.get(key);
      Pair<ByteBuffer, Throwable> expectedValuePair = KEY_VALUES_MAP.get(key);
      if (expectedValuePair == null) {
        // Ensure that the value is not present in the response either if it was not
        // returned by the Reader.
        assertNull(response);
        continue;
      }
      if (expectedValuePair.getRight() != null) {
        assertFalse(response.isSetValue());
        assertEquals(TerrapinGetErrorCode.READ_ERROR, response.getErrorCode());
      } else {
        assertFalse(response.isSetErrorCode());
        assertEquals(new String(expectedValuePair.getLeft().array()),
                new String(response.getValue()));
      }
    }
  }

  @Test
  public void testGetPartialSuccessReaderUnavailable() throws Throwable {
    Reader mockReader = mock(Reader.class);

    when(mockResourcePartitionMap.getReader(eq(RESOURCE), eq("1"))).thenThrow(
        new TerrapinGetException("", TerrapinGetErrorCode.NOT_SERVING_PARTITION));
    when(mockResourcePartitionMap.getReader(eq(RESOURCE), eq("2"))).thenReturn(mockReader);
    when(mockReader.getValues(eq(KEYS_PART_2))).thenAnswer(GET_VALUES_ANSWER);

    TerrapinResponse response = serverImpl.get(prepareRequest()).get();
    Map<ByteBuffer, TerrapinSingleResponse> responseMap = response.getResponseMap();
    assertEquals(9, responseMap.size());

    // Ensure that the first batch throws back a NOT_SERVING_PARTITION.
    for (ByteBuffer key : KEYS_PART_1) {
      assertEquals(TerrapinGetErrorCode.NOT_SERVING_PARTITION,
                   responseMap.get(key).getErrorCode());
    }
    // Ensure that the remaining keys arrive correctly.
    checkLookup(KEYS_PART_2, responseMap);
  }
    
  @Test
  public void testGetPartialSuccess() throws Throwable {
    Reader mockReader1 = mock(Reader.class);
    Reader mockReader2 = mock(Reader.class);

    when(mockResourcePartitionMap.getReader(eq(RESOURCE), eq("1"))).thenReturn(mockReader1);
    when(mockResourcePartitionMap.getReader(eq(RESOURCE), eq("2"))).thenReturn(mockReader2);
    when(mockReader1.getValues(eq(KEYS_PART_1))).thenAnswer(GET_VALUES_ANSWER);
    when(mockReader2.getValues(eq(KEYS_PART_2))).thenAnswer(GET_VALUES_ANSWER);
      
    TerrapinResponse response = serverImpl.get(prepareRequest()).get();
    Map<ByteBuffer, TerrapinSingleResponse> responseMap = response.getResponseMap();
    assertEquals(8, responseMap.size());

    // Ensure that the keys arrive correctly.
    checkLookup(KEYS_PART_1, responseMap);
    checkLookup(KEYS_PART_2, responseMap);
  }
}
