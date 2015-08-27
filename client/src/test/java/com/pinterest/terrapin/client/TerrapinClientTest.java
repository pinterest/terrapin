package com.pinterest.terrapin.client;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pinterest.terrapin.base.BytesUtil;
import com.pinterest.terrapin.thrift.generated.MultiKey;
import com.pinterest.terrapin.thrift.generated.PartitionerType;
import com.pinterest.terrapin.thrift.generated.TerrapinGetErrorCode;
import com.pinterest.terrapin.thrift.generated.TerrapinInternalGetRequest;
import com.pinterest.terrapin.thrift.generated.TerrapinResponse;
import com.pinterest.terrapin.thrift.generated.TerrapinServerInternal;
import com.pinterest.terrapin.thrift.generated.TerrapinSingleResponse;
import com.pinterest.terrapin.zookeeper.FileSetInfo;
import com.pinterest.terrapin.zookeeper.ViewInfo;
import com.twitter.util.Future;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.helix.model.ExternalView;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

/**
 * Unit tests for terrapin client. We use a TestTerrapinClient which keeps a map of host name
 * to mock clients. For each mock client, a mockito answer is setup to successfully return
 * results or to error out certain keys.
 */
public class TerrapinClientTest {
  class TestTerrapinClient extends TerrapinClient {
    private Map<String, TerrapinServerInternal.ServiceIface> hostNameClientMap;

    public TestTerrapinClient(FileSetViewManager fsViewManager) throws Exception {
      super(fsViewManager, "test", 9090, 100, 100);
    }

    public TerrapinClient setHostNameClientMap(
        Map<String, TerrapinServerInternal.ServiceIface> hostNameClientMap) {
      this.hostNameClientMap = hostNameClientMap;
      return this;
    }

    @Override
    protected Future<TerrapinServerInternal.ServiceIface> getClientFuture(final String hostName) {
      return Future.value(hostNameClientMap.get(hostName));
    }
  }

  private static final String FILE_SET = "file_set";
  private static final String RESOURCE = "resource";

  private static final String HOST1 = "host1";
  private static final String HOST2 = "host2";
  private static final String HOST3 = "host3";

  @MockitoAnnotations.Mock
  private TerrapinServerInternal.ServiceIface mockClient1;

  @MockitoAnnotations.Mock
  private TerrapinServerInternal.ServiceIface mockClient2;

  @MockitoAnnotations.Mock
  private TerrapinServerInternal.ServiceIface mockClient3;

  @MockitoAnnotations.Mock
  private FileSetViewManager mockFsViewManager;

  private Map<String, TerrapinServerInternal.ServiceIface> hostNameClientMap;
  private TerrapinClient terrapinClient;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    hostNameClientMap = new ImmutableMap.Builder().put(
        HOST1, mockClient1).put(
        HOST2, mockClient2).put(
        HOST3, mockClient3).build();
    terrapinClient = new TestTerrapinClient(mockFsViewManager).setHostNameClientMap(hostNameClientMap);

    ViewInfo viewInfo = createViewInfo((Map)ImmutableMap.of(
        HOST1, ImmutableList.of("0", "1", "3"),
        HOST2, ImmutableList.of("1", "2"),
        HOST3, ImmutableList.of("2", "3")));

    FileSetInfo fsInfo = new FileSetInfo();
    fsInfo.fileSetName = FILE_SET;
    fsInfo.oldServingInfoList = Lists.newArrayList();
    fsInfo.deleted = false;
    fsInfo.numVersionsToKeep = 1;
    fsInfo.servingInfo = new FileSetInfo.ServingInfo("", RESOURCE, 4, PartitionerType.MODULUS);

    when(mockFsViewManager.getFileSetViewInfo(eq(FILE_SET))).thenReturn(
        new ImmutablePair(fsInfo, viewInfo));
    setUpResponseForHost("host1",
        (Map)ImmutableMap.of("0", ImmutableMap.of("a", "value_a"),
                             "1", ImmutableMap.of("b", "value_b")),
        (Map)Maps.newHashMap());
    setUpResponseForHost("host2",
            (Map) ImmutableMap.of("2", ImmutableMap.of("c", "value_c")),
            (Map) ImmutableMap.of("1", ImmutableMap.of("b", TerrapinGetErrorCode.READ_ERROR)));
    setUpResponseForHost("host3",
            (Map)ImmutableMap.of("2", ImmutableMap.of("c", "value_c"),
                                 "3", ImmutableMap.of("d", "value_d")),
            (Map)ImmutableMap.of("3", ImmutableMap.of("h", TerrapinGetErrorCode.OTHER)));
  }

  private static class TerrapinAnswer implements Answer<Future<TerrapinResponse>> {
    Map<String, Map<String, String>> successKeys;
    Map<String, Map<String, TerrapinGetErrorCode>> errorKeys;

    public TerrapinAnswer(Map<String, Map<String, String>> successKeys,
                          Map<String, Map<String, TerrapinGetErrorCode>> errorKeys) {
      this.successKeys = successKeys;
      this.errorKeys = errorKeys;
    }

    @Override
    public Future<TerrapinResponse> answer(InvocationOnMock invocationOnMock) {
      TerrapinInternalGetRequest getRequest = (TerrapinInternalGetRequest)(
          invocationOnMock.getArguments()[0]);
      TerrapinResponse response = new TerrapinResponse();
      response.setResponseMap(Maps.<ByteBuffer, TerrapinSingleResponse>newHashMap());
      for (MultiKey multiKey : getRequest.getKeyList()) {
        String partition = multiKey.getPartition();
        if (!(successKeys.containsKey(partition) || errorKeys.containsKey(partition))) {
          for (ByteBuffer key : multiKey.getKey()) {
            TerrapinSingleResponse singleResponse = new TerrapinSingleResponse();
            singleResponse.setErrorCode(TerrapinGetErrorCode.NOT_SERVING_PARTITION);
            response.getResponseMap().put(key, singleResponse);
          }
          continue;
        }
        Map<String, String> kvSuccessMap = successKeys.get(partition);
        Map<String, TerrapinGetErrorCode> kvErrorMap = errorKeys.get(partition);
        for (ByteBuffer key : multiKey.getKey()) {
          String keyStr = new String(BytesUtil.readBytesFromByteBufferWithoutConsume(key));
          TerrapinSingleResponse singleResponse = new TerrapinSingleResponse();
          if (kvSuccessMap != null && kvSuccessMap.containsKey(keyStr)) {
            singleResponse.setValue(ByteBuffer.wrap(kvSuccessMap.get(keyStr).getBytes()));
            response.getResponseMap().put(key, singleResponse);
          } else if (kvErrorMap != null && kvErrorMap.containsKey(keyStr)) {
            singleResponse.setErrorCode(kvErrorMap.get(keyStr));
            response.getResponseMap().put(key, singleResponse);
          }
        }
      }
      return Future.value(response);
    }
  }

  private static class GetRequestMatcher extends ArgumentMatcher<TerrapinInternalGetRequest> {
    @Override
    public boolean matches(Object o) {
      if (o == null) {
        return false;
      }
      return (o instanceof TerrapinInternalGetRequest);
    }
  }

  // Sets up appropriate answers for each host in accordance with @successKeys and @errorKeys.
  // Each is map from a partition # to a map of keys to values/errors.
  private void setUpResponseForHost(String host,
                                    Map<String, Map<String, String>> successKeys,
                                    Map<String, Map<String, TerrapinGetErrorCode>> errorKeys) {
    TerrapinServerInternal.ServiceIface mockIface = hostNameClientMap.get(host);
    when(mockIface.get(Matchers.argThat(new GetRequestMatcher()))).thenAnswer(
            new TerrapinAnswer(successKeys, errorKeys));
  }

  private ViewInfo createViewInfo(Map<String, List<String>> partitionHostMap) {
    ExternalView externalView = new ExternalView(RESOURCE);
    for (Map.Entry<String, List<String>> entry : partitionHostMap.entrySet()) {
      String host = entry.getKey();
      for (String partition : entry.getValue()) {
        String partitionInHelix = RESOURCE + "$" + partition;
        Map<String, String> stateMap = externalView.getStateMap(partitionInHelix);
        if (stateMap == null) {
          stateMap = Maps.newHashMap();
          stateMap.put(host, "ONLINE");
          externalView.setStateMap(partitionInHelix, stateMap);
        } else {
          stateMap.put(host, "ONLINE");
        }
      }
    }
    return new ViewInfo(externalView);
  }

  private void checkValue(String key, String value,
                          Map<ByteBuffer, TerrapinSingleResponse> responseMap) {
    assertEquals(value, new String(responseMap.get(ByteBuffer.wrap(key.getBytes())).getValue()));
  }
    
  private void checkError(String key, TerrapinGetErrorCode errorCode,
                          Map<ByteBuffer, TerrapinSingleResponse> responseMap) {
    assertEquals(errorCode, responseMap.get(ByteBuffer.wrap(key.getBytes())).getErrorCode());
  }

  @Test
  public void testGetMany() throws Exception {
    // First off issue a request with no retries. At this point, the requests for keys "b" and
    // "h" will error out.
    TerrapinResponse response = terrapinClient.getManyNoRetries(FILE_SET, (Set) ImmutableSet.of(
        ByteBuffer.wrap("a".getBytes()),
        ByteBuffer.wrap("b".getBytes()),
        ByteBuffer.wrap("c".getBytes()),
        ByteBuffer.wrap("d".getBytes()),
        ByteBuffer.wrap("h".getBytes()),
        ByteBuffer.wrap("i".getBytes()))).get();
    Map<ByteBuffer, TerrapinSingleResponse> responseMap = response.getResponseMap();
    assertEquals(5, responseMap.size());
    checkValue("a", "value_a", responseMap);
    checkValue("c", "value_c", responseMap);
    checkValue("d", "value_d", responseMap);
    checkError("b", TerrapinGetErrorCode.READ_ERROR, responseMap);
    checkError("h", TerrapinGetErrorCode.OTHER, responseMap);

    // First off issue a request with no retries. At this point, the requests for keys "b" and
    // "h" will error out.
    response = terrapinClient.getMany(FILE_SET, (Set) ImmutableSet.of(
            ByteBuffer.wrap("a".getBytes()),
            ByteBuffer.wrap("b".getBytes()),
            ByteBuffer.wrap("c".getBytes()),
            ByteBuffer.wrap("d".getBytes()),
            ByteBuffer.wrap("h".getBytes()),
            ByteBuffer.wrap("i".getBytes()))).get();
    responseMap = response.getResponseMap();
    assertEquals(5, responseMap.size());
    checkValue("a", "value_a", responseMap);
    checkValue("c", "value_c", responseMap);
    checkValue("d", "value_d", responseMap);
    // "b" gets retried on host3 and succeeds.
    checkValue("b", "value_b", responseMap);
    // "h" gets retried on host1 but host1 is not serving partition #3 and we get a
    // NOT_SERVING_PARTITION error.
    checkError("h", TerrapinGetErrorCode.NOT_SERVING_PARTITION, responseMap);
  }

  @Test
  public void testGetManyWithHostError() throws Exception {
    // Setup failure for host2 causing failure for "b" and "c" for the first try.
    when(mockClient2.get(Matchers.argThat(new GetRequestMatcher()))).thenReturn(
        Future.<TerrapinResponse>exception(new IOException()));
    TerrapinResponse response = terrapinClient.getManyNoRetries(FILE_SET, (Set) ImmutableSet.of(
        ByteBuffer.wrap("a".getBytes()),
        ByteBuffer.wrap("b".getBytes()),
        ByteBuffer.wrap("c".getBytes()),
        ByteBuffer.wrap("d".getBytes()),
        ByteBuffer.wrap("h".getBytes()),
        ByteBuffer.wrap("i".getBytes()))).get();
    Map<ByteBuffer, TerrapinSingleResponse> responseMap = response.getResponseMap();
    assertEquals(5, responseMap.size());
    checkValue("a", "value_a", responseMap);
    checkError("c", TerrapinGetErrorCode.READ_ERROR, responseMap);
    checkValue("d", "value_d", responseMap);
    checkError("b", TerrapinGetErrorCode.READ_ERROR, responseMap);
    checkError("h", TerrapinGetErrorCode.OTHER, responseMap);

    // Running with retries clears gets the correct values for "b" and "c".
    response = terrapinClient.getMany(FILE_SET, (Set) ImmutableSet.of(
              ByteBuffer.wrap("a".getBytes()),
              ByteBuffer.wrap("b".getBytes()),
              ByteBuffer.wrap("c".getBytes()),
              ByteBuffer.wrap("d".getBytes()),
              ByteBuffer.wrap("h".getBytes()),
              ByteBuffer.wrap("i".getBytes()))).get();
    responseMap = response.getResponseMap();
    assertEquals(5, responseMap.size());
    checkValue("a", "value_a", responseMap);
    checkValue("c", "value_c", responseMap);
    checkValue("d", "value_d", responseMap);
    // "b" gets retried on host3 and succeeds.
    checkValue("b", "value_b", responseMap);
    // "h" gets retried on host1 but host1 is not serving partition #3 and we get a
    // NOT_SERVING_PARTITION error.
    checkError("h", TerrapinGetErrorCode.NOT_SERVING_PARTITION, responseMap);
  }
}
