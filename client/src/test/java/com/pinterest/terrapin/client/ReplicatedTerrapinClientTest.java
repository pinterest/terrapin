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
package com.pinterest.terrapin.client;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.pinterest.terrapin.thrift.generated.RequestOptions;
import com.pinterest.terrapin.thrift.generated.SelectionPolicy;
import com.pinterest.terrapin.thrift.generated.TerrapinResponse;
import com.pinterest.terrapin.thrift.generated.TerrapinSingleResponse;
import com.twitter.util.Future;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import static junit.framework.Assert.assertEquals;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Unit tests for ReplicatedTerrapinClient.
 */
public class ReplicatedTerrapinClientTest {
  @MockitoAnnotations.Mock
  private TerrapinClient primaryClientMock;

  @MockitoAnnotations.Mock
  private TerrapinClient secondaryClientMock;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
  }

  private static final Map<ByteBuffer, ByteBuffer> KEY_VALUE_MAP = new ImmutableMap.Builder().
      put(ByteBuffer.wrap("k1".getBytes()), ByteBuffer.wrap("v1".getBytes())).
      put(ByteBuffer.wrap("k2".getBytes()), ByteBuffer.wrap("v2".getBytes())).
      put(ByteBuffer.wrap("k3".getBytes()), ByteBuffer.wrap("v3".getBytes())).build();

  static class GetOneAnswer implements Answer<Future<TerrapinSingleResponse>> {
    @Override
    public Future<TerrapinSingleResponse> answer(
        InvocationOnMock invocationOnMock) throws Throwable {
      ByteBuffer key = (ByteBuffer)invocationOnMock.getArguments()[1];
      ByteBuffer value = KEY_VALUE_MAP.get(key);
      TerrapinSingleResponse response = new TerrapinSingleResponse();
      if (value != null) {
        response.setValue(value);
      }
      return Future.value(response);
    }
  }

  static class GetManyAnswer implements Answer<Future<TerrapinResponse>> {
    @Override
    public Future<TerrapinResponse> answer(InvocationOnMock invocationOnMock) throws Throwable {
      Set<ByteBuffer> keys = (Set<ByteBuffer>)invocationOnMock.getArguments()[1];
      TerrapinResponse response = new TerrapinResponse();
      response.setResponseMap((Map)Maps.newHashMap());
      for (ByteBuffer key : keys) {
        if (KEY_VALUE_MAP.containsKey(key)) {
          TerrapinSingleResponse singleResponse = new TerrapinSingleResponse();
          singleResponse.setValue(KEY_VALUE_MAP.get(key));
          response.getResponseMap().put(key, singleResponse);
        }
      }
      return Future.value(response);
    }
  }

  private static final GetOneAnswer GET_ONE_ANSWER = new GetOneAnswer();
  private static final GetManyAnswer GET_MANY_ANSWER = new GetManyAnswer();
  private static final RequestOptions OPTIONS = new RequestOptions();
  static {
    OPTIONS.setSelectionPolicy(SelectionPolicy.PRIMARY_FIRST);
    OPTIONS.setSpeculativeTimeoutMillis(1000);
  }

  private void checkResponse(TerrapinResponse response) {
    for (Map.Entry<ByteBuffer, TerrapinSingleResponse> entry :
         response.getResponseMap().entrySet()) {
      assertEquals(KEY_VALUE_MAP.get(entry.getKey()), ByteBuffer.wrap(entry.getValue().getValue()));
    }
  }

  @Test
  public void testOneNullClient() {
    when(primaryClientMock.getOne(anyString(), (ByteBuffer) anyObject())).thenAnswer(
        GET_ONE_ANSWER);
    when(secondaryClientMock.getOne(anyString(), (ByteBuffer) anyObject())).thenAnswer(
        GET_ONE_ANSWER);
    when(primaryClientMock.getMany(anyString(), (Set<ByteBuffer>) anyObject())).thenAnswer(
        GET_MANY_ANSWER);
    when(secondaryClientMock.getMany(anyString(), (Set<ByteBuffer>) anyObject())).
        thenAnswer(GET_MANY_ANSWER);

    ReplicatedTerrapinClient replClient1 = new ReplicatedTerrapinClient(primaryClientMock, null);
    ReplicatedTerrapinClient replClient2 = new ReplicatedTerrapinClient(null, secondaryClientMock);

    TerrapinSingleResponse response1 = replClient1.getOne(
        "test", ByteBuffer.wrap("k2".getBytes()), OPTIONS).get();
    assertEquals("v2", new String(response1.getValue()));

    TerrapinSingleResponse response2 = replClient2.getOne(
        "test", ByteBuffer.wrap("k2".getBytes()), OPTIONS).get();
    assertEquals("v2", new String(response2.getValue()));

    Set<ByteBuffer> keys = new ImmutableSet.Builder().
        add(ByteBuffer.wrap("k1".getBytes())).
        add(ByteBuffer.wrap("k2".getBytes())).build();
    checkResponse(replClient1.getMany("test", keys, OPTIONS).get());
    checkResponse(replClient2.getMany("test", keys, OPTIONS).get());
  }

  @Test
  public void testPrimaryFailure() {
    when(primaryClientMock.getOne(anyString(), (ByteBuffer) anyObject())).thenReturn(
        Future.<TerrapinSingleResponse>exception(new IOException()));
    when(primaryClientMock.getMany(anyString(), (Set<ByteBuffer>) anyObject())).thenReturn(
        Future.<TerrapinResponse>exception(new IOException()));
    when(secondaryClientMock.getOneNoRetries(anyString(), (ByteBuffer) anyObject())).thenAnswer(
        GET_ONE_ANSWER);
    when(secondaryClientMock.getManyNoRetries(anyString(), (Set<ByteBuffer>) anyObject())).
        thenAnswer(GET_MANY_ANSWER);

    ReplicatedTerrapinClient replClient = new ReplicatedTerrapinClient(
        primaryClientMock, secondaryClientMock);
    TerrapinSingleResponse response1 = replClient.getOne(
        "test", ByteBuffer.wrap("k2".getBytes()), OPTIONS).get();
    assertEquals("v2", new String(response1.getValue()));

    Set<ByteBuffer> keys = new ImmutableSet.Builder().
        add(ByteBuffer.wrap("k1".getBytes())).
        add(ByteBuffer.wrap("k2".getBytes())).build();
    checkResponse(replClient.getMany("test", keys, OPTIONS).get());
  }
}
