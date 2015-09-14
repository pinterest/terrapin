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
package com.pinterest.terrapin.thrift;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.pinterest.terrapin.client.TerrapinClient;
import com.pinterest.terrapin.thrift.generated.*;
import com.twitter.util.Duration;
import com.twitter.util.Future;
import com.twitter.util.Throw;
import com.twitter.util.Try;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.when;

/**
 * Unit test for TerrapinServiceImpl.
 */
public class TerrapinServiceImplTest {
  private static final String CLUSTER1 = "cluster1";
  private static final String CLUSTER2 = "cluster2";
  private static final String FILESET = "fileset";
  private static final byte[] KEY = "key".getBytes();

  @MockitoAnnotations.Mock
  TerrapinClient mockClient1;

  @MockitoAnnotations.Mock
  TerrapinClient mockClient2;

  private TerrapinService.ServiceIface serviceIface;

  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    serviceIface = new TerrapinServiceImpl(ImmutableMap.of(CLUSTER1, mockClient1, CLUSTER2, mockClient2));
  }

  private TerrapinGetRequest prepareGetRequest() {
    TerrapinGetRequest request = new TerrapinGetRequest();

    request.setFileSet(FILESET);
    request.setClusterList(ImmutableList.of(CLUSTER1));
    ByteBuffer key = ByteBuffer.wrap(KEY);
    request.setKey(key);
    return request;
  }

  private TerrapinMultiGetRequest prepareMultiGetRequest() {
    TerrapinMultiGetRequest request = new TerrapinMultiGetRequest();

    request.setFileSet(FILESET);
    request.setClusterList(ImmutableList.of(CLUSTER1));
    request.addToKeyList(ByteBuffer.wrap("key1".getBytes()));
    request.addToKeyList(ByteBuffer.wrap("key2".getBytes()));
    request.addToKeyList(ByteBuffer.wrap("key3".getBytes()));

    return request;
  }

  @Test
  public void testGetSuccess() {
    ByteBuffer key = ByteBuffer.wrap(KEY);
    TerrapinGetRequest request = prepareGetRequest();

    TerrapinResponse response = new TerrapinResponse().setResponseMap(ImmutableMap.of(
        key, new TerrapinSingleResponse().setValue(ByteBuffer.wrap("value".getBytes()))));
    when(mockClient1.getMany(eq(FILESET), eq(Sets.newHashSet(key)))).thenReturn(
        Future.value(response));

    TerrapinSingleResponse singleResponse = serviceIface.get(request).get();
    assertFalse(singleResponse.isSetErrorCode());
    assertEquals("value", new String(singleResponse.getValue()));
  }
  
  @Test
  public void testGetMultiClusters() {
    ByteBuffer key = ByteBuffer.wrap(KEY);
    TerrapinGetRequest request = prepareGetRequest();
    RequestOptions options = new RequestOptions();
    options.setSelectionPolicy(SelectionPolicy.PRIMARY_FIRST);
    request.setOptions(options);
    request.setClusterList(ImmutableList.of(CLUSTER1, CLUSTER2));

    TerrapinResponse response = new TerrapinResponse().setResponseMap(ImmutableMap.of(
        key, new TerrapinSingleResponse().setValue(ByteBuffer.wrap("value".getBytes()))));
    when(mockClient1.getMany(eq(FILESET), eq(Sets.newHashSet(key)))).thenReturn(
        Future.<TerrapinResponse>exception(new IOException()));
    when(mockClient2.getManyNoRetries(eq(FILESET), eq(Sets.newHashSet(key)))).thenReturn(
        Future.value(response));    
    
    TerrapinSingleResponse singleResponse = serviceIface.get(request).get();
    assertFalse(singleResponse.isSetErrorCode());
    assertEquals("value", new String(singleResponse.getValue()));
  }
  
  @Test
  public void testGetEmptyClusters() {
    ByteBuffer key = ByteBuffer.wrap(KEY);
    TerrapinGetRequest request = prepareGetRequest();
    RequestOptions options = new RequestOptions();
    options.setSelectionPolicy(SelectionPolicy.PRIMARY_FIRST);
    request.setOptions(options);
    request.setClusterList(ImmutableList.copyOf(new String[]{}));

    TerrapinResponse response = new TerrapinResponse().setResponseMap(ImmutableMap.of(
        key, new TerrapinSingleResponse().setValue(ByteBuffer.wrap("value".getBytes()))));
    when(mockClient1.getMany(eq(FILESET), eq(Sets.newHashSet(key)))).thenReturn(
        Future.value(response));
    when(mockClient2.getManyNoRetries(eq(FILESET), eq(Sets.newHashSet(key)))).thenReturn(
        Future.value(response));

    Try<TerrapinSingleResponse> singleResponseTry = serviceIface.get(request).get(Duration.forever());
    assertTrue(singleResponseTry.isThrow());
    assertEquals(TerrapinGetErrorCode.INVALID_REQUEST,
        ((TerrapinGetException)((Throw)singleResponseTry).e()).getErrorCode());

  }

  @Test
  public void testGetNotFound() {
    ByteBuffer key = ByteBuffer.wrap(KEY);
    TerrapinGetRequest request = prepareGetRequest();

    TerrapinResponse response = new TerrapinResponse().setResponseMap((Map)Maps.newHashMap());
    when(mockClient1.getMany(eq(FILESET), eq(Sets.newHashSet(key)))).thenReturn(
        Future.value(response));

    TerrapinSingleResponse singleResponse = serviceIface.get(request).get();
    assertFalse(singleResponse.isSetErrorCode());
    assertFalse(singleResponse.isSetValue());
  }

  @Test
  public void testGetClusterNotFound() {
    TerrapinGetRequest request = prepareGetRequest().setClusterList(ImmutableList.of(
        "random-cluster"));

    Try<TerrapinSingleResponse> singleResponseTry =
        serviceIface.get(request).get(Duration.forever());
    assertTrue(singleResponseTry.isThrow());
    assertEquals(TerrapinGetErrorCode.CLUSTER_NOT_FOUND,
        ((TerrapinGetException)((Throw)singleResponseTry).e()).getErrorCode());
  }

  @Test
  public void testGetError() {
    // Test the case where We get back an error set through an error code set in
    // TerrapinSingleResponse.
    ByteBuffer key = ByteBuffer.wrap(KEY);
    TerrapinGetRequest request = prepareGetRequest();

    TerrapinResponse response = new TerrapinResponse().setResponseMap(ImmutableMap.of(
        key, new TerrapinSingleResponse().setErrorCode(TerrapinGetErrorCode.OTHER)));
    when(mockClient1.getMany(eq(FILESET), eq(Sets.newHashSet(key)))).thenReturn(
        Future.value(response));

    Try<TerrapinSingleResponse> singleResponseTry = serviceIface.get(request).get(
        Duration.forever());
    assertTrue(singleResponseTry.isThrow());
    assertEquals(TerrapinGetErrorCode.OTHER,
        ((TerrapinGetException)((Throw)singleResponseTry).e()).getErrorCode());

    // Test the case where the call to the client library itself bails out due to a
    // legit error.
    when(mockClient1.getMany(eq(FILESET), eq(Sets.newHashSet(key)))).thenReturn(
            Future.<TerrapinResponse>exception(new TerrapinGetException("Failed.",
                    TerrapinGetErrorCode.FILE_SET_NOT_FOUND)));
    singleResponseTry = serviceIface.get(request).get(Duration.forever());
    assertTrue(singleResponseTry.isThrow());
    assertEquals(TerrapinGetErrorCode.FILE_SET_NOT_FOUND,
        ((TerrapinGetException)((Throw)singleResponseTry).e()).getErrorCode());

    // Test the case where the call to the client library bails out due to a runtime
    // exception.
    when(mockClient1.getMany(eq(FILESET), eq(Sets.newHashSet(key)))).thenThrow(
        new RuntimeException(new NullPointerException()));
    singleResponseTry = serviceIface.get(request).get(Duration.forever());
    assertTrue(singleResponseTry.isThrow());
    assertEquals(TerrapinGetErrorCode.OTHER,
        ((TerrapinGetException)((Throw)singleResponseTry).e()).getErrorCode());
  }

  @Test
  public void testMultiGetSuccess() {
    TerrapinMultiGetRequest request = prepareMultiGetRequest();
    TerrapinResponse response = new TerrapinResponse().setResponseMap(ImmutableMap.of(
        ByteBuffer.wrap("key1".getBytes()),
        new TerrapinSingleResponse().setValue("value1".getBytes()),
        ByteBuffer.wrap("key2".getBytes()),
        new TerrapinSingleResponse().setErrorCode(TerrapinGetErrorCode.READ_ERROR)));
    Set<ByteBuffer> keys = Sets.newHashSet(ByteBuffer.wrap("key1".getBytes()),
        ByteBuffer.wrap("key2".getBytes()),
        ByteBuffer.wrap("key3".getBytes()));
    when(mockClient1.getMany(eq(FILESET), eq(keys))).thenReturn(Future.value(response));
    TerrapinResponse returnResponse = serviceIface.multiGet(request).get();
    assertEquals(response, returnResponse);
  }

  @Test
  public void testMultiGetMultiClusters() {
    TerrapinMultiGetRequest request = prepareMultiGetRequest();
    RequestOptions options = new RequestOptions();
    options.setSelectionPolicy(SelectionPolicy.PRIMARY_FIRST);
    request.setOptions(options);
    request.setClusterList(ImmutableList.of(CLUSTER1, CLUSTER2));

    TerrapinResponse response = new TerrapinResponse().setResponseMap(ImmutableMap.of(
        ByteBuffer.wrap("key1".getBytes()),
        new TerrapinSingleResponse().setValue("value1".getBytes()),
        ByteBuffer.wrap("key2".getBytes()),
        new TerrapinSingleResponse().setErrorCode(TerrapinGetErrorCode.READ_ERROR)));
    Set<ByteBuffer> keys = Sets.newHashSet(ByteBuffer.wrap("key1".getBytes()),
        ByteBuffer.wrap("key2".getBytes()),
        ByteBuffer.wrap("key3".getBytes()));

    when(mockClient1.getMany(eq(FILESET), eq(keys))).thenReturn(
        Future.<TerrapinResponse>exception(new IOException()));
    when(mockClient2.getManyNoRetries(eq(FILESET), eq(keys))).thenReturn(Future.value(response));

    TerrapinResponse returnResponse = serviceIface.multiGet(request).get();
    assertEquals(response, returnResponse);
  }

  @Test
  public void testMultiGetEmptyClusters() {
    TerrapinMultiGetRequest request = prepareMultiGetRequest();
    RequestOptions options = new RequestOptions();
    options.setSelectionPolicy(SelectionPolicy.PRIMARY_FIRST);
    request.setOptions(options);
    request.setClusterList(ImmutableList.copyOf(new String[]{}));

    TerrapinResponse response = new TerrapinResponse().setResponseMap(ImmutableMap.of(
        ByteBuffer.wrap("key1".getBytes()),
        new TerrapinSingleResponse().setValue("value1".getBytes()),
        ByteBuffer.wrap("key2".getBytes()),
        new TerrapinSingleResponse().setErrorCode(TerrapinGetErrorCode.READ_ERROR)));
    Set<ByteBuffer> keys = Sets.newHashSet(ByteBuffer.wrap("key1".getBytes()),
        ByteBuffer.wrap("key2".getBytes()),
        ByteBuffer.wrap("key3".getBytes()));

    when(mockClient1.getMany(eq(FILESET), eq(keys))).thenReturn(Future.value(response));
    when(mockClient2.getManyNoRetries(eq(FILESET), eq(keys))).thenReturn(Future.value(response));

    Try<TerrapinResponse> returnResponseTry = serviceIface.multiGet(request).get(Duration.forever());
    assertTrue(returnResponseTry.isThrow());
    assertEquals(TerrapinGetErrorCode.INVALID_REQUEST,
        ((TerrapinGetException)((Throw)returnResponseTry).e()).getErrorCode());
  }
  
  @Test
  public void testMultiGetError() {
    TerrapinMultiGetRequest request = prepareMultiGetRequest();
    Set<ByteBuffer> keys = Sets.newHashSet(ByteBuffer.wrap("key1".getBytes()),
        ByteBuffer.wrap("key2".getBytes()),
        ByteBuffer.wrap("key3".getBytes()));

    when (mockClient1.getMany(eq(FILESET), eq(keys))).thenReturn(
        Future.<TerrapinResponse>exception(new TerrapinGetException("Failed",
            TerrapinGetErrorCode.FILE_SET_NOT_FOUND)));
    Try<TerrapinResponse> responseTry = serviceIface.multiGet(request).get(Duration.forever());
    assertTrue(responseTry.isThrow());
    assertEquals(TerrapinGetErrorCode.FILE_SET_NOT_FOUND,
                 ((TerrapinGetException)((Throw)responseTry).e()).getErrorCode());

    when(mockClient1.getMany(eq(FILESET), eq(keys))).thenThrow(
        new RuntimeException(new NullPointerException()));
    responseTry = serviceIface.multiGet(request).get(Duration.forever());
    assertTrue(responseTry.isThrow());
    assertEquals(TerrapinGetErrorCode.OTHER,
                 ((TerrapinGetException)((Throw)responseTry).e()).getErrorCode());
  }

  @Test
  public void testMultiGetClusterNotFound() {
    TerrapinMultiGetRequest request = prepareMultiGetRequest().setClusterList(ImmutableList.of(
        "random-cluster"));

    Try<TerrapinResponse> responseTry =
    serviceIface.multiGet(request).get(Duration.forever());
    assertTrue(responseTry.isThrow());
    assertEquals(TerrapinGetErrorCode.CLUSTER_NOT_FOUND,
                 ((TerrapinGetException)((Throw)responseTry).e()).getErrorCode());
  }
}
