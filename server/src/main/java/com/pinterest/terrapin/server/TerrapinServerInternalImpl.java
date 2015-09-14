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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pinterest.terrapin.storage.Reader;
import com.pinterest.terrapin.thrift.generated.MultiKey;
import com.pinterest.terrapin.thrift.generated.TerrapinGetErrorCode;
import com.pinterest.terrapin.thrift.generated.TerrapinGetException;
import com.pinterest.terrapin.thrift.generated.TerrapinInternalGetRequest;
import com.pinterest.terrapin.thrift.generated.TerrapinResponse;
import com.pinterest.terrapin.thrift.generated.TerrapinServerInternal;
import com.pinterest.terrapin.thrift.generated.TerrapinSingleResponse;
import com.twitter.util.Function;
import com.twitter.util.Future;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Implementation of the Terrapin thrift server.
 */
public class TerrapinServerInternalImpl implements TerrapinServerInternal.ServiceIface {
  private final PropertiesConfiguration configuration;
  private final ResourcePartitionMap resourcePartitionMap;

  public TerrapinServerInternalImpl(PropertiesConfiguration configuration,
                                    ResourcePartitionMap resourcePartitionMap) {
    this.configuration = configuration;
    this.resourcePartitionMap = resourcePartitionMap;
  }

  @Override
  public Future<TerrapinResponse> get(TerrapinInternalGetRequest request) {
    final TerrapinResponse response = new TerrapinResponse().setResponseMap(
        (Map)Maps.newHashMap());
    List<Future<Map<ByteBuffer, Pair<ByteBuffer, Throwable>>>> readFutures =
        Lists.newArrayListWithCapacity(request.getKeyListSize());
    // Go through all the keys and retrieve the corresponding Reader object.
    String resource = null;
    for (MultiKey multiKey : request.getKeyList()) {
      Reader r = null;
      try {
        String keyResource = multiKey.getResource();
        if (resource != null) {
          if (!resource.equals(keyResource)) {
            return Future.exception(new TerrapinGetException(
                "Multiple resources within same get call.",
                TerrapinGetErrorCode.INVALID_REQUEST));
          }
        } else {
          resource = keyResource;
        }
        r = this.resourcePartitionMap.getReader(keyResource,
                                                multiKey.getPartition());
      } catch (TerrapinGetException e) {
        for (ByteBuffer key : multiKey.getKey()) {
          response.getResponseMap().put(key,
              new TerrapinSingleResponse().setErrorCode(e.getErrorCode()));
        }
        continue;
      }
      // If we found a reader, issue reads on it.
      try {
        readFutures.add(r.getValues(multiKey.getKey()));
      } catch (Throwable t) {
        t.printStackTrace();
        // The entire batch failed.
        for (ByteBuffer key : multiKey.getKey()) {
          response.getResponseMap().put(key,
              new TerrapinSingleResponse().setErrorCode(TerrapinGetErrorCode.READ_ERROR));
        }
      }
    }
    // Wait for all the futures to finish and return the response.
    return Future.collect(readFutures).map(new Function<List<Map<ByteBuffer,
                                                                 Pair<ByteBuffer, Throwable>>>,
                                                        TerrapinResponse>() {
      @Override
      public TerrapinResponse apply(List<Map<ByteBuffer, Pair<ByteBuffer, Throwable>>> mapList) {
        for (Map<ByteBuffer, Pair<ByteBuffer, Throwable>> map : mapList) {
          for (Map.Entry<ByteBuffer, Pair<ByteBuffer, Throwable>> entry : map.entrySet()) {
            if (entry.getValue().getRight() != null) {
              response.getResponseMap().put(entry.getKey(),
                  new TerrapinSingleResponse().setErrorCode(TerrapinGetErrorCode.READ_ERROR));
            } else {
              response.getResponseMap().put(entry.getKey(),
                  new TerrapinSingleResponse().setValue(entry.getValue().getLeft()));
            }
          }
        }
        return response;
      }
    });
  }
}
