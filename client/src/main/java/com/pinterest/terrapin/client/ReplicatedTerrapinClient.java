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

import com.google.common.base.Preconditions;
import com.pinterest.terrapin.base.FutureUtil;
import com.pinterest.terrapin.thrift.generated.RequestOptions;
import com.pinterest.terrapin.thrift.generated.SelectionPolicy;
import com.pinterest.terrapin.thrift.generated.TerrapinResponse;
import com.pinterest.terrapin.thrift.generated.TerrapinSingleResponse;
import com.twitter.util.Function0;
import com.twitter.util.Future;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.nio.ByteBuffer;
import java.util.Set;

/**
 * A replicated terrapin client for issuing requests across multiple terrapin clusters.
 * It uses speculative execution for minimizing latency.
 */
public class ReplicatedTerrapinClient {
  private final TerrapinClient primaryClient;
  private final TerrapinClient secondaryClient;

  public ReplicatedTerrapinClient(TerrapinClient primaryClient, TerrapinClient secondaryClient) {
    Preconditions.checkArgument(primaryClient != null || secondaryClient != null,
        "Both clients cannot be null.");
    this.primaryClient = primaryClient;
    this.secondaryClient = secondaryClient;
  }

  Pair<TerrapinClient, TerrapinClient> getClientTuple(RequestOptions options) {
    if (primaryClient == null || secondaryClient == null) {
      return new ImmutablePair(primaryClient == null ? secondaryClient : primaryClient, null);
    }
    boolean primaryFirst = Math.random() < 0.5;
    final TerrapinClient firstClient = options.selectionPolicy == SelectionPolicy.PRIMARY_FIRST ?
        primaryClient : (primaryFirst ? primaryClient : secondaryClient);
    final TerrapinClient secondClient = firstClient == primaryClient ?
              secondaryClient : primaryClient;
    return new ImmutablePair(firstClient, secondClient);
  }

  public Future<TerrapinSingleResponse> getOne(
      final String fileSet, final ByteBuffer key, RequestOptions options) {
    Pair<TerrapinClient, TerrapinClient> clientPair = getClientTuple(options);
    final TerrapinClient firstClient = clientPair.getLeft();
    final TerrapinClient secondClient = clientPair.getRight();

    // We perform the request on the primary cluster with retries to second replica.
    if (secondClient == null) {
      return firstClient.getOne(fileSet, key);
    }
    return FutureUtil.getSpeculativeFuture(firstClient.getOne(fileSet, key),
        new Function0<Future<TerrapinSingleResponse>>() {
          @Override
          public Future<TerrapinSingleResponse> apply() {
            return secondClient.getOneNoRetries(fileSet, key);
          }
        },
        options.speculativeTimeoutMillis,
        "terrapin-get-one");
  }

  public Future<TerrapinResponse> getMany(
      final String fileSet, final Set<ByteBuffer> keys, RequestOptions options) {
    Pair<TerrapinClient, TerrapinClient> clientPair = getClientTuple(options);
    final TerrapinClient firstClient = clientPair.getLeft();
    final TerrapinClient secondClient = clientPair.getRight();

     // We perform the request on the primary cluster with retries to second replica.
    if (secondClient == null) {
      return firstClient.getMany(fileSet, keys);
    }
    return FutureUtil.getSpeculativeFuture(firstClient.getMany(fileSet, keys),
        new Function0<Future<TerrapinResponse>>() {
          @Override
          public Future<TerrapinResponse> apply() {
            return secondClient.getManyNoRetries(fileSet, keys);
          }
        },
        options.speculativeTimeoutMillis,
        "terrapin-get-many");
  }
}