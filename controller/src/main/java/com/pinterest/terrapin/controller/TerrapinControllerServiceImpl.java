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
package com.pinterest.terrapin.controller;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.pinterest.terrapin.Constants;
import com.pinterest.terrapin.TerrapinUtil;
import com.pinterest.terrapin.thrift.generated.ControllerErrorCode;
import com.pinterest.terrapin.thrift.generated.ControllerException;
import com.pinterest.terrapin.thrift.generated.TerrapinController;
import com.pinterest.terrapin.thrift.generated.TerrapinDeleteRequest;
import com.pinterest.terrapin.thrift.generated.TerrapinLoadRequest;
import com.pinterest.terrapin.zookeeper.FileSetInfo;
import com.pinterest.terrapin.zookeeper.ViewInfo;
import com.pinterest.terrapin.zookeeper.ZooKeeperManager;
import com.twitter.util.ExceptionalFunction0;
import com.twitter.util.ExecutorServiceFuturePool;
import com.twitter.util.Future;
import com.twitter.util.FuturePool;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.helix.HelixAdmin;
import org.apache.helix.model.IdealState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of the terrapin controller thrift API.
 */
public class TerrapinControllerServiceImpl implements TerrapinController.ServiceIface {
  private static final Logger LOG = LoggerFactory.getLogger(TerrapinControllerServiceImpl.class);

  private final PropertiesConfiguration configuration;
  private final ZooKeeperManager zkManager;
  private final DFSClient hdfsClient;
  private final HelixAdmin helixAdmin;
  private final String clusterName;

  private final FuturePool futurePool;

  public TerrapinControllerServiceImpl(PropertiesConfiguration configuration,
                                       ZooKeeperManager zkManager,
                                       DFSClient hdfsClient,
                                       HelixAdmin helixAdmin,
                                       String clusterName) {
    this.configuration = configuration;
    this.zkManager = zkManager;
    this.hdfsClient = hdfsClient;
    this.helixAdmin = helixAdmin;
    this.clusterName = clusterName;

    ExecutorService threadPool = new ThreadPoolExecutor(100,
        100,
        0,
        TimeUnit.SECONDS,
        new LinkedBlockingDeque<Runnable>(1000),
        new ThreadFactoryBuilder().setDaemon(false)
                      .setNameFormat("controller-pool-%d")
                      .build());
   this.futurePool = new ExecutorServiceFuturePool(threadPool);
  }

  private static Exception getException(Exception e) {
    if (e instanceof ControllerException) {
      return e;
    } else {
      return new ControllerException(e.toString(), ControllerErrorCode.OTHER);
    }
  }

  @Override
  public Future<Void> loadFileSet(final TerrapinLoadRequest request) {
    return futurePool.apply(new ExceptionalFunction0<Void>() {
      @Override
      public Void applyE() throws Throwable {
        try {
          String hdfsDir = request.getHdfsDirectory();
          LOG.info("Loading resource " + hdfsDir);
          long startTimeMillis = System.currentTimeMillis();
          String resourceName = TerrapinUtil.hdfsDirToHelixResource(hdfsDir);
          FileSetInfo oldFileSetInfo = zkManager.getFileSetInfo(request.getFileSet());
          boolean resourceExists = helixAdmin.getResourcesInCluster(clusterName).contains(
              resourceName);
          if (oldFileSetInfo != null && oldFileSetInfo.servingInfo.hdfsPath.equals(hdfsDir) &&
              resourceExists) {
            LOG.info("Resource and fileset already exist. Not doing anything.");
            return null;
          }
          // If we have already have a resource created, do not recreate the resource. Getting
          // all the resources is the only way to check the existence of a resource, this
          // will need optimization in the future once we have many resources.
          IdealState idealState = ControllerUtil.buildIdealStateForHdfsDir(
              hdfsClient,
              hdfsDir,
              resourceName,
              request.getOptions().getPartitioner(),
              configuration.getInt(Constants.NUM_SERVING_REPLICAS, 3),
              configuration.getBoolean(Constants.ENABLE_ZK_COMPRESSION,
                  Constants.ENABLE_ZK_COMPRESSION_DEFAULT));
          if (idealState.getNumPartitions() != request.getExpectedNumPartitions()) {
            throw new ControllerException(
                "Partition count mismatch for hdfs dir " + hdfsDir +
                " FOUND : " + idealState.getNumPartitions() + " EXPECTED : " +
                request.getExpectedNumPartitions(),
                ControllerErrorCode.INVALID_DATA_SET);
          }
          try {
            if (idealState.getBucketSize() > 1) {
              if (!resourceExists) {
                // Add the resource without the ideal state - thats the only way to make buckets
                // work with the addResource call.
                helixAdmin.addResource(clusterName,
                                       resourceName,
                                       idealState.getNumPartitions(),
                                       "OnlineOffline",
                                       "CUSTOMIZED",
                                       idealState.getBucketSize());
              }
              helixAdmin.setResourceIdealState(clusterName, resourceName, idealState);
            } else {
              if (!resourceExists) {
                helixAdmin.addResource(clusterName, resourceName, idealState);
              } else {
                helixAdmin.setResourceIdealState(clusterName, resourceName, idealState);
              }
            }
          } catch (Exception e) {
            LOG.warn("Resource creation failed for " + resourceName + ", rolling back.", e);
            throw new ControllerException("Resource creation failure for " +
                resourceName, ControllerErrorCode.HELIX_ERROR);
          }
          LOG.info("Time taken for building resource for " + hdfsDir + ": " +
                   (System.currentTimeMillis() - startTimeMillis) + "ms.");
          LOG.info("New resource " + resourceName);
          // Wait for loading of the resource.
          long startLoadTimeMillis = System.currentTimeMillis();
          boolean isLoaded = false;
          while (!isLoaded) {
            long timeElapsedMillis = System.currentTimeMillis() - startLoadTimeMillis;
            if (timeElapsedMillis > Constants.LOAD_TIMEOUT_SECONDS * 1000) {
              throw new ControllerException("Timeout while trying to online resource " +
                  resourceName, ControllerErrorCode.SHARD_LOAD_TIMEOUT);
            }
            Thread.sleep(5000);
            timeElapsedMillis = System.currentTimeMillis() - startLoadTimeMillis;
            ViewInfo viewInfo = zkManager.getViewInfo(resourceName);
            if (viewInfo == null) {
              continue;
            }
            int numLoaded = viewInfo.getNumOnlinePartitions();
            if (numLoaded == idealState.getNumPartitions()) {
              LOG.info("Onlined " + resourceName +
                       " in " + timeElapsedMillis + "ms.");
              isLoaded = true;
            } else {
              LOG.info("Onlined " + numLoaded + "/" + idealState.getNumPartitions() +
                       " partitions for resource " + resourceName + " in " +
                       timeElapsedMillis + "ms.");
            }
          }
          // Wait for a couple of seconds just for the zk watches to propagate.
          Thread.sleep(2000);
          List<FileSetInfo.ServingInfo> oldServingInfoList = null;

          if (oldFileSetInfo == null) {
            oldServingInfoList = Lists.newArrayList();
          } else {
             // Drop the oldest path from the oldFileSetInfo.
            oldServingInfoList = oldFileSetInfo.oldServingInfoList;
            oldServingInfoList.add(0, oldFileSetInfo.servingInfo);
            while (oldServingInfoList.size() >= request.getOptions().getNumVersionsToKeep() &&
                   !oldServingInfoList.isEmpty()) {
              oldServingInfoList.remove(oldServingInfoList.size() - 1);
            }
          }
          FileSetInfo fileSetInfo = new FileSetInfo(request.getFileSet(),
              hdfsDir,
              idealState.getNumPartitions(),
              oldServingInfoList,
              request.getOptions());
          zkManager.setFileSetInfo(request.getFileSet(), fileSetInfo);
        } catch (Exception e) {
          LOG.warn("Exception while loading.", e);
          throw getException(e);
        }
        return null;
      }
    });
  }
}
