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

import com.pinterest.terrapin.client.TerrapinClient;
import com.pinterest.terrapin.zookeeper.ZooKeeperManager;

import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.http.HttpServer;

import java.io.IOException;


public class StatusServer extends HttpServer {
  public StatusServer(String name, String bindAddress, int port, boolean findPort,
                      String clusterName, ZooKeeperManager zkManager, DFSClient hdfsClient,
                      TerrapinClient sampleClient)
      throws IOException {
    super(name, bindAddress, port, findPort);
    this.setAttribute("cluster_name", clusterName);
    this.setAttribute("zookeeper-manager", zkManager);
    this.setAttribute("hdfs-client", hdfsClient);
    this.setAttribute("sample-client", sampleClient);

    this.addServlet("cluster-status", ClusterStatusServlet.BASE_URI, ClusterStatusServlet.class);
    this.addServlet("fileset-status", FileSetStatusServlet.BASE_URI + "/*",
        FileSetStatusServlet.class);
    this.addServlet("lookup-keys", LookupKeyServlet.BASE_URI, LookupKeyServlet.class);
  }
}
