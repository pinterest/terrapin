package com.pinterest.terrapin.controller;

import com.pinterest.terrapin.zookeeper.ZooKeeperManager;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.http.HttpServer;

import java.io.IOException;


public class StatusServer extends HttpServer {
  public StatusServer(String name, String bindAddress, int port, boolean findPort,
                      String clusterName, ZooKeeperManager zkManager, DFSClient hdfsClient)
      throws IOException {
    super(name, bindAddress, port, findPort);
    this.setAttribute("cluster_name", clusterName);
    this.setAttribute("zookeeper-manager", zkManager);
    this.setAttribute("hdfs-client", hdfsClient);

    this.addServlet("cluster-status", ClusterStatusServlet.BASE_URI, ClusterStatusServlet.class);
    this.addServlet("fileset-status", FileSetStatusServlet.BASE_URI + "/*",
        FileSetStatusServlet.class);
  }
}
