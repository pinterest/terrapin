package com.pinterest.terrapin.zookeeper;

import com.pinterest.terrapin.Constants;
import org.codehaus.jackson.map.ObjectMapper;

/**
 * Cluster level settings such as HDFS replication factor/HDFS namenode address etc.
 * used by loading jobs etc. for writing data into the cluster.
 */
public class ClusterInfo {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  // The HDFS namenode in the format host:port.
  public String hdfsNameNode;

  // The hdfs replication factor - this needs to be set from the distcp job.
  public int hdfsReplicationFactor;

  public ClusterInfo() {
    this.hdfsNameNode = "";
    this.hdfsReplicationFactor = Constants.DEFAULT_HDFS_REPLICATION;
  }

  public ClusterInfo(String hdfsNameNode,
                     int hdfsReplicationFactor) {
    this.hdfsNameNode = hdfsNameNode;
    this.hdfsReplicationFactor = hdfsReplicationFactor;
  }

  public byte[] toJson() throws Exception {
    return OBJECT_MAPPER.writeValueAsBytes(this);
  }

  public static ClusterInfo fromJson(byte[] json) throws Exception {
    return OBJECT_MAPPER.readValue(json, ClusterInfo.class);
  }
}
