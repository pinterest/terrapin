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
