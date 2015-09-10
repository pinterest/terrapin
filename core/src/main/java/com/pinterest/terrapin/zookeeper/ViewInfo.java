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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pinterest.terrapin.TerrapinUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.helix.model.ExternalView;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.TreeMap;
import java.util.Collections;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Contains a mapping from partition to list of instances serving the partition. This
 * is needed to reduce amount of data being transferred from zookeeper. It serializes
 * The state as compressed JSON.
 */
public class ViewInfo {
  private static final Logger LOG = LoggerFactory.getLogger(ViewInfo.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @JsonProperty
  private final String resource;
  @JsonProperty
  private Map<String, List<String>> partitionMap;

  public ViewInfo() {
    this.resource = null;
    this.partitionMap = null;
  }

  public ViewInfo(ExternalView externalView) {
    this.resource = externalView.getResourceName();
    Set<String> partitionSet = externalView.getPartitionSet();
    this.partitionMap = Maps.newTreeMap();
    for (String partition : partitionSet) {
      Map<String, String> stateMap = externalView.getStateMap(partition);
      Pair<String, Integer> resourceAndPartitionNum =
          TerrapinUtil.getResourceAndPartitionNum(partition);
      if (resourceAndPartitionNum == null) {
        LOG.warn("Invalid helix partition for " + resource + " : " + partition);
        continue;
      }
      if (stateMap == null) {
        continue;
      }
      List<String> instanceList = Lists.newArrayListWithCapacity(5);
      for (Map.Entry<String, String> entry : stateMap.entrySet()) {
        if (entry.getValue().equals("ONLINE")) {
          instanceList.add(entry.getKey());
        }
      }
      // Keep the list in alphabetical order.
      if (!instanceList.isEmpty()) {
        Collections.sort(instanceList);
        this.partitionMap.put(TerrapinUtil.getViewPartitionName(resource,
                resourceAndPartitionNum.getRight()), instanceList);
      }
    }
  }

  public static ViewInfo fromJson(byte[] json) throws Exception {
    ViewInfo viewInfo = OBJECT_MAPPER.readValue(json, ViewInfo.class);
    for (Map.Entry<String, List<String>> entry : viewInfo.partitionMap.entrySet()) {
      Collections.sort(entry.getValue());
    }
    return viewInfo;
  }

  /**
   * Returns the set of instances for a partition. Note that this would
   */
  public List<String> getInstancesForPartition(String partition) {
    if (this.partitionMap == null || !this.partitionMap.containsKey(partition)) {
      return Lists.newArrayListWithCapacity(0);
    }
    return partitionMap.get(partition);
  }

  public boolean hasOnlinePartitions() {
    return partitionMap != null && partitionMap.size() > 0;
  }

  @JsonIgnore
  public int getNumOnlinePartitions() {
    return partitionMap == null ? 0 : partitionMap.size();
  }

  @JsonIgnore
  public String getResource() {
    return resource;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof ViewInfo)) {
      return false;
    }
    ViewInfo viewInfo = (ViewInfo)o;
    if (!resource.equals(viewInfo.resource)) {
      return false;
    }
    return partitionMap.equals(viewInfo.partitionMap);
  }

  public byte[] toJson() throws Exception {
    return OBJECT_MAPPER.writeValueAsBytes(this);
  }

  public static ViewInfo fromCompressedJson(byte[] compressedJson) throws Exception {
    ByteArrayInputStream in = new ByteArrayInputStream(compressedJson);
    GZIPInputStream zipIs = new GZIPInputStream(in);
    ViewInfo viewInfo = fromJson(IOUtils.toByteArray(zipIs));
    in.close();
    zipIs.close();
    return viewInfo;
  }
    
  public byte[] toCompressedJson() throws Exception {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GZIPOutputStream zipOs = new GZIPOutputStream(out);
    zipOs.write(this.toJson());
    zipOs.close();
    byte[] data = out.toByteArray();
    return data;
  }

  /**
   * To pretty printing JSON format with simplified partition key. The returned JSON string is a
   * ordered object sort by partition key. This function is only used for displaying status in
   * the html page
   * @return JSON string
   * @throws IOException if the dumping process raises any exceptions
   */
  public String toPrettyPrintingJson() throws IOException {
    Map<Integer, List<String>> simplifiedPartitionMap = new TreeMap<Integer, List<String>>();
    for (Map.Entry<String, List<String>> entry : partitionMap.entrySet()) {
      String longPartitionName = entry.getKey();
      int partitionNumber = TerrapinUtil.getViewPartitionNumber(longPartitionName);
      simplifiedPartitionMap.put(partitionNumber, entry.getValue());
    }
    return OBJECT_MAPPER.defaultPrettyPrintingWriter().writeValueAsString(simplifiedPartitionMap);
  }

  /**
   * Check partition is online or not
   * @param partition partition number
   * @return true if partition is online, otherwise, return false
   */
  public boolean isOnlinePartition(String partition) {
    return partitionMap != null && partitionMap.containsKey(partition);
  }
}
