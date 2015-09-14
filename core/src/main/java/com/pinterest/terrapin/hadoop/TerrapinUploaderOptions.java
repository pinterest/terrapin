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
package com.pinterest.terrapin.hadoop;

import com.google.common.base.Preconditions;
import com.pinterest.terrapin.thrift.generated.Options;
import com.pinterest.terrapin.thrift.generated.PartitionerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by varun on 8/24/15.
 */
public class TerrapinUploaderOptions {
  private static final Logger LOG = LoggerFactory.getLogger(TerrapinUploaderOptions.class);

  public String terrapinZkQuorum;
  public String terrapinNamenode;
  public String terrapinCluster;
  public String terrapinFileSet;
  public Options loadOptions;

  public TerrapinUploaderOptions() {
    loadOptions = new Options();
  }

  public static TerrapinUploaderOptions initFromSystemProperties() {
      Properties properties = System.getProperties();
    TerrapinUploaderOptions options = new TerrapinUploaderOptions();

    options.terrapinZkQuorum = properties.getProperty("terrapin.zk_quorum");
    options.terrapinNamenode = properties.getProperty("terrapin.namenode");
    options.terrapinCluster = properties.getProperty("terrapin.cluster");

    options.terrapinFileSet = properties.getProperty("fileset");
    if (options.terrapinFileSet != null) {
      LOG.warn("Property fileset is deprecated. Please use terrapin.fileset.");
    } else {
      options.terrapinFileSet = properties.getProperty("terrapin.fileset");
    }
    String s = properties.getProperty("terrapin.num_versions");
    try {
      if (s != null) {
        options.loadOptions.setNumVersionsToKeep(Integer.parseInt(s));
      }
    } catch (NumberFormatException e) {
      LOG.error("Invalid num versions : " + s + ". Using num versions as 1.");
      options.loadOptions.setNumVersionsToKeep(1);
    }
    options.loadOptions.setPartitioner(Enum.valueOf(PartitionerType.class,
        properties.getProperty("terrapin.partitioner", "MODULUS")));
    return options;
  }

  public void validate() {
    Preconditions.checkNotNull(terrapinZkQuorum);
    Preconditions.checkNotNull(terrapinCluster);
    Preconditions.checkNotNull(terrapinFileSet);
  }
}
