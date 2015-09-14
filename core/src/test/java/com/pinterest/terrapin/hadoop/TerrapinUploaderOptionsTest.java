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

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.pinterest.terrapin.thrift.generated.PartitionerType;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Properties;

@RunWith(PowerMockRunner.class)
public class TerrapinUploaderOptionsTest {

  @Test
  @PrepareForTest(TerrapinUploaderOptions.class)
  public void testInitFromSystemProperties() {
    PowerMockito.mockStatic(System.class);
    Properties props = new Properties();
    props.put("terrapin.zk_quorum", "test_quorum");
    props.put("terrapin.namenode", "test_namenode");
    props.put("terrapin.cluster", "test_cluster");
    props.put("terrapin.fileset", "test_fileset");
    props.put("terrapin.num_versions", "4");
    props.put("terrapin.partitioner", PartitionerType.CASCADING.name());
    when(System.getProperties()).thenReturn(props);

    TerrapinUploaderOptions options = TerrapinUploaderOptions.initFromSystemProperties();
    options.validate();

    assertEquals(props.get("terrapin.zk_quorum"), options.terrapinZkQuorum);
    assertEquals(props.get("terrapin.namenode"), options.terrapinNamenode);
    assertEquals(props.get("terrapin.cluster"), options.terrapinCluster);
    assertEquals(props.get("terrapin.fileset"), options.terrapinFileSet);
    assertEquals(Integer.valueOf((String) props.get("terrapin.num_versions")),
        Integer.valueOf(options.loadOptions.getNumVersionsToKeep()));
    assertEquals(props.get("terrapin.partitioner"), options.loadOptions.partitioner.name());
  }
}
