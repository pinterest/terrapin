package com.pinterest.terrapin.hadoop;

import static org.junit.Assert.assertEquals;

import com.pinterest.terrapin.thrift.generated.PartitionerType;

import org.junit.Test;

import java.util.Properties;

public class TerrapinUploaderOptionsTest {

  @Test
  public void testInitFromSystemProperties() {
    Properties props = new Properties();
    props.put("terrapin.zk_quorum", "test_quorum");
    props.put("terrapin.namenode", "test_namenode");
    props.put("terrapin.cluster", "test_cluster");
    props.put("terrapin.fileset", "test_fileset");
    props.put("terrapin.num_versions", "4");
    props.put("terrapin.partitioner", PartitionerType.CASCADING.name());
    System.setProperties(props);

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
