package com.pinterest.terrapin.zookeeper;

import com.google.common.collect.Lists;
import com.pinterest.terrapin.thrift.generated.Options;
import com.pinterest.terrapin.thrift.generated.PartitionerType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Unit test for FileSetInfo class.
 */
public class FileSetInfoTest {
  private static final String FILE_SET_INFO_JSON =
      "{\"fileSetName\":\"some_file_set\"," +
       "\"numVersionsToKeep\":2," +
       "\"servingInfo\":{\"hdfsPath\":\"/terrapin/data/2\"," +
                        "\"helixResource\":\"$terrapin$data$2\"," +
                        "\"numPartitions\":10," +
                        "\"partitionerType\":\"MODULUS\"}," +
       "\"oldServingInfoList\":[" +
           "{\"hdfsPath\":\"/terrapin/data/1\"," +
            "\"helixResource\":\"$terrapin$data$1\"," +
            "\"numPartitions\":100," +
            "\"partitionerType\":\"MODULUS\"}" +
       "]," +
       "\"valid\":true," +
       "\"deleted\":false}";

  private static final FileSetInfo FILE_SET_INFO = new FileSetInfo(
      "some_file_set",
      "/terrapin/data/2",
      10,
      Lists.newArrayList(new FileSetInfo.ServingInfo("/terrapin/data/1",
          "$terrapin$data$1", 100, PartitionerType.MODULUS)),
      new Options().setNumVersionsToKeep(2).setPartitioner(PartitionerType.MODULUS));

  private static final String INVALID_FILE_SET_INFO_JSON =
      "{\"fileSetName\":null," +
       "\"numVersionsToKeep\":0," +
       "\"servingInfo\":null," +
       "\"oldServingInfoList\":[]," +
       "\"valid\":false," +
       "\"deleted\":false}";

  @Test
  public void testJsonParseForValid() throws Exception {
    assertEquals(FILE_SET_INFO, FileSetInfo.fromJson(FILE_SET_INFO_JSON.getBytes()));
  }

  @Test
  public void testJsonSerializeForValid() throws Exception {
    assertEquals(FILE_SET_INFO_JSON, new String(FILE_SET_INFO.toJson()));
  }

  @Test
  public void testJsonParseAndSerializeForInvalid() throws Exception {
    assertEquals(new FileSetInfo(), FileSetInfo.fromJson(INVALID_FILE_SET_INFO_JSON.getBytes()));
    assertEquals(INVALID_FILE_SET_INFO_JSON, new String(new FileSetInfo().toJson()));
  }
}
