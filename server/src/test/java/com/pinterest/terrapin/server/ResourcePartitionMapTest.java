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
package com.pinterest.terrapin.server;

import com.pinterest.terrapin.storage.Reader;
import com.pinterest.terrapin.thrift.generated.TerrapinGetErrorCode;
import com.pinterest.terrapin.thrift.generated.TerrapinGetException;
import com.twitter.util.Future;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Unit test for ResourcePartitionMap.
 */
public class ResourcePartitionMapTest {
  private ResourcePartitionMap resourcePartitionMap;

  @Before
  public void setUp() {
    this.resourcePartitionMap = new ResourcePartitionMap();
  }

  static class TestReader implements Reader {
    @Override
    public Future<Map<ByteBuffer, Pair<ByteBuffer, Throwable>>> getValues(
        List<ByteBuffer> keyList) throws Throwable {
      return null;
    }

    @Override
    public void close() throws IOException {
      // Do nothing.
    }
  }

  private void testGetExceptionErrorCode(String resource,
                                         String partition,
                                         TerrapinGetErrorCode errorCode) {
    boolean gotException = false;
    try {
      resourcePartitionMap.getReader(resource, partition);
    } catch (TerrapinGetException e) {
      if (e.getErrorCode() == errorCode) {
        gotException = true;
      }
    }
    assertTrue(gotException);
  }

  @Test
  public void testAddReader() throws Exception {
    String resource = "resource1";

    TestReader reader1 = new TestReader();
    resourcePartitionMap.addReader(resource, "part1", reader1);
    assertEquals(reader1, resourcePartitionMap.getReader(resource, "part1"));

    // Check that we do not overwrite already existing reader objects.
    TestReader reader2 = new TestReader();
    resourcePartitionMap.addReader(resource, "part1", reader2);
    assertEquals(reader1, resourcePartitionMap.getReader(resource, "part1"));

    // Check that we throw the correct exception for missing partitions.
    testGetExceptionErrorCode(resource, "part2", TerrapinGetErrorCode.NOT_SERVING_PARTITION);

    // Check that we throw the correct exception for resources missing in entirety.
    testGetExceptionErrorCode("resource2", "part1", TerrapinGetErrorCode.NOT_SERVING_RESOURCE);
  }

  @Test
  public void removeReader() throws Exception {
    String resource = "resource1";

    TestReader reader1 = new TestReader();
    resourcePartitionMap.addReader(resource, "part1", reader1);
    TestReader reader2 = new TestReader();
    resourcePartitionMap.addReader(resource, "part2", reader2);

    // Remove reader1.
    assertEquals(reader1, resourcePartitionMap.removeReader(resource, "part1"));

    // Make sure we throw the correct exception if we try to access resource1, part1.
    testGetExceptionErrorCode(resource, "part1", TerrapinGetErrorCode.NOT_SERVING_PARTITION);

    // Remove a partition for which none of the resources exist. Check that we throw
    // UnsupportedOperationException.
    boolean gotUnsupportedOpException = false;
    try {
      resourcePartitionMap.removeReader(resource, "part3");
    } catch (UnsupportedOperationException e) {
      gotUnsupportedOpException = true;
    }
    assertTrue(gotUnsupportedOpException);

    // We should still have access to reader2.
    assertEquals(reader2, resourcePartitionMap.getReader(resource, "part2"));

    // Remove reader2.
    assertEquals(reader2, resourcePartitionMap.removeReader(resource, "part2"));

    // Make sure we throw the correct exception if we try to access resource1, part2.
    // We throw NOT_SERVING_RESOURCE since all partitions for the resource have been removed.
    testGetExceptionErrorCode(resource, "part2", TerrapinGetErrorCode.NOT_SERVING_RESOURCE);
  }
}
