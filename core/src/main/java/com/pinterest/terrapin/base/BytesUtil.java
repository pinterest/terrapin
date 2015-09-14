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
package com.pinterest.terrapin.base;

import java.nio.ByteBuffer;

/**
 * Utility functions for dealing with ByteBuffer(s). Includes convenience functions
 * for consuming ByteBuffer(s) without exhausting them.
 */
public class BytesUtil {
  /**
   * Reads the remaining bytes in a ByteBuffer into a byte[] without consuming.
   *
   * @param byteBuffer byte buffer to read from.
   * @return           byte[] containing the bytes read.
   */
  public static byte[] readBytesFromByteBufferWithoutConsume(ByteBuffer byteBuffer) {
    byte[] buffer = new byte[byteBuffer.remaining()];
    byteBuffer.duplicate().get(buffer);
    return buffer;
  }

  /**
   * Reads the bytes in a ByteBuffer into a byte[]
   *
   * @param byteBuffer byte buffer to read from.
   * @return           byte[] containing the bytes read.
   */
  public static byte[] readBytesFromByteBuffer(ByteBuffer byteBuffer) {
    byte[] buffer = new byte[byteBuffer.remaining()];
    byteBuffer.get(buffer);
    return buffer;
  }

}
