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
