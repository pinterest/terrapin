package com.pinterest.terrapin.base;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import java.nio.ByteBuffer;

public class BytesUtilTest {

  @Test
  public void testReadBytesFromByteBufferWithoutConsume() {
    byte[] expectedBytes = "hello world".getBytes();
    ByteBuffer bb = ByteBuffer.wrap(expectedBytes);
    byte[] returnedBytes = BytesUtil.readBytesFromByteBufferWithoutConsume(bb);
    assertArrayEquals(expectedBytes, returnedBytes);
    assertEquals(0, bb.position());
  }

  @Test
  public void testReadBytesFromByteBuffer() {
    byte[] expectedBytes = "hello world".getBytes();
    ByteBuffer bb = ByteBuffer.wrap(expectedBytes);
    byte[] returnedBytes = BytesUtil.readBytesFromByteBuffer(bb);
    assertArrayEquals(expectedBytes, returnedBytes);
    assertEquals(expectedBytes.length, bb.position());
  }
}
