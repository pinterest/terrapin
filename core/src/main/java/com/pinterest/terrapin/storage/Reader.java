package com.pinterest.terrapin.storage;

import com.twitter.util.Future;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * Creates a Reader for performing lookups on underlying files such as HFiles.
 */
public interface Reader {
  /**
   * Returns a map from key to union of value or exception (if an error occurs).
   * If a higher level error occurs, it will throw an exception.
   *
   * @param keyList The list of keys to query.
   * @return A map from key to either the value or an exception (if an error occurs.
   */
  public Future<Map<ByteBuffer, Pair<ByteBuffer, Throwable>>> getValues(
      List<ByteBuffer> keyList) throws Throwable;

  /**
   * Closes the reader releasing underlying resources.
   * @throws IOException
   */
  public void close() throws IOException;
}
