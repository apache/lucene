package org.apache.lucene.codecs;

import java.io.IOException;

/**
 * Provides access to a byte vector value.
 *
 * @lucene.experimental
 */
public interface ByteVectorProvider {

  /**
   * Returns the byte vector value for the given target ordinal.
   *
   * @param targetOrd the ordinal of the target vector
   * @return the byte vector value
   */
  byte[] vectorValue(int targetOrd) throws IOException;
}
