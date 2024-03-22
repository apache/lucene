package org.apache.lucene.codecs;

import java.io.IOException;

/**
 * Provides access to a float vector value.
 *
 * @lucene.experimental
 */
public interface FloatVectorProvider {
  /**
   * Returns the float vector value for the given target ordinal.
   *
   * @param targetOrd the ordinal of the target vector
   * @return the float vector value
   */
  float[] vectorValue(int targetOrd) throws IOException;
}
