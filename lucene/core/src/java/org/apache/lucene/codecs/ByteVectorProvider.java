/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.codecs;

import java.io.IOException;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

/**
 * Provides access to a byte vector value.
 *
 * @lucene.experimental
 */
public interface ByteVectorProvider {

  /**
   * Returns the dimension of the byte vector.
   *
   * @return the dimension of the byte vector
   */
  int dimension();

  /**
   * Returns the byte vector value for the given target ordinal.
   *
   * @param targetOrd the ordinal of the target vector
   * @return the byte vector value
   */
  byte[] vectorValue(int targetOrd) throws IOException;

  /**
   * Returns the copy of the byte vector provider. This is useful for on-heap caching of the float
   * vector provider if on heap calculations are required
   *
   * @return the copy of the float vector provider
   * @throws IOException if an I/O error occurs
   */
  ByteVectorProvider copy() throws IOException;

  /** Returns a {@link ByteVectorProvider} from the given {@link RandomAccessVectorValues}. */
  static ByteVectorProvider fromRandomAccessVectorValues(
      final RandomAccessVectorValues<byte[]> vectors) {
    return new ByteVectorProvider() {
      @Override
      public byte[] vectorValue(int targetOrd) throws IOException {
        return vectors.vectorValue(targetOrd);
      }

      @Override
      public ByteVectorProvider copy() throws IOException {
        return fromRandomAccessVectorValues(vectors.copy());
      }

      @Override
      public int dimension() {
        return vectors.dimension();
      }
    };
  }
}
