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

package org.apache.lucene.index;

import java.io.IOException;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;

/**
 * Provides the common functionality to be implemented by the different classes like {@link
 * ByteVectorValues} and {@link FloatVectorValues}, providing access to per-document float/byte
 * vector values indexed as {@link KnnByteVectorField} or {@link KnnFloatVectorField}
 */
public interface VectorValues {

  /** Return the dimension of the vectors */
  int dimension();

  /**
   * Return the number of vectors for this field.
   *
   * @return the number of vectors returned by this iterator
   */
  int size();

  /**
   * Return the vector value (byte/float) for the current document ID. It is illegal to call this
   * method when the iterator is not positioned: before advancing, or after failing to advance. The
   * caller needs to cast the returned value correctly into byte[] or float[]. The returned array
   * may be shared across calls, re-used, and modified as the iterator advances.
   *
   * @return the vector value (byte/float)
   */
  default Object vectorValue() throws IOException {
    if (this instanceof FloatVectorValues) {
      return vectorFloatValue();
    } else {
      return vectorByteValue();
    }
  }

  /**
   * Return the byte vector value for the current document ID. It is illegal to call this method
   * when the iterator is not positioned: before advancing, or after failing to advance. The
   * returned array may be shared across calls, re-used, and modified as the iterator advances.
   *
   * @return the byte vector value
   */
  default byte[] vectorByteValue() throws IOException {
    throw new UnsupportedOperationException();
  }

  /**
   * Return the float vector value for the current document ID. It is illegal to call this method
   * when the iterator is not positioned: before advancing, or after failing to advance. The
   * returned array may be shared across calls, re-used, and modified as the iterator advances.
   *
   * @return the float vector value
   */
  default float[] vectorFloatValue() throws IOException {
    throw new UnsupportedOperationException();
  }
}
