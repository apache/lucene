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
package org.apache.lucene.document.column;

import java.lang.reflect.Array;

/**
 * A values cursor over a dense {@link VectorColumn}. The cursor produces exactly {@link #size()}
 * vectors of {@link #dimension()} elements each, for consecutive batch-local doc-ids starting at 0.
 * Vectors are consumed one at a time with {@link #next()} or in bulk with {@link #fill}.
 *
 * <p>The type parameter {@code T} is the vector array type: {@code float[]}, {@code short[]}
 * (float16 bits) or {@code byte[]}, matching the column's {@link VectorColumn} type parameter.
 *
 * <p>Implementations must throw an exception if more than {@link #size()} vectors are consumed
 * across {@link #next()} and {@link #fill}.
 *
 * @param <T> the vector array type
 * @lucene.experimental
 */
public abstract class VectorValuesCursor<T> {

  private final int size;
  private final int dimension;

  /**
   * Creates a cursor that will produce exactly {@code size} vectors of {@code dimension} elements,
   * one per batch-local doc-id in {@code [0, size)}. Both are fixed for the cursor's lifetime:
   * {@code size} must equal the dense column's {@code numDocs} and {@code dimension} must equal the
   * field type's {@code vectorDimension()}.
   *
   * <p>Lucene's internal indexing paths will not consume past {@code size} across {@link #next()}
   * and {@link #fill}. Defensive throws on overrun are still encouraged to catch misuse from
   * external callers.
   */
  protected VectorValuesCursor(int size, int dimension) {
    if (size < 0) {
      throw new IllegalArgumentException("size must be >= 0; got " + size);
    }
    if (dimension <= 0) {
      throw new IllegalArgumentException("dimension must be > 0; got " + dimension);
    }
    this.size = size;
    this.dimension = dimension;
  }

  /** Total number of vectors this cursor will produce. */
  public final int size() {
    return size;
  }

  /** Number of elements in each vector. */
  public final int dimension() {
    return dimension;
  }

  /**
   * Returns the next vector, which must have exactly {@link #dimension()} elements. The returned
   * array may be reused by the cursor, so it is only valid until the next call to {@link #next()}
   * or {@link #fill}. Must not be called more than {@link #size()} times.
   */
  public abstract T next();

  /**
   * Bulk-fill the next {@code count} vectors into {@code dst}, advancing the cursor by {@code
   * count}. The vectors are written in flat row-major order: {@code count * dimension()} elements
   * starting at element {@code dstOffset}, so vector {@code i} occupies {@code [dstOffset + i *
   * dimension(), dstOffset + (i + 1) * dimension())}. Combined {@link #next()} and {@code fill}
   * calls must not consume more than {@link #size()} vectors.
   *
   * <p>The default implementation calls {@link #next()} in a loop, checks that each vector has
   * {@link #dimension()} elements, and copies it with {@link System#arraycopy}. Override to provide
   * a more efficient bulk fill, for example a single {@link System#arraycopy} from a flat backing
   * array; such overrides take responsibility for every vector having {@link #dimension()}
   * elements, since the indexing chain only validates the values written to {@code dst}.
   *
   * @throws IllegalArgumentException if a vector returned by {@link #next()} does not have {@link
   *     #dimension()} elements
   */
  public void fill(T dst, int dstOffset, int count) {
    for (int i = 0; i < count; i++) {
      T vector = next();
      int length = Array.getLength(vector);
      if (length != dimension) {
        throw new IllegalArgumentException(
            "expected dimension " + dimension + " but got vector of length " + length);
      }
      System.arraycopy(vector, 0, dst, dstOffset + i * dimension, dimension);
    }
  }
}
