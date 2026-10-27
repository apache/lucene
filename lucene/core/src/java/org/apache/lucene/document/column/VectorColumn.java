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

import org.apache.lucene.index.IndexableFieldType;
import org.apache.lucene.index.VectorEncoding;

/**
 * A {@link Column} that provides KNN vector values. Vector columns are vector-only: the field type
 * must declare {@code vectorDimension() > 0}, and must not also set doc values, points, stored, or
 * {@code indexOptions}. Vectors are single-valued, so the tuple cursor yields strictly increasing
 * batch-local doc-ids.
 *
 * <p>The type parameter {@code T} must match {@link IndexableFieldType#vectorEncoding()}: {@code
 * float[]} for {@link VectorEncoding#FLOAT32 FLOAT32}, {@code short[]} (float16 bits) for {@link
 * VectorEncoding#FLOAT16 FLOAT16}, and {@code byte[]} for {@link VectorEncoding#BYTE BYTE}. A
 * mismatch is reported when values are consumed during indexing, as a {@link ClassCastException}
 * or, when a dense cursor bulk-fills a writer-owned buffer, an {@link ArrayStoreException}.
 *
 * <p>{@link Column.Density#DENSE DENSE} indicates that every batch-local doc has a vector; {@link
 * Column.Density#SPARSE SPARSE} allows gaps. {@link #tuples()} is always available and is used for
 * sparse columns. {@link #values()} is a bulk cursor over consecutive doc-ids; it must be
 * overridden when {@link #density()} is {@link Column.Density#DENSE DENSE} and is only consulted in
 * that case.
 *
 * @param <T> the vector array type: {@code float[]}, {@code short[]} or {@code byte[]}
 * @lucene.experimental
 */
public abstract class VectorColumn<T> extends Column {

  /**
   * Creates a VectorColumn with the given field name, type, and density.
   *
   * @throws IllegalArgumentException if {@code fieldType.vectorDimension() <= 0}
   */
  protected VectorColumn(String name, IndexableFieldType fieldType, Density density) {
    super(name, fieldType, density);
    if (fieldType.vectorDimension() <= 0) {
      throw new IllegalArgumentException(
          "VectorColumn \""
              + name
              + "\" requires fieldType.vectorDimension() > 0; got "
              + fieldType.vectorDimension());
    }
  }

  /** Returns a fresh tuple cursor starting at the beginning of the batch. */
  public abstract ObjectTupleCursor<T> tuples();

  /**
   * Returns a fresh values cursor iterating dense vectors for doc-ids {@code [0, numDocs)}. Must be
   * overridden when {@link Column#density()} is {@link Column.Density#DENSE DENSE}; the default
   * implementation throws {@link UnsupportedOperationException} and is never called for {@link
   * Column.Density#SPARSE SPARSE} columns.
   */
  public VectorValuesCursor<T> values() {
    throw new UnsupportedOperationException(
        "values() requires density() == DENSE for column \"" + name() + "\"");
  }
}
