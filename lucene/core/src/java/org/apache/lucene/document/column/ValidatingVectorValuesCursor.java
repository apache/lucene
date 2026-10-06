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

import org.apache.lucene.index.VectorSimilarityFunction;

/**
 * Wraps the cursor of a DENSE {@link VectorColumn} and validates every vector as the vectors writer
 * consumes it, applying the same checks as the tuple-cursor path: dimension, non-finite values, and
 * zero vectors with {@link VectorSimilarityFunction#COSINE COSINE}.
 *
 * <p>Validation travels with the cursor rather than running inline in the indexing chain because
 * dense vectors are handed to {@link
 * org.apache.lucene.codecs.KnnFieldVectorsWriter#addDenseValues}, which is implemented by pluggable
 * codecs; the indexing chain cannot rely on each codec to validate. Wrapping keeps validation to a
 * single pass over the data, however the writer consumes it.
 *
 * <p>Vectors returned by {@link #next()} are checked for dimension and value. Vectors produced by
 * {@link #fill} are checked in place in the writer's buffer; their per-vector length is the
 * responsibility of the wrapped cursor's {@code fill}.
 *
 * @param <T> the vector array type
 * @lucene.internal
 */
public abstract class ValidatingVectorValuesCursor<T> extends VectorValuesCursor<T> {

  private final VectorColumn<?> column;
  private final VectorValuesCursor<T> in;
  private int consumed;

  private ValidatingVectorValuesCursor(VectorColumn<?> column, VectorValuesCursor<T> in) {
    super(in.size(), in.dimension());
    this.column = column;
    this.in = in;
  }

  /**
   * Wraps a {@code byte[]} cursor for a {@link org.apache.lucene.index.VectorEncoding#BYTE} field.
   */
  public static ValidatingVectorValuesCursor<byte[]> ofBytes(
      VectorColumn<?> column,
      VectorValuesCursor<byte[]> in,
      VectorSimilarityFunction similarityFunction) {
    return new ValidatingVectorValuesCursor<>(column, in) {
      @Override
      int length(byte[] vector) {
        return vector.length;
      }

      @Override
      void checkValue(byte[] data, int offset, int batchDocID) {
        ColumnValidation.checkByteVectorValue(
            column, data, offset, dimension(), similarityFunction, batchDocID);
      }
    };
  }

  /**
   * Wraps a {@code short[]} cursor for a {@link org.apache.lucene.index.VectorEncoding#FLOAT16}
   * field.
   */
  public static ValidatingVectorValuesCursor<short[]> ofFloat16s(
      VectorColumn<?> column,
      VectorValuesCursor<short[]> in,
      VectorSimilarityFunction similarityFunction) {
    return new ValidatingVectorValuesCursor<>(column, in) {
      @Override
      int length(short[] vector) {
        return vector.length;
      }

      @Override
      void checkValue(short[] data, int offset, int batchDocID) {
        ColumnValidation.checkFloat16VectorValue(
            column, data, offset, dimension(), similarityFunction, batchDocID);
      }
    };
  }

  /**
   * Wraps a {@code float[]} cursor for a {@link org.apache.lucene.index.VectorEncoding#FLOAT32}
   * field.
   */
  public static ValidatingVectorValuesCursor<float[]> ofFloats(
      VectorColumn<?> column,
      VectorValuesCursor<float[]> in,
      VectorSimilarityFunction similarityFunction) {
    return new ValidatingVectorValuesCursor<>(column, in) {
      @Override
      int length(float[] vector) {
        return vector.length;
      }

      @Override
      void checkValue(float[] data, int offset, int batchDocID) {
        ColumnValidation.checkFloatVectorValue(
            column, data, offset, dimension(), similarityFunction, batchDocID);
      }
    };
  }

  abstract int length(T vector);

  abstract void checkValue(T data, int offset, int batchDocID);

  /** Number of vectors consumed so far through {@link #next()} and {@link #fill}. */
  public int consumed() {
    return consumed;
  }

  private void checkRemaining(int count) {
    if (count < 0 || count > size() - consumed) {
      throw new IllegalStateException(
          "cannot consume "
              + count
              + " vectors from dense column \""
              + column.name()
              + "\": "
              + consumed
              + " of "
              + size()
              + " already consumed");
    }
  }

  @Override
  public T next() {
    checkRemaining(1);
    T vector = in.next();
    ColumnValidation.checkVectorDimension(column, length(vector), dimension(), consumed);
    checkValue(vector, 0, consumed);
    consumed++;
    return vector;
  }

  @Override
  public void fill(T dst, int dstOffset, int count) {
    checkRemaining(count);
    in.fill(dst, dstOffset, count);
    final int dimension = dimension();
    for (int i = 0; i < count; i++) {
      checkValue(dst, dstOffset + i * dimension, consumed + i);
    }
    consumed += count;
  }
}
