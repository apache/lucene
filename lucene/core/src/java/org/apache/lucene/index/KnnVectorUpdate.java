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

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_HEADER;
import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.apache.lucene.util.BytesRef;

/**
 * An in-place update to a KNN vector field. Modeled structurally on {@link DocValuesUpdate}, but
 * kept as a separate (parallel) hierarchy because the doc-values code paths are keyed on {@link
 * DocValuesType}, which does not apply to vector fields.
 *
 * <p>The {@link Term} selects all matching documents in each segment; every matched document's
 * vector for {@code field} is replaced with the new value. Last-write-wins across multiple updates.
 */
abstract class KnnVectorUpdate {

  /* Rough logic: OBJ_HEADER + 3*PTR + INT
   * Term: OBJ_HEADER + 2*PTR
   *   Term.field: 2*OBJ_HEADER + 4*INT + PTR + string.length*CHAR
   *   Term.bytes: 2*OBJ_HEADER + 2*INT + PTR + bytes.length
   * String: 2*OBJ_HEADER + 4*INT + PTR + string.length*CHAR
   */
  private static final int RAW_SIZE_IN_BYTES =
      8 * NUM_BYTES_OBJECT_HEADER + 8 * NUM_BYTES_OBJECT_REF + 8 * Integer.BYTES;

  final VectorEncoding encoding;
  final Term term;
  final String field;
  // used in BufferedDeletes to apply this update only to a slice of docs. It's initialized to
  // BufferedUpdates.MAX_INT since it's safe and most often used this way.
  final int docIDUpTo;
  // Vector updates always carry a value; removing a vector in place is not supported.
  final boolean hasValue = true;

  protected KnnVectorUpdate(VectorEncoding encoding, Term term, String field, int docIDUpTo) {
    assert docIDUpTo >= 0 : docIDUpTo + " must be >= 0";
    this.encoding = encoding;
    this.term = term;
    this.field = field;
    this.docIDUpTo = docIDUpTo;
  }

  abstract long valueSizeInBytes();

  /**
   * Encodes this update's vector value as a {@link BytesRef} for compact buffering. Float vectors
   * are encoded little-endian (4 bytes per component); byte vectors are stored verbatim. The
   * dimension is implied by the field, so no length prefix is written.
   */
  abstract BytesRef getValueAsBytes();

  /**
   * Decodes a float vector previously encoded by {@link FloatKnnVectorUpdate#getValueAsBytes()}.
   */
  static float[] decodeFloatValue(BytesRef bytes, int dimension) {
    float[] value = new float[dimension];
    ByteBuffer.wrap(bytes.bytes, bytes.offset, bytes.length)
        .order(ByteOrder.LITTLE_ENDIAN)
        .asFloatBuffer()
        .get(value);
    return value;
  }

  /** Decodes a byte vector previously encoded by {@link ByteKnnVectorUpdate#getValueAsBytes()}. */
  static byte[] decodeByteValue(BytesRef bytes, int dimension) {
    byte[] value = new byte[dimension];
    System.arraycopy(bytes.bytes, bytes.offset, value, 0, dimension);
    return value;
  }

  final long sizeInBytes() {
    long sizeInBytes = RAW_SIZE_IN_BYTES;
    sizeInBytes += term.field.length() * (long) Character.BYTES;
    sizeInBytes += term.bytes.bytes.length;
    sizeInBytes += field.length() * (long) Character.BYTES;
    sizeInBytes += valueSizeInBytes();
    return sizeInBytes;
  }

  abstract String valueToString();

  @Override
  public String toString() {
    return "term="
        + term
        + ",field="
        + field
        + ",value="
        + valueToString()
        + ",docIDUpTo="
        + docIDUpTo;
  }

  /** An in-place update to a float ({@link VectorEncoding#FLOAT32}) KNN vector field. */
  static final class FloatKnnVectorUpdate extends KnnVectorUpdate {
    private final float[] value;

    FloatKnnVectorUpdate(Term term, String field, float[] value) {
      this(term, field, value, BufferedUpdates.MAX_INT);
    }

    private FloatKnnVectorUpdate(Term term, String field, float[] value, int docIDUpTo) {
      super(VectorEncoding.FLOAT32, term, field, docIDUpTo);
      this.value = value;
    }

    FloatKnnVectorUpdate prepareForApply(int docIDUpTo) {
      if (docIDUpTo == this.docIDUpTo) {
        return this; // it's a final value so we can safely reuse this instance
      }
      return new FloatKnnVectorUpdate(term, field, value, docIDUpTo);
    }

    float[] getValue() {
      return value;
    }

    @Override
    long valueSizeInBytes() {
      return NUM_BYTES_ARRAY_HEADER + (long) value.length * Float.BYTES;
    }

    @Override
    BytesRef getValueAsBytes() {
      byte[] bytes = new byte[value.length * Float.BYTES];
      ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().put(value);
      return new BytesRef(bytes);
    }

    @Override
    String valueToString() {
      return Arrays.toString(value);
    }
  }

  /** An in-place update to a byte ({@link VectorEncoding#BYTE}) KNN vector field. */
  static final class ByteKnnVectorUpdate extends KnnVectorUpdate {
    private final byte[] value;

    ByteKnnVectorUpdate(Term term, String field, byte[] value) {
      this(term, field, value, BufferedUpdates.MAX_INT);
    }

    private ByteKnnVectorUpdate(Term term, String field, byte[] value, int docIDUpTo) {
      super(VectorEncoding.BYTE, term, field, docIDUpTo);
      this.value = value;
    }

    ByteKnnVectorUpdate prepareForApply(int docIDUpTo) {
      if (docIDUpTo == this.docIDUpTo) {
        return this;
      }
      return new ByteKnnVectorUpdate(term, field, value, docIDUpTo);
    }

    byte[] getValue() {
      return value;
    }

    @Override
    long valueSizeInBytes() {
      return NUM_BYTES_ARRAY_HEADER + value.length;
    }

    @Override
    BytesRef getValueAsBytes() {
      return new BytesRef(value);
    }

    @Override
    String valueToString() {
      return Arrays.toString(value);
    }
  }
}
