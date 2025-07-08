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

package org.apache.lucene.document;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import org.apache.lucene.util.BytesRef;

/**
 * A field for storing multi-vector values for late interaction models.
 *
 * <p>The value is stored as a binary payload, and can be retrieved using {@link
 * LateInteractionField#decode(BytesRef)}. Multi-vectors are expected to have the same dimension for
 * each composing token vector. This is stored along with the token vectors in the first 4 bytes of
 * the payload.
 *
 * <p>Note: This field does not ensure consistency in token vector dimensions for values across
 * documents.
 */
public class LateInteractionField extends BinaryDocValuesField {

  /**
   * Creates a new {@link LateInteractionField} from the provided multi-vector matrix.
   *
   * @param name field name
   * @param value multi-vector value
   */
  public LateInteractionField(String name, float[][] value) {
    super(name, encode(value));
  }

  /**
   * Set multi-vector value for the field.
   *
   * <p>Value should not be null or empty. Composing token vectors for provided multi-vector value
   * should have the same dimension.
   */
  public void setValue(float[][] value) {
    this.fieldsData = encode(value);
  }

  /** Returns the multi-vector value stored in this field */
  public float[][] getValue() {
    return decode((BytesRef) fieldsData);
  }

  /**
   * Encodes provided multi-vector matrix into a {@link BytesRef} that can be stored in the {@link
   * LateInteractionField}.
   *
   * <p>Composing token vectors for the multi-vector are expected to have the same dimension, which
   * is stored along with the token vectors in the first 4 bytes of the payload. Use {@link
   * LateInteractionField#decode(BytesRef)} to retrieve the multi-vector.
   *
   * @param value Multi-Vector to encode
   * @return BytesRef representation for provided multi-vector
   */
  public static BytesRef encode(float[][] value) {
    if (value == null || value.length == 0) {
      throw new IllegalArgumentException("Value should not be null or empty");
    }
    if (value[0] == null || value[0].length == 0) {
      throw new IllegalArgumentException("Composing token vectors should not be null or empty");
    }
    final int tokenVectorDimension = value[0].length;
    final ByteBuffer buffer =
        ByteBuffer.allocate(Integer.BYTES + value.length * tokenVectorDimension * Float.BYTES)
            .order(ByteOrder.LITTLE_ENDIAN);
    // TODO: Should we store dimension in FieldType to ensure consistency across all documents?
    buffer.putInt(tokenVectorDimension);
    FloatBuffer floatBuffer = buffer.asFloatBuffer();
    for (int i = 0; i < value.length; i++) {
      if (value[i].length != tokenVectorDimension) {
        throw new IllegalArgumentException(
            "Composing token vectors should have the same dimension. "
                + "Mismatching dimensions detected between token[0] and token["
                + i
                + "], "
                + value[0].length
                + " != "
                + value[i].length);
      }
      floatBuffer.put(value[i]);
    }
    return new BytesRef(buffer.array());
  }

  /**
   * Decodes provided {@link BytesRef} into a multi-vector matrix.
   *
   * <p>The token vectors are expected to have the same dimension, which is stored along with the
   * token vectors in the first 4 bytes of the payload. Meant to be used as a counterpart to {@link
   * LateInteractionField#encode(float[][])}
   *
   * @param payload to decode into multi-vector value
   * @return decoded multi-vector value
   */
  public static float[][] decode(BytesRef payload) {
    final ByteBuffer buffer = ByteBuffer.wrap(payload.bytes, payload.offset, payload.length);
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    final int tokenVectorDimension = buffer.getInt();
    int numVectors = (payload.length - Integer.BYTES) / (tokenVectorDimension * Float.BYTES);
    if (numVectors * tokenVectorDimension * Float.BYTES + Integer.BYTES != payload.length) {
      throw new IllegalArgumentException(
          "Provided payload does not appear to have been encoded via LateInteractionField.encode. "
              + "Payload length should be equal to 4 + numVectors * tokenVectorDimension, "
              + "got "
              + payload.length
              + " != 4 + "
              + numVectors
              + " * "
              + tokenVectorDimension);
    }
    var floatBuffer = buffer.asFloatBuffer();
    float[][] value = new float[numVectors][];
    for (int i = 0; i < numVectors; i++) {
      value[i] = new float[tokenVectorDimension];
      floatBuffer.get(value[i]);
    }
    return value;
  }
}
