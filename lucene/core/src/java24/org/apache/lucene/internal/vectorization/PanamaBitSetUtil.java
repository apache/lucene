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
package org.apache.lucene.internal.vectorization;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;

public class PanamaBitSetUtil extends BitSetUtil {

  static final PanamaBitSetUtil INSTANCE = new PanamaBitSetUtil();

  private static final byte[] IDENTITY_BYTES = new byte[64];

  static {
    for (int i = 0; i < IDENTITY_BYTES.length; i++) {
      IDENTITY_BYTES[i] = (byte) i;
    }
  }

  PanamaBitSetUtil() {}

  @Override
  @SuppressWarnings("fallthrough")
  int word2Array(long word, int base, int[] docs, int offset) {
    if (PanamaVectorConstants.PREFERRED_VECTOR_BITSIZE != 512) {
      return super.word2Array(word, base, docs, offset);
    }

    if (word == 0L) {
      return offset;
    }

    int bitCount = Long.bitCount(word);

    VectorMask<Byte> mask = VectorMask.fromLong(ByteVector.SPECIES_512, word);
    ByteVector indices =
        ByteVector.fromArray(ByteVector.SPECIES_512, IDENTITY_BYTES, 0).compress(mask);

    switch ((bitCount - 1) >> 4) {
      case 3:
        indices
            .convert(VectorOperators.B2I, 3)
            .reinterpretAsInts()
            .add(base)
            .intoArray(docs, offset + 48);
      case 2:
        indices
            .convert(VectorOperators.B2I, 2)
            .reinterpretAsInts()
            .add(base)
            .intoArray(docs, offset + 32);
      case 1:
        indices
            .convert(VectorOperators.B2I, 1)
            .reinterpretAsInts()
            .add(base)
            .intoArray(docs, offset + 16);
      case 0:
        indices
            .convert(VectorOperators.B2I, 0)
            .reinterpretAsInts()
            .add(base)
            .intoArray(docs, offset);
        break;
      default:
        throw new IllegalStateException(bitCount + "");
    }

    return offset + bitCount;
  }
}
