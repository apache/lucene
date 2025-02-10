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

import java.io.IOException;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.apache.lucene.store.IndexInput;

/**
 * Utility using Vector API to decode BKD doc ids.
 *
 * <p>We need this because BKD leaves size are variable, which prevents JIT auto-vectorize the
 * decoding loops for {@link BKDDecodingUtil#decodeDelta16} and the remainder decoding in {@link
 * BKDDecodingUtil#decode24}.
 */
final class BKDVectorDecodingUtil extends BKDDecodingUtil {

  private static final VectorSpecies<Integer> INT_SPECIES =
      PanamaVectorConstants.PRERERRED_INT_SPECIES;

  BKDVectorDecodingUtil() {
    super();
  }

  @Override
  public void decodeDelta16(IndexInput in, int[] docIds, int count) throws IOException {
    final int min = in.readVInt();
    final int halfLen = count >> 1;
    in.readInts(docIds, 0, halfLen);
    final int upperBound = INT_SPECIES.loopBound(halfLen);
    int i = 0;
    for (; i < upperBound; i += INT_SPECIES.length()) {
      IntVector vector = IntVector.fromArray(INT_SPECIES, docIds, i);
      vector
          .lanewise(VectorOperators.LSHR, 16)
          .lanewise(VectorOperators.ADD, min)
          .intoArray(docIds, i);
      vector
          .lanewise(VectorOperators.AND, 0xFFFF)
          .lanewise(VectorOperators.ADD, min)
          .intoArray(docIds, i + halfLen);
    }
    for (; i < halfLen; ++i) {
      int l = docIds[i];
      docIds[i] = (l >>> 16) + min;
      docIds[halfLen + i] = (l & 0xFFFF) + min;
    }
    if ((count & 1) == 1) {
      docIds[count - 1] = Short.toUnsignedInt(in.readShort()) + min;
    }
  }

  @Override
  public void decode24(IndexInput in, int[] docIds, int[] scratch, int count) throws IOException {
    final int quarterLen = count >> 2;
    final int halfLen = quarterLen << 1;
    final int quarterLen3 = quarterLen * 3;
    in.readInts(scratch, 0, quarterLen3);
    int upperBound = INT_SPECIES.loopBound(quarterLen3);
    int i = 0;
    for (; i < upperBound; i += INT_SPECIES.length()) {
      IntVector.fromArray(INT_SPECIES, scratch, i)
          .lanewise(VectorOperators.LSHR, 8)
          .intoArray(docIds, i);
    }
    for (; i < quarterLen3; ++i) {
      docIds[i] = scratch[i] >>> 8;
    }

    final int upperBound2 = INT_SPECIES.loopBound(quarterLen);
    for (i = 0; i < upperBound2; i += INT_SPECIES.length()) {
      IntVector.fromArray(INT_SPECIES, scratch, i)
          .lanewise(VectorOperators.AND, 0xFF)
          .lanewise(VectorOperators.LSHL, 16)
          .lanewise(
              VectorOperators.OR,
              IntVector.fromArray(INT_SPECIES, scratch, i + quarterLen)
                  .lanewise(VectorOperators.AND, 0xFF)
                  .lanewise(VectorOperators.LSHL, 8))
          .lanewise(
              VectorOperators.OR,
              IntVector.fromArray(INT_SPECIES, scratch, i + halfLen)
                  .lanewise(VectorOperators.AND, 0xFF))
          .intoArray(docIds, quarterLen3 + i);
    }
    for (; i < quarterLen; i++) {
      docIds[i + quarterLen3] =
          ((scratch[i] & 0xFF) << 16)
              | ((scratch[i + quarterLen] & 0xFF) << 8)
              | (scratch[i + quarterLen * 2] & 0xFF);
    }

    int remainder = count & 0x3;
    if (remainder > 0) {
      in.readInts(docIds, quarterLen << 2, remainder);
    }
  }
}
