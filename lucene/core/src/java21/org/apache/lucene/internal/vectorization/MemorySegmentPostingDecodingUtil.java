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
import java.lang.foreign.MemorySegment;
import java.nio.ByteOrder;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;
import org.apache.lucene.store.IndexInput;

final class MemorySegmentPostingDecodingUtil extends PostingDecodingUtil {

  private static final VectorSpecies<Long> LONG_SPECIES =
      PanamaVectorConstants.PRERERRED_LONG_SPECIES;

  private final MemorySegment memorySegment;

  MemorySegmentPostingDecodingUtil(IndexInput in, MemorySegment memorySegment) {
    super(in);
    this.memorySegment = memorySegment;
  }

  private static void shift(
      LongVector vector, int bShift, int dec, int maxIter, long bMask, long[] b, int count, int i) {
    for (int j = 0; j <= maxIter; ++j) {
      vector
          .lanewise(VectorOperators.LSHR, bShift - j * dec)
          .lanewise(VectorOperators.AND, bMask)
          .intoArray(b, count * j + i);
    }
  }

  @Override
  public void splitLongs(
      int count, long[] b, int bShift, int dec, long bMask, long[] c, int cIndex, long cMask)
      throws IOException {
    if (count < LONG_SPECIES.length()) {
      // Not enough data to vectorize without going out-of-bounds. In practice, this branch is never
      // used if the bit width is 256, and is used for 2 and 3 bits per value if the bit width is
      // 512.
      super.splitLongs(count, b, bShift, dec, bMask, c, cIndex, cMask);
      return;
    }

    int maxIter = (bShift - 1) / dec;
    long offset = in.getFilePointer();
    long endOffset = offset + count * Long.BYTES;
    int loopBound = LONG_SPECIES.loopBound(count - 1);
    for (int i = 0;
        i < loopBound;
        i += LONG_SPECIES.length(), offset += LONG_SPECIES.length() * Long.BYTES) {
      LongVector vector =
          LongVector.fromMemorySegment(
              LONG_SPECIES, memorySegment, offset, ByteOrder.LITTLE_ENDIAN);
      shift(vector, bShift, dec, maxIter, bMask, b, count, i);
      vector.lanewise(VectorOperators.AND, cMask).intoArray(c, cIndex + i);
    }

    // Handle the tail by reading a vector that is aligned with `count` on the right side.
    int i = count - LONG_SPECIES.length();
    offset = endOffset - LONG_SPECIES.length() * Long.BYTES;
    LongVector vector =
        LongVector.fromMemorySegment(LONG_SPECIES, memorySegment, offset, ByteOrder.LITTLE_ENDIAN);
    shift(vector, bShift, dec, maxIter, bMask, b, count, i);
    vector.lanewise(VectorOperators.AND, cMask).intoArray(c, cIndex + i);

    in.seek(endOffset);
  }
}
