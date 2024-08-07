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
import java.util.Optional;
import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorShape;
import jdk.incubator.vector.VectorSpecies;
import org.apache.lucene.store.IndexInput;

final class MemorySegmentPostingDecodingUtil extends PostingDecodingUtil {

  static Optional<PostingDecodingUtil> wrap(IndexInput in, MemorySegment memorySegment) {
    if (PanamaVectorizationProvider.HAS_FAST_INTEGER_VECTORS == false) {
      // No point in using the Vector API if it doesn't actually make things faster.
      return Optional.empty();
    }
    if (LONG_SPECIES.length() > PostingDecodingUtil.PADDING_LONGS + 1) {
      // Required to meet PostingDecodingUtil's contract that we do not read/write more than 7 extra
      // longs.
      return Optional.empty();
    }
    return Optional.of(new MemorySegmentPostingDecodingUtil(in, memorySegment));
  }

  private static final VectorSpecies<Long> LONG_SPECIES =
      VectorSpecies.of(
          long.class, VectorShape.forBitSize(PanamaVectorizationProvider.PREFERRED_VECTOR_BITSIZE));

  private final IndexInput in;
  private final MemorySegment memorySegment;

  private MemorySegmentPostingDecodingUtil(IndexInput in, MemorySegment memorySegment) {
    this.in = in;
    this.memorySegment = memorySegment;
  }

  @Override
  public void splitLongs(int count, long[] b, int bShift, long bMask, long[] c, long cMask)
      throws IOException {
    long offset = in.getFilePointer();
    long endOffset = offset + count * Long.BYTES;
    int i;
    // Note: this loop may apply to more than `count` entries due to the width of the preferred
    // species. But doing this is faster than handling the remainder with scalar code.
    for (i = 0;
        i < count;
        i += LONG_SPECIES.length(), offset += LONG_SPECIES.length() * Long.BYTES) {
      LongVector vector =
          LongVector.fromMemorySegment(
              LONG_SPECIES, memorySegment, offset, ByteOrder.LITTLE_ENDIAN);
      vector
          .lanewise(VectorOperators.LSHR, bShift)
          .lanewise(VectorOperators.AND, bMask)
          .intoArray(b, i);
      vector.lanewise(VectorOperators.AND, cMask).intoArray(c, i);
    }

    in.seek(endOffset);
  }
}
