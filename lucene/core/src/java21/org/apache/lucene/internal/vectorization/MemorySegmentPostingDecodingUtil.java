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
import org.apache.lucene.codecs.lucene912.PostingDecodingUtil;
import org.apache.lucene.store.IndexInput;

public class MemorySegmentPostingDecodingUtil extends PostingDecodingUtil {

  private static final VectorSpecies<Long> LONG_SPECIES = VectorSpecies.ofPreferred(long.class);

  private final IndexInput in;
  private final MemorySegment memorySegment;

  MemorySegmentPostingDecodingUtil(IndexInput in, MemorySegment memorySegment) {
    if (64 % LONG_SPECIES.length() != 0) {
      // Required to meet PostingDecodingUtil's contract that we do not write entries past index 64
      // for any `count` in 0..64.
      throw new UnsupportedOperationException(
          "This implementation is only applicable if 64 is a multiple of the length of the preferred long species");
    }
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
