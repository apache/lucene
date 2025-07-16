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

import java.util.stream.IntStream;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

public class PanamaBitSetUtil extends BitSetUtil {

  static final PanamaBitSetUtil INSTANCE = new PanamaBitSetUtil();

  private static final VectorSpecies<Integer> INT_SPECIES =
      PanamaVectorConstants.PRERERRED_INT_SPECIES;
  private static final int MASK = (1 << INT_SPECIES.length()) - 1;
  private static final int[] IDENTITY = IntStream.range(0, Long.SIZE).toArray();
  private static final int[] IDENTITY_MASK = IntStream.range(0, 16).map(i -> 1 << i).toArray();

  PanamaBitSetUtil() {}

  @Override
  int word2Array(long word, int base, int[] docs, int offset) {
    offset = intWord2Array((int) word, docs, offset, base);
    return intWord2Array((int) (word >>> 32), docs, offset, base + 32);
  }

  private static int intWord2Array(int word, int[] resultArray, int offset, int base) {
    IntVector bitMask = IntVector.fromArray(INT_SPECIES, IDENTITY_MASK, 0);

    for (int i = 0; i < Integer.SIZE; i += INT_SPECIES.length()) {
      IntVector.fromArray(INT_SPECIES, IDENTITY, i)
          .add(base)
          .compress(bitMask.and(word).compare(VectorOperators.NE, 0))
          .reinterpretAsInts()
          .intoArray(resultArray, offset);

      offset += Integer.bitCount(word & MASK); // faster than mask.trueCount()
      word >>>= INT_SPECIES.length();
    }

    return offset;
  }
}
