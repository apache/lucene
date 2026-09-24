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

package org.apache.lucene.sandbox.codecs.segmentivf;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.tests.util.LuceneTestCase;

/** Tests the SIMD kernels against the scalar ones, including tail lengths. */
public class TestKernels extends LuceneTestCase {
  public void testHammingMatchesScalarAtEveryShape() {
    Kernels scalar = new Kernels();
    Kernels vector = Kernels.INSTANCE;
    List<Integer> dims = new ArrayList<>(List.of(96, 100, 200, 960, 1000, 1023));
    for (int dim = 1; dim <= 40; dim++) dims.add(dim);
    for (int dim = 128; dim <= 2048; dim += 128) dims.add(dim);
    for (int dim : dims) {
      int codeBytes = Tiers.Nitrox2.bytesPerVector(dim);
      byte[] query = new byte[codeBytes];
      random().nextBytes(query);
      for (int rows = 1; rows <= 9; rows++) {
        int offset = 13;
        byte[] codes = new byte[offset + rows * codeBytes];
        random().nextBytes(codes);
        int[] expected = new int[rows];
        int[] actual = new int[rows];
        scalar.hamming(query, codes, offset, rows, expected);
        vector.hamming(query, codes, offset, rows, actual);
        assertArrayEquals("dim=" + dim + " rows=" + rows, expected, actual);

        try (Arena arena = Arena.ofConfined()) {
          MemorySegment nativeCodes = arena.allocate(codes.length);
          for (int i = 0; i < codes.length; i++) {
            nativeCodes.set(ValueLayout.JAVA_BYTE, i, codes[i]);
          }
          Arrays.fill(actual, 0);
          vector.hamming(query, nativeCodes, offset, rows, actual);
          assertArrayEquals("native dim=" + dim + " rows=" + rows, expected, actual);
          for (int r = 0; r < rows; r++) {
            long at = offset + (long) r * codeBytes;
            assertEquals("single dim=" + dim, expected[r], vector.hamming(query, nativeCodes, at));
          }
        }
      }
    }
  }

  public void testDotProductsInPlaceMatchScalar() {
    Kernels scalar = new Kernels();
    Kernels vector = Kernels.INSTANCE;
    for (int dim : new int[] {1, 7, 8, 9, 31, 64, 100, 128, 384, 1000, 1024}) {
      byte[] query = new byte[dim];
      random().nextBytes(query);
      int rows = 1 + random().nextInt(9), stride = dim + random().nextInt(40);
      byte[] records = new byte[rows * stride];
      random().nextBytes(records);
      int[] expected = new int[rows];
      scalar.dotProducts(query, records, stride, rows, expected);
      try (Arena arena = Arena.ofConfined()) {
        // Records in reverse order at odd addresses, as a rerank reads them from a mapped section.
        MemorySegment segment = arena.allocate(records.length + 3);
        MemorySegment.copy(records, 0, segment, ValueLayout.JAVA_BYTE, 3, records.length);
        long[] offsets = new long[rows];
        for (int r = 0; r < rows; r++) offsets[r] = 3 + (long) (rows - 1 - r) * stride;
        int[] actual = new int[rows];
        vector.dotProducts(query, segment, offsets, rows, actual);
        for (int r = 0; r < rows; r++) {
          assertEquals("dim=" + dim + " row=" + r, expected[rows - 1 - r], actual[r]);
        }
      }
    }
  }
}
