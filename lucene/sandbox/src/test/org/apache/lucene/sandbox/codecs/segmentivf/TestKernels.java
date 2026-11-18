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
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.VectorUtil;

/** Tests the SIMD kernels against the scalar ones, including tail lengths. */
public class TestKernels extends LuceneTestCase {
  /** With the vector module present the SIMD kernels load, so these tests compare two codes. */
  public void testSimdKernelsLoadWhenTheVectorModuleIsPresent() {
    boolean simd = ModuleLayer.boot().findModule("jdk.incubator.vector").isPresent();
    assertEquals(simd, Kernels.INSTANCE instanceof Kernels.Panama);
  }

  public void testHammingMatchesScalarAtEveryShape() {
    Kernels scalar = new Kernels();
    Kernels vector = Kernels.INSTANCE;
    List<Integer> dims = new ArrayList<>(List.of(96, 100, 200, 960, 1000, 1023));
    for (int dim = 1; dim <= 40; dim++) {
      dims.add(dim);
    }
    for (int dim = 128; dim <= 2048; dim += 128) {
      dims.add(dim);
    }
    dims.addAll(List.of(4095, SegmentIVFVectorsFormat.MAX_DIMENSIONS));
    for (int dim : dims) {
      // A centroid sign code, and a query leading with one and followed by other bytes.
      int codeBytes = new CoarseCodec.S1(dim).centroidBytes;
      byte[] query = new byte[codeBytes + 17];
      random().nextBytes(query);
      int rows = 1 + random().nextInt(9);
      int offset = 13;
      byte[] codes = new byte[offset + rows * codeBytes];
      random().nextBytes(codes);
      try (Arena arena = Arena.ofConfined()) {
        MemorySegment nativeCodes = arena.allocate(codes.length);
        MemorySegment.copy(codes, 0, nativeCodes, ValueLayout.JAVA_BYTE, 0, codes.length);
        for (int r = 0; r < rows; r++) {
          int at = offset + r * codeBytes;
          int expected = scalar.hamming(query, codeBytes, codes, at);
          assertEquals("dim=" + dim, expected, vector.hamming(query, codeBytes, codes, at));
          assertEquals(
              "native dim=" + dim, expected, vector.hamming(query, codeBytes, nativeCodes, at));
          assertEquals(
              "heap segment dim=" + dim,
              expected,
              vector.hamming(query, codeBytes, MemorySegment.ofArray(codes), at));
        }
      }
    }
  }

  /**
   * At 4096 dimensions every kernel's largest result still fits an int: saturated inputs reach each
   * maximum, on the array, in-place and single-row paths, and on the four-row and tail paths.
   */
  public void testSaturatedCodesAtMaxDimensions() {
    int dim = SegmentIVFVectorsFormat.MAX_DIMENSIONS;
    int rows = 9;
    for (Kernels k : List.of(new Kernels(), Kernels.INSTANCE)) {
      // INT8: 127 * -127 in every dimension.
      byte[] query = new byte[dim];
      Arrays.fill(query, (byte) 127);
      byte[] records = new byte[rows * dim];
      Arrays.fill(records, (byte) -127);
      int[] dots = new int[rows];
      k.dotProducts(query, records, dim, rows, dots);
      int[] expectedDots = new int[rows];
      Arrays.fill(expectedDots, -127 * 127 * dim);
      assertArrayEquals(expectedDots, dots);
      try (Arena arena = Arena.ofConfined()) {
        MemorySegment segment = arena.allocate(records.length);
        MemorySegment.copy(records, 0, segment, ValueLayout.JAVA_BYTE, 0, records.length);
        long[] offsets = new long[rows];
        for (int r = 0; r < rows; r++) {
          offsets[r] = (long) r * dim;
        }
        Arrays.fill(dots, 0);
        k.dotProducts(query, segment, offsets, rows, dots);
        assertArrayEquals(expectedDots, dots);

        // Centroid sign codes: every bit differs.
        int codeBytes = new CoarseCodec.S1(dim).centroidBytes;
        byte[] zeros = new byte[codeBytes];
        MemorySegment ones = arena.allocate((long) rows * codeBytes);
        ones.fill((byte) -1);
        for (int r = 0; r < rows; r++) {
          assertEquals(
              codeBytes * Byte.SIZE, k.hamming(zeros, codeBytes, ones, (long) r * codeBytes));
        }

        // S1 tables: every entry at its maximum, as many code bytes as the short sums hold.
        int bytes = dim / Byte.SIZE;
        byte[] tables = new byte[Kernels.TABLE_BYTES * bytes];
        Arrays.fill(tables, (byte) CoarseCodec.S1.MAX_ENTRY);
        int tableRows = 5 * Kernels.BLOCK_ROWS;
        MemorySegment blocks = arena.allocate((long) bytes * tableRows);
        blocks.fill((byte) 0x5A);
        int[] sums = new int[tableRows];
        // The identity map keeps the bins the sums themselves.
        k.tableBins(tables, 0, bytes, blocks, 0, tableRows, 0f, 1f, Integer.MAX_VALUE, sums);
        int[] expectedSums = new int[tableRows];
        Arrays.fill(expectedSums, 2 * bytes * CoarseCodec.S1.MAX_ENTRY);
        assertArrayEquals(expectedSums, sums);
      }
    }
  }

  public void testDotProductsInPlaceMatchScalar() {
    Kernels scalar = new Kernels();
    Kernels vector = Kernels.INSTANCE;
    for (int dim : new int[] {1, 7, 8, 9, 31, 64, 100, 128, 384, 1000, 1024, 2048, 4095, 4096}) {
      byte[] query = new byte[dim];
      random().nextBytes(query);
      int rows = 1 + random().nextInt(9), stride = dim + random().nextInt(40);
      byte[] records = new byte[rows * stride];
      random().nextBytes(records);
      int[] expected = new int[rows];
      scalar.dotProducts(query, records, stride, rows, expected);
      int[] fromArray = new int[rows];
      vector.dotProducts(query, records, stride, rows, fromArray);
      assertArrayEquals("array dim=" + dim, expected, fromArray);
      try (Arena arena = Arena.ofConfined()) {
        // Records in reverse order at odd addresses, as a rerank reads them from a mapped section.
        MemorySegment segment = arena.allocate(records.length + 3);
        MemorySegment.copy(records, 0, segment, ValueLayout.JAVA_BYTE, 3, records.length);
        long[] offsets = new long[rows];
        for (int r = 0; r < rows; r++) {
          offsets[r] = 3 + (long) (rows - 1 - r) * stride;
        }
        int[] actual = new int[rows];
        vector.dotProducts(query, segment, offsets, rows, actual);
        for (int r = 0; r < rows; r++) {
          assertEquals("dim=" + dim + " row=" + r, expected[rows - 1 - r], actual[r]);
        }
      }
    }
  }

  /** Table sums match the scalar ones in every layout: partial blocks, flushes, both memories. */
  public void testTableSumsMatchScalar() {
    Kernels scalar = new Kernels();
    Kernels vector = Kernels.INSTANCE;
    for (int bytes : new int[] {1, 3, 4, 8, 16, 100, 128, 255, 256, 300, 512, 516}) {
      byte[] tables = new byte[Kernels.TABLE_BYTES * bytes + 7];
      int tablesOffset = 7;
      for (int j = 0; j < bytes; j++) {
        // The low nibble's table, stored twice, then the high nibble's, stored twice.
        int table = tablesOffset + Kernels.TABLE_BYTES * j;
        for (int e = 0; e < 16; e++) {
          tables[table + e] = (byte) random().nextInt(CoarseCodec.S1.MAX_ENTRY + 1);
          tables[table + 16 + e] = tables[table + e];
          tables[table + Kernels.HIGH_TABLE + e] =
              (byte) random().nextInt(CoarseCodec.S1.MAX_ENTRY + 1);
          tables[table + Kernels.HIGH_TABLE + 16 + e] = tables[table + Kernels.HIGH_TABLE + e];
        }
      }
      for (int rows : new int[] {1, 5, 31, 32, 33, 64, 100, 127, 128, 129, 200, 300}) {
        int blocks = (rows + Kernels.BLOCK_ROWS - 1) / Kernels.BLOCK_ROWS;
        int offset = 64;
        byte[] codes = new byte[offset + blocks * bytes * Kernels.BLOCK_ROWS];
        random().nextBytes(codes);
        try (Arena arena = Arena.ofConfined()) {
          MemorySegment segment = arena.allocate(codes.length);
          MemorySegment.copy(codes, 0, segment, ValueLayout.JAVA_BYTE, 0, codes.length);
          int[] expected = new int[rows];
          for (int r = 0; r < rows; r++) {
            expected[r] = scalar.tableSum(tables, tablesOffset, bytes, segment, offset, r);
            // Brute force from the layout: byte j of row r at j * BLOCK_ROWS within its block, at
            // the row's interleaved position: of each 32 rows, the first 16 at even positions and
            // the last 16 at odd ones.
            int block = offset + (r / Kernels.BLOCK_ROWS) * bytes * Kernels.BLOCK_ROWS;
            int position = (r % Kernels.BLOCK_ROWS) / 32 * 32 + 2 * (r % 16) + (r % 32) / 16;
            int sum = 0;
            for (int j = 0; j < bytes; j++) {
              int code = codes[block + j * Kernels.BLOCK_ROWS + position] & 0xFF;
              int table = tablesOffset + Kernels.TABLE_BYTES * j;
              sum += tables[table + (code & 15)];
              sum += tables[table + Kernels.HIGH_TABLE + (code >>> 4)];
            }
            assertEquals("bytes=" + bytes + " row=" + r, sum, expected[r]);
          }
          for (MemorySegment memory : List.of(segment, MemorySegment.ofArray(codes))) {
            int[] actual = new int[rows];
            vector.tableBins(
                tables,
                tablesOffset,
                bytes,
                memory,
                offset,
                rows,
                0f,
                1f,
                Integer.MAX_VALUE,
                actual);
            assertArrayEquals("bytes=" + bytes + " rows=" + rows, expected, actual);
            // A map that clamps at both ends, as S1's does.
            float binBase = 1000.5f, binSlope = -0.37f;
            int maxBin = 900;
            int[] expectedBins = new int[rows];
            for (int r = 0; r < rows; r++) {
              expectedBins[r] = Kernels.tableBin(expected[r], binBase, binSlope, maxBin);
            }
            vector.tableBins(
                tables,
                tablesOffset,
                bytes,
                memory,
                offset,
                rows,
                binBase,
                binSlope,
                maxBin,
                actual);
            assertArrayEquals("bytes=" + bytes + " rows=" + rows, expectedBins, actual);
          }
        }
      }
    }
  }

  public void testHammingOverAPrefix() {
    Kernels scalar = new Kernels();
    Kernels vector = Kernels.INSTANCE;
    for (int length : new int[] {1, 31, 32, 64, 128, 512}) {
      byte[] query = new byte[length + 1000];
      random().nextBytes(query);
      byte[] codes = new byte[3 + length];
      random().nextBytes(codes);
      int expected = 0;
      for (int i = 0; i < length; i++) {
        expected += Integer.bitCount((query[i] ^ codes[3 + i]) & 0xFF);
      }
      assertEquals(expected, scalar.hamming(query, length, codes, 3));
      assertEquals(expected, vector.hamming(query, length, codes, 3));
    }
  }

  /**
   * S1 distances estimate the dot product with the cell's members, from sign codes in blocks and
   * the cell's parameters as the writer keeps them; every row agrees with its block scan.
   */
  public void testS1DistancesEstimateDotProducts() {
    for (int dim : new int[] {24, 100, 384, 1024, 4096}) {
      CoarseCodec.S1 s1 = new CoarseCodec.S1(dim);
      float[] anchor = unit(dim), query = unit(dim);
      // The query leans toward the cell, so some members are near neighbours.
      for (int d = 0; d < dim; d++) {
        query[d] += anchor[d];
      }
      VectorUtil.l2normalize(query);
      int rows = 70;
      byte[] codes = new byte[rows * s1.codeBytes];
      float[][] vectors = new float[rows][];
      float[] shares = new float[s1.parameters()];
      float[] parameters = new float[s1.parameters()];
      for (int r = 0; r < rows; r++) {
        // A cell's members: the centroid plus an offset of similar size for each.
        vectors[r] = unit(dim);
        for (int d = 0; d < dim; d++) {
          vectors[r][d] = anchor[d] + 0.8f * vectors[r][d];
        }
        VectorUtil.l2normalize(vectors[r]);
        s1.encode(vectors[r], anchor, codes, r * s1.codeBytes, shares, 0);
        for (int k = 0; k < shares.length; k++) {
          parameters[k] += shares[k] / rows;
        }
      }
      // The region, block by block, as the writer lays it out.
      int blockBytes = Kernels.BLOCK_ROWS * s1.codeBytes;
      byte[] region = new byte[(int) s1.regionBytes(rows)];
      byte[] block = new byte[blockBytes];
      for (int first = 0; first < rows; first += Kernels.BLOCK_ROWS) {
        int n = Math.min(Kernels.BLOCK_ROWS, rows - first);
        s1.writeBlock(
            ArrayUtil.copyOfSubArray(codes, first * s1.codeBytes, (first + n) * s1.codeBytes),
            n,
            block);
        System.arraycopy(block, 0, region, (first / Kernels.BLOCK_ROWS) * blockBytes, blockBytes);
      }
      byte[] q = new byte[s1.queryBytes];
      s1.encodeQuery(query, q, 0);
      s1.anchorQuery(q, VectorUtil.dotProduct(query, anchor), parameters);
      int[] distances = new int[rows];
      try (Arena arena = Arena.ofConfined()) {
        MemorySegment segment = arena.allocate(region.length);
        MemorySegment.copy(region, 0, segment, ValueLayout.JAVA_BYTE, 0, region.length);
        s1.distances(q, segment, 0, rows, distances);
        for (int r = 0; r < rows; r++) {
          assertEquals(distances[r], s1.distance(q, segment, 0, r));
        }
      }
      // One-bit codes are noisy at low dimension; the error shrinks as 1/sqrt(dim).
      double squared = 0, scale = 1 / Math.sqrt(dim);
      for (int r = 0; r < rows; r++) {
        float estimate = 1f - (float) distances[r] / CoarseCodec.S1.SCALE;
        float exact = VectorUtil.dotProduct(query, vectors[r]);
        assertEquals("dim=" + dim + " row=" + r, exact, estimate, 5 * scale + 0.02);
        squared += (estimate - exact) * (estimate - exact);
      }
      double rms = Math.sqrt(squared / rows);
      assertTrue("dim=" + dim + " rms=" + rms, rms <= 1.5 * scale);
    }
  }

  /**
   * S1 centroid codes are sign bits, compared by Hamming distance with the query's, which are the
   * signs of its INT8 levels: a component rounding to level 0 counts as not positive.
   */
  public void testS1CentroidDistanceCountsSignDisagreements() {
    for (int dim : new int[] {24, 100, 1024, 4096}) {
      CoarseCodec.S1 s1 = new CoarseCodec.S1(dim);
      float[] query = unit(dim);
      byte[] q = new byte[s1.queryBytes];
      s1.encodeQuery(query, q, 0);
      int rows = 16;
      byte[] centroids = new byte[rows * s1.centroidBytes];
      float[][] vectors = new float[rows][];
      for (int r = 0; r < rows; r++) {
        vectors[r] = unit(dim);
        s1.encodeCentroid(vectors[r], centroids, r * s1.centroidBytes);
      }
      byte[] levels = new byte[dim];
      FineCodec.Int8.queryLevels(query, dim, levels);
      int[] distances = new int[rows];
      s1.centroidDistances(q, centroids, 0, rows, distances);
      for (int r = 0; r < rows; r++) {
        int disagreements = 0;
        for (int d = 0; d < dim; d++) {
          if ((levels[d] > 0) != (vectors[r][d] > 0)) {
            disagreements++;
          }
        }
        assertEquals("dim=" + dim, disagreements, distances[r]);
        assertEquals(distances[r], s1.centroidDistance(q, centroids, r * s1.centroidBytes));
      }
    }
  }

  /**
   * A search quantizes its query once, at its own length, for both the INT8 rerank and the S1
   * tables: the rerank scores exactly as from its own quantization, and S1 prepares the unit query
   * as from its own, but for rounding at a level's half-way point when rescaled by the norm.
   */
  public void testSharedQueryLevelsMatchSeparateQuantization() {
    for (int dim : new int[] {24, 100, 1024, 4096}) {
      FineCodec.Int8 int8 = new FineCodec.Int8(dim);
      CoarseCodec.S1 s1 = new CoarseCodec.S1(dim);
      int rows = 8;
      byte[] codes = new byte[rows * dim];
      for (int r = 0; r < rows; r++) {
        int8.encode(unit(dim), codes, r * dim);
      }
      int floatsOffset = s1.codeBytes + Kernels.TABLE_BYTES * s1.codeBytes;
      for (int iter = 0; iter < 20; iter++) {
        float[] rotated = unit(dim);
        float norm = iter == 0 ? 0f : 0.25f + 4 * random().nextFloat();
        if (norm == 0f) {
          Arrays.fill(rotated, 0f);
        }
        float[] scaled = new float[dim];
        for (int d = 0; d < dim; d++) {
          scaled[d] = rotated[d] * norm;
        }
        QueryLevels levels = new QueryLevels(scaled, norm);

        float[] expected = new float[rows];
        float[] actual = new float[rows];
        int8.query(scaled, null).score(codes, dim, rows, expected);
        int8.query(scaled, null, levels).score(codes, dim, rows, actual);
        assertArrayEquals("dim=" + dim, expected, actual, 0f);

        byte[] separate = new byte[s1.queryBytes];
        byte[] shared = new byte[s1.queryBytes];
        s1.encodeQuery(rotated, separate, 0);
        s1.encodeQuery(rotated, levels, shared, 0);
        // The shared levels are of the query at its own length, the separate ones of the unit
        // query: the same up to rounding, so a table entry may land one unit apart where its sum
        // sits on a rounding boundary. More dimensions mean more entries and more such ties.
        int differing = 0;
        for (int i = 0; i < floatsOffset; i++) {
          if (separate[i] != shared[i]) {
            differing++;
            assertTrue(
                "dim=" + dim + " byte " + i + ": " + separate[i] + " vs " + shared[i],
                Math.abs(separate[i] - shared[i]) <= 1);
          }
        }
        assertTrue(
            "dim=" + dim + " differing=" + differing + " of " + floatsOffset,
            differing <= Math.max(2, floatsOffset / 100));
        // The floats agree to rounding, except the level sum (the second float, the query's
        // offset: levelScale * sum |l|), which each level rounded the other way moves by one
        // level step. At small dimensions the sum is only hundreds, so a step is far more than
        // rounding.
        byte[] unitLevels = new byte[dim];
        FineCodec.Int8.queryLevels(rotated, dim, unitLevels);
        byte[] sharedLevels = levels.levels();
        int flips = 0;
        for (int d = 0; d < dim; d++) {
          flips += unitLevels[d] != sharedLevels[d] ? 1 : 0;
        }
        float levelStep =
            Math.abs(Float.intBitsToFloat((int) BitUtil.VH_LE_INT.get(separate, floatsOffset)))
                / 32;
        for (int f = floatsOffset; f < s1.queryBytes; f += Float.BYTES) {
          float a = Float.intBitsToFloat((int) BitUtil.VH_LE_INT.get(separate, f));
          float b = Float.intBitsToFloat((int) BitUtil.VH_LE_INT.get(shared, f));
          float tolerance = 1e-4f * Math.abs(a);
          if (f == floatsOffset + Float.BYTES) {
            tolerance += flips * levelStep;
          }
          assertEquals("dim=" + dim + " flips=" + flips, a, b, tolerance);
        }
      }
    }
  }

  private static float[] unit(int dim) {
    float[] v = new float[dim];
    for (int d = 0; d < dim; d++) {
      v[d] = (float) random().nextGaussian();
    }
    return VectorUtil.l2normalize(v);
  }
}
