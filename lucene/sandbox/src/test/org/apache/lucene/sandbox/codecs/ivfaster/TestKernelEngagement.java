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
package org.apache.lucene.sandbox.codecs.ivfaster;

import java.lang.foreign.MemorySegment;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Asserts that the vectorized kernels are actually LOADED on a JVM that can run them, and that each
 * one is bit-identical to its scalar reference.
 *
 * <h2>Why this test exists</h2>
 *
 * <p>Every kernel here resolves its Panama implementation reflectively, by string, and falls back
 * to a scalar loop on ANY failure: a missing {@code --add-modules jdk.incubator.vector}, an
 * unreadable module, a stale class name after a package rename. That fallback is correct but
 * SILENT: the codec still returns right answers, just several times slower. That situation
 * publishes wrong performance numbers, because a scalar run still produces a plausible latency and
 * nothing in the output says which path ran.
 *
 * <p>So two things are checked, and the first is the unusual one:
 *
 * <ol>
 *   <li><b>The vectorized path is engaged.</b> Gradle's test JVMs are started with {@code
 *       --add-modules jdk.incubator.vector}, so on this platform a scalar kernel means something is
 *       broken, most likely a {@code Class.forName} string that no longer resolves. A parity test
 *       alone would not catch it: it would silently compare the scalar path against itself and
 *       pass.
 *   <li><b>Vector and scalar agree.</b> Bit-identically for the integer kernels, since a fast
 *       kernel that is subtly wrong is worse than a slow one.
 * </ol>
 */
public class TestKernelEngagement extends LuceneTestCase {

  /**
   * The production width, plus widths that exercise the tail handling and the 2*STEP fused branch.
   */
  // 512 -> planeBytes 64 == 2*STEP at 256-bit, so it exercises the two-vector-per-plane fused path
  // that
  // a 512-bit (AVX512) machine takes at the production dim=1024. 768 (3*STEP) hits the generic
  // fall-through; 256 (STEP) and 96 (sub-vector tail) exercise the narrow paths.
  private static final int[] DIMS = {1024, 768, 512, 256, 96};

  public void testHammingKernelIsVectorizedAndMatchesScalar() {
    HammingKernel vector = HammingKernel.get();
    assertVectorizedUnlessDisabled(vector, "ivfaster.noSimdHamming", "Hamming");

    HammingKernel scalar = new HammingKernel.Scalar();
    for (int dim : DIMS) {
      final int bytes = dim / 8;
      for (int trial = 0; trial < 20; trial++) {
        byte[] q = randomBytes(bytes);
        MemorySegment doc = segmentOf(randomBytes(bytes));
        assertEquals(
            "Hamming distance must match scalar at dim=" + dim,
            scalar.distance(q, doc, 0, bytes),
            vector.distance(q, doc, 0, bytes));
      }
    }
  }

  /**
   * The CONTIGUOUS COARSE CELL SCAN, which is what the 2-bit coarse tier actually runs.
   *
   * <p>The coarse code is the two thermometer planes concatenated into one {@code 2*planeBytes}
   * string, and Hamming is additive over it, so the two-plane level distance is exactly one Hamming
   * over the whole code, with no separate fused kernel. This pins that identity: a single {@code
   * bulkDistances} over the concatenated code must equal the SUM of the two independent plane
   * distances. It also exercises the {@code len == 8*STEP} branch (256 B, the production shape at
   * 256-bit) that the generic path does not.
   */
  public void testConcatenatedCoarseHammingIsSumOfPlanes() {
    HammingKernel vector = HammingKernel.get();
    HammingKernel scalar = new HammingKernel.Scalar();
    for (int dim : DIMS) {
      final int planeBytes = dim / 8;
      final int len = 2 * planeBytes; // the whole coarse code: hi plane then lo plane
      final int rows = 13; // not a multiple of any plausible tile width
      // Both sides are [hi | lo], so the whole-code Hamming is the sum of the plane distances.
      byte[] qHi = randomBytes(planeBytes);
      byte[] qLo = randomBytes(planeBytes);
      byte[] qCode = concat(qHi, qLo);
      byte[] rowsBytes = randomBytes(rows * len);
      MemorySegment code = segmentOf(rowsBytes);

      int[] out = new int[rows];
      vector.bulkDistances(qCode, code, 0, len, rows, out);

      for (int r = 0; r < rows; r++) {
        final long base = (long) r * len;
        int expected =
            scalar.distance(qHi, code, base, planeBytes)
                + scalar.distance(qLo, code, base + planeBytes, planeBytes);
        assertEquals(
            "concatenated coarse distance for row " + r + " at dim=" + dim, expected, out[r]);
      }
    }
  }

  /**
   * The STRIDED heap-array path, which is what the centroid graph's descent runs.
   *
   * <p>Three things are only checked here. The rows are NOT contiguous and are visited out of
   * order, as a descent's neighbour set is; each node's coarse code is a {@code 2*planeBytes}
   * prefix of a WIDER record that also carries adjacency bytes, so a stride mistake would read the
   * wrong bytes and silently misrank the beam; and the operand is a {@code byte[]}, whose whole
   * point is avoiding the heap-MemorySegment load. A wrong distance here does not throw: it
   * reorders cell selection and reads as slightly-off recall. As in the contiguous case, the coarse
   * distance is one Hamming over the concatenated code, so it must equal the sum of the two plane
   * distances.
   */
  public void testStridedHeapHammingMatchesScalar() {
    HammingKernel vector = HammingKernel.get();
    HammingKernel scalarKernel = new HammingKernel.Scalar();
    for (int dim : DIMS) {
      final int planeBytes = dim / 8;
      final int coarseBytes = 2 * planeBytes;
      // A record holds the code plus trailing adjacency bytes, so the stride is NOT coarseBytes.
      final int stride = coarseBytes + 34;
      final int nodeCount = 23;
      byte[] qHi = randomBytes(planeBytes);
      byte[] qLo = randomBytes(planeBytes);
      byte[] qCode = concat(qHi, qLo);
      byte[] nodes = randomBytes(nodeCount * stride);

      // Out of order and non-contiguous, and deliberately including a repeat.
      final int[] ords = {7, 0, 22, 3, 3, 14, 9};
      final int rows = ords.length;
      final int[] offsets = new int[rows];
      for (int r = 0; r < rows; r++) {
        offsets[r] = ords[r] * stride;
      }
      int[] out = new int[rows];
      final long before = PanamaHammingKernel.strided2Rows.get();
      vector.bulkDistancesAtBytes(qCode, nodes, offsets, coarseBytes, rows, out);
      if (dim == 1024) {
        // ENGAGEMENT rather than parity, since the scalar default is bit-identical.
        assertEquals(
            "strided heap branch must engage at dim=1024",
            before + rows,
            PanamaHammingKernel.strided2Rows.get());
      }

      MemorySegment seg = segmentOf(nodes);
      for (int r = 0; r < rows; r++) {
        final long base = (long) ords[r] * stride;
        int expected =
            scalarKernel.distance(qHi, seg, base, planeBytes)
                + scalarKernel.distance(qLo, seg, base + planeBytes, planeBytes);
        assertEquals(
            "strided heap coarse distance for row " + r + " at dim=" + dim, expected, out[r]);
      }
    }
  }

  /** The contiguous bulk path must agree with the single-row path, row for row. */
  public void testBulkDistancesMatchSingle() {
    HammingKernel kernel = HammingKernel.get();
    final int len = 128;
    final int rows = 19;
    byte[] q = randomBytes(len);
    MemorySegment seg = segmentOf(randomBytes(rows * len));
    int[] out = new int[rows];
    kernel.bulkDistances(q, seg, 0, len, rows, out);
    for (int r = 0; r < rows; r++) {
      assertEquals("bulk row " + r, kernel.distance(q, seg, (long) r * len, len), out[r]);
    }
  }

  /**
   * The VECTOR ADMISSION FILTER, which the streaming coarse select runs under {@code
   * -Divfaster.simdAdmit}.
   *
   * <p>Pins the whole contract the reader relies on: {@code filterAtMost} must return exactly the
   * LOCAL indices {@code i} with {@code rowDist[from + i] <= thr}, in ascending order, and no
   * others. A wrong survivor set does not throw: it drops or over-admits candidates and reads as
   * off recall, the same failure class as a wrong Hamming distance. Checked against the interface's
   * scalar default over the same distances.
   *
   * <p>Exercises: a non-zero {@code from} (the compressed indices are relative to it), a {@code
   * count} that is not a multiple of the vector width (the scalar tail), and thresholds spanning
   * all-reject, partial, and all-admit.
   */
  public void testAdmissionFilterMatchesScalar() {
    HammingKernel vector = HammingKernel.get();
    HammingKernel scalar = new HammingKernel.Scalar();
    for (int trial = 0; trial < 200; trial++) {
      final int total = 1 + random().nextInt(600);
      final int[] rowDist = new int[total];
      // Popcount-shaped: non-negative, in the range a two-plane coarse distance spans.
      final int maxDist = 1 + random().nextInt(2048);
      for (int i = 0; i < total; i++) {
        rowDist[i] = random().nextInt(maxDist + 1);
      }
      final int from = random().nextInt(total);
      final int count = random().nextInt(total - from + 1);
      // Thresholds that force all-reject (-1), all-admit (maxDist), and everything between.
      final int thr =
          switch (trial % 4) {
            case 0 -> -1;
            case 1 -> maxDist;
            default -> random().nextInt(maxDist + 2) - 1;
          };
      final int[] outV = new int[count];
      final int[] outS = new int[count];
      final int kv = vector.filterAtMost(rowDist, from, count, thr, outV);
      final int ks = scalar.filterAtMost(rowDist, from, count, thr, outS);
      assertEquals(
          "survivor count must match scalar (from="
              + from
              + " count="
              + count
              + " thr="
              + thr
              + ")",
          ks,
          kv);
      for (int j = 0; j < ks; j++) {
        assertEquals("survivor index " + j, outS[j], outV[j]);
        // The returned index really is a survivor, and it is local to `from`.
        assertTrue("survivor must satisfy the threshold", rowDist[from + outV[j]] <= thr);
        if (j > 0) {
          assertTrue("survivors must be ascending", outV[j] > outV[j - 1]);
        }
      }
    }
  }

  public void testBulkDotKernelIsVectorizedAndMatchesScalar() {
    BulkDotKernel vector = BulkDotKernel.get();
    assertVectorizedUnlessDisabled(vector, "ivfaster.noSimdBulkDot", "BulkDot");

    BulkDotKernel scalar = new BulkDotKernel.Scalar();
    for (int dim : DIMS) {
      for (int trial = 0; trial < 20; trial++) {
        byte[] q = randomBytes(dim);
        byte[] doc = randomBytes(dim);
        assertEquals(
            "unsigned int8 dot must match scalar at dim=" + dim,
            scalar.dot(q, doc, dim),
            vector.dot(q, doc, dim));
      }
    }
  }

  /** The bulk-at-offset path is what the rerank uses; it must agree with the single-target dot. */
  public void testBulkDotAtMatchesSingleDot() {
    BulkDotKernel kernel = BulkDotKernel.get();
    final int dim = 1024;
    final int codeOffset = 8;
    final int n = 17; // deliberately not a multiple of any plausible tile width
    byte[] q = randomBytes(dim);
    byte[][] records = new byte[n][];
    for (int i = 0; i < n; i++) {
      records[i] = randomBytes(codeOffset + dim);
    }
    int[] out = new int[n];
    kernel.bulkDotAt(q, records, n, codeOffset, dim, out);
    for (int i = 0; i < n; i++) {
      byte[] code = new byte[dim];
      System.arraycopy(records[i], codeOffset, code, 0, dim);
      assertEquals("bulkDotAt target " + i, kernel.dot(q, code, dim), out[i]);
    }
  }

  /** Reports which kernels engaged, so a failing benchmark run has something to grep. */
  public void testReportEngagement() {
    StringBuilder sb = new StringBuilder("[ivfaster kernels] ");
    sb.append("hamming=").append(HammingKernel.get().isVectorized());
    sb.append(" bulkDot=").append(BulkDotKernel.get().isVectorized());
    sb.append(" vectorBits=").append(HammingKernel.get().vectorBits());
    if (VERBOSE) {
      System.out.println(sb);
    }
    assertTrue("engagement report must be non-empty", sb.length() > 0);
  }

  /**
   * Requires the vectorized implementation unless its kill switch is set.
   *
   * <p>Gradle starts test JVMs with {@code --add-modules jdk.incubator.vector}, so a scalar kernel
   * here indicates a real defect, typically a reflective class name that stopped resolving after a
   * package move, rather than an unsupported platform.
   */
  private static void assertVectorizedUnlessDisabled(
      Object kernel, String killSwitch, String name) {
    if (Boolean.getBoolean(killSwitch)) {
      return; // deliberately running the scalar arm
    }
    boolean vectorized =
        switch (kernel) {
          case HammingKernel k -> k.isVectorized();
          case BulkDotKernel k -> k.isVectorized();
          default -> throw new AssertionError("unknown kernel type: " + kernel.getClass());
        };
    assertTrue(
        name
            + " kernel fell back to scalar on a JVM that should support Panama. Run with"
            + " -Divfaster.simdDebug=true to see why; the usual cause is a stale reflective class"
            + " name. A silent scalar fallback invalidates any performance measurement.",
        vectorized);
  }

  /** Wraps a heap array as a MemorySegment, which is how the kernels receive mmapped doc data. */
  private static MemorySegment segmentOf(byte[] b) {
    return MemorySegment.ofArray(b);
  }

  private byte[] randomBytes(int n) {
    byte[] b = new byte[n];
    random().nextBytes(b);
    return b;
  }

  /** The whole coarse code: hi plane then lo plane, as the encoder concatenates them. */
  private static byte[] concat(byte[] hi, byte[] lo) {
    byte[] out = new byte[hi.length + lo.length];
    System.arraycopy(hi, 0, out, 0, hi.length);
    System.arraycopy(lo, 0, out, hi.length, lo.length);
    return out;
  }
}
