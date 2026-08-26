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

import java.util.Arrays;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.VectorUtil;

/**
 * Pins every {@link BulkDotKernel} implementation, scalar and the Panama SIMD one, to be
 * bit-identical to {@link VectorUtil#uint8DotProduct}, per target.
 *
 * <p>WHY THIS MATTERS. This kernel is the fine tier: it produces the rerank scores, and it also
 * drives doc-to-centroid ASSIGNMENT during clustering. A wrong dot does not crash. It silently
 * reorders the shortlist, or moves which cell a document lands in, and surfaces only as shifted
 * recall, the same hard-to-notice failure mode the coarse Hamming kernel is guarded against.
 *
 * <p>Both kernels are asserted explicitly so the suite cannot pass vacuously by checking the scalar
 * fallback against itself, and {@link #testSimdKernelIsActuallyLoaded()} asserts the SIMD path is
 * really live under the test runner (which passes {@code --add-modules jdk.incubator.vector}).
 *
 * <p>The reference is core's {@code uint8DotProduct} over a COPIED code run, which is deliberately
 * not how any kernel path computes it: the kernels read the run in place at an offset. That is
 * exactly the mistake worth catching: an off-by-stride or a dropped tail reads neighbouring bytes
 * and still returns a plausible number.
 */
public class TestBulkDotKernel extends LuceneTestCase {

  private static BulkDotKernel[] kernels() {
    BulkDotKernel resolved = BulkDotKernel.get();
    BulkDotKernel scalar = new BulkDotKernel.Scalar();
    return resolved.isVectorized()
        ? new BulkDotKernel[] {scalar, resolved}
        : new BulkDotKernel[] {scalar};
  }

  private static String name(BulkDotKernel k) {
    return k.getClass().getSimpleName();
  }

  public void testSimdKernelIsActuallyLoaded() {
    assertTrue(
        "SIMD BulkDotKernel not loaded under the test runner, so every equivalence assertion would only"
            + " be checking scalar-vs-scalar. Is --add-modules jdk.incubator.vector passed?",
        BulkDotKernel.get().isVectorized());
  }

  /** Unsigned int8 code: bytes in [0,255], as document/query codes are (u = s + 128). */
  private byte[] randomCode(int len) {
    byte[] c = new byte[len];
    for (int i = 0; i < len; i++) {
      c[i] = (byte) random().nextInt(256);
    }
    return c;
  }

  /** Every dim/target-count combination must match core's per-target reference exactly. */
  public void testMatchesCoreReference() {
    // Dims that exercise: exact multiple of 8 (no tail), +tail, small, and the production 1024.
    int[] dims = {8, 16, 24, 64, 128, 127, 129, 1000, 1024};
    // Target counts around the TILE, including non-multiples so the remainder loop runs.
    int[] counts = {1, 2, 3, 4, 5, 7, 8, 11, 16, 33};
    for (BulkDotKernel k : kernels()) {
      for (int dim : dims) {
        for (int n : counts) {
          byte[] q = randomCode(dim);
          byte[][] targets = new byte[n][];
          int[] expected = new int[n];
          for (int t = 0; t < n; t++) {
            targets[t] = randomCode(dim);
            expected[t] = VectorUtil.uint8DotProduct(q, targets[t]);
          }
          int[] out = new int[n];
          k.bulkDot(q, targets, n, dim, out);
          for (int t = 0; t < n; t++) {
            assertEquals(name(k) + " dim=" + dim + " n=" + n + " target=" + t, expected[t], out[t]);
          }
        }
      }
    }
  }

  /** The single-target 4-accumulator dot() must be bit-identical to core at every dim. */
  public void testSingleDotMatchesCore() {
    int[] dims = {1, 7, 8, 15, 16, 24, 63, 64, 127, 128, 129, 255, 1000, 1024};
    for (BulkDotKernel k : kernels()) {
      for (int dim : dims) {
        for (int rep = 0; rep < 8; rep++) {
          byte[] q = randomCode(dim);
          byte[] doc = randomCode(dim);
          assertEquals(
              name(k) + " dot dim=" + dim, VectorUtil.uint8DotProduct(q, doc), k.dot(q, doc, dim));
        }
      }
    }
    // The all-max extreme, where int32 accumulators must not overflow and must agree with core.
    int dim = 1024;
    byte[] q = new byte[dim];
    byte[] doc = new byte[dim];
    Arrays.fill(q, (byte) 0xFF);
    Arrays.fill(doc, (byte) 0xFF);
    for (BulkDotKernel k : kernels()) {
      assertEquals(name(k) + " dot max", VectorUtil.uint8DotProduct(q, doc), k.dot(q, doc, dim));
    }
  }

  /**
   * {@code bulkDotAt} must equal core's reference when the code sits at a non-zero offset inside a
   * larger record, the rerank's byte[][] shape. A wrong offset scores every reranked document
   * against shifted bytes.
   */
  public void testBulkDotAtMatchesCoreAtOffset() {
    int[] dims = {8, 64, 128, 127, 129, 1000, 1024};
    int[] counts = {1, 2, 3, 4, 5, 7, 8, 11, 33};
    int[] offsets = {0, 1, 8, 13, 64};
    for (BulkDotKernel k : kernels()) {
      for (int dim : dims) {
        for (int n : counts) {
          for (int off : offsets) {
            byte[] q = randomCode(dim);
            int recordLen = off + dim + 16;
            byte[][] recs = new byte[n][];
            int[] expected = new int[n];
            for (int t = 0; t < n; t++) {
              recs[t] = randomCode(recordLen);
              expected[t] =
                  VectorUtil.uint8DotProduct(q, Arrays.copyOfRange(recs[t], off, off + dim));
            }
            int[] out = new int[n];
            k.bulkDotAt(q, recs, n, off, dim, out);
            for (int t = 0; t < n; t++) {
              assertEquals(
                  name(k) + " bulkDotAt dim=" + dim + " n=" + n + " off=" + off + " target=" + t,
                  expected[t],
                  out[t]);
            }
          }
        }
      }
    }
  }

  /**
   * {@code bulkDotStrided} must equal core's reference for every record in a FLAT staged buffer.
   * This is the shape the int8 rerank actually scores from, so it is the path the shipped recall
   * depends on.
   *
   * <p>Sweeps a stride with slack past {@code codeOffset + dim}, because the real staged stride is
   * a whole record and the codes are therefore NOT adjacent; a kernel that assumed adjacency would
   * pass a stride-equals-dim test and fail here.
   */
  public void testBulkDotStridedMatchesCore() {
    int[] dims = {8, 64, 127, 128, 129, 1000, 1024};
    int[] counts = {1, 2, 3, 4, 5, 7, 8, 11, 33};
    int[] offsets = {0, 1, 8, 13};
    // Slack past codeOffset+dim, since a production record has trailing corrections.
    int[] slacks = {0, 1, 16, 24};
    for (BulkDotKernel k : kernels()) {
      for (int dim : dims) {
        for (int n : counts) {
          for (int off : offsets) {
            for (int slack : slacks) {
              final int stride = off + dim + slack;
              byte[] q = randomCode(dim);
              byte[] flat = randomCode(n * stride);
              int[] out = new int[n];
              k.bulkDotStrided(q, flat, n, stride, off, dim, out);
              for (int i = 0; i < n; i++) {
                final int base = i * stride + off;
                int expected =
                    VectorUtil.uint8DotProduct(q, Arrays.copyOfRange(flat, base, base + dim));
                assertEquals(
                    name(k)
                        + " bulkDotStrided dim="
                        + dim
                        + " n="
                        + n
                        + " off="
                        + off
                        + " stride="
                        + stride
                        + " rec="
                        + i,
                    expected,
                    out[i]);
              }
            }
          }
        }
      }
    }
  }

  /**
   * The strided and byte[][] paths must agree exactly at the production shape. The reader picks
   * between them on whether the tier overrides strided staging, so a divergence would make recall
   * depend on which staging path a tier happened to select.
   */
  public void testStridedAgreesWithBulkDotAtAtProductionShape() {
    final int dim = 1024;
    final int off = 8;
    final int stride = off + dim + 16;
    final int n = 700; // around the production bruteN
    for (BulkDotKernel k : kernels()) {
      byte[] q = randomCode(dim);
      byte[] flat = randomCode(n * stride);
      // The same records, re-presented as separate arrays.
      byte[][] recs = new byte[n][];
      for (int i = 0; i < n; i++) {
        recs[i] = Arrays.copyOfRange(flat, i * stride, i * stride + stride);
      }
      int[] strided = new int[n];
      int[] perRecord = new int[n];
      k.bulkDotStrided(q, flat, n, stride, off, dim, strided);
      k.bulkDotAt(q, recs, n, off, dim, perRecord);
      for (int i = 0; i < n; i++) {
        assertEquals(name(k) + " strided-vs-at rec=" + i, perRecord[i], strided[i]);
      }
    }
  }
}
