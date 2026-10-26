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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * The staged corpus: what goes into the temp file comes back out, by cursor and by gather, and the
 * files are gone afterwards.
 */
@ThreadLeakFilters(defaultFilters = true, filters = IvfasterBuildThreadsFilter.class)
public class TestStagedVectors extends LuceneTestCase {

  public void testRoundTrip() throws Exception {
    final int dim = 40;
    final int count = 300;
    final float[][] rotated = unitVectors(count, dim);
    try (Directory dir = newDirectory()) {
      final Int8Quantizer q = new Int8Quantizer();
      final StagedVectors.Builder b =
          StagedVectors.begin(dir, "_s", IOContext.DEFAULT, dim, q, null, true);
      final int chunkOrds = 64;
      final byte[] chunk = b.chunk(chunkOrds);
      final float[] raw = b.rawChunk(chunkOrds);
      final StagedVectors.Builder.EncodeScratch sc = b.encodeScratch();
      for (int start = 0; start < count; start += chunkOrds) {
        final int n = Math.min(chunkOrds, count - start);
        for (int j = 0; j < n; j++) {
          final int i = start + j;
          b.encodeInto(rotated[i], 2 * i, chunk, j * b.stride, sc);
          System.arraycopy(rotated[i], 0, raw, j * dim, dim);
        }
        b.writeChunk(chunk, raw, n);
      }
      try (StagedVectors staged = b.finish()) {
        assertEquals(count, staged.count());
        assertEquals(dim, staged.dim());
        final int coarseBytes = Nitrox2.bytesPerVector(dim);
        final byte[] expectPlanes = new byte[coarseBytes];
        final byte[] gotPlanes = new byte[coarseBytes];
        final byte[] rec = new byte[staged.recordLen()];
        final float[] decoded = new float[dim];
        final float[] corrections = new float[CodeRecord.CORRECTIONS];
        try (VectorSource.Cursor cur = staged.cursor()) {
          for (int i = 0; i < count; i++) {
            cur.load(i);
            assertTrue("decoded vector " + i, cosine(cur.vector(), rotated[i]) > 0.995);
            assertEquals(2 * i, staged.docId(i));
            Nitrox2.packPlanes(rotated[i], dim, expectPlanes, 0, Nitrox2.planeBytes(dim));
            cur.coarseInto(gotPlanes);
            assertArrayEquals("coarse planes of " + i, expectPlanes, gotPlanes);
            staged.copyCoarse(i, gotPlanes);
            assertArrayEquals("gathered planes of " + i, expectPlanes, gotPlanes);
            staged.copyRecord(i, rec);
            assertEquals(
                2 * i, CodeRecord.readIntLE(rec, CodeRecord.docIdOffset(q.codeBytes(dim))));
            for (int k = 0; k < CodeRecord.CORRECTIONS; k++) {
              corrections[k] =
                  Float.intBitsToFloat(
                      CodeRecord.readIntLE(rec, CodeRecord.correctionOffset(q.codeBytes(dim), k)));
            }
            q.decode(rec, CodeRecord.codeOffset(), dim, null, corrections, decoded);
            org.apache.lucene.util.VectorUtil.l2normalize(decoded, false);
            assertArrayEquals("gathered record of " + i, cur.vector(), decoded, 0f);
          }
        }
        // The raw section holds the caller's floats, in order.
        assertEquals((long) count * dim * Float.BYTES, staged.rawLength());
        final IndexInput rawIn = staged.rawInput();
        for (int i = 0; i < count; i++) {
          for (int d = 0; d < dim; d++) {
            assertEquals(rotated[i][d], Float.intBitsToFloat(rawIn.readInt()), 0f);
          }
        }
      }
      assertNoStagingFiles(dir);
    }
  }

  /** Cursors are per thread and share only the file; disjoint ranges read back their own docs. */
  public void testCursorsAreIndependentAcrossThreads() throws Exception {
    final int dim = 24;
    final int count = 2000;
    final float[][] rotated = unitVectors(count, dim);
    try (Directory dir = newDirectory()) {
      final StagedVectors.Builder b =
          StagedVectors.begin(dir, "_t", IOContext.DEFAULT, dim, new Fp32Quantizer(), null, false);
      final byte[] chunk = b.chunk(count);
      final StagedVectors.Builder.EncodeScratch sc = b.encodeScratch();
      for (int i = 0; i < count; i++) {
        b.encodeInto(rotated[i], i, chunk, i * b.stride, sc);
      }
      b.writeChunk(chunk, null, count);
      try (StagedVectors staged = b.finish()) {
        final int threads = 4;
        final List<Thread> workers = new ArrayList<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        for (int t = 0; t < threads; t++) {
          final int lo = t * count / threads;
          final int hi = (t + 1) * count / threads;
          final Thread worker =
              new Thread(
                  () -> {
                    try (VectorSource.Cursor cur = staged.cursor()) {
                      for (int round = 0; round < 3; round++) {
                        for (int i = lo; i < hi; i++) {
                          cur.load(i);
                          assertArrayEquals(rotated[i], cur.vector(), 0f);
                        }
                      }
                    } catch (Throwable e) {
                      failure.compareAndSet(null, e);
                    }
                  });
          workers.add(worker);
          worker.start();
        }
        for (Thread worker : workers) {
          worker.join();
        }
        if (failure.get() != null) {
          throw new AssertionError(failure.get());
        }
      }
      assertNoStagingFiles(dir);
    }
  }

  /** Records must arrive in ascending doc order; a violation fails the build and cleans up. */
  public void testRejectsUnorderedDocsAndCleansUp() throws Exception {
    final int dim = 16;
    try (Directory dir = newDirectory()) {
      final StagedVectors.Builder b =
          StagedVectors.begin(dir, "_u", IOContext.DEFAULT, dim, new Int8Quantizer(), null, true);
      final byte[] chunk = b.chunk(2);
      final float[] raw = b.rawChunk(2);
      final StagedVectors.Builder.EncodeScratch sc = b.encodeScratch();
      final float[][] v = unitVectors(2, dim);
      b.encodeInto(v[0], 7, chunk, 0, sc);
      b.encodeInto(v[1], 7, chunk, b.stride, sc);
      expectThrows(IllegalStateException.class, () -> b.writeChunk(chunk, raw, 2));
      b.abort();
      assertNoStagingFiles(dir);
      // Abort is idempotent, and a builder aborted before finish leaves nothing behind.
      b.abort();
      assertNoStagingFiles(dir);
    }
  }

  private static void assertNoStagingFiles(Directory dir) throws IOException {
    for (String name : dir.listAll()) {
      assertFalse("staging file left behind: " + name, name.contains("ivfstage"));
      assertFalse("raw staging file left behind: " + name, name.contains("ivfraw"));
    }
  }

  private static double cosine(float[] a, float[] b) {
    double dot = 0;
    double na = 0;
    double nb = 0;
    for (int d = 0; d < a.length; d++) {
      dot += (double) a[d] * b[d];
      na += (double) a[d] * a[d];
      nb += (double) b[d] * b[d];
    }
    return dot / Math.sqrt(na * nb);
  }

  private float[][] unitVectors(int count, int dim) {
    final float[][] out = new float[count][];
    for (int i = 0; i < count; i++) {
      out[i] = new float[dim];
      double norm = 0;
      for (int d = 0; d < dim; d++) {
        out[i][d] = (float) random().nextGaussian();
        norm += (double) out[i][d] * out[i][d];
      }
      norm = Math.sqrt(norm);
      for (int d = 0; d < dim; d++) {
        out[i][d] /= (float) norm;
      }
    }
    return out;
  }
}
