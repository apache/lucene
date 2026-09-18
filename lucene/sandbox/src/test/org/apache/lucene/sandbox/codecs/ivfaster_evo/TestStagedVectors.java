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

package org.apache.lucene.sandbox.codecs.ivfaster_evo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestStagedVectors extends LuceneTestCase {
  /** FP32 fine records, so staged training values are the original vectors. */
  private static StagedVectors staged(Directory dir, String segment, int dim) throws IOException {
    return new StagedVectors(
        dir,
        segment,
        IOContext.DEFAULT,
        new TierCodec(IVFasterEvoVectorsFormat.Tier.NITROX2, dim),
        new TierCodec(IVFasterEvoVectorsFormat.Tier.FP32, dim));
  }

  public void testScratchReuseAndRoundTrip() throws Exception {
    try (Directory dir = newDirectory()) {
      String[] before = dir.listAll();
      try (StagedVectors staged = staged(dir, "_test", 128)) {
        float[] vector = new float[128];
        for (int ord = 0; ord < 10000; ord++) {
          Arrays.fill(vector, ord);
          staged.addValue(ord * 2, vector);
        }
        // Staging 5 MiB of records must not create a corresponding heap buffer.
        assertTrue(staged.ramBytesUsed() < 100000);
        FloatVectorValues values = staged.values();
        assertEquals(10000, values.size());
        float[] first = values.vectorValue(0);
        assertSame(first, values.vectorValue(9999));
        assertEquals(9999f, first[127], 0f);
        var copy = values.copy();
        assertNotSame(first, copy.vectorValue(0));
        assertEquals(0f, copy.vectorValue(0)[127], 0f);
        assertEquals(9999f, first[127], 0f);
        var docs = staged.getDocsWithFieldSet().iterator();
        assertEquals(0, docs.nextDoc());
        assertEquals(2, docs.nextDoc());
        expectThrows(IllegalStateException.class, () -> staged.addValue(20000, vector));
      }
      assertArrayEquals(before, dir.listAll());
    }
  }

  public void testQuantizedRecordsTrainInRotatedSpace() throws Exception {
    int dim = 64, count = 200;
    var coarse = new TierCodec(IVFasterEvoVectorsFormat.Tier.NITROX2, dim);
    var fine = new TierCodec(IVFasterEvoVectorsFormat.Tier.U8, dim);
    try (Directory dir = newDirectory()) {
      float[][] vectors = new float[count][dim];
      try (var staged = new StagedVectors(dir, "s", IOContext.DEFAULT, coarse, fine)) {
        for (int i = 0; i < count; i++) {
          for (int d = 0; d < dim; d++) vectors[i][d] = random().nextFloat() - .5f;
          staged.addValue(i, vectors[i]);
        }
        var a = staged.values();
        var b = a.copy();
        assertTrue(a.rotated());
        var rotation = TierCodec.HadamardRotation.create(dim, 42);
        try (var sections = dir.createOutput("sections", IOContext.DEFAULT)) {
          for (int i = 0; i < count; i++) {
            byte[] code = new byte[coarse.bytes];
            a.coarseCode(i, code);
            assertArrayEquals(coarse.encode(vectors[i]), code);
            float[] expected = new float[dim];
            fine.decode(fine.encode(vectors[i]), expected);
            float[] restored = new float[dim];
            rotation.inverseRotate(a.vectorValue(i), restored);
            assertArrayEquals(expected, restored, 0f);
            // Views never share a cursor or scratch.
            float[] saved = a.vectorValue(i).clone();
            b.vectorValue((i + 1) % count);
            assertArrayEquals(saved, a.vectorValue(i), 0f);
            staged.copySection(i, false, sections);
            staged.copySection(i, true, sections);
          }
        }
        try (var sections = dir.openInput("sections", IOContext.DEFAULT)) {
          for (int i = 0; i < count; i++) {
            byte[] f = new byte[fine.bytes], c = new byte[coarse.bytes];
            sections.readBytes(f, 0, f.length);
            sections.readBytes(c, 0, c.length);
            assertArrayEquals(fine.encode(vectors[i]), f);
            assertArrayEquals(coarse.encode(vectors[i]), c);
          }
        }
        // Seeds rotate into training space and centroids rotate back out.
        float[][] seeds = staged.rotateSeeds(new float[][] {vectors[0].clone()});
        assertNotSame(vectors[0], seeds[0]);
        staged.restoreCentroids(seeds);
        assertArrayEquals(vectors[0], seeds[0], 1e-5f);
      }
      assertArrayEquals(new String[] {"sections"}, dir.listAll());
    }
  }

  public void testCleanupOnAbortAndReadOpenFailure() throws Exception {
    try (Directory dir = newDirectory()) {
      try (StagedVectors aborted = staged(dir, "_abort", 2)) {
        aborted.addValue(0, new float[] {1, 2});
      }
      assertEquals(0, dir.listAll().length);
      Directory failing =
          new FilterDirectory(dir) {
            @Override
            public IndexInput openInput(String name, IOContext context) throws IOException {
              throw new IOException("injected staging read failure");
            }
          };
      try (StagedVectors staged = staged(failing, "_failure", 2)) {
        staged.addValue(0, new float[] {1, 2});
        expectThrows(IOException.class, staged::values);
      }
      assertEquals(0, dir.listAll().length);
    }
  }

  public void testStreamingClusteringMatchesHeapFixtures() throws Exception {
    for (VectorSimilarityFunction similarity : VectorSimilarityFunction.values()) {
      List<float[]> vectors = new ArrayList<>();
      try (Directory dir = newDirectory();
          StagedVectors staged = staged(dir, "_cluster", 5)) {
        for (int i = 0; i < 80; i++) {
          float[] value = new float[5];
          for (int d = 0; d < value.length; d++) {
            value[d] = random().nextFloat() * 20 - 10;
          }
          vectors.add(value);
          staged.addValue(i, value);
        }
        var expected = TestClustering.cluster(vectors, 6, similarity);
        var actual =
            Clustering.cluster(
                staged.values(),
                6,
                similarity,
                null,
                null,
                0,
                1.4,
                org.apache.lucene.util.InfoStream.NO_OUTPUT);
        assertArrayEquals(expected.assignments(), actual.assignments());
        for (int c = 0; c < actual.centroids().length; c++) {
          assertArrayEquals(expected.centroids()[c], actual.centroids()[c], 0f);
        }
        // In particular, cosine normalization must not rewrite the original vectors.
        var values = staged.values();
        for (int i = 0; i < vectors.size(); i++) {
          assertArrayEquals(vectors.get(i), values.vectorValue(i), 0f);
        }
      }
    }
  }

  public void testParallelRangesMatchSerialWithSpillAndHotStart() throws Exception {
    try (Directory dir = newDirectory();
        StagedVectors staged = staged(dir, "_parallel", 7)) {
      for (int i = 0; i < 513; i++) {
        float[] value = new float[7];
        for (int d = 0; d < value.length; d++) value[d] = random().nextFloat() * 10 - 5;
        staged.addValue(i, value);
      }
      var exact = new Clustering.Options(32, 0, true, org.apache.lucene.util.InfoStream.NO_OUTPUT);
      for (VectorSimilarityFunction similarity : VectorSimilarityFunction.values()) {
        var serial =
            Clustering.cluster(staged.values(), 8, similarity, null, null, 2, 1.05, 1, exact);
        var parallel =
            Clustering.cluster(staged.values(), 8, similarity, null, null, 2, 1.05, 4, exact);
        assertEquivalent(serial, parallel);
        assertEquivalent(
            parallel,
            Clustering.cluster(staged.values(), 8, similarity, null, null, 2, 1.05, 4, exact));
        int[] assignments = serial.assignments().clone();
        for (int i = 0; i < assignments.length; i += 3) assignments[i] = -1;
        var hot =
            Clustering.cluster(
                staged.values(), 8, similarity, serial.centroids(), assignments, 2, 1.05, 4, exact);
        // A hot start re-measures every carried member, which approximate reaper skips do not,
        // so it may improve on the donor's result; it must never be meaningfully worse.
        double donorObjective = objective(staged.values(), similarity, serial);
        double hotObjective = objective(staged.values(), similarity, hot);
        assertTrue(
            hotObjective + " vs " + donorObjective,
            hotObjective <= TestClustering.OBJECTIVE_TOLERANCE * donorObjective);
      }
    }
  }

  public void testParallelReadersAndFailureBarrier() throws Exception {
    var started = new java.util.concurrent.CountDownLatch(4);
    var active = new java.util.concurrent.atomic.AtomicInteger();
    var copies = new java.util.concurrent.atomic.AtomicInteger();
    class Values extends FloatVectorValues {
      final boolean worker;
      final float[] scratch = new float[1];
      boolean first = true;

      Values(boolean worker) {
        this.worker = worker;
      }

      @Override
      public int size() {
        return 100;
      }

      @Override
      public int dimension() {
        return 1;
      }

      @Override
      public FloatVectorValues copy() {
        copies.incrementAndGet();
        return new Values(true);
      }

      @Override
      public DocIndexIterator iterator() {
        return createDenseIterator();
      }

      @Override
      public float[] vectorValue(int ord) throws IOException {
        assertTrue(worker);
        active.incrementAndGet();
        try {
          if (first) {
            first = false;
            started.countDown();
            try {
              assertTrue(started.await(10, java.util.concurrent.TimeUnit.SECONDS));
            } catch (InterruptedException e) {
              throw new IOException(e);
            }
          }
          if (ord >= 75) throw new IOException("injected worker read failure");
          scratch[0] = ord;
          return scratch;
        } finally {
          active.decrementAndGet();
        }
      }
    }
    IOException failure =
        expectThrows(
            IOException.class,
            () ->
                Clustering.cluster(
                    new Values(false),
                    2,
                    VectorSimilarityFunction.EUCLIDEAN,
                    new float[][] {{0}, {100}},
                    null,
                    1,
                    1.05,
                    4,
                    Clustering.Options.DEFAULT));
    assertEquals("injected worker read failure", failure.getMessage());
    assertEquals(4, copies.get());
    assertEquals(0, active.get());
  }

  private static double objective(
      FloatVectorValues values, VectorSimilarityFunction similarity, Clustering.Result result)
      throws IOException {
    List<float[]> vectors = new ArrayList<>();
    for (int i = 0; i < values.size(); i++) vectors.add(values.vectorValue(i).clone());
    return TestClustering.objective(vectors, similarity, result);
  }

  private static void assertEquivalent(Clustering.Result expected, Clustering.Result actual) {
    assertArrayEquals(expected.assignments(), actual.assignments());
    for (int c = 0; c < expected.centroids().length; c++) {
      assertArrayEquals(expected.centroids()[c], actual.centroids()[c], 1e-6f);
    }
    for (int i = 0; i < expected.assignments().length; i++) {
      assertArrayEquals(expected.spill()[i], actual.spill()[i]);
    }
  }

  public void testIncrementalRefinementDoesNotRescan() throws Exception {
    int[] reads = {0};
    float[] data = {0, 2, 3, 10};
    FloatVectorValues values =
        new FloatVectorValues() {
          private final float[] scratch = new float[1];

          @Override
          public int size() {
            return data.length;
          }

          @Override
          public int dimension() {
            return 1;
          }

          @Override
          public float[] vectorValue(int ord) {
            reads[0]++;
            scratch[0] = data[ord];
            return scratch;
          }

          @Override
          public FloatVectorValues copy() {
            throw new UnsupportedOperationException();
          }

          @Override
          public DocIndexIterator iterator() {
            return createDenseIterator();
          }
        };
    var result =
        Clustering.cluster(
            values,
            2,
            VectorSimilarityFunction.EUCLIDEAN,
            new float[][] {{0}, {2}},
            null,
            0,
            1.4,
            org.apache.lucene.util.InfoStream.NO_OUTPUT);
    assertEquals(3, result.iterations());
    assertArrayEquals(new int[] {0, 0, 0, 1}, result.assignments());
    assertEquals(5f / 3, result.centroids()[0][0], 0f);
    assertEquals(10f, result.centroids()[1][0], 0f);
    // 4 initial routes + 4 initial accumulation reads + 4, 2, and 4 reaper reads.
    assertEquals(18, reads[0]);
  }

  public void testReaperSkipsStorageReads() throws Exception {
    int[] reads = {0};
    FloatVectorValues values =
        new FloatVectorValues() {
          private final float[] scratch = new float[1];

          @Override
          public int size() {
            return 100;
          }

          @Override
          public int dimension() {
            return 1;
          }

          @Override
          public float[] vectorValue(int ord) {
            // Initial routing and mean refinement must each read a sequential pass.
            assertEquals(reads[0] % size(), ord);
            reads[0]++;
            scratch[0] = ord < 50 ? 0 : 100;
            return scratch;
          }

          @Override
          public FloatVectorValues copy() {
            throw new UnsupportedOperationException();
          }

          @Override
          public DocIndexIterator iterator() {
            return createDenseIterator();
          }
        };
    var result =
        Clustering.cluster(
            values,
            2,
            VectorSimilarityFunction.EUCLIDEAN,
            new float[][] {{0}, {100}},
            null,
            1,
            1.4,
            org.apache.lucene.util.InfoStream.NO_OUTPUT);
    assertEquals(1, result.iterations());
    assertEquals(
        200, reads[0]); // Both the Lloyd reaper and final spill pass skip all 100 interior vectors.
  }
}
