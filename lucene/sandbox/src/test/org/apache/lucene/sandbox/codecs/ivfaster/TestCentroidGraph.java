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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Tests the centroid graph.
 *
 * <p>Two of these matter most:
 *
 * <ul>
 *   <li><b>Connectivity</b> fails SILENTLY. An unreachable centroid makes every document in its
 *       cell permanently unretrievable and nothing reports it: the index looks fine and never
 *       returns those documents.
 *   <li><b>Selection quality</b> is why the graph exists. It must choose nearly the cells an exact
 *       scan would, so that the descent's approximation is one the exact rerank repairs.
 * </ul>
 */
public class TestCentroidGraph extends LuceneTestCase {

  private static final int DIM = 64;

  /**
   * EVERY centroid must be reachable from the entry point.
   *
   * <p>A property of the construction: insertion links each node to nodes already present, so the
   * entry point reaches everything. The test remains because this failure mode is silent and a
   * change to the prune or the shrink rule could reintroduce it.
   */
  public void testEveryCentroidIsReachable() throws IOException {
    for (int nlist : new int[] {32, 200, 700}) {
      final CentroidCodes codes = codesFor(nlist);
      final CentroidGraph graph = CentroidGraph.build(codes, DIM);

      final boolean[] seen = new boolean[nlist];
      final Deque<Integer> queue = new ArrayDeque<>();
      queue.add(graph.entryPoint());
      seen[graph.entryPoint()] = true;
      int reached = 1;
      while (queue.isEmpty() == false) {
        for (int next : graph.neighboursOf(queue.poll())) {
          if (seen[next] == false) {
            seen[next] = true;
            reached++;
            queue.add(next);
          }
        }
      }
      assertEquals(
          "every centroid must be reachable at nlist="
              + nlist
              + "; an unreachable one strands its"
              + " documents with no error",
          nlist,
          reached);
    }
  }

  /**
   * The graph's cell choice must closely match an exact scan's.
   *
   * <p>Measured as overlap of the top-{@code probe} sets. The descent scores hops with a 2-bit
   * code, so exact agreement is not expected, but the sets must overlap heavily, since a cell the
   * graph never offers is a cell the exact rerank cannot recover.
   */
  public void testSelectionAgreesWithExactScan() throws IOException {
    final int nlist = 500;
    final int probe = 8;
    final CentroidCodes codes = codesFor(nlist);
    final CentroidGraph graph = CentroidGraph.build(codes, DIM);

    int overlapTotal = 0;
    int topHits = 0;
    final int trials = 100;
    for (int t = 0; t < trials; t++) {
      final float[] q = documentsAround(codes, nlist, 1)[0];

      // Exact top-probe.
      final int[] exact = new int[probe];
      final float[] exactD = new float[probe];
      java.util.Arrays.fill(exactD, Float.MAX_VALUE);
      java.util.Arrays.fill(exact, -1);
      for (int c = 0; c < nlist; c++) {
        final float d = codes.exactDistance(q, c);
        int pos = probe - 1;
        if (d >= exactD[pos]) {
          continue;
        }
        while (pos > 0 && exactD[pos - 1] > d) {
          exactD[pos] = exactD[pos - 1];
          exact[pos] = exact[pos - 1];
          pos--;
        }
        exactD[pos] = d;
        exact[pos] = c;
      }

      // Graph candidates, then exact rerank: the real selection path.
      final byte[] qCode = new byte[Nitrox2.bytesPerVector(DIM)];
      Nitrox2.encode(q, DIM, qCode, 0);
      final int ef = Math.max(CentroidGraph.MIN_EF, probe * CentroidGraph.EF_MULTIPLIER);
      // Sized for the VISITED set, which is several times `ef`; see CentroidGraph.search.
      final int[] cands = new int[nlist];
      final int got = graph.search(qCode, ef, cands);
      assertTrue("the descent must return candidates", got > 0);

      // Rerank exactly and keep the best `probe`, as the reader does.
      final Integer[] order = new Integer[got];
      final float[] cd = new float[got];
      for (int i = 0; i < got; i++) {
        cd[i] = codes.exactDistance(q, cands[i]);
        order[i] = i;
      }
      java.util.Arrays.sort(order, (a, b) -> Float.compare(cd[a], cd[b]));

      final java.util.Set<Integer> chosen = new java.util.HashSet<>();
      for (int i = 0; i < Math.min(probe, got); i++) {
        chosen.add(cands[order[i]]);
      }
      for (int c : exact) {
        if (c >= 0 && chosen.contains(c)) {
          overlapTotal++;
        }
      }
      if (chosen.contains(exact[0])) {
        topHits++;
      }
    }

    final double overlap = overlapTotal / (double) (trials * probe);
    final double topRate = topHits / (double) trials;
    assertTrue(
        "graph selection overlapped the exact top-"
            + probe
            + " only "
            + overlap
            + "; the descent is losing cells the rerank cannot recover",
        overlap >= 0.80);
    assertTrue(
        "the exact nearest cell was selected only " + topRate + " of the time", topRate >= 0.90);
  }

  /** Node records must be exactly the documented stride, and 64 B aligned. */
  public void testRecordStrideAndAlignment() {
    // Derived rather than hardcoded, so the 1-bit sketch is covered by the same assertion.
    final int rawProd = Nitrox2.PLANES * 128 + 2 + 16 * 2;
    assertEquals(
        "the documented production stride",
        (rawProd + 63) / 64 * 64,
        CentroidGraph.strideFor(1024));
    for (int dim : new int[] {64, 128, 256, 768, 1024}) {
      final int stride = CentroidGraph.strideFor(dim);
      assertEquals("stride must be 64-byte aligned at dim=" + dim, 0, stride % 64);
      final int raw = Nitrox2.bytesPerVector(dim) + 2 + CentroidGraph.M * 2;
      assertTrue("stride must hold the record at dim=" + dim, stride >= raw);
      assertTrue("stride must not waste a whole line at dim=" + dim, stride - raw < 64);
    }
  }

  /** Degree must never exceed M, since the record layout has no room for more. */
  public void testDegreeIsCapped() throws IOException {
    final int nlist = 300;
    final CentroidCodes codes = codesFor(nlist);
    final CentroidGraph graph = CentroidGraph.build(codes, DIM);
    for (int c = 0; c < nlist; c++) {
      final int[] nbrs = graph.neighboursOf(c);
      assertTrue(
          "degree " + nbrs.length + " exceeds M at node " + c, nbrs.length <= CentroidGraph.M);
      assertTrue("node " + c + " has no neighbours; the descent would dead-end", nbrs.length > 0);
      for (int x : nbrs) {
        assertTrue("neighbour ordinal out of range: " + x, x >= 0 && x < nlist);
        assertNotEquals("a node must not be its own neighbour", c, x);
      }
    }
  }

  /**
   * Construction must be deterministic: same input, identical graph.
   *
   * <p>SEQUENTIAL construction, which is the only thing determinism is claimed for. Insertion is
   * parallel above {@link CentroidGraph#PARALLEL_INSERT_GRAIN} insertions past the serial seed
   * prefix, and a worker descends whatever graph exists when it looks, so edge choice there depends
   * on thread interleaving by design, as in Lucene's concurrent HNSW merge. The nlist here stays
   * below that threshold; asserting bit-identical adjacency above it would assert something the
   * build does not promise.
   */
  public void testDeterminism() throws IOException {
    final int nlist = 200;
    assertTrue(
        "this test asserts a property of the SEQUENTIAL build, so nlist must stay below the parallel"
            + " threshold; raise nothing here without reading the javadoc",
        nlist - 1 - Math.max(64, 4 * CentroidGraph.M) < CentroidGraph.PARALLEL_INSERT_GRAIN);
    final CentroidCodes codes = codesFor(nlist);
    final CentroidGraph a = CentroidGraph.build(codes, DIM);
    final CentroidGraph b = CentroidGraph.build(codes, DIM);
    assertEquals("entry point must be stable", a.entryPoint(), b.entryPoint());
    for (int c = 0; c < nlist; c++) {
      assertArrayEquals(
          "node " + c + " adjacency must be stable", a.neighboursOf(c), b.neighboursOf(c));
    }
  }

  /** A graph must survive a write/read round trip byte-for-byte. */
  public void testPersistenceRoundTrip() throws IOException {
    final int nlist = 150;
    final CentroidCodes codes = codesFor(nlist);
    final CentroidGraph built = CentroidGraph.build(codes, DIM);

    try (org.apache.lucene.store.Directory dir = newDirectory()) {
      try (org.apache.lucene.store.IndexOutput out =
          dir.createOutput("graph", newIOContext(random()))) {
        built.write(out);
      }
      try (org.apache.lucene.store.IndexInput in = dir.openInput("graph", newIOContext(random()))) {
        final CentroidGraph read =
            CentroidGraph.read(in.randomAccessSlice(0, in.length()), DIM, in.length());
        assertEquals(built.nlist(), read.nlist());
        assertEquals(built.entryPoint(), read.entryPoint());
        assertEquals(built.stride(), read.stride());
        for (int c = 0; c < nlist; c++) {
          assertArrayEquals(
              "node " + c + " must survive the round trip",
              built.neighboursOf(c),
              read.neighboursOf(c));
        }
        // And the payloads must still score identically, which the adjacency check does not cover.
        final byte[] qCode = new byte[Nitrox2.bytesPerVector(DIM)];
        final float[] q = documentsAround(codes, nlist, 1)[0];
        Nitrox2.encode(q, DIM, qCode, 0);
        final int[] a = new int[32];
        final int[] b = new int[32];
        final int na = built.search(qCode, 32, a);
        final int nb = read.search(qCode, 32, b);
        assertEquals(na, nb);
        assertArrayEquals("a round-tripped graph must search identically", a, b);
      }
    }
  }

  /** A two-centroid graph is the smallest interesting case and must not degenerate. */
  public void testTinyGraph() throws IOException {
    final CentroidCodes codes = codesFor(2);
    final CentroidGraph graph = CentroidGraph.build(codes, DIM);
    assertEquals(2, graph.nlist());
    for (int c = 0; c < 2; c++) {
      assertEquals("each of two nodes must point at the other", 1, graph.neighboursOf(c).length);
      assertEquals(1 - c, graph.neighboursOf(c)[0]);
    }
  }

  private CentroidCodes codesFor(int nlist) {
    final float[][] centroids = new float[nlist][];
    final double std = 1.0 / Math.sqrt(DIM);
    for (int c = 0; c < nlist; c++) {
      centroids[c] = new float[DIM];
      for (int d = 0; d < DIM; d++) {
        centroids[c][d] = (float) (random().nextGaussian() * std);
      }
    }
    return new CentroidCodes(centroids, DIM, VectorSimilarityFunction.EUCLIDEAN, null);
  }

  /** Documents drawn near real centroids: the distribution the graph is meant to model. */
  private float[][] documentsAround(CentroidCodes codes, int nlist, int count) {
    final float[][] out = new float[count][];
    final double std = 1.0 / Math.sqrt(DIM);
    for (int i = 0; i < count; i++) {
      final float[] centre = codes.centroidAt(random().nextInt(nlist));
      out[i] = new float[DIM];
      for (int d = 0; d < DIM; d++) {
        out[i][d] = (float) (centre[d] + random().nextGaussian() * std * 0.3);
      }
    }
    return out;
  }
}
