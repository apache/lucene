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

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.Random;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.sandbox.codecs.segmentivf.SegmentIVFVectorsFormat.CoarseTier;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;

/**
 * Tests the centroid graph's reachability repair, its search, its placement and validation on read,
 * and its per-thread visited marks.
 */
@ThreadLeakFilters(defaultFilters = true, filters = SegmentIVFBuildThreadsFilter.class)
public class TestCentroidGraph extends LuceneTestCase {
  private static final int MAX = CentroidGraph.MAX_NEIGHBOURS;

  /**
   * The entry sits in a clique whose lists are all full, so every attachment must overwrite a slot.
   * With no repair rounds the fallback alone must reach an unreachable cycle, isolated nodes and
   * unreachable nodes whose own lists are full, without orphaning a clique node.
   */
  public void testFallbackAttachesNodesWhenEveryReachableListIsFull() {
    int cliqueSize = MAX + 1;
    int n = cliqueSize + 30;
    int[][] lists = new int[n][];
    for (int c = 0; c < cliqueSize; c++) {
      lists[c] = new int[MAX];
      int slot = 0;
      for (int other = 0; other < cliqueSize; other++) {
        if (other != c) {
          lists[c][slot] = other;
          slot++;
        }
      }
    }
    int cycleEnd = cliqueSize + 10;
    for (int c = cliqueSize; c < cycleEnd; c++) {
      int next = c + 1;
      if (next == cycleEnd) {
        next = cliqueSize;
      }
      lists[c] = new int[] {next};
    }
    int isolatedEnd = cycleEnd + 10;
    for (int c = cycleEnd; c < isolatedEnd; c++) {
      lists[c] = new int[0];
    }
    for (int c = isolatedEnd; c < n; c++) {
      lists[c] = new int[MAX];
      for (int slot = 0; slot < MAX; slot++) {
        lists[c][slot] = slot;
      }
    }
    int entry = 3;
    assertEquals(cliqueSize, countReachable(lists, entry));
    CentroidGraph.ensureReachable(lists, entry, 0);
    assertValidAndFullyReachable(lists, entry);
  }

  /** Random graphs, many with unreachable nodes, are fully reachable after any number of rounds. */
  public void testEnsureReachableOnRandomGraphs() {
    Random random = random();
    for (int iter = 0; iter < 200; iter++) {
      int n = 1 + random.nextInt(300);
      int[][] lists = new int[n][];
      double sparsity = random.nextDouble();
      for (int c = 0; c < n; c++) {
        int degree = 0;
        if (random.nextDouble() > sparsity) {
          degree = random.nextInt(Math.min(MAX, n - 1) + 1);
        }
        lists[c] = distinctOthers(random, n, c, degree);
      }
      int entry = random.nextInt(n);
      int rounds = random.nextInt(CentroidGraph.MAX_REPAIR_ROUNDS + 1);
      CentroidGraph.ensureReachable(lists, entry, rounds);
      assertValidAndFullyReachable(lists, entry);
    }
  }

  /** A built graph, including the parallel inserts, reaches every node from its entry. */
  public void testBuiltGraphReachesEveryNode() throws IOException {
    for (CoarseTier tier : CoarseTier.values()) {
      int nlist = 1000 + random().nextInt(500);
      CentroidGraph graph =
          CentroidGraph.build(randomCodes(tier, 64, nlist), Clustering.Parallel.DEFAULT);
      int n = graph.nlist();
      int[][] lists = new int[n][];
      int[] adjacent = new int[MAX];
      for (int c = 0; c < n; c++) {
        int degree = graph.neighbours(c, adjacent);
        lists[c] = ArrayUtil.copyOfSubArray(adjacent, 0, degree);
      }
      assertEquals(tier.toString(), n, countReachable(lists, graph.entryNode()));
    }
  }

  /**
   * A repair round attaches each unreachable node {@code c} from {@code c - 1}. When {@code c - 1}
   * was itself attached this round, its full list may already link to {@code c}; the repair must
   * leave that list alone rather than overwrite its last slot with a second link to {@code c}.
   */
  public void testRepairKeepsAFullHostThatAlreadyLinks() {
    int n = MAX + 2;
    int[][] lists = new int[n][];
    lists[0] = new int[0];
    // Node 1 is unreachable from the entry 0, its list full and already linking to 2.
    lists[1] = new int[MAX];
    for (int slot = 0; slot < MAX; slot++) {
      lists[1][slot] = slot + 2;
    }
    int[] original = lists[1].clone();
    for (int c = 2; c < n; c++) {
      lists[c] = new int[0];
    }
    CentroidGraph.ensureReachable(lists, 0, 1);
    assertArrayEquals(original, lists[1]);
    assertNoDuplicateLinks(lists);
    assertValidAndFullyReachable(lists, 0);
  }

  /**
   * With {@code ef} 1 each neighbour list scored nearest last pushes all 16 onto the queue while
   * only the last stays kept, so stale entries pile up 15 per level. Eight levels lead to the one
   * node at distance 0; the walk must expand every level's nearest node to reach it, however many
   * stale entries are queued.
   */
  public void testSearchExpandsPastStaleQueueEntries() {
    int dim = 128;
    CoarseCodec codec = CoarseCodec.of(CoarseTier.S1, dim);
    int levels = 8;
    int nlist = 1 + levels * MAX;
    int stride = 64;
    assertTrue(codec.centroidBytes + 2 + 2 * MAX <= stride);
    byte[] nodes = new byte[nlist * stride];
    // Against an all-zero query, node k's distance is its code's popcount: dim - k.
    for (int k = 0; k < nlist; k++) {
      int bits = dim - k;
      for (int b = 0; b < bits; b++) {
        nodes[k * stride + (b >>> 3)] |= (byte) (1 << (b & 7));
      }
    }
    // The entry, then each level's last (nearest) node, links to the next level in id order, so
    // every list is scored farthest first.
    for (int level = 0; level < levels; level++) {
      int parent = level * MAX;
      int at = parent * stride + codec.centroidBytes;
      writeOrd(nodes, at, MAX);
      for (int i = 0; i < MAX; i++) {
        writeOrd(nodes, at + 2 * (i + 1), parent + 1 + i);
      }
    }
    CentroidGraph graph =
        new CentroidGraph(codec, nlist, stride, 0, MemorySegment.ofArray(nodes), null);
    int[] visited = new int[nlist];
    int[] distances = new int[nlist];
    int count = graph.search(new byte[codec.queryBytes], 1, visited, distances);
    assertEquals(nlist, count);
    int target = nlist - 1;
    int found = -1;
    for (int i = 0; i < count; i++) {
      if (visited[i] == target) {
        found = i;
      }
    }
    assertTrue("the walk never reached node " + target, found >= 0);
    assertEquals(0, distances[found]);
  }

  /**
   * Every header or node value a search trusts is checked on read: a mismatched or corrupt section
   * throws {@link CorruptIndexException}, never an index error or a graph that proposes cells the
   * field does not have.
   */
  public void testReadRejectsCorruptGraphs() throws IOException {
    CentroidCodes codes = randomCodes(CoarseTier.S1, 64, 100);
    CoarseCodec codec = codes.codec;
    CentroidGraph graph = CentroidGraph.build(codes, Clustering.Parallel.DEFAULT);
    int n = graph.nlist();
    int stride = graph.stride();
    int entry = graph.entryNode();
    byte[] nodes = graph.nodes().toArray(ValueLayout.JAVA_BYTE);
    int degreeAt = codec.centroidBytes;
    assertTrue(graph.neighbours(0, new int[MAX]) > 0);

    CentroidGraph read = readSection(codec, section(n, stride, entry, nodes), n);
    assertArrayEquals(nodes, read.nodes().toArray(ValueLayout.JAVA_BYTE));

    expectCorrupt(codec, new byte[0], n);
    expectCorrupt(codec, new byte[] {(byte) 0x80}, n);
    expectCorrupt(codec, section(n, stride, entry, nodes), n + 1);
    expectCorrupt(codec, section(n, stride + 64, entry, nodes), n);
    expectCorrupt(codec, section(n, stride, n, nodes), n);
    byte[] shorter = ArrayUtil.copyOfSubArray(nodes, 0, nodes.length - 1);
    expectCorrupt(codec, section(n, stride, entry, shorter), n);
    byte[] longer = ArrayUtil.growExact(nodes, nodes.length + 1);
    expectCorrupt(codec, section(n, stride, entry, longer), n);
    byte[] badDegree = nodes.clone();
    writeOrd(badDegree, degreeAt, MAX + 1);
    expectCorrupt(codec, section(n, stride, entry, badDegree), n);
    byte[] badNeighbour = nodes.clone();
    writeOrd(badNeighbour, degreeAt + 2, n);
    expectCorrupt(codec, section(n, stride, entry, badNeighbour), n);
  }

  private void expectCorrupt(CoarseCodec codec, byte[] section, int expectedNlist) {
    expectThrows(CorruptIndexException.class, () -> readSection(codec, section, expectedNlist));
  }

  private static byte[] section(int nlist, int stride, int entry, byte[] nodes) throws IOException {
    ByteBuffersDataOutput out = new ByteBuffersDataOutput();
    out.writeVInt(nlist);
    out.writeVInt(stride);
    out.writeVInt(entry);
    out.writeBytes(nodes, nodes.length);
    return out.toArrayCopy();
  }

  private CentroidGraph readSection(CoarseCodec codec, byte[] section, int expectedNlist)
      throws IOException {
    try (Directory dir = newDirectory()) {
      try (IndexOutput out = dir.createOutput("graph", IOContext.DEFAULT)) {
        out.writeBytes(section, section.length);
      }
      try (IndexInput in = dir.openInput("graph", IOContext.DEFAULT)) {
        long length = in.length();
        return CentroidGraph.read(in.randomAccessSlice(0, length), codec, length, expectedNlist);
      }
    }
  }

  private static void writeOrd(byte[] nodes, int at, int ord) {
    nodes[at] = (byte) ord;
    nodes[at + 1] = (byte) (ord >>> 8);
  }

  private static void assertNoDuplicateLinks(int[][] lists) {
    for (int c = 0; c < lists.length; c++) {
      int[] sorted = lists[c].clone();
      Arrays.sort(sorted);
      for (int i = 1; i < sorted.length; i++) {
        assertTrue("node " + c + " links twice to " + sorted[i], sorted[i] != sorted[i - 1]);
      }
    }
  }

  /**
   * Tiers that compare centroids by Hamming distance, as S1 does, keep the graph in native memory
   * aligned to 64 bytes, built or read back; any other would wrap a heap array. Either way the
   * written bytes survive a round trip, and a native graph searches exactly like a heap copy of it.
   * Dimension 100 leaves a code tail shorter than a vector step.
   */
  public void testGraphPlacementAndRoundTrip() throws IOException {
    for (CoarseTier tier : CoarseTier.values()) {
      for (int dim : new int[] {100, 1024}) {
        CentroidCodes codes = randomCodes(tier, dim, 500 + random().nextInt(500));
        CentroidGraph built = CentroidGraph.build(codes, Clustering.Parallel.DEFAULT);
        assertPlacement(tier, built);
        CentroidGraph read;
        try (Directory dir = newDirectory()) {
          try (IndexOutput out = dir.createOutput("graph", IOContext.DEFAULT)) {
            built.write(out);
          }
          try (IndexInput in = dir.openInput("graph", IOContext.DEFAULT)) {
            long length = in.length();
            read =
                CentroidGraph.read(
                    in.randomAccessSlice(0, length), codes.codec, length, codes.nlist);
          }
        }
        assertPlacement(tier, read);
        byte[] bytes = built.nodes().toArray(ValueLayout.JAVA_BYTE);
        assertArrayEquals(bytes, read.nodes().toArray(ValueLayout.JAVA_BYTE));
        CentroidGraph heap =
            new CentroidGraph(
                codes.codec,
                read.nlist(),
                read.stride(),
                read.entryNode(),
                MemorySegment.ofArray(bytes),
                null);
        int n = read.nlist();
        for (int q = 0; q < 4; q++) {
          byte[] query = new byte[codes.codec.queryBytes];
          codes.prepare(randomUnitVector(random(), dim), query);
          int[] expected = new int[n];
          int[] expectedDist = new int[n];
          int count = heap.search(query, 64, expected, expectedDist);
          assertSameSearch(read, query, expected, expectedDist, count);
        }
      }
    }
  }

  private static void assertPlacement(CoarseTier tier, CentroidGraph graph) {
    boolean aligned = graph.codec().nativeCentroidCodes();
    assertEquals(tier.toString(), tier == CoarseTier.S1, aligned);
    assertEquals(tier.toString(), aligned, graph.nodes().isNative());
    if (aligned) {
      assertEquals(tier.toString(), 0, graph.nodes().address() % 64);
    }
  }

  /**
   * A wrap of the generation counter must not leave nodes looking visited: without the reset,
   * generation 0 would match every never-stamped slot and the walk would stop almost at once.
   */
  public void testSearchIsUnchangedAcrossGenerationWrap() throws Exception {
    CentroidCodes codes = randomCodes(CoarseTier.S1, 64, 2000);
    CentroidGraph graph = CentroidGraph.build(codes, Clustering.Parallel.DEFAULT);
    int n = graph.nlist();
    byte[][] queries = new byte[2][codes.codec.queryBytes];
    int[][] expected = new int[2][n];
    int[][] expectedDist = new int[2][n];
    int[] expectedCount = new int[2];
    for (int q = 0; q < 2; q++) {
      codes.prepare(randomUnitVector(random(), 64), queries[q]);
      expectedCount[q] = graph.search(queries[q], 64, expected[q], expectedDist[q]);
      assertTrue(expectedCount[q] > MAX);
    }
    // A fresh thread has never stamped a slot, so every slot is 0: exactly what generation 0
    // would match. Each query runs right after a forced wrap.
    Throwable[] failure = new Throwable[1];
    Thread searcher =
        new Thread(() -> {
          try {
            for (int q = 0; q < 2; q++) {
              graph.setVisitedGenerationForTesting(-1);
              assertSameSearch(graph, queries[q], expected[q], expectedDist[q], expectedCount[q]);
            }
            // The counter restarted at 1, so this search uses 2 and must still match.
            assertSameSearch(graph, queries[0], expected[0], expectedDist[0], expectedCount[0]);
          } catch (Throwable t) {
            failure[0] = t;
          }
        });
    searcher.start();
    searcher.join();
    if (failure[0] != null) {
      throw new AssertionError(failure[0]);
    }
  }

  private static void assertSameSearch(
      CentroidGraph graph, byte[] query, int[] expected, int[] expectedDist, int expectedCount) {
    int n = graph.nlist();
    int[] actual = new int[n];
    int[] actualDist = new int[n];
    int count = graph.search(query, 64, actual, actualDist);
    assertEquals(expectedCount, count);
    assertArrayEquals(
        ArrayUtil.copyOfSubArray(expected, 0, count), ArrayUtil.copyOfSubArray(actual, 0, count));
    assertArrayEquals(
        ArrayUtil.copyOfSubArray(expectedDist, 0, count),
        ArrayUtil.copyOfSubArray(actualDist, 0, count));
  }

  private static CentroidCodes randomCodes(CoarseTier tier, int dim, int nlist) {
    float[][] centroids = new float[nlist][];
    for (int c = 0; c < nlist; c++) {
      centroids[c] = randomUnitVector(random(), dim);
    }
    return new CentroidCodes(centroids, CoarseCodec.of(tier, dim));
  }

  private static float[] randomUnitVector(Random random, int dim) {
    float[] vector = new float[dim];
    for (int d = 0; d < dim; d++) {
      vector[d] = (float) random.nextGaussian();
    }
    return VectorUtil.l2normalize(vector);
  }

  /** {@code count} distinct ordinals below {@code n}, none equal to {@code self}. */
  private static int[] distinctOthers(Random random, int n, int self, int count) {
    int[] picked = new int[count];
    int found = 0;
    while (found < count) {
      int candidate = random.nextInt(n);
      boolean fresh = candidate != self;
      for (int i = 0; i < found && fresh; i++) {
        fresh = picked[i] != candidate;
      }
      if (fresh) {
        picked[found] = candidate;
        found++;
      }
    }
    return picked;
  }

  private static void assertValidAndFullyReachable(int[][] lists, int entry) {
    int n = lists.length;
    for (int c = 0; c < n; c++) {
      assertTrue("node " + c + " degree " + lists[c].length, lists[c].length <= MAX);
      for (int next : lists[c]) {
        assertTrue("node " + c + " links to " + next, next >= 0 && next < n);
      }
    }
    assertEquals(n, countReachable(lists, entry));
  }

  private static int countReachable(int[][] lists, int entry) {
    boolean[] reached = new boolean[lists.length];
    int[] queue = new int[lists.length];
    int head = 0;
    int tail = 0;
    queue[tail] = entry;
    tail++;
    reached[entry] = true;
    while (head < tail) {
      int node = queue[head];
      head++;
      for (int next : lists[node]) {
        if (reached[next] == false) {
          reached[next] = true;
          queue[tail] = next;
          tail++;
        }
      }
    }
    return tail;
  }
}
