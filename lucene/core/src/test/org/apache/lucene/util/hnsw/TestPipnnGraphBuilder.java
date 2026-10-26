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
package org.apache.lucene.util.hnsw;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.VectorUtil;

public class TestPipnnGraphBuilder extends LuceneTestCase {

  private static final int M = 16;
  private static final int BEAM_WIDTH = 100;
  private static final int SEARCH_BEAM = 50;

  /** Small leaves and fanouts so that the partition recurses even on test-sized inputs. */
  private static final PipnnParams SMALL_PARAMS =
      new PipnnParams(64, new int[] {4, 2}, 40, 0.02, 2, 0, 12, 1.0f);

  public void testInvalidParams() {
    expectThrows(
        IllegalArgumentException.class,
        () -> new PipnnParams(1, new int[] {1}, 10, 0.1, 2, 0, 12, 1f));
    expectThrows(
        IllegalArgumentException.class,
        () -> new PipnnParams(64, new int[0], 10, 0.1, 2, 0, 12, 1f));
    expectThrows(
        IllegalArgumentException.class,
        () -> new PipnnParams(64, new int[] {1}, 10, 0.1, 0, 0, 12, 1f));
    expectThrows(
        IllegalArgumentException.class,
        () -> new PipnnParams(64, new int[] {1}, 10, 0.1, 2, 128, 12, 1f));
    expectThrows(
        IllegalArgumentException.class,
        () -> new PipnnParams(64, new int[] {1}, 10, 0.1, 2, 0, 17, 1f));
    expectThrows(
        IllegalArgumentException.class,
        () -> new PipnnParams(64, new int[] {1}, 10, 0.1, 2, 0, 12, 0.9f));
    assertEquals(30, PipnnParams.DEFAULT.replication());
    assertEquals(8, SMALL_PARAMS.replication());
    assertEquals(SMALL_PARAMS, new PipnnParams(64, new int[] {4, 2}, 40, 0.02, 2, 0, 12, 1.0f));
    assertEquals(SMALL_PARAMS.hashCode(), SMALL_PARAMS.hashCode());
    assertTrue(SMALL_PARAMS.toString().contains("fanouts=[4, 2]"));
  }

  public void testBuildValidatesArguments() throws IOException {
    MockVectorValues vectors = randomVectors(10, 4);
    RandomVectorScorerSupplier supplier = supplier(VectorSimilarityFunction.EUCLIDEAN, vectors);
    PipnnGraphBuilder builder =
        PipnnGraphBuilder.create(
            supplier,
            vectors,
            VectorSimilarityFunction.EUCLIDEAN,
            M,
            BEAM_WIDTH,
            42,
            10,
            SMALL_PARAMS,
            null,
            1);
    expectThrows(IllegalArgumentException.class, () -> builder.build(9));
    expectThrows(UnsupportedOperationException.class, () -> builder.addGraphNode(0));
    expectThrows(UnsupportedOperationException.class, () -> builder.addGraphNode(0, null));
    builder.build(10);
    expectThrows(IllegalStateException.class, () -> builder.build(10));
    expectThrows(
        IllegalArgumentException.class,
        () ->
            PipnnGraphBuilder.create(
                supplier,
                vectors,
                VectorSimilarityFunction.EUCLIDEAN,
                M,
                BEAM_WIDTH,
                42,
                -1,
                SMALL_PARAMS,
                null,
                1));
  }

  public void testTinyGraphs() throws IOException {
    for (int size : new int[] {1, 2, 3, 5}) {
      MockVectorValues vectors = randomVectors(size, 3);
      OnHeapHnswGraph graph =
          build(vectors, VectorSimilarityFunction.EUCLIDEAN, SMALL_PARAMS, 42, null, 1);
      assertEquals(size, graph.size());
      assertGraphInvariants(graph, size);
      if (size > 1) {
        assertTrue(HnswUtil.isRooted(graph));
        for (int node = 0; node < size; node++) {
          assertTrue(graph.getNeighbors(0, node).size() >= 1);
        }
      }
    }
  }

  public void testEmptyGraph() throws IOException {
    MockVectorValues vectors = randomVectors(1, 3);
    RandomVectorScorerSupplier supplier = supplier(VectorSimilarityFunction.EUCLIDEAN, vectors);
    PipnnGraphBuilder builder =
        PipnnGraphBuilder.create(
            supplier,
            vectors,
            VectorSimilarityFunction.EUCLIDEAN,
            M,
            BEAM_WIDTH,
            42,
            0,
            SMALL_PARAMS,
            null,
            1);
    OnHeapHnswGraph graph = builder.build(0);
    assertEquals(0, graph.size());
  }

  public void testGraphInvariantsAndRecall() throws IOException {
    int size = TestUtil.nextInt(random(), 500, 3000);
    int dim = TestUtil.nextInt(random(), 4, 16);
    VectorSimilarityFunction similarity =
        RandomizedTest.randomFrom(VectorSimilarityFunction.values());
    MockVectorValues vectors = randomVectors(size, dim);
    PipnnParams params =
        random().nextBoolean()
            ? SMALL_PARAMS
            : new PipnnParams(
                TestUtil.nextInt(random(), 32, 256),
                new int[] {TestUtil.nextInt(random(), 2, 6), TestUtil.nextInt(random(), 1, 3)},
                TestUtil.nextInt(random(), 10, 100),
                0.02,
                TestUtil.nextInt(random(), 2, 4),
                random().nextBoolean() ? 0 : TestUtil.nextInt(random(), M, 127),
                random().nextBoolean() ? 0 : TestUtil.nextInt(random(), 4, 16),
                random().nextBoolean() ? 1f : 1.2f);
    OnHeapHnswGraph graph = build(vectors, similarity, params, random().nextLong(), null, 1);
    assertGraphInvariants(graph, size);
    assertTrue("level 0 not fully connected", HnswUtil.isRooted(graph));
    float recall = recall(graph, vectors, similarity, 10, 50);
    assertTrue("recall@10 too low: " + recall + " for " + params, recall >= 0.9f);
  }

  public void testDefaultParamsOnLargerGraph() throws IOException {
    // large enough that the default partition (leaf size 1024) actually carves
    int size = 4000;
    MockVectorValues vectors = randomVectors(size, 8);
    OnHeapHnswGraph graph =
        build(vectors, VectorSimilarityFunction.DOT_PRODUCT, PipnnParams.DEFAULT, 7, null, 1);
    assertGraphInvariants(graph, size);
    assertTrue(HnswUtil.isRooted(graph));
    float recall = recall(graph, vectors, VectorSimilarityFunction.DOT_PRODUCT, 10, 100);
    assertTrue("recall@10 too low: " + recall, recall >= 0.95f);
  }

  /**
   * The same seed must give the same graph for serial builds, and the same level 0 and level
   * assignment for parallel builds; the upper levels are inserted concurrently and may differ.
   */
  public void testDeterministic() throws IOException {
    int size = TestUtil.nextInt(random(), 500, 2000);
    MockVectorValues vectors = randomVectors(size, 6);
    long seed = random().nextLong();
    OnHeapHnswGraph serial =
        build(vectors, VectorSimilarityFunction.EUCLIDEAN, SMALL_PARAMS, seed, null, 1);
    OnHeapHnswGraph serialAgain =
        build(vectors, VectorSimilarityFunction.EUCLIDEAN, SMALL_PARAMS, seed, null, 1);
    assertGraphsEqual(serial, serialAgain);

    ExecutorService exec = Executors.newFixedThreadPool(4, new NamedThreadFactory("pipnn-test"));
    try {
      TaskExecutor taskExecutor = new TaskExecutor(exec);
      for (int workers : new int[] {2, 4}) {
        OnHeapHnswGraph parallel =
            build(
                vectors,
                VectorSimilarityFunction.EUCLIDEAN,
                SMALL_PARAMS,
                seed,
                taskExecutor,
                workers);
        assertGraphsEqual(serial, parallel, 0);
      }
    } finally {
      exec.shutdownNow();
    }
  }

  public void testDifferentSeedsDifferentGraphs() throws IOException {
    int size = 1000;
    MockVectorValues vectors = randomVectors(size, 6);
    OnHeapHnswGraph a =
        build(vectors, VectorSimilarityFunction.EUCLIDEAN, SMALL_PARAMS, 1, null, 1);
    OnHeapHnswGraph b =
        build(vectors, VectorSimilarityFunction.EUCLIDEAN, SMALL_PARAMS, 2, null, 1);
    boolean different = false;
    for (int node = 0; node < size && different == false; node++) {
      different = graph0Neighbors(a, node).equals(graph0Neighbors(b, node)) == false;
    }
    assertTrue(different);
  }

  public void testByteVectors() throws IOException {
    int size = TestUtil.nextInt(random(), 500, 1500);
    int dim = TestUtil.nextInt(random(), 4, 16);
    MockByteVectorValues vectors =
        MockByteVectorValues.fromValues(
            HnswGraphTestCase.createRandomByteVectors(size, dim, random()));
    VectorSimilarityFunction similarity =
        random().nextBoolean()
            ? VectorSimilarityFunction.EUCLIDEAN
            : VectorSimilarityFunction.DOT_PRODUCT;
    OnHeapHnswGraph graph = build(vectors, similarity, SMALL_PARAMS, random().nextLong(), null, 1);
    assertGraphInvariants(graph, size);
    assertTrue(HnswUtil.isRooted(graph));
  }

  public void testFloat16Vectors() throws IOException {
    int size = TestUtil.nextInt(random(), 500, 1500);
    int dim = TestUtil.nextInt(random(), 4, 16);
    MockFloat16VectorValues vectors =
        MockFloat16VectorValues.fromValues(
            HnswGraphTestCase.createRandomFloat16Vectors(size, dim, random()));
    OnHeapHnswGraph graph =
        build(
            vectors,
            VectorSimilarityFunction.EUCLIDEAN,
            SMALL_PARAMS,
            random().nextLong(),
            null,
            1);
    assertGraphInvariants(graph, size);
    assertTrue(HnswUtil.isRooted(graph));
  }

  /** Many identical vectors: the partition cannot separate them, the build must still work. */
  public void testDuplicateVectors() throws IOException {
    int size = 700;
    float[][] values = new float[size][];
    for (int i = 0; i < size; i++) {
      values[i] = i % 5 == 0 ? new float[] {1f, 1f, 1f} : new float[] {0f, 0f, 1f};
    }
    MockVectorValues vectors = MockVectorValues.fromValues(values);
    OnHeapHnswGraph graph =
        build(vectors, VectorSimilarityFunction.EUCLIDEAN, SMALL_PARAMS, 3, null, 1);
    assertGraphInvariants(graph, size);
  }

  public void testAbortCheck() throws IOException {
    int size = 1000;
    MockVectorValues vectors = randomVectors(size, 4);
    RandomVectorScorerSupplier supplier = supplier(VectorSimilarityFunction.EUCLIDEAN, vectors);
    PipnnGraphBuilder builder =
        PipnnGraphBuilder.create(
            supplier,
            vectors,
            VectorSimilarityFunction.EUCLIDEAN,
            M,
            BEAM_WIDTH,
            42,
            size,
            SMALL_PARAMS,
            null,
            1);
    builder.setAbortCheck(
        () -> {
          throw new MergePolicy.MergeAbortedException("aborted");
        });
    expectThrows(MergePolicy.MergeAbortedException.class, () -> builder.build(size));
  }

  /** The alpha rule translated to similarities must agree with the rule stated on distances. */
  public void testPruneThresholdMatchesDistanceRule() throws IOException {
    for (VectorSimilarityFunction similarity :
        new VectorSimilarityFunction[] {
          VectorSimilarityFunction.EUCLIDEAN,
          VectorSimilarityFunction.DOT_PRODUCT,
          VectorSimilarityFunction.COSINE
        }) {
      float alpha = 1f + random().nextFloat();
      PipnnParams params = new PipnnParams(64, new int[] {2}, 10, 0.1, 2, 0, 0, alpha);
      MockVectorValues vectors = randomVectors(3, 8);
      PipnnGraphBuilder builder =
          PipnnGraphBuilder.create(
              supplier(similarity, vectors),
              vectors,
              similarity,
              M,
              BEAM_WIDTH,
              42,
              3,
              params,
              null,
              1);
      // copy: MockVectorValues may hand out a shared scratch array
      float[] node = vectors.vectorValue(0).clone();
      float[] selected = vectors.vectorValue(1).clone();
      float[] candidate = vectors.vectorValue(2).clone();
      float scoreNodeCandidate = similarity.compare(node, candidate);
      float scoreSelectedCandidate = similarity.compare(selected, candidate);
      boolean prunedBySimilarity =
          scoreSelectedCandidate >= builder.pruneThreshold(scoreNodeCandidate);
      boolean prunedByDistance =
          alpha * distance(similarity, selected, candidate)
              <= distance(similarity, node, candidate);
      assertEquals(similarity.toString(), prunedByDistance, prunedBySimilarity);
    }
  }

  public void testPruneThresholdWithAlphaOneIsIdentity() throws IOException {
    MockVectorValues vectors = randomVectors(3, 4);
    for (VectorSimilarityFunction similarity : VectorSimilarityFunction.values()) {
      PipnnGraphBuilder builder =
          PipnnGraphBuilder.create(
              supplier(similarity, vectors),
              vectors,
              similarity,
              M,
              BEAM_WIDTH,
              42,
              3,
              SMALL_PARAMS,
              null,
              1);
      float score = random().nextFloat();
      assertEquals(score, builder.pruneThreshold(score), 0f);
    }
  }

  public void testSortByScoreDescending() {
    int count = TestUtil.nextInt(random(), 0, 40);
    int[] nodes = new int[count + 3];
    float[] scores = new float[count + 3];
    for (int i = 0; i < count; i++) {
      nodes[i] = i;
      scores[i] = TestUtil.nextInt(random(), 0, 10);
    }
    float[] original = scores.clone();
    PipnnGraphBuilder.sortByScoreDescending(nodes, scores, count);
    Set<Integer> seen = new HashSet<>();
    for (int i = 0; i < count; i++) {
      if (i > 0) {
        assertTrue(scores[i - 1] >= scores[i]);
      }
      assertTrue(seen.add(nodes[i]));
      // scores travel with their nodes
      assertEquals(original[nodes[i]], scores[i], 0f);
    }
    // entries past count are untouched
    assertEquals(0, nodes[count]);
  }

  private static double distance(VectorSimilarityFunction similarity, float[] a, float[] b) {
    return switch (similarity) {
      case EUCLIDEAN -> Math.sqrt(VectorUtil.squareDistance(a, b));
      case DOT_PRODUCT -> 1 - VectorUtil.dotProduct(a, b);
      case COSINE -> 1 - VectorUtil.cosine(a, b);
      case MAXIMUM_INNER_PRODUCT -> throw new AssertionError("no distance for " + similarity);
    };
  }

  private static MockVectorValues randomVectors(int size, int dim) {
    // unit vectors so that every similarity function is well defined
    float[][] values = HnswGraphTestCase.createRandomFloatVectors(size, dim, random());
    for (float[] value : values) {
      VectorUtil.l2normalize(value);
    }
    return MockVectorValues.fromValues(values);
  }

  private static RandomVectorScorerSupplier supplier(
      VectorSimilarityFunction similarity, KnnVectorValues vectors) throws IOException {
    return DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorerSupplier(similarity, vectors);
  }

  private static OnHeapHnswGraph build(
      KnnVectorValues vectors,
      VectorSimilarityFunction similarity,
      PipnnParams params,
      long seed,
      TaskExecutor taskExecutor,
      int numWorkers)
      throws IOException {
    PipnnGraphBuilder builder =
        PipnnGraphBuilder.create(
            supplier(similarity, vectors),
            vectors,
            similarity,
            M,
            BEAM_WIDTH,
            seed,
            vectors.size(),
            params,
            taskExecutor,
            numWorkers);
    return builder.build(vectors.size());
  }

  private static void assertGraphInvariants(OnHeapHnswGraph graph, int size) throws IOException {
    assertEquals(size, graph.size());
    assertEquals(graph.numLevels() - 1, levelOf(graph, graph.entryNode()));
    for (int node = 0; node < size; node++) {
      int topLevel = levelOf(graph, node);
      assertTrue(topLevel <= graph.numLevels() - 1);
      for (int level = 0; level <= topLevel; level++) {
        NeighborArray neighbors = graph.getNeighbors(level, node);
        int maxConn = level == 0 ? 2 * M : M;
        assertTrue(
            "node " + node + " level " + level + " has " + neighbors.size() + " neighbors",
            neighbors.size() <= maxConn);
        Set<Integer> seen = new HashSet<>();
        for (int i = 0; i < neighbors.size(); i++) {
          int neighbor = neighbors.nodes()[i];
          assertNotEquals("self loop at node " + node, node, neighbor);
          assertTrue(neighbor >= 0 && neighbor < size);
          assertTrue("duplicate neighbor " + neighbor + " of " + node, seen.add(neighbor));
          assertTrue(
              "neighbor " + neighbor + " missing on level " + level,
              graph.nodeExistAtLevel(level, neighbor));
        }
      }
    }
    // the upper levels are a sample of the nodes, level 0 has everybody
    assertEquals(size, graph.getNodesOnLevel(0).size());
    for (int level = 1; level < graph.numLevels(); level++) {
      assertTrue(graph.getNodesOnLevel(level).size() <= graph.getNodesOnLevel(level - 1).size());
    }
  }

  private static int levelOf(OnHeapHnswGraph graph, int node) {
    int level = 0;
    while (graph.nodeExistAtLevel(level + 1, node)) {
      level++;
    }
    return level;
  }

  private static void assertGraphsEqual(OnHeapHnswGraph a, OnHeapHnswGraph b) throws IOException {
    assertEquals(a.entryNode(), b.entryNode());
    assertGraphsEqual(a, b, Integer.MAX_VALUE);
  }

  /** Asserts equal sizes, level assignments and neighbor lists on levels up to {@code maxLevel}. */
  private static void assertGraphsEqual(OnHeapHnswGraph a, OnHeapHnswGraph b, int maxLevel)
      throws IOException {
    assertEquals(a.size(), b.size());
    assertEquals(a.numLevels(), b.numLevels());
    for (int node = 0; node < a.size(); node++) {
      assertEquals(levelOf(a, node), levelOf(b, node));
      for (int level = 0; level <= Math.min(maxLevel, levelOf(a, node)); level++) {
        NeighborArray na = a.getNeighbors(level, node);
        NeighborArray nb = b.getNeighbors(level, node);
        assertEquals("node " + node + " level " + level, na.size(), nb.size());
        for (int i = 0; i < na.size(); i++) {
          assertEquals("node " + node + " level " + level, na.nodes()[i], nb.nodes()[i]);
        }
      }
    }
  }

  private static Set<Integer> graph0Neighbors(OnHeapHnswGraph graph, int node) {
    NeighborArray neighbors = graph.getNeighbors(0, node);
    Set<Integer> set = new HashSet<>();
    for (int i = 0; i < neighbors.size(); i++) {
      set.add(neighbors.nodes()[i]);
    }
    return set;
  }

  /** Average recall@k of graph searches against brute force over random query vectors. */
  private static float recall(
      OnHeapHnswGraph graph,
      MockVectorValues vectors,
      VectorSimilarityFunction similarity,
      int k,
      int numQueries)
      throws IOException {
    int dim = vectors.dimension();
    int hits = 0;
    for (int q = 0; q < numQueries; q++) {
      float[] query = VectorUtil.l2normalize(HnswGraphTestCase.randomVector(random(), dim));
      RandomVectorScorer scorer =
          DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorer(similarity, vectors.copy(), query);
      // brute force
      NeighborQueue exact = new NeighborQueue(k, false);
      for (int ord = 0; ord < vectors.size(); ord++) {
        exact.insertWithOverflow(ord, scorer.score(ord));
      }
      Set<Integer> expected = new HashSet<>();
      while (exact.size() > 0) {
        expected.add(exact.pop());
      }
      // search with a beam wider than k, as a real query would, and keep the k best
      KnnCollector collector =
          HnswGraphSearcher.search(scorer, SEARCH_BEAM, graph, null, Integer.MAX_VALUE);
      TopDocs topDocs = collector.topDocs();
      for (int i = 0; i < Math.min(k, topDocs.scoreDocs.length); i++) {
        if (expected.contains(topDocs.scoreDocs[i].doc)) {
          hits++;
        }
      }
    }
    return hits / (float) (k * numQueries);
  }
}
