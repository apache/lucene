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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SplittableRandom;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.NamedThreadFactory;

public class TestPipnnPartitioner extends LuceneTestCase {

  public void testInvalidParameters() {
    expectThrows(
        IllegalArgumentException.class, () -> new PipnnPartitioner(1, new int[] {1}, 10, 0.1));
    expectThrows(
        IllegalArgumentException.class, () -> new PipnnPartitioner(10, new int[] {1}, 1, 0.1));
    expectThrows(
        IllegalArgumentException.class, () -> new PipnnPartitioner(10, new int[] {1}, 10, 0));
    expectThrows(
        IllegalArgumentException.class, () -> new PipnnPartitioner(10, new int[] {0}, 10, 0.1));
  }

  public void testNumLeadersAndFanout() {
    PipnnPartitioner partitioner = new PipnnPartitioner(100, new int[] {4, 2}, 50, 0.01);
    assertEquals(8, partitioner.numLeaders(10)); // at least MIN_LEADERS
    assertEquals(5, partitioner.numLeaders(5)); // but never more than the points
    assertEquals(10, partitioner.numLeaders(1000));
    assertEquals(3, new PipnnPartitioner(100, new int[] {1}, 3, 0.5).numLeaders(10)); // maxLeaders
    assertEquals(50, partitioner.numLeaders(100_000)); // capped
    assertEquals(4, partitioner.fanout(0, 10));
    assertEquals(2, partitioner.fanout(1, 10));
    assertEquals(1, partitioner.fanout(2, 10)); // beyond the configured depths
    assertEquals(3, partitioner.fanout(0, 3)); // never more than the leaders
  }

  public void testSampleLeadersAreDistinctMembers() {
    int size = TestUtil.nextInt(random(), 1, 100);
    int[] points = new int[size + 10];
    for (int i = 0; i < points.length; i++) {
      points[i] = 1000 + i;
    }
    int count = TestUtil.nextInt(random(), 1, size);
    int[] leaders =
        PipnnPartitioner.sampleLeaders(
            points, size, count, new SplittableRandom(random().nextLong()));
    assertEquals(count, leaders.length);
    Set<Integer> seen = new HashSet<>();
    for (int leader : leaders) {
      assertTrue(leader >= 1000 && leader < 1000 + size);
      assertTrue("duplicate leader " + leader, seen.add(leader));
    }
    // the input is left untouched
    for (int i = 0; i < points.length; i++) {
      assertEquals(1000 + i, points[i]);
    }
  }

  public void testSelectTopMatchesSorting() {
    for (int iter = 0; iter < 100; iter++) {
      int size = TestUtil.nextInt(random(), 1, 50);
      int k = TestUtil.nextInt(random(), 1, size);
      float[] scores = new float[size];
      for (int i = 0; i < size; i++) {
        scores[i] = TestUtil.nextInt(random(), 0, 10) / 10f; // plenty of ties
      }
      int[] best = new int[k];
      float[] bestScores = new float[k];
      Arrays.fill(bestScores, Float.NaN); // stale state must not matter
      PipnnPartitioner.selectTop(scores, size, k, best, bestScores);

      Integer[] expected = new Integer[size];
      for (int i = 0; i < size; i++) {
        expected[i] = i;
      }
      // descending score, ties towards the lower index
      Arrays.sort(
          expected,
          (a, b) ->
              Float.compare(scores[b], scores[a]) != 0
                  ? Float.compare(scores[b], scores[a])
                  : Integer.compare(a, b));
      for (int i = 0; i < k; i++) {
        assertEquals("position " + i, (int) expected[i], best[i]);
        assertEquals(scores[expected[i]], bestScores[i], 0f);
      }
    }
  }

  public void testToClusters() {
    int[] points = {10, 11, 12};
    // fanout 2: point 10 -> leaders 0 and 2, point 11 -> 2 and 1, point 12 -> 0 and 2
    int[] assignments = {0, 2, 2, 1, 0, 2};
    PipnnPartitioner.Clusters clusters = PipnnPartitioner.toClusters(points, 3, 3, 2, assignments);
    assertEquals(3, clusters.count());
    assertEquals(2, clusters.size(0));
    assertEquals(1, clusters.size(1));
    assertEquals(3, clusters.size(2));
    assertArrayEquals(new int[] {0, 2, 3, 6}, clusters.offsets());
    assertArrayEquals(new int[] {10, 12, 11, 10, 11, 12}, clusters.members());
  }

  public void testPartitionCoversEveryPointAndRespectsLeafSize() throws IOException {
    int size = TestUtil.nextInt(random(), 1, 3000);
    int dim = TestUtil.nextInt(random(), 2, 8);
    int leafSize = TestUtil.nextInt(random(), 2, 300);
    int[] fanouts = {TestUtil.nextInt(random(), 1, 3), TestUtil.nextInt(random(), 1, 2)};
    PipnnPartitioner partitioner =
        new PipnnPartitioner(leafSize, fanouts, TestUtil.nextInt(random(), 2, 40), 0.05);
    MockVectorValues vectors =
        MockVectorValues.fromValues(
            HnswGraphTestCase.createRandomFloatVectors(size, dim, random()));
    UpdateableRandomVectorScorer scorer = scorer(vectors);

    int[] memberships = new int[size];
    List<int[]> leaves = new ArrayList<>();
    partitioner.partition(
        identity(size),
        size,
        0,
        random().nextLong(),
        scorer,
        (members, count) -> {
          assertTrue("leaf of " + count + " > " + leafSize, count <= leafSize);
          Set<Integer> distinct = new HashSet<>();
          for (int i = 0; i < count; i++) {
            assertTrue(distinct.add(members[i]));
            memberships[members[i]]++;
          }
          leaves.add(Arrays.copyOf(members, count));
        });
    int replication = fanouts[0] * fanouts[1];
    for (int i = 0; i < size; i++) {
      assertTrue("point " + i + " in no leaf", memberships[i] >= 1);
      assertTrue("point " + i + " in " + memberships[i] + " leaves", memberships[i] <= replication);
    }
    if (size <= leafSize) {
      assertEquals(1, leaves.size());
    }
  }

  public void testPartitionIsDeterministic() throws IOException {
    int size = 2000;
    MockVectorValues vectors =
        MockVectorValues.fromValues(HnswGraphTestCase.createRandomFloatVectors(size, 4, random()));
    PipnnPartitioner partitioner = new PipnnPartitioner(64, new int[] {3, 2}, 50, 0.01);
    long seed = random().nextLong();
    List<int[]> first = collectLeaves(partitioner, vectors, size, seed);
    List<int[]> second = collectLeaves(partitioner, vectors, size, seed);
    assertEquals(first.size(), second.size());
    for (int i = 0; i < first.size(); i++) {
      assertArrayEquals(first.get(i), second.get(i));
    }
  }

  /** All vectors identical: carving cannot separate them and must still terminate. */
  public void testPartitionOfDuplicatesTerminates() throws IOException {
    int size = 500;
    float[][] values = new float[size][];
    for (int i = 0; i < size; i++) {
      values[i] = new float[] {1f, 2f, 3f};
    }
    MockVectorValues vectors = MockVectorValues.fromValues(values);
    PipnnPartitioner partitioner = new PipnnPartitioner(16, new int[] {2, 2}, 10, 0.05);
    int[] memberships = new int[size];
    partitioner.partition(
        identity(size),
        size,
        0,
        random().nextLong(),
        scorer(vectors),
        (members, count) -> {
          assertTrue(count <= 16);
          for (int i = 0; i < count; i++) {
            memberships[members[i]]++;
          }
        });
    for (int i = 0; i < size; i++) {
      assertTrue(memberships[i] >= 1);
    }
  }

  public void testSketchHashIsAntisymmetric() throws IOException {
    int size = 50;
    int dim = 6;
    int bits = TestUtil.nextInt(random(), 1, 16);
    MockVectorValues vectors =
        MockVectorValues.fromValues(
            HnswGraphTestCase.createRandomFloatVectors(size, dim, random()));
    PipnnSketches sketches = PipnnSketches.compute(vectors, size, bits, random().nextLong());
    assertEquals(bits, sketches.bits());
    for (int a = 0; a < size; a++) {
      assertEquals(0, sketches.hash(a, a));
      for (int b = 0; b < size; b++) {
        int ab = sketches.hash(a, b);
        int ba = sketches.hash(b, a);
        assertTrue(ab < (1 << bits));
        // a bit is set in at most one direction
        assertEquals(0, ab & ba);
      }
    }
  }

  public void testSketchesAreEncodingIndependent() throws IOException {
    int size = 30;
    int dim = 5;
    byte[][] bytes = HnswGraphTestCase.createRandomByteVectors(size, dim, random());
    float[][] floats = new float[size][dim];
    for (int i = 0; i < size; i++) {
      for (int j = 0; j < dim; j++) {
        floats[i][j] = bytes[i][j];
      }
    }
    long seed = random().nextLong();
    PipnnSketches fromBytes =
        PipnnSketches.compute(MockByteVectorValues.fromValues(bytes), size, 8, seed);
    PipnnSketches fromFloats =
        PipnnSketches.compute(MockVectorValues.fromValues(floats), size, 8, seed);
    for (int a = 0; a < size; a++) {
      for (int b = 0; b < size; b++) {
        assertEquals(fromFloats.hash(a, b), fromBytes.hash(a, b));
      }
    }
  }

  public void testParallelSketchesMatchSequential() throws IOException {
    // above the chunk size so that several workers get a range
    int size = atLeast(9000);
    int dim = 4;
    int bits = TestUtil.nextInt(random(), 1, 16);
    long seed = random().nextLong();
    MockVectorValues vectors =
        MockVectorValues.fromValues(
            HnswGraphTestCase.createRandomFloatVectors(size, dim, random()));
    PipnnSketches sequential = PipnnSketches.compute(vectors, size, bits, seed);
    ExecutorService exec = Executors.newFixedThreadPool(4, new NamedThreadFactory("sketches"));
    try {
      PipnnSketches parallel =
          PipnnSketches.compute(vectors, size, bits, seed, new TaskExecutor(exec), 4);
      for (int i = 0; i < 200; i++) {
        int a = random().nextInt(size);
        int b = random().nextInt(size);
        assertEquals(sequential.hash(a, b), parallel.hash(a, b));
      }
    } finally {
      exec.shutdownNow();
    }
  }

  public void testSketchesInvalidBits() {
    MockVectorValues vectors =
        MockVectorValues.fromValues(HnswGraphTestCase.createRandomFloatVectors(2, 2, random()));
    expectThrows(IllegalArgumentException.class, () -> PipnnSketches.compute(vectors, 2, 0, 1));
    expectThrows(IllegalArgumentException.class, () -> PipnnSketches.compute(vectors, 2, 17, 1));
  }

  private static List<int[]> collectLeaves(
      PipnnPartitioner partitioner, MockVectorValues vectors, int size, long seed)
      throws IOException {
    List<int[]> leaves = new ArrayList<>();
    partitioner.partition(
        identity(size),
        size,
        0,
        seed,
        scorer(vectors),
        (members, count) -> leaves.add(Arrays.copyOf(members, count)));
    return leaves;
  }

  private static UpdateableRandomVectorScorer scorer(MockVectorValues vectors) throws IOException {
    return DefaultFlatVectorScorer.INSTANCE
        .getRandomVectorScorerSupplier(VectorSimilarityFunction.EUCLIDEAN, vectors)
        .scorer();
  }

  private static int[] identity(int size) {
    int[] points = new int[size];
    for (int i = 0; i < size; i++) {
      points[i] = i;
    }
    return points;
  }
}
