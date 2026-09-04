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
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IORunnable;
import org.junit.Before;

/** Tests HNSW KNN graphs */
public class TestHnswFloatVectorGraph extends HnswGraphTestCase<float[]> {

  @Before
  public void setup() {
    similarityFunction = RandomizedTest.randomFrom(VectorSimilarityFunction.values());
  }

  @Override
  VectorEncoding getVectorEncoding() {
    return VectorEncoding.FLOAT32;
  }

  @Override
  Query knnQuery(String field, float[] vector, int k) {
    return new KnnFloatVectorQuery(field, vector, k);
  }

  @Override
  float[] randomVector(int dim) {
    return randomVector(random(), dim);
  }

  @Override
  MockVectorValues vectorValues(int size, int dimension) {
    return MockVectorValues.fromValues(createRandomFloatVectors(size, dimension, random()));
  }

  @Override
  MockVectorValues vectorValues(float[][] values) {
    return MockVectorValues.fromValues(values);
  }

  public void testReconnectsNodeDecayedBelowFloorDuringMerge() throws IOException {
    // Pin vectors, both graph seeds, and similarity so the merged graph is identical every run.
    long savedRandSeed = HnswGraphBuilder.randSeed;
    try {
      HnswGraphBuilder.randSeed = 42;
      similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
      int M = 16;
      int beamWidth = 100;
      int size = 128;
      int dim = 16;
      MockVectorValues vectors =
          MockVectorValues.fromValues(createRandomFloatVectors(size, dim, new Random(42)));
      RandomVectorScorerSupplier supplier = buildScorerSupplier(vectors);
      OnHeapHnswGraph graph = HnswGraphBuilder.create(supplier, M, beamWidth, 42).build(size);

      int entry = graph.entryNode();
      int target = entry == 0 ? 1 : 0;

      // Degree 10, below the floor (0.5 * 2M = 16). Deleting one neighbor leaves 9: too small a
      // loss for the per-merge check, but below the floor, so only the cumulative check flags it.
      int degreeBefore = 10;
      NeighborArray targetNeighbors = graph.getNeighbors(0, target);
      targetNeighbors.clear();
      for (int node = 0, added = 0; added < degreeBefore; node++) {
        if (node != target && node != entry) {
          targetNeighbors.addOutOfOrder(node, Float.NaN);
          added++;
        }
      }
      int deleted = targetNeighbors.nodes()[0];
      int[] newOrdMap = new int[size];
      for (int i = 0; i < size; i++) {
        newOrdMap[i] = i;
      }
      newOrdMap[deleted] = -1;

      OnHeapHnswGraph merged =
          InitializedHnswGraphBuilder.initGraph(graph, newOrdMap, size, beamWidth, supplier);

      // Old per-merge-only check leaves the target at 9; the cumulative check reconnects it.
      assertTrue(
          "target below the cumulative floor should be reconnected during merge",
          merged.getNeighbors(0, target).size() >= degreeBefore);
    } finally {
      HnswGraphBuilder.randSeed = savedRandSeed;
    }
  }

  /**
   * A cancelled merge must be able to abort HNSW graph construction during the graph-join
   * initialization and disconnected-node repair phases, not only during the subsequent incremental
   * node insertion.
   */
  public void testAbortCheckInterruptsGraphInitAndRepair() throws IOException {
    // Pin vectors, both graph seeds, and similarity so the merged graph is identical every run.
    long savedRandSeed = HnswGraphBuilder.randSeed;
    try {
      HnswGraphBuilder.randSeed = 42;
      similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
      int M = 16;
      int beamWidth = 100;
      int size = 256;
      int dim = 16;
      MockVectorValues vectors =
          MockVectorValues.fromValues(createRandomFloatVectors(size, dim, new Random(42)));
      RandomVectorScorerSupplier supplier = buildScorerSupplier(vectors);
      OnHeapHnswGraph graph = HnswGraphBuilder.create(supplier, M, beamWidth, 42).build(size);

      // Delete every other document (never the entry node) so the merge takes the graph-join path
      // with deletes: many survivors lose neighbors and get repaired by fixDisconnectedNodes.
      int entry = graph.entryNode();
      int[] newOrdMap = new int[size];
      for (int i = 0; i < size; i++) {
        newOrdMap[i] = (i == entry || (i & 1) == 0) ? i : -1;
      }

      // copyGraphStructure polls the abort check exactly once per source node across every level.
      int copyLoopChecks = 0;
      for (int level = 0; level < graph.numLevels(); level++) {
        copyLoopChecks += graph.getNodesOnLevel(level).size();
      }

      // Test that aborting on the very first check stops the graph initialization immediately
      IORunnable abortImmediately =
          () -> {
            throw new MergePolicy.MergeAbortedException("aborted before init");
          };
      expectThrows(
          MergePolicy.MergeAbortedException.class,
          () ->
              InitializedHnswGraphBuilder.initGraph(
                  graph, newOrdMap, size, beamWidth, supplier, abortImmediately));

      // Test that aborting during repair stops the graph initialization immediately
      AtomicInteger checksUntilAbort = new AtomicInteger(copyLoopChecks + 1);
      IORunnable abortDuringRepair =
          () -> {
            if (checksUntilAbort.decrementAndGet() == 0) {
              throw new MergePolicy.MergeAbortedException("aborted mid-repair");
            }
          };
      expectThrows(
          MergePolicy.MergeAbortedException.class,
          () ->
              InitializedHnswGraphBuilder.initGraph(
                  graph, newOrdMap, size, beamWidth, supplier, abortDuringRepair));

      // Test that the graph can be correctly initialized
      OnHeapHnswGraph merged =
          InitializedHnswGraphBuilder.initGraph(graph, newOrdMap, size, beamWidth, supplier, null);
      assertNotNull(merged);
    } finally {
      HnswGraphBuilder.randSeed = savedRandSeed;
    }
  }

  @Override
  MockVectorValues vectorValues(LeafReader reader, String fieldName) throws IOException {
    FloatVectorValues vectorValues = reader.getFloatVectorValues(fieldName);
    float[][] vectors = new float[reader.maxDoc()][];
    for (int i = 0; i < vectorValues.size(); i++) {
      vectors[vectorValues.ordToDoc(i)] =
          ArrayUtil.copyOfSubArray(vectorValues.vectorValue(i), 0, vectorValues.dimension());
    }
    return MockVectorValues.fromValues(vectors);
  }

  @Override
  MockVectorValues vectorValues(
      int size, int dimension, KnnVectorValues pregeneratedVectorValues, int pregeneratedOffset) {
    MockVectorValues pvv = (MockVectorValues) pregeneratedVectorValues;
    float[][] vectors = new float[size][];
    float[][] randomVectors =
        createRandomFloatVectors(size - pvv.values.length, dimension, random());

    for (int i = 0; i < pregeneratedOffset; i++) {
      vectors[i] = randomVectors[i];
    }

    for (int currentOrd = 0; currentOrd < pvv.size(); currentOrd++) {
      vectors[pregeneratedOffset + currentOrd] = pvv.values[currentOrd];
    }

    for (int i = pregeneratedOffset + pvv.values.length; i < vectors.length; i++) {
      vectors[i] = randomVectors[i - pvv.values.length];
    }

    return MockVectorValues.fromValues(vectors);
  }

  @Override
  Field knnVectorField(String name, float[] vector, VectorSimilarityFunction similarityFunction) {
    return new KnnFloatVectorField(name, vector, similarityFunction);
  }

  @Override
  CircularFloatVectorValues circularVectorValues(int nDoc) {
    return new CircularFloatVectorValues(nDoc);
  }

  @Override
  float[] getTargetVector() {
    return new float[] {1f, 0f};
  }

  public void testSearchWithSkewedAcceptOrds() throws IOException {
    int nDoc = 1000;
    similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
    FloatVectorValues vectors = circularVectorValues(nDoc);
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectors);
    HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, 16, 100, random().nextInt());
    OnHeapHnswGraph hnsw = builder.build(vectors.size());

    // Skip over half of the documents that are closest to the query vector
    FixedBitSet acceptOrds = new FixedBitSet(nDoc);
    for (int i = 500; i < nDoc; i++) {
      acceptOrds.set(i);
    }
    KnnCollector nn =
        HnswGraphSearcher.search(
            buildScorer(vectors, getTargetVector()), 10, hnsw, acceptOrds, Integer.MAX_VALUE);

    TopDocs nodes = nn.topDocs();
    assertEquals("Number of found results is not equal to [10].", 10, nodes.scoreDocs.length);
    int sum = 0;
    for (ScoreDoc node : nodes.scoreDocs) {
      assertTrue("the results include a deleted document: " + node, acceptOrds.get(node.doc));
      sum += node.doc;
    }
    // We still expect to get reasonable recall. The lowest non-skipped docIds
    // are closest to the query vector: sum(500,509) = 5045
    assertTrue("sum(result docs)=" + sum, sum < 5100);
  }
}
