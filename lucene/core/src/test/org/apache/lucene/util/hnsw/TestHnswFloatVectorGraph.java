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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TaskExecutor;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IORunnable;
import org.apache.lucene.util.NamedThreadFactory;
import org.apache.lucene.util.PrintStreamInfoStream;
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

  /**
   * With a single worker, {@link HnswConcurrentMergeBuilder}'s locked repair branch (non-null
   * {@code hnswLock}) must produce the same graph as the serial {@link
   * InitializedHnswGraphBuilder#initGraph} path (null {@code hnswLock}). Copy, repair and the
   * seeded rebalance are all deterministic, and one worker repairs in the same order as the serial
   * loop, so every level must match.
   */
  public void testSingleWorkerRepairMatchesSerial() throws IOException {
    long savedRandSeed = HnswGraphBuilder.randSeed;
    ExecutorService exec = Executors.newFixedThreadPool(1, new NamedThreadFactory("hnswRepair1"));
    try {
      HnswGraphBuilder.randSeed = 42;
      similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
      int M = 16;
      int beamWidth = 100;
      int size = 512;
      int dim = 16;
      MockVectorValues vectors =
          MockVectorValues.fromValues(createRandomFloatVectors(size, dim, new Random(42)));
      RandomVectorScorerSupplier supplier = buildScorerSupplier(vectors);
      OnHeapHnswGraph base = HnswGraphBuilder.create(supplier, M, beamWidth, 42).build(size);

      // Delete ~33% (<= 40% so the base graph is reused), compacting survivors into a dense
      // ordinal space so the merged graph has no holes.
      int[] newOrdMap = new int[size];
      int liveCount = 0;
      for (int old = 0; old < size; old++) {
        newOrdMap[old] = (old % 3 == 1) ? -1 : liveCount++;
      }

      OnHeapHnswGraph serial =
          InitializedHnswGraphBuilder.initGraph(base, newOrdMap, liveCount, beamWidth, supplier);

      InitializedHnswGraphBuilder.CopiedGraph copied =
          InitializedHnswGraphBuilder.copyGraph(
              supplier, beamWidth, base, newOrdMap, liveCount, null);
      assertTrue("deletes must trigger repair for this test to be meaningful", copied.hasDeletes());
      List<Integer> flaggedAtLevel0 = copied.disconnectedNodesByLevel().get(0);
      assertTrue(
          "level 0 must have flagged nodes, else the equality is vacuous",
          flaggedAtLevel0 != null && !flaggedAtLevel0.isEmpty());
      FixedBitSet initialized = new FixedBitSet(liveCount);
      initialized.set(0, liveCount); // all live nodes pre-populated; no extra inserts in build()
      HnswConcurrentMergeBuilder builder =
          new HnswConcurrentMergeBuilder(
              new TaskExecutor(exec), 1, supplier, beamWidth, copied.graph(), initialized, copied);
      builder.build(liveCount);
      OnHeapHnswGraph concurrent = builder.getCompletedGraph();

      assertEquals(serial.size(), concurrent.size());
      assertEquals(serial.numLevels(), concurrent.numLevels());
      assertEquals(serial.entryNode(), concurrent.entryNode());
      for (int level = 0; level < serial.numLevels(); level++) {
        for (int node = 0; node < liveCount; node++) {
          assertEquals(
              "node existence differs at level " + level + " for node " + node,
              serial.nodeExistAtLevel(level, node),
              concurrent.nodeExistAtLevel(level, node));
          if (serial.nodeExistAtLevel(level, node)) {
            assertEquals(
                "neighbors differ at level " + level + " for node " + node,
                sortedNeighbors(serial, level, node),
                sortedNeighbors(concurrent, level, node));
          }
        }
      }
    } finally {
      exec.shutdownNow();
      HnswGraphBuilder.randSeed = savedRandSeed;
    }
  }

  /**
   * Multi-worker repair must be thread-safe and correct: no exception or deadlock, the merged graph
   * has the right size and a single rooted level-0 component, and every flagged node is repaired
   * (gains at least one neighbor and never drops below its pre-repair degree). The graph data is
   * pinned so the flagged set is deterministic; the real thread pool supplies the interleaving that
   * {@code -Ptests.dups} stresses. Assertions are floors/invariants only, never full-graph
   * equality.
   */
  public void testConcurrentRepairIsThreadSafeAndRepairsFlaggedNodes() throws IOException {
    long savedRandSeed = HnswGraphBuilder.randSeed;
    ExecutorService exec = Executors.newFixedThreadPool(2, new NamedThreadFactory("hnswRepair2"));
    try {
      HnswGraphBuilder.randSeed = 17;
      similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
      int M = 16;
      int beamWidth = 100;
      int size = 2000;
      int dim = 16;
      MockVectorValues vectors =
          MockVectorValues.fromValues(createRandomFloatVectors(size, dim, new Random(17)));
      RandomVectorScorerSupplier supplier = buildScorerSupplier(vectors);
      OnHeapHnswGraph base = HnswGraphBuilder.create(supplier, M, beamWidth, 17).build(size);

      int[] newOrdMap = new int[size];
      int liveCount = 0;
      for (int old = 0; old < size; old++) {
        newOrdMap[old] = (old % 3 == 1) ? -1 : liveCount++; // ~33% deleted, <= 40%
      }

      InitializedHnswGraphBuilder.CopiedGraph copied =
          InitializedHnswGraphBuilder.copyGraph(
              supplier, beamWidth, base, newOrdMap, liveCount, null);
      assertTrue("deletes must trigger repair", copied.hasDeletes());

      // Snapshot the flagged nodes and their pre-repair degrees, and confirm repair partitions
      // across >= 2 levels so the top-down per-level barrier is actually exercised.
      Map<Integer, List<Integer>> flaggedByLevel = copied.disconnectedNodesByLevel();
      List<int[]> flaggedBefore = new ArrayList<>(); // {level, node, degreeBeforeRepair}
      int levelsWithFlags = 0;
      for (Map.Entry<Integer, List<Integer>> e : flaggedByLevel.entrySet()) {
        if (e.getValue().isEmpty()) {
          continue;
        }
        levelsWithFlags++;
        for (int node : e.getValue()) {
          flaggedBefore.add(
              new int[] {e.getKey(), node, copied.graph().getNeighbors(e.getKey(), node).size()});
        }
      }
      assertTrue(
          "expected flagged nodes on >= 2 levels, got " + levelsWithFlags, levelsWithFlags >= 2);
      assertFalse("expected some flagged nodes", flaggedBefore.isEmpty());
      int maxFlaggedOnAnyLevel = 0;
      for (List<Integer> nodesAtLevel : flaggedByLevel.values()) {
        maxFlaggedOnAnyLevel = Math.max(maxFlaggedOnAnyLevel, nodesAtLevel.size());
      }
      assertTrue(
          "need >= 2 flagged nodes on some level to force concurrent same-level repair, got "
              + maxFlaggedOnAnyLevel,
          maxFlaggedOnAnyLevel >= 2);

      FixedBitSet initialized = new FixedBitSet(liveCount);
      initialized.set(0, liveCount);
      HnswConcurrentMergeBuilder builder =
          new HnswConcurrentMergeBuilder(
              new TaskExecutor(exec), 2, supplier, beamWidth, copied.graph(), initialized, copied);
      builder.build(liveCount);
      OnHeapHnswGraph merged = builder.getCompletedGraph();

      assertEquals("merged graph should contain every live node", liveCount, merged.size());
      // No isolated nodes: level 0 is a single rooted component after concurrent repair.
      assertEquals(
          "graph should be a single rooted component after repair",
          1,
          HnswUtil.componentSizes(merged).size());

      // Every flagged node gained >= 1 neighbor and never lost neighbors during repair.
      for (int[] fb : flaggedBefore) {
        int level = fb[0];
        int node = fb[1];
        int before = fb[2];
        int after = merged.getNeighbors(level, node).size();
        assertTrue(
            "flagged node "
                + node
                + " lost neighbors on level "
                + level
                + ": "
                + before
                + " -> "
                + after,
            after >= before);
        assertTrue(
            "flagged node " + node + " has no neighbors after repair on level " + level, after > 0);
      }
    } finally {
      exec.shutdownNow();
      HnswGraphBuilder.randSeed = savedRandSeed;
    }
  }

  /**
   * End-to-end: a real force-merge with 2 merge workers over an index whose largest segment carries
   * deletes (&lt;= 40%, so its graph is reused as the merge base) drives {@link
   * org.apache.lucene.util.hnsw.ConcurrentHnswMerger} through {@code copyGraph} and the concurrent
   * repair path. The merged graph must be sized correctly, rooted, and searchable.
   */
  public void testConcurrentMergeReuseWithDeletesEndToEnd() throws IOException {
    similarityFunction = RandomizedTest.randomFrom(VectorSimilarityFunction.values());
    int M = 16;
    int beamWidth = 100;
    int dim = 16;
    String field = "vec";
    int baseSize = atLeast(1200);
    int addSize = 300;
    int total = baseSize + addSize;
    MockVectorValues vectors =
        MockVectorValues.fromValues(createRandomFloatVectors(total, dim, random()));
    ExecutorService mergeExec =
        Executors.newFixedThreadPool(2, new NamedThreadFactory("hnsw-merge-worker"));
    try (Directory dir = newDirectory()) {
      ByteArrayOutputStream infoBytes = new ByteArrayOutputStream();
      Codec codec =
          TestUtil.alwaysKnnVectorsFormat(
              // 2 merge workers + always build a graph (tinySegmentsThreshold = 0)
              new Lucene99HnswVectorsFormat(M, beamWidth, 2, mergeExec, 0));
      // ingest with merging off, so the force-merge below sees exactly these two segments
      try (IndexWriter w =
          new IndexWriter(
              dir,
              new IndexWriterConfig().setCodec(codec).setMergePolicy(NoMergePolicy.INSTANCE))) {
        for (int i = 0; i < baseSize; i++) {
          Document doc = new Document();
          doc.add(knnVectorField(field, vectors.vectorValue(i), similarityFunction));
          doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
          w.addDocument(doc);
        }
        w.flush(); // segment 1 = the base graph
        for (int i = baseSize; i < total; i++) {
          Document doc = new Document();
          doc.add(knnVectorField(field, vectors.vectorValue(i), similarityFunction));
          doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
          w.addDocument(doc);
        }
        w.flush(); // segment 2, deletion-free
        // Delete ~30% of the base segment: leaves it eligible as the reuse base AND with deletes.
        for (int d = 0; d < baseSize; d += 10) {
          for (int off = 0; off < 3 && d + off < baseSize; off++) {
            w.deleteDocuments(new Term("id", Integer.toString(d + off)));
          }
        }
        w.commit();
      }
      try (IndexWriter w =
          new IndexWriter(
              dir,
              new IndexWriterConfig()
                  .setCodec(codec)
                  .setInfoStream(
                      new PrintStreamInfoStream(
                          new PrintStream(infoBytes, true, StandardCharsets.UTF_8))))) {
        w.forceMerge(1);
      }
      assertTrue(
          "expected the concurrent reuse+repair path to run",
          infoBytes.toString(StandardCharsets.UTF_8).contains("repaired reused graph"));
      try (IndexReader reader = DirectoryReader.open(dir)) {
        int liveCount = reader.numDocs();
        assertTrue("expected some docs to be deleted", liveCount < total);
        for (LeafReaderContext ctx : reader.leaves()) {
          HnswGraph graph =
              ((Lucene99HnswVectorsReader)
                      ((CodecReader) ctx.reader()).getVectorReader().unwrapReaderForField(field))
                  .getGraph(field);
          assertEquals(liveCount, graph.size());
          for (int node = 0; node < graph.size(); node++) {
            graph.seek(0, node);
            assertNotEquals(
                "node " + node + " has no level-0 neighbors after repair",
                NO_MORE_DOCS,
                graph.nextNeighbor());
          }
        }

        IndexSearcher searcher = newSearcher(reader);
        TopDocs td = searcher.search(knnQuery(field, randomVector(dim), 10), 10);
        assertEquals("KNN search should return k results", 10, td.scoreDocs.length);
      }
    } finally {
      mergeExec.shutdownNow();
    }
  }

  private static List<Integer> sortedNeighbors(OnHeapHnswGraph graph, int level, int node) {
    NeighborArray arr = graph.getNeighbors(level, node);
    List<Integer> neighbors = new ArrayList<>(arr.size());
    int[] nodes = arr.nodes();
    for (int i = 0; i < arr.size(); i++) {
      neighbors.add(nodes[i]);
    }
    Collections.sort(neighbors);
    return neighbors;
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
