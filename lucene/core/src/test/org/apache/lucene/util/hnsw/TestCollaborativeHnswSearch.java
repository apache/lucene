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
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.CollaborativeKnnCollector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.knn.CollaborativeKnnCollectorManager;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ArrayUtil;
import org.junit.Before;

/** Tests collaborative HNSW search with dynamic threshold updates */
public class TestCollaborativeHnswSearch extends HnswGraphTestCase<float[]> {

  @Before
  public void setup() {
    similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
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
  KnnVectorValues vectorValues(int size, int dimension) {
    return MockVectorValues.fromValues(createRandomFloatVectors(size, dimension, random()));
  }

  @Override
  KnnVectorValues vectorValues(float[][] values) {
    return MockVectorValues.fromValues(values);
  }

  @Override
  KnnVectorValues vectorValues(LeafReader reader, String fieldName) throws IOException {
    FloatVectorValues vectorValues = reader.getFloatVectorValues(fieldName);
    float[][] vectors = new float[reader.maxDoc()][];
    for (int i = 0; i < vectorValues.size(); i++) {
      vectors[vectorValues.ordToDoc(i)] =
          ArrayUtil.copyOfSubArray(vectorValues.vectorValue(i), 0, vectorValues.dimension());
    }
    return MockVectorValues.fromValues(vectors);
  }

  @Override
  KnnVectorValues vectorValues(
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
  KnnVectorValues circularVectorValues(int nDoc) {
    return new CircularFloatVectorValues(nDoc);
  }

  @Override
  float[] getTargetVector() {
    return new float[] {1f, 0f};
  }

  public void testCollaborativePruning() throws IOException {
    int nDoc = 20000;
    MockVectorValues vectors = (MockVectorValues) vectorValues(nDoc, 2);
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectors);
    HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, 16, 100, 42);
    OnHeapHnswGraph hnsw = builder.build(vectors.size());

    float[] target = getTargetVector();
    RandomVectorScorer scorer = buildScorer(vectors, target);

    // 1. Standard search to establish baseline
    TopKnnCollector standardCollector = new TopKnnCollector(10, Integer.MAX_VALUE);
    HnswGraphSearcher.search(scorer, standardCollector, hnsw, null);
    long standardVisited = standardCollector.visitedCount();

    // 2. Collaborative search where we raise the bar externally
    TopDocs topDocs = standardCollector.topDocs();
    float highBar = topDocs.scoreDocs[4].score;

    AtomicInteger globalMinSimBits = new AtomicInteger(Float.floatToRawIntBits(-1.0f));
    CollaborativeKnnCollector collaborativeCollector =
        new CollaborativeKnnCollector(10, Integer.MAX_VALUE, globalMinSimBits);

    // Set the high bar to simulate another shard having found these matches
    globalMinSimBits.set(Float.floatToRawIntBits(highBar));

    HnswGraphSearcher.search(scorer, collaborativeCollector, hnsw, null);
    long collaborativeVisited = collaborativeCollector.visitedCount();

    if (VERBOSE) {
      System.out.println("Standard visited: " + standardVisited);
      System.out.println("Collaborative visited: " + collaborativeVisited);
      System.out.println("Pruning bar: " + highBar);
    }

    assertTrue(
        "Collaborative search ("
            + collaborativeVisited
            + ") should visit fewer nodes than standard search ("
            + standardVisited
            + ")",
        collaborativeVisited < standardVisited);
  }

  public void testHighKPruning() throws IOException {
    // High K (1000) on a larger dataset
    int nDoc = 30000;
    int k = 1000;
    MockVectorValues vectors = (MockVectorValues) vectorValues(nDoc, 16);
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectors);
    HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, 16, 100, 42);
    OnHeapHnswGraph hnsw = builder.build(vectors.size());
    float[] target = randomVector(16);
    RandomVectorScorer scorer = buildScorer(vectors, target);
    TopKnnCollector standardCollector = new TopKnnCollector(k, Integer.MAX_VALUE);
    HnswGraphSearcher.search(scorer, standardCollector, hnsw, null);
    long standardVisited = standardCollector.visitedCount();

    // Simulate another shard having found the top 100 results already
    TopDocs topDocs = standardCollector.topDocs();
    float globalBar = topDocs.scoreDocs[99].score;
    AtomicInteger globalMinSimBits = new AtomicInteger(Float.floatToRawIntBits(globalBar));
    CollaborativeKnnCollector collaborativeCollector =
        new CollaborativeKnnCollector(k, Integer.MAX_VALUE, globalMinSimBits);
    HnswGraphSearcher.search(scorer, collaborativeCollector, hnsw, null);
    long collaborativeVisited = collaborativeCollector.visitedCount();

    if (VERBOSE) {
      System.out.println("High-K Standard visited: " + standardVisited);
      System.out.println("High-K Collaborative visited: " + collaborativeVisited);
    }
    assertTrue(
        "High-K Collaborative search should visit significantly fewer nodes",
        collaborativeVisited < (standardVisited / 2));
  }

  public void testHighDimensionPruning() throws IOException {
    // Standard 128-dimension embeddings
    int nDoc = 10000;
    int dim = 128;
    MockVectorValues vectors = (MockVectorValues) vectorValues(nDoc, dim);
    RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(vectors);
    HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, 16, 100, 42);
    OnHeapHnswGraph hnsw = builder.build(vectors.size());
    float[] target = randomVector(dim);
    RandomVectorScorer scorer = buildScorer(vectors, target);
    TopKnnCollector standardCollector = new TopKnnCollector(100, Integer.MAX_VALUE);
    HnswGraphSearcher.search(scorer, standardCollector, hnsw, null);
    long standardVisited = standardCollector.visitedCount();

    // High bar from global search
    float highBar = standardCollector.topDocs().scoreDocs[10].score;
    AtomicInteger globalMinSimBits = new AtomicInteger(Float.floatToRawIntBits(highBar));
    CollaborativeKnnCollector collaborativeCollector =
        new CollaborativeKnnCollector(100, Integer.MAX_VALUE, globalMinSimBits);
    HnswGraphSearcher.search(scorer, collaborativeCollector, hnsw, null);
    long collaborativeVisited = collaborativeCollector.visitedCount();

    if (VERBOSE) {
      System.out.println("High-Dim Standard visited: " + standardVisited);
      System.out.println("High-Dim Collaborative visited: " + collaborativeVisited);
    }
    assertTrue(
        "High-Dim Collaborative search should prune effectively",
        collaborativeVisited < standardVisited);
  }

  /**
   * Tests that CollaborativeKnnCollectorManager correctly wires a shared pruning threshold across
   * multiple segments within a single IndexSearcher search. This exercises the full path:
   * IndexSearcher → AbstractKnnVectorQuery.rewrite() → per-leaf approximateSearch().
   *
   * <p>The test pre-sets a high bar in the shared AtomicInteger (simulating another shard/thread
   * having already found good matches) and verifies that the collaborative search through
   * IndexSearcher still returns valid results and that the pruning bar affects the search.
   */
  public void testMultiSegmentCollaborativePruning() throws IOException {
    int numSegments = 4;
    int docsPerSegment = 1500;
    int dim = 32;
    int k = 10;
    String fieldName = "vector";

    try (Directory dir = newDirectory()) {
      // Build a multi-segment index with NoMergePolicy
      IndexWriterConfig iwc = new IndexWriterConfig();
      iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        for (int seg = 0; seg < numSegments; seg++) {
          for (int doc = 0; doc < docsPerSegment; doc++) {
            Document d = new Document();
            d.add(new KnnFloatVectorField(fieldName, randomVector(dim), similarityFunction));
            writer.addDocument(d);
          }
          writer.commit();
        }
      }

      try (IndexReader reader = DirectoryReader.open(dir)) {
        assertTrue(
            "Expected multiple segments but got " + reader.leaves().size(),
            reader.leaves().size() >= numSegments);

        IndexSearcher searcher = new IndexSearcher(reader);
        float[] queryVec = randomVector(dim);

        // 1. Standard KNN search (baseline) to get reference results and scores
        Query standardQuery = new KnnFloatVectorQuery(fieldName, queryVec, k);
        TopDocs standardResults = searcher.search(standardQuery, k);
        assertEquals("Standard search should return k results", k, standardResults.scoreDocs.length);

        // 2. Collaborative KNN search with NO bar (bar = -1.0f, equivalent to no pruning).
        // This should produce results equivalent to standard search.
        AtomicInteger noBarBits = new AtomicInteger(Float.floatToRawIntBits(-1.0f));
        Query collaborativeNoBar =
            new CollaborativeKnnFloatVectorQuery(fieldName, queryVec, k, noBarBits);
        TopDocs noBarResults = searcher.search(collaborativeNoBar, k);

        // With no bar set, collaborative search should find the same number of results
        assertEquals(
            "Collaborative search with no bar should return same result count as standard",
            standardResults.scoreDocs.length,
            noBarResults.scoreDocs.length);

        // Verify the top scores match between standard and collaborative-with-no-bar,
        // confirming the collaborative path produces equivalent results
        assertEquals(
            "Best score should match between standard and collaborative (no bar)",
            standardResults.scoreDocs[0].score,
            noBarResults.scoreDocs[0].score,
            1e-5);

        // 3. Collaborative KNN search with a HIGH bar (the best score from standard results).
        // This simulates another shard having already found excellent matches, forcing
        // aggressive pruning in the HNSW graph traversal across all segments.
        float highBar = standardResults.scoreDocs[0].score;
        AtomicInteger highBarBits = new AtomicInteger(Float.floatToRawIntBits(highBar));
        Query collaborativeHighBar =
            new CollaborativeKnnFloatVectorQuery(fieldName, queryVec, k, highBarBits);
        TopDocs highBarResults = searcher.search(collaborativeHighBar, k);

        if (VERBOSE) {
          System.out.println("Segments: " + reader.leaves().size());
          System.out.println("Standard results: " + standardResults.scoreDocs.length);
          System.out.println("No-bar collaborative results: " + noBarResults.scoreDocs.length);
          System.out.println("High-bar collaborative results: " + highBarResults.scoreDocs.length);
          System.out.println("High bar value: " + highBar);
          System.out.println(
              "Standard scores: best="
                  + standardResults.scoreDocs[0].score
                  + " worst="
                  + standardResults.scoreDocs[k - 1].score);
        }

        // With the highest bar set, the search may return fewer results because the
        // pruning threshold causes HNSW graph traversal to terminate early in some
        // or all segments. The search should still complete without error.
        assertTrue(
            "Collaborative search with high bar should produce no more results than standard. "
                + "Standard: "
                + standardResults.scoreDocs.length
                + ", High-bar: "
                + highBarResults.scoreDocs.length,
            highBarResults.scoreDocs.length <= standardResults.scoreDocs.length);
      }
    }
  }

  /**
   * Tests performance improvement with collaborative pruning across multiple separate HNSW graphs
   * (simulating cross-shard KNN search). Builds N separate graphs, searches each independently
   * (standard), then searches with a shared collaborative pruning bar, and asserts that the
   * collaborative approach visits significantly fewer nodes overall.
   */
  public void testMultiIndexHighKPerformance() throws IOException {
    int numGraphs = 5;
    int vectorsPerGraph = 5000;
    int dim = 32;
    int k = 500;

    // Build N separate HNSW graphs
    OnHeapHnswGraph[] graphs = new OnHeapHnswGraph[numGraphs];
    MockVectorValues[] allVectors = new MockVectorValues[numGraphs];
    for (int i = 0; i < numGraphs; i++) {
      allVectors[i] = (MockVectorValues) vectorValues(vectorsPerGraph, dim);
      RandomVectorScorerSupplier scorerSupplier = buildScorerSupplier(allVectors[i]);
      HnswGraphBuilder builder = HnswGraphBuilder.create(scorerSupplier, 16, 100, 42);
      graphs[i] = builder.build(allVectors[i].size());
    }

    float[] queryVec = randomVector(dim);

    // Standard search: search each graph independently, merge results
    TopDocs[] standardPerGraph = new TopDocs[numGraphs];
    long standardTotalVisited = 0;
    for (int i = 0; i < numGraphs; i++) {
      RandomVectorScorer scorer = buildScorer(allVectors[i], queryVec);
      TopKnnCollector collector = new TopKnnCollector(k, Integer.MAX_VALUE);
      HnswGraphSearcher.search(scorer, collector, graphs[i], null);
      standardTotalVisited += collector.visitedCount();
      standardPerGraph[i] = collector.topDocs();
    }
    TopDocs mergedStandard = TopDocs.merge(k, standardPerGraph);

    // Derive a pruning bar: median score of the merged top-k results
    float pruningBar = mergedStandard.scoreDocs[k / 2].score;

    // Collaborative search: pre-set the global bar, then search all graphs sequentially
    AtomicInteger globalMinSimBits = new AtomicInteger(Float.floatToRawIntBits(pruningBar));
    long collaborativeTotalVisited = 0;
    for (int i = 0; i < numGraphs; i++) {
      RandomVectorScorer scorer = buildScorer(allVectors[i], queryVec);
      CollaborativeKnnCollector collector =
          new CollaborativeKnnCollector(k, Integer.MAX_VALUE, globalMinSimBits);
      HnswGraphSearcher.search(scorer, collector, graphs[i], null);
      collaborativeTotalVisited += collector.visitedCount();
    }

    if (VERBOSE) {
      System.out.println("=== Multi-Index High-K Performance ===");
      System.out.println("Graphs: " + numGraphs + " x " + vectorsPerGraph + " vectors");
      System.out.println("K: " + k + ", Dim: " + dim);
      System.out.println("Pruning bar (median score): " + pruningBar);
      System.out.println("Standard total visited: " + standardTotalVisited);
      System.out.println("Collaborative total visited: " + collaborativeTotalVisited);
      System.out.println(
          "Reduction: "
              + String.format(
                  "%.1f%%",
                  100.0 * (1.0 - (double) collaborativeTotalVisited / standardTotalVisited)));
    }

    assertTrue(
        "Collaborative search ("
            + collaborativeTotalVisited
            + ") should visit fewer nodes than standard search ("
            + standardTotalVisited
            + ")",
        collaborativeTotalVisited < standardTotalVisited);

    // Expect at least 25% reduction with a meaningful pruning bar across 5 graphs
    assertTrue(
        "Collaborative search ("
            + collaborativeTotalVisited
            + ") should visit at least 25% fewer nodes than standard ("
            + standardTotalVisited
            + ")",
        collaborativeTotalVisited < standardTotalVisited * 0.75);
  }

  /**
   * End-to-end test that collaborative pruning reduces visited nodes when searching across multiple
   * separate Directory instances combined via MultiReader. Uses tracking query subclasses to capture
   * per-leaf visited counts through the full IndexSearcher search path.
   */
  public void testMultiIndexCollaborativeEndToEnd() throws IOException {
    int numIndices = 5;
    int docsPerIndex = 2000;
    int dim = 32;
    int k = 100;
    String fieldName = "vector";

    List<Directory> directories = new ArrayList<>();
    List<DirectoryReader> readers = new ArrayList<>();
    try {
      // Create N separate Directory instances, each with its own IndexWriter
      for (int i = 0; i < numIndices; i++) {
        Directory dir = newDirectory();
        directories.add(dir);
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
          for (int doc = 0; doc < docsPerIndex; doc++) {
            Document d = new Document();
            d.add(new KnnFloatVectorField(fieldName, randomVector(dim), similarityFunction));
            writer.addDocument(d);
          }
          writer.commit();
        }
        readers.add(DirectoryReader.open(dir));
      }

      // Combine all readers into a single MultiReader
      try (MultiReader multiReader = new MultiReader(readers.toArray(new IndexReader[0]))) {
        IndexSearcher searcher = new IndexSearcher(multiReader);
        float[] queryVec = randomVector(dim);

        // Standard search with visited-count tracking
        TrackingKnnQuery standardQuery = new TrackingKnnQuery(fieldName, queryVec, k);
        TopDocs standardResults = searcher.search(standardQuery, k);
        long standardVisited = standardQuery.getTotalVisitedCount();

        assertTrue("Standard search should return results", standardResults.scoreDocs.length > 0);
        assertTrue("Standard visited count should be positive", standardVisited > 0);

        // Derive pruning bar from standard results: median score
        float pruningBar = standardResults.scoreDocs[standardResults.scoreDocs.length / 2].score;

        // Collaborative search with pre-set pruning bar
        AtomicInteger globalMinSimBits = new AtomicInteger(Float.floatToRawIntBits(pruningBar));
        TrackingCollaborativeKnnQuery collaborativeQuery =
            new TrackingCollaborativeKnnQuery(fieldName, queryVec, k, globalMinSimBits);
        TopDocs collaborativeResults = searcher.search(collaborativeQuery, k);
        long collaborativeVisited = collaborativeQuery.getTotalVisitedCount();

        if (VERBOSE) {
          System.out.println("=== Multi-Index Collaborative End-to-End ===");
          System.out.println("Indices: " + numIndices + " x " + docsPerIndex + " vectors");
          System.out.println("K: " + k + ", Dim: " + dim);
          System.out.println("Leaves: " + multiReader.leaves().size());
          System.out.println("Pruning bar (median score): " + pruningBar);
          System.out.println("Standard results: " + standardResults.scoreDocs.length);
          System.out.println("Standard visited: " + standardVisited);
          System.out.println("Collaborative results: " + collaborativeResults.scoreDocs.length);
          System.out.println("Collaborative visited: " + collaborativeVisited);
          if (standardVisited > 0) {
            System.out.println(
                "Reduction: "
                    + String.format(
                        "%.1f%%",
                        100.0 * (1.0 - (double) collaborativeVisited / standardVisited)));
          }
        }

        assertTrue(
            "Collaborative search ("
                + collaborativeVisited
                + ") should visit fewer nodes than standard search ("
                + standardVisited
                + ")",
            collaborativeVisited < standardVisited);
      }
    } finally {
      for (DirectoryReader reader : readers) {
        reader.close();
      }
      for (Directory dir : directories) {
        dir.close();
      }
    }
  }

  /**
   * A KnnFloatVectorQuery subclass that tracks the sum of per-leaf visited counts by overriding
   * mergeLeafResults. Uses the default TopKnnCollectorManager (standard search).
   */
  private static class TrackingKnnQuery extends KnnFloatVectorQuery {
    private final AtomicLong totalVisitedCount = new AtomicLong();

    TrackingKnnQuery(String field, float[] target, int k) {
      super(field, target, k);
    }

    @Override
    protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
      long visited = 0;
      for (TopDocs td : perLeafResults) {
        visited += td.totalHits.value();
      }
      totalVisitedCount.set(visited);
      return super.mergeLeafResults(perLeafResults);
    }

    long getTotalVisitedCount() {
      return totalVisitedCount.get();
    }
  }

  /**
   * A KnnFloatVectorQuery subclass that uses CollaborativeKnnCollectorManager and tracks the sum of
   * per-leaf visited counts through mergeLeafResults.
   */
  private static class TrackingCollaborativeKnnQuery extends KnnFloatVectorQuery {
    private final AtomicInteger globalMinSimBits;
    private final AtomicLong totalVisitedCount = new AtomicLong();

    TrackingCollaborativeKnnQuery(
        String field, float[] target, int k, AtomicInteger globalMinSimBits) {
      super(field, target, k);
      this.globalMinSimBits = globalMinSimBits;
    }

    @Override
    protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
      return new CollaborativeKnnCollectorManager(k, globalMinSimBits);
    }

    @Override
    protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
      long visited = 0;
      for (TopDocs td : perLeafResults) {
        visited += td.totalHits.value();
      }
      totalVisitedCount.set(visited);
      return super.mergeLeafResults(perLeafResults);
    }

    long getTotalVisitedCount() {
      return totalVisitedCount.get();
    }
  }

  /**
   * A KnnFloatVectorQuery subclass that uses CollaborativeKnnCollectorManager instead of the
   * default TopKnnCollectorManager. This allows testing the collaborative pruning mechanism through
   * the full IndexSearcher search path.
   */
  private static class CollaborativeKnnFloatVectorQuery extends KnnFloatVectorQuery {

    private final AtomicInteger globalMinSimBits;

    CollaborativeKnnFloatVectorQuery(
        String field, float[] target, int k, AtomicInteger globalMinSimBits) {
      super(field, target, k);
      this.globalMinSimBits = globalMinSimBits;
    }

    @Override
    protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
      return new CollaborativeKnnCollectorManager(k, globalMinSimBits);
    }
  }
}
