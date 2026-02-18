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
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
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
import org.apache.lucene.util.NamedThreadFactory;
import org.junit.Before;

/** Tests collaborative HNSW search with dynamic threshold updates and recall validation */
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

    System.arraycopy(randomVectors, 0, vectors, 0, pregeneratedOffset);
    for (int currentOrd = 0; currentOrd < pvv.size(); currentOrd++) {
      vectors[pregeneratedOffset + currentOrd] = pvv.values[currentOrd];
    }
    System.arraycopy(
        randomVectors,
        pregeneratedOffset,
        vectors,
        pregeneratedOffset + pvv.values.length,
        size - (pregeneratedOffset + pvv.values.length));

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
    TopDocs standardTopDocs = standardCollector.topDocs();

    // 2. Collaborative search where we raise the bar externally
    float highBar = standardTopDocs.scoreDocs[0].score;
    int highBarDocId = standardTopDocs.scoreDocs[0].doc;

    LongAccumulator minScoreAcc = new LongAccumulator(Math::max, Long.MIN_VALUE);
    minScoreAcc.accumulate(CollaborativeKnnCollector.encode(highBarDocId, highBar));

    CollaborativeKnnCollector collaborativeCollector =
        new CollaborativeKnnCollector(10, Integer.MAX_VALUE, minScoreAcc, vectors.size() + 1);

    HnswGraphSearcher.search(scorer, collaborativeCollector, hnsw, null);
    long collaborativeVisited = collaborativeCollector.visitedCount();

    if (VERBOSE) {
      System.out.println("Standard visited: " + standardVisited);
      System.out.println("Collaborative visited: " + collaborativeVisited);
    }

    assertTrue(
        "Collaborative search should visit fewer nodes", collaborativeVisited <= standardVisited);
  }

  @Nightly
  public void testHighKPruning() throws IOException {
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
    TopDocs standardTopDocs = standardCollector.topDocs();

    float globalBar = standardTopDocs.scoreDocs[199].score;
    int globalBarDocId = standardTopDocs.scoreDocs[199].doc;

    LongAccumulator minScoreAcc = new LongAccumulator(Math::max, Long.MIN_VALUE);
    minScoreAcc.accumulate(CollaborativeKnnCollector.encode(globalBarDocId, globalBar));

    CollaborativeKnnCollector collaborativeCollector =
        new CollaborativeKnnCollector(k, Integer.MAX_VALUE, minScoreAcc, vectors.size() + 1);
    HnswGraphSearcher.search(scorer, collaborativeCollector, hnsw, null);
    long collaborativeVisited = collaborativeCollector.visitedCount();

    if (VERBOSE) {
      System.out.println("High-K Standard visited: " + standardVisited);
      System.out.println("High-K Collaborative visited: " + collaborativeVisited);
    }
    assertTrue(
        "High-K Collaborative search should visit fewer nodes",
        collaborativeVisited <= standardVisited);
  }

  @Nightly
  public void testHighDimensionPruning() throws IOException {
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
    TopDocs standardTopDocs = standardCollector.topDocs();

    float highBar = standardTopDocs.scoreDocs[49].score;
    int highBarDocId = standardTopDocs.scoreDocs[49].doc;

    LongAccumulator minScoreAcc = new LongAccumulator(Math::max, Long.MIN_VALUE);
    minScoreAcc.accumulate(CollaborativeKnnCollector.encode(highBarDocId, highBar));

    CollaborativeKnnCollector collaborativeCollector =
        new CollaborativeKnnCollector(100, Integer.MAX_VALUE, minScoreAcc, vectors.size() + 1);
    HnswGraphSearcher.search(scorer, collaborativeCollector, hnsw, null);
    long collaborativeVisited = collaborativeCollector.visitedCount();

    if (VERBOSE) {
      System.out.println("High-Dim Standard visited: " + standardVisited);
      System.out.println("High-Dim Collaborative visited: " + collaborativeVisited);
    }
    assertTrue(
        "High-Dim Collaborative search should prune effectively",
        collaborativeVisited <= standardVisited);
  }

  /**
   * Simulates a "Cluster Production Environment" where multiple nodes (shards) each with their own
   * thread pool search concurrently and share a global bar.
   */
  @Nightly
  public void testClusterProductionSimulation() throws IOException, InterruptedException {
    int numShards = 3;
    int docsPerShard = 5000;
    int dim = 64;
    int k = 100;
    String fieldName = "vector";

    List<Directory> shardDirs = new ArrayList<>();
    List<ExecutorService> shardPools = new ArrayList<>();
    List<DirectoryReader> shardReaders = new ArrayList<>();

    try {
      // 1. Build the "Cluster" (3 independent indices)
      for (int i = 0; i < numShards; i++) {
        Directory dir = newDirectory();
        shardDirs.add(dir);
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
          for (int doc = 0; doc < docsPerShard; doc++) {
            Document d = new Document();
            d.add(new KnnFloatVectorField(fieldName, randomVector(dim), similarityFunction));
            writer.addDocument(d);
          }
          writer.commit();
        }
        shardReaders.add(DirectoryReader.open(dir));
        shardPools.add(
            Executors.newFixedThreadPool(
                4, new NamedThreadFactory("shard-" + i))); // Each node has its own pool
      }

      float[] queryVec = randomVector(dim);
      int[] exactIds = computeExactTopKFromMultiShard(shardReaders, fieldName, queryVec, k);

      // 2. Collaborative Multi-Shard Search
      LongAccumulator globalBar = new LongAccumulator(Math::max, Long.MIN_VALUE);
      List<CompletableFuture<TopDocs>> futures = new ArrayList<>();
      List<TrackingCollaborativeKnnQuery> queries = new ArrayList<>();

      for (int i = 0; i < numShards; i++) {
        IndexSearcher shardSearcher = new IndexSearcher(shardReaders.get(i), shardPools.get(i));
        TrackingCollaborativeKnnQuery q =
            new TrackingCollaborativeKnnQuery(fieldName, queryVec, k, globalBar);
        queries.add(q);
        // Execute on the specific shard's pool to simulate independent node execution
        final int shardIdx = i;
        futures.add(
            CompletableFuture.supplyAsync(
                () -> {
                  try {
                    return shardSearcher.search(q, k);
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                },
                shardPools.get(shardIdx)));
      }

      CompletableFuture.allOf(futures.toArray(new CompletableFuture<?>[0])).join();

      long totalCollaborativeVisited = 0;
      for (var q : queries) totalCollaborativeVisited += q.getTotalVisitedCount();

      // 3. Measure Recall of Merged Collaborative Results
      TopDocs[] shardResults = new TopDocs[numShards];
      for (int i = 0; i < numShards; i++) shardResults[i] = futures.get(i).getNow(null);
      TopDocs mergedResults = TopDocs.merge(k, shardResults);
      double collaborativeRecall =
          computeOverlap(topDocIds(mergedResults, k), exactIds) / (double) k;

      if (VERBOSE) {
        System.out.println("=== Cluster Production Simulation ===");
        System.out.println("Total Shards: " + numShards);
        System.out.println("Collaborative Visited: " + totalCollaborativeVisited);
        System.out.println("Collaborative Recall: " + collaborativeRecall);
      }

      assertTrue(
          "Collaborative recall should be non-trivial (" + collaborativeRecall + ")",
          collaborativeRecall >= 0.1);

    } finally {
      for (var p : shardPools) {
        p.shutdown();
        assertTrue(
            "Thread pool did not terminate gracefully", p.awaitTermination(5, TimeUnit.SECONDS));
      }
      for (var r : shardReaders) r.close();
      for (var d : shardDirs) d.close();
    }
  }

  private int[] computeExactTopKFromMultiShard(
      List<DirectoryReader> readers, String field, float[] target, int k) throws IOException {
    NeighborQueue queue = new NeighborQueue(k, false);
    int docBase = 0;
    for (var reader : readers) {
      for (LeafReaderContext ctx : reader.leaves()) {
        FloatVectorValues vectors = ctx.reader().getFloatVectorValues(field);
        if (vectors == null) continue;
        FloatVectorValues copy = vectors.copy();
        for (int i = 0; i < copy.size(); i++) {
          float score = similarityFunction.compare(target, copy.vectorValue(i));
          queue.insertWithOverflow(docBase + ctx.docBase + copy.ordToDoc(i), score);
        }
      }
      docBase += reader.maxDoc();
    }
    return queue.nodes();
  }

  private static int[] topDocIds(TopDocs topDocs, int k) {
    int n = Math.min(k, topDocs.scoreDocs.length);
    int[] docs = new int[n];
    for (int i = 0; i < n; i++) docs[i] = topDocs.scoreDocs[i].doc;
    return docs;
  }

  private static int computeOverlap(int[] a, int[] b) {
    Arrays.sort(a);
    Arrays.sort(b);
    int overlap = 0;
    for (int i = 0, j = 0; i < a.length && j < b.length; ) {
      if (a[i] == b[j]) {
        overlap++;
        i++;
        j++;
      } else if (a[i] > b[j]) j++;
      else i++;
    }
    return overlap;
  }

  private static class TrackingCollaborativeKnnQuery extends KnnFloatVectorQuery {
    private final LongAccumulator minScoreAcc;
    private final AtomicLong totalVisitedCount = new AtomicLong();

    TrackingCollaborativeKnnQuery(
        String field, float[] target, int k, LongAccumulator minScoreAcc) {
      super(field, target, k);
      this.minScoreAcc = minScoreAcc;
    }

    @Override
    protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
      return new CollaborativeKnnCollectorManager(k, minScoreAcc);
    }

    @Override
    protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
      long visited = 0;
      for (TopDocs td : perLeafResults) visited += td.totalHits.value();
      totalVisitedCount.set(visited);
      return super.mergeLeafResults(perLeafResults);
    }

    long getTotalVisitedCount() {
      return totalVisitedCount.get();
    }
  }
}
