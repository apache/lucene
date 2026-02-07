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
import java.util.Locale;
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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.knn.CollaborativeKnnCollectorManager;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.ArrayUtil;
import org.junit.Before;

/**
 * A definitive scaling test for Collaborative HNSW Search. Sweeps through various K values and
 * Vector Space sizes to demonstrate real-world gains in distributed-like environments.
 */
public class TestCollaborativeHnswScaling extends HnswGraphTestCase<float[]> {

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
    for (int i = 0; i < pregeneratedOffset; i++) vectors[i] = randomVectors[i];
    for (int currentOrd = 0; currentOrd < pvv.size(); currentOrd++)
      vectors[pregeneratedOffset + currentOrd] = pvv.values[currentOrd];
    for (int i = pregeneratedOffset + pvv.values.length; i < vectors.length; i++)
      vectors[i] = randomVectors[i - pvv.values.length];
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

  @Nightly
  public void testScalingMatrix() throws IOException, InterruptedException {
    int[] kValues = {10, 100, 1000};
    int[] docsPerShardValues = {2000, 10000};
    int numShards = 4;
    int dim = 128; // Modern embedding size

    if (VERBOSE) {
      System.out.println("\n=== Collaborative HNSW Scaling Matrix ===");
      System.out.println(
          String.format(
              Locale.ROOT,
              "%-10s | %-10s | %-15s | %-15s | %-10s | %-10s",
              "K",
              "Total Docs",
              "Std Visited",
              "Collab Visited",
              "Reduction",
              "Recall"));
      System.out.println(
          "-----------|------------|-----------------|-----------------|------------|----------");
    }

    for (int docsPerShard : docsPerShardValues) {
      // Build the shards
      List<Directory> shardDirs = new ArrayList<>();
      List<DirectoryReader> shardReaders = new ArrayList<>();
      List<ExecutorService> shardPools = new ArrayList<>();

      try {
        for (int i = 0; i < numShards; i++) {
          Directory dir = newDirectory();
          shardDirs.add(dir);
          IndexWriterConfig iwc = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
          try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (int d = 0; d < docsPerShard; d++) {
              Document doc = new Document();
              doc.add(new KnnFloatVectorField("vector", randomVector(dim), similarityFunction));
              writer.addDocument(doc);
            }
            writer.commit();
          }
          shardReaders.add(DirectoryReader.open(dir));
          shardPools.add(Executors.newFixedThreadPool(2));
        }

        try (MultiReader multiReader = new MultiReader(shardReaders.toArray(new IndexReader[0]))) {
          for (int k : kValues) {
            float[] queryVec = randomVector(dim);
            int[] exactIds = computeExactTopKFromMultiShard(shardReaders, "vector", queryVec, k);

            // 1. Standard search (Baseline)
            long stdVisited;
            IndexSearcher stdSearcher = new IndexSearcher(multiReader, shardPools.get(0));
            TrackingKnnQuery stdQuery = new TrackingKnnQuery("vector", queryVec, k);
            stdSearcher.search(stdQuery, k);
            stdVisited = stdQuery.getTotalVisitedCount();

            // 2. Collaborative search
            long collabVisited;
            LongAccumulator sharedBar = new LongAccumulator(Math::max, Long.MIN_VALUE);
            IndexSearcher collabSearcher = new IndexSearcher(multiReader, shardPools.get(0));
            TrackingCollaborativeKnnQuery collabQuery =
                new TrackingCollaborativeKnnQuery("vector", queryVec, k, sharedBar);
            TopDocs collabResults = collabSearcher.search(collabQuery, k);
            collabVisited = collabQuery.getTotalVisitedCount();

            // 3. Compute Recall
            double recall = computeOverlap(topDocIds(collabResults, k), exactIds) / (double) k;

            if (VERBOSE) {
              double reduction =
                  stdVisited > 0 ? 100.0 * (1.0 - (double) collabVisited / stdVisited) : 0;
              System.out.println(
                  String.format(
                      Locale.ROOT,
                      "%-10d | %-10d | %-15d | %-15d | %-9.1f%% | %-10.2f",
                      k,
                      docsPerShard * numShards,
                      stdVisited,
                      collabVisited,
                      reduction,
                      recall));
            }
          }
        }
      } finally {
        for (var p : shardPools) {
          p.shutdown();
          p.awaitTermination(5, TimeUnit.SECONDS);
        }
        for (var r : shardReaders) r.close();
        for (var d : shardDirs) d.close();
      }
    }
  }

  /**
   * Stress test specifically for High-K (K=1000+) deep traversal. This demonstrates that as K
   * grows, collaborative search provides increasing technical leverage.
   *
   * <p>This is a "Monster" test that requires significant heap and time.
   */
  @Monster("takes ~1 minute and needs extra heap")
  @Nightly
  public void testHighKScalingStressTest() throws IOException, InterruptedException {
    int numShards = 4;
    int docsPerShard = 25000; // 100K total docs
    int dim = 128;
    int k = 1000; // Large K search
    String fieldName = "vector";

    List<Directory> shardDirs = new ArrayList<>();
    List<DirectoryReader> shardReaders = new ArrayList<>();
    List<ExecutorService> shardPools = new ArrayList<>();

    if (VERBOSE) {
      System.out.println("\n=== High-K Scaling Stress Test (K=" + k + ") ===");
    }

    try {
      for (int i = 0; i < numShards; i++) {
        Directory dir = newDirectory();
        shardDirs.add(dir);
        IndexWriterConfig iwc = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
          for (int d = 0; d < docsPerShard; d++) {
            Document doc = new Document();
            doc.add(new KnnFloatVectorField(fieldName, randomVector(dim), similarityFunction));
            writer.addDocument(doc);
          }
          writer.commit();
        }
        shardReaders.add(DirectoryReader.open(dir));
        shardPools.add(Executors.newFixedThreadPool(4));
      }

      float[] queryVec = randomVector(dim);
      int[] exactIds = computeExactTopKFromMultiShard(shardReaders, fieldName, queryVec, k);

      try (MultiReader multiReader = new MultiReader(shardReaders.toArray(new IndexReader[0]))) {
        // 1. Standard search baseline
        IndexSearcher stdSearcher = new IndexSearcher(multiReader, shardPools.get(0));
        TrackingKnnQuery stdQuery = new TrackingKnnQuery(fieldName, queryVec, k);
        TopDocs stdResults = stdSearcher.search(stdQuery, k);
        long stdVisited = stdQuery.getTotalVisitedCount();
        double stdRecall = computeOverlap(topDocIds(stdResults, k), exactIds) / (double) k;

        // 2. Collaborative search
        LongAccumulator sharedBar = new LongAccumulator(Math::max, Long.MIN_VALUE);
        IndexSearcher collabSearcher = new IndexSearcher(multiReader, shardPools.get(0));
        TrackingCollaborativeKnnQuery collabQuery =
            new TrackingCollaborativeKnnQuery(fieldName, queryVec, k, sharedBar);
        TopDocs collabResults = collabSearcher.search(collabQuery, k);
        long collabVisited = collabQuery.getTotalVisitedCount();
        double collabRecall = computeOverlap(topDocIds(collabResults, k), exactIds) / (double) k;

        if (VERBOSE) {
          System.out.println(
              "Standard Visited:      " + stdVisited + " (Recall: " + stdRecall + ")");
          System.out.println(
              "Collaborative Visited: " + collabVisited + " (Recall: " + collabRecall + ")");
          System.out.println(
              "Work Reduction:        "
                  + String.format(
                      Locale.ROOT,
                      "%.1f%%",
                      (100.0 * (1.0 - (double) collabVisited / stdVisited))));
        }

        assertTrue(
            "Collaborative search should save work in High-K scenario", collabVisited < stdVisited);
        // We expect recall to be lower than standard in randomized tests, but still non-trivial.
        assertTrue("Collaborative recall should be non-trivial", collabRecall >= 0.1);
      }
    } finally {
      for (var p : shardPools) {
        p.shutdown();
        p.awaitTermination(5, TimeUnit.SECONDS);
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

  private static class TrackingKnnQuery extends KnnFloatVectorQuery {
    private final AtomicLong totalVisitedCount = new AtomicLong();

    TrackingKnnQuery(String field, float[] target, int k) {
      super(field, target, k);
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
      KnnCollectorManager delegate = new CollaborativeKnnCollectorManager(k, minScoreAcc);
      return new KnnCollectorManager() {
        @Override
        public KnnCollector newCollector(
            int visitedLimit, KnnSearchStrategy searchStrategy, LeafReaderContext context)
            throws IOException {
          KnnCollector c = delegate.newCollector(visitedLimit, searchStrategy, context);
          return new KnnCollector.Decorator(c) {
            @Override
            public void incVisitedCount(int count) {
              super.incVisitedCount(count);
              totalVisitedCount.addAndGet(count);
            }
          };
        }
      };
    }

    long getTotalVisitedCount() {
      return totalVisitedCount.get();
    }
  }
}
