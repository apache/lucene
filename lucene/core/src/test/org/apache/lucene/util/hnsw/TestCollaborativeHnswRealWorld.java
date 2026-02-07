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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAccumulator;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.knn.CollaborativeKnnCollectorManager;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Before;

/**
 * Monster test that uses real-world 1024-dimension embeddings from classic literature. Sweeps
 * across segment counts (4, 8, 16, 32) to show how collaborative search pruning scales as
 * the number of segments increases — simulating sharded environments.
 *
 * <p>Both standard and collaborative queries override getKnnCollectorManager to disable Lucene's
 * optimistic per-leaf-k collection, ensuring a fair apples-to-apples comparison of visited nodes.
 */
public class TestCollaborativeHnswRealWorld extends LuceneTestCase {

  private VectorSimilarityFunction similarityFunction;

  @Before
  public void setup() {
    similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;
  }

  @Monster("Loads ~73k real embeddings and sweeps segment counts")
  public void testRealWorldSegmentSweep() throws Exception {
    String dataFileName = "sentences_1024.bin";
    String dataDir = System.getProperty("tests.embeddings.dir");
    assumeTrue(
        "Set -Dtests.embeddings.dir=/path/to/data to enable this test",
        dataDir != null);
    java.nio.file.Path hostPath =
        java.nio.file.Paths.get(dataDir, dataFileName).toAbsolutePath();
    assumeTrue(
        "Data file not found: " + hostPath,
        java.nio.file.Files.exists(hostPath));

    int k = 1000;
    String fieldName = "vector";
    int[] segmentCounts = {4, 8, 16, 32};

    // Read all vectors once into memory
    java.nio.file.Path localPath = createTempDir().resolve(dataFileName);
    java.nio.file.Files.copy(hostPath, localPath);

    int totalDocsAvailable;
    int dim;
    float[][] allVectors;

    try (DataInputStream dis =
        new DataInputStream(new BufferedInputStream(new FileInputStream(localPath.toFile())))) {
      totalDocsAvailable = dis.readInt();
      dim = dis.readInt();
      allVectors = new float[totalDocsAvailable][dim];
      for (int i = 0; i < totalDocsAvailable; i++) {
        int textLen = dis.readInt();
        dis.skipBytes(textLen); // skip text for vector loading
        allVectors[i] = new float[dim];
        for (int v = 0; v < dim; v++) allVectors[i][v] = dis.readFloat();
      }
    }

    // Use doc 100 as the query vector (same across all runs for consistency)
    float[] queryVec = Arrays.copyOf(allVectors[100], dim);

    if (VERBOSE) {
      System.out.println("\n=== Real-World Collaborative Search — Segment Sweep ===");
      System.out.println(
          "Data: " + totalDocsAvailable + " docs, " + dim + "d, k=" + k);
      System.out.println();
      System.out.println(
          String.format(
              Locale.ROOT,
              "%-10s | %-10s | %-12s | %-12s | %-12s | %-10s | %-12s | %-12s",
              "Segments",
              "Docs/Seg",
              "Std Visited",
              "Col Visited",
              "Reduction",
              "Std Recall",
              "Col Recall",
              "Recall Delta"));
      System.out.println(
          "-----------|------------|--------------|--------------|--------------|------------|--------------|-------------");
    }

    for (int numSegments : segmentCounts) {
      int docsPerSegment = totalDocsAvailable / numSegments;
      int totalDocs = docsPerSegment * numSegments;

      // Compute brute-force exact top-k for this doc subset
      int[] exactIds = computeExactTopK(allVectors, totalDocs, queryVec, k);

      Directory dir = newDirectory();
      ExecutorService executor = Executors.newFixedThreadPool(Math.min(numSegments, 16));

      try {
        // Re-read the data file to index with text stored fields
        try (DataInputStream dis =
            new DataInputStream(
                new BufferedInputStream(new FileInputStream(localPath.toFile())))) {
          dis.readInt(); // skip totalDocs header
          dis.readInt(); // skip dim header

          IndexWriterConfig iwc =
              new IndexWriterConfig()
                  .setMergePolicy(NoMergePolicy.INSTANCE)
                  .setRAMBufferSizeMB(512);
          try (IndexWriter writer = new IndexWriter(dir, iwc)) {
            for (int s = 0; s < numSegments; s++) {
              for (int d = 0; d < docsPerSegment; d++) {
                int textLen = dis.readInt();
                byte[] textBytes = new byte[textLen];
                dis.readFully(textBytes);
                String text = new String(textBytes, StandardCharsets.UTF_8);

                float[] vector = new float[dim];
                for (int v = 0; v < dim; v++) vector[v] = dis.readFloat();

                Document doc = new Document();
                doc.add(new KnnFloatVectorField(fieldName, vector, similarityFunction));
                doc.add(new StoredField("content", text));
                writer.addDocument(doc);
              }
              writer.commit();
            }
          }
        }

        try (IndexReader reader = DirectoryReader.open(dir)) {
          assertEquals(
              "Expected " + numSegments + " segments",
              numSegments,
              reader.leaves().size());

          IndexSearcher searcher = new IndexSearcher(reader, executor);

          // 1. Standard search baseline
          TrackingKnnQuery stdQuery = new TrackingKnnQuery(fieldName, queryVec, k);
          TopDocs stdResults = searcher.search(stdQuery, k);
          long stdVisited = stdQuery.getTotalVisitedCount();
          double stdRecall = computeOverlap(topDocIds(stdResults, k), exactIds) / (double) k;

          // 2. Collaborative search
          LongAccumulator sharedBar = new LongAccumulator(Math::max, Long.MIN_VALUE);
          TrackingCollaborativeKnnQuery collabQuery =
              new TrackingCollaborativeKnnQuery(fieldName, queryVec, k, sharedBar);
          TopDocs collabResults = searcher.search(collabQuery, k);
          long collabVisited = collabQuery.getTotalVisitedCount();
          double collabRecall =
              computeOverlap(topDocIds(collabResults, k), exactIds) / (double) k;

          double reduction =
              stdVisited > 0 ? 100.0 * (1.0 - (double) collabVisited / stdVisited) : 0;
          double recallDelta = collabRecall - stdRecall;

          if (VERBOSE) {
            System.out.println(
                String.format(
                    Locale.ROOT,
                    "%-10d | %-10d | %-12d | %-12d | %-11.1f%% | %-10.3f | %-12.3f | %-+12.3f",
                    numSegments,
                    docsPerSegment,
                    stdVisited,
                    collabVisited,
                    reduction,
                    stdRecall,
                    collabRecall,
                    recallDelta));
          }

          assertTrue(
              "Collaborative search should visit fewer or equal nodes with "
                  + numSegments
                  + " segments ("
                  + collabVisited
                  + " vs "
                  + stdVisited
                  + ")",
              collabVisited <= stdVisited);
          assertTrue(
              "Standard recall should be non-trivial with "
                  + numSegments
                  + " segments ("
                  + stdRecall
                  + ")",
              stdRecall >= 0.5);
          assertTrue(
              "Collaborative recall should be non-trivial with "
                  + numSegments
                  + " segments ("
                  + collabRecall
                  + ")",
              collabRecall >= 0.1);
        }
      } finally {
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);
        dir.close();
      }
    }

    if (VERBOSE) {
      System.out.println(
          "-----------|------------|--------------|--------------|--------------|------------|--------------|-------------");
    }
  }

  private int[] computeExactTopK(float[][] allVectors, int numDocs, float[] query, int k) {
    NeighborQueue queue = new NeighborQueue(k, false);
    for (int i = 0; i < numDocs; i++) {
      float score = similarityFunction.compare(query, allVectors[i]);
      queue.insertWithOverflow(i, score);
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

  /**
   * Standard KNN query with non-optimistic collection and visited count tracking. By disabling
   * optimistic collection (isOptimistic=false), each segment searches with full k, matching the
   * same execution path as the collaborative query for a fair comparison.
   */
  private static class TrackingKnnQuery extends KnnFloatVectorQuery {
    private final AtomicLong totalVisitedCount = new AtomicLong();

    TrackingKnnQuery(String field, float[] target, int k) {
      super(field, target, k);
    }

    @Override
    protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
      return new KnnCollectorManager() {
        @Override
        public KnnCollector newCollector(
            int visitedLimit, KnnSearchStrategy searchStrategy, LeafReaderContext context)
            throws IOException {
          return new TopKnnCollector(k, visitedLimit, searchStrategy);
        }
      };
    }

    @Override
    protected TopDocs mergeLeafResults(TopDocs[] perLeafResults) {
      long visited = 0;
      for (TopDocs td : perLeafResults) {
        if (td != null && td.totalHits != null) visited += td.totalHits.value();
      }
      totalVisitedCount.set(visited);
      return super.mergeLeafResults(perLeafResults);
    }

    long getTotalVisitedCount() {
      return totalVisitedCount.get();
    }
  }

  /**
   * Collaborative KNN query with non-optimistic collection and visited count tracking via
   * mergeLeafResults. Uses the same measurement approach as TrackingKnnQuery for consistency.
   */
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
      for (TopDocs td : perLeafResults) {
        if (td != null && td.totalHits != null) visited += td.totalHits.value();
      }
      totalVisitedCount.set(visited);
      return super.mergeLeafResults(perLeafResults);
    }

    long getTotalVisitedCount() {
      return totalVisitedCount.get();
    }
  }
}
