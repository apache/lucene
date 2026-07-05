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

package org.apache.lucene.sandbox.search.knn;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.NamedThreadFactory;

/**
 * End-to-end tests of kNN search through {@link SharedFloorKnnCollectorManager}: the shared floor
 * must reduce work without changing what stock search would have found.
 *
 * <p>Queries here activate the floor at every k (see {@link SharedFloorKnnQuery}), because the
 * point is to exercise the floor path; the activation default is covered by its own test. The k
 * values exceed {@link FloorAwareKnnCollector#DEFAULT_MIN_EXPLORATION_SLOTS}, since at or below it
 * the clamp neutralizes the floor by design and the tests would not be testing anything.
 */
public class TestSharedFloorKnnSearch extends LuceneTestCase {

  private static final String FIELD = "vector";
  private static final VectorSimilarityFunction SIMILARITY = VectorSimilarityFunction.EUCLIDEAN;

  /**
   * The floor must not cost recall relative to stock search, regardless of how the index is cut
   * into segments.
   */
  public void testRecallParityAcrossSegmentCounts() throws IOException {
    int dim = 16;
    int numDocs = 1200;
    int k = 64;
    int numQueries = 10;
    for (int segments : new int[] {1, 2, 5}) {
      float[][] vectors = new float[numDocs][];
      for (int i = 0; i < numDocs; i++) {
        vectors[i] = randomVector(dim);
      }
      try (Directory dir = newDirectory()) {
        indexInSegments(dir, vectors, segments);
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          assertEquals(segments, reader.leaves().size());
          IndexSearcher searcher = new IndexSearcher(reader);
          double stockRecallSum = 0;
          double flooredRecallSum = 0;
          for (int i = 0; i < numQueries; i++) {
            float[] query = randomVector(dim);
            Set<Integer> truth = exactTopK(reader, query, k);
            stockRecallSum +=
                recall(searcher.search(new KnnFloatVectorQuery(FIELD, query, k), k), truth, k);
            flooredRecallSum +=
                recall(searcher.search(new SharedFloorKnnQuery(FIELD, query, k), k), truth, k);
          }
          double stockRecall = stockRecallSum / numQueries;
          double flooredRecall = flooredRecallSum / numQueries;
          assertTrue(
              "shared-floor search lost recall at "
                  + segments
                  + " segments: stock="
                  + stockRecall
                  + " floored="
                  + flooredRecall,
              flooredRecall >= stockRecall - 0.05);
        }
      }
    }
  }

  /**
   * With no executor, the leaves of a query are searched one after another, and a segment that
   * converges early establishes a floor before later segments have collected anything. This is the
   * harshest ordering for a shared bound, and the ascent gate is what makes it safe. The scenario
   * is made adversarial: the first segment is a tight cluster of mediocre near-duplicates that
   * converges quickly to a high local cutoff, while every true neighbor lives in later segments,
   * still unsearched when that cutoff is published.
   */
  public void testDecoyFirstSegmentDoesNotStarveLaterSegments() throws IOException {
    int dim = 16;
    int k = 64;
    int trueNeighborSegments = 4;
    int trueNeighborsPerSegment = 16;
    int backgroundPerSegment = 300;
    int decoyCount = 400;

    float[] center = randomVector(dim);
    float[] decoyDirection = randomUnitVector(dim);

    // Segment 0: the decoy cluster, at moderate distance from the query, internally very dense.
    List<float[]> decoySegment = new ArrayList<>(decoyCount);
    for (int i = 0; i < decoyCount; i++) {
      decoySegment.add(displaced(center, decoyDirection, 2f, 0.02f));
    }

    // Segments 1..4: a few true nearest neighbors each, hidden among distant background vectors.
    List<List<float[]>> laterSegments = new ArrayList<>(trueNeighborSegments);
    for (int s = 0; s < trueNeighborSegments; s++) {
      List<float[]> segment = new ArrayList<>(trueNeighborsPerSegment + backgroundPerSegment);
      for (int i = 0; i < trueNeighborsPerSegment; i++) {
        segment.add(
            displaced(center, randomUnitVector(dim), 0.9f + random().nextFloat() * 0.2f, 0f));
      }
      for (int i = 0; i < backgroundPerSegment; i++) {
        segment.add(displaced(center, randomUnitVector(dim), 5f + random().nextFloat(), 0f));
      }
      laterSegments.add(segment);
    }

    try (Directory dir = newDirectory()) {
      try (IndexWriter writer =
          new IndexWriter(dir, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
        addSegment(writer, decoySegment);
        for (List<float[]> segment : laterSegments) {
          addSegment(writer, segment);
        }
        writer.commit();
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(1 + trueNeighborSegments, reader.leaves().size());
        IndexSearcher searcher = new IndexSearcher(reader);
        int numQueries = 5;
        double stockRecallSum = 0;
        double flooredRecallSum = 0;
        for (int i = 0; i < numQueries; i++) {
          float[] query = displaced(center, randomUnitVector(dim), 0.05f, 0f);
          Set<Integer> truth = exactTopK(reader, query, k);
          stockRecallSum +=
              recall(searcher.search(new KnnFloatVectorQuery(FIELD, query, k), k), truth, k);
          flooredRecallSum +=
              recall(searcher.search(new SharedFloorKnnQuery(FIELD, query, k), k), truth, k);
        }
        double stockRecall = stockRecallSum / numQueries;
        double flooredRecall = flooredRecallSum / numQueries;
        assertTrue(
            "an early-converging decoy segment starved the segments holding the true neighbors: "
                + "stock="
                + stockRecall
                + " floored="
                + flooredRecall,
            flooredRecall >= stockRecall - 0.05);
      }
    }
  }

  /**
   * Without an executor, execution is single-threaded and the floor evolves identically on every
   * run, so two executions of the same search must return exactly the same documents and scores.
   */
  public void testSequentialSearchIsDeterministic() throws IOException {
    int dim = 16;
    int numDocs = 1000;
    int k = 32;
    float[][] vectors = new float[numDocs][];
    for (int i = 0; i < numDocs; i++) {
      vectors[i] = randomVector(dim);
    }
    try (Directory dir = newDirectory()) {
      indexInSegments(dir, vectors, 4);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        for (int i = 0; i < 5; i++) {
          float[] query = randomVector(dim);
          // Fresh query objects: a manager and its floor carry single-execution state.
          TopDocs first = searcher.search(new SharedFloorKnnQuery(FIELD, query, k), k);
          TopDocs second = searcher.search(new SharedFloorKnnQuery(FIELD, query, k), k);
          assertEquals(first.scoreDocs.length, second.scoreDocs.length);
          for (int j = 0; j < first.scoreDocs.length; j++) {
            assertEquals("doc at rank " + j, first.scoreDocs[j].doc, second.scoreDocs[j].doc);
            assertEquals(
                "score at rank " + j, first.scoreDocs[j].score, second.scoreDocs[j].score, 0.0f);
          }
        }
      }
    }
  }

  /**
   * Under an executor the leaves are searched concurrently and the floor's evolution depends on
   * thread interleaving, which may legitimately vary visit counts; result quality must not suffer.
   */
  public void testParallelRecallMatchesSequential() throws Exception {
    int dim = 16;
    int numDocs = 1200;
    int k = 64;
    int numQueries = 10;
    float[][] vectors = new float[numDocs][];
    for (int i = 0; i < numDocs; i++) {
      vectors[i] = randomVector(dim);
    }
    ExecutorService executor =
        Executors.newFixedThreadPool(4, new NamedThreadFactory("shared-floor-knn-test"));
    try (Directory dir = newDirectory()) {
      indexInSegments(dir, vectors, 5);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher sequentialSearcher = new IndexSearcher(reader);
        IndexSearcher parallelSearcher = new IndexSearcher(reader, executor);
        double sequentialRecallSum = 0;
        double parallelRecallSum = 0;
        for (int i = 0; i < numQueries; i++) {
          float[] query = randomVector(dim);
          Set<Integer> truth = exactTopK(reader, query, k);
          sequentialRecallSum +=
              recall(
                  sequentialSearcher.search(new SharedFloorKnnQuery(FIELD, query, k), k), truth, k);
          parallelRecallSum +=
              recall(
                  parallelSearcher.search(new SharedFloorKnnQuery(FIELD, query, k), k), truth, k);
        }
        double sequentialRecall = sequentialRecallSum / numQueries;
        double parallelRecall = parallelRecallSum / numQueries;
        assertTrue(
            "parallel execution lost recall: sequential="
                + sequentialRecall
                + " parallel="
                + parallelRecall,
            parallelRecall >= sequentialRecall - 0.05);
      }
    } finally {
      executor.shutdown();
      assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  /**
   * A bound advertised from outside the searching process with realistic slack, here the query's
   * exact similarity at global rank 4k, must not cost recall. Tightness, not validity, is what
   * risks recall with an externally advertised bound: a bar close to the final cutoff, imposed
   * before the graph search has discovered its neighborhood, can sever the paths through mediocre
   * intermediate nodes that graph navigation depends on. An advertiser is therefore expected to
   * leave rank headroom (for example, a scout advertising its k'-th best for k' several times k),
   * and this test pins down that a bound with such headroom is harmless.
   */
  public void testSlackAdvertisedBoundPreservesRecall() throws IOException {
    int dim = 16;
    int numDocs = 1200;
    int k = 64;
    int numQueries = 10;
    float[][] vectors = new float[numDocs][];
    for (int i = 0; i < numDocs; i++) {
      vectors[i] = randomVector(dim);
    }
    try (Directory dir = newDirectory()) {
      indexInSegments(dir, vectors, 4);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        double unadvertisedRecallSum = 0;
        double advertisedRecallSum = 0;
        for (int i = 0; i < numQueries; i++) {
          float[] query = randomVector(dim);
          Set<Integer> truth = exactTopK(reader, query, k);
          unadvertisedRecallSum +=
              recall(searcher.search(new SharedFloorKnnQuery(FIELD, query, k), k), truth, k);

          SharedFloorKnnQuery advertisedQuery = new SharedFloorKnnQuery(FIELD, query, k);
          advertisedQuery
              .manager
              .getGlobalFloor()
              .advertise(exactKthBestScore(reader, query, 4 * k));
          advertisedRecallSum += recall(searcher.search(advertisedQuery, k), truth, k);
        }
        double unadvertisedRecall = unadvertisedRecallSum / numQueries;
        double advertisedRecall = advertisedRecallSum / numQueries;
        assertTrue(
            "an advertised bound with rank headroom cost recall: unadvertised="
                + unadvertisedRecall
                + " advertised="
                + advertisedRecall,
            advertisedRecall >= unadvertisedRecall - 0.05);
      }
    }
  }

  /**
   * At {@code greediness = 0} the non-competitive queue is at least as large as the local queue, so
   * the effective bound can never exceed what the local search would have imposed on itself: the
   * shared floor is fully neutralized. Recall must then match stock search even under the tightest
   * bound that exists, the query's exact final k-th best similarity, advertised before the search
   * starts. This pins down the greediness dial's safe endpoint; the recall cost of tighter settings
   * under tight bounds is a measured trade, not a correctness property.
   */
  public void testZeroGreedinessNeutralizesTightestBound() throws IOException {
    int dim = 16;
    int numDocs = 1200;
    int k = 64;
    int numQueries = 10;
    float[][] vectors = new float[numDocs][];
    for (int i = 0; i < numDocs; i++) {
      vectors[i] = randomVector(dim);
    }
    try (Directory dir = newDirectory()) {
      indexInSegments(dir, vectors, 4);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        double stockRecallSum = 0;
        double advertisedRecallSum = 0;
        for (int i = 0; i < numQueries; i++) {
          float[] query = randomVector(dim);
          Set<Integer> truth = exactTopK(reader, query, k);
          stockRecallSum +=
              recall(searcher.search(new KnnFloatVectorQuery(FIELD, query, k), k), truth, k);

          SharedFloorKnnQuery advertisedQuery = new SharedFloorKnnQuery(FIELD, query, k, 0f);
          advertisedQuery.manager.getGlobalFloor().advertise(exactKthBestScore(reader, query, k));
          advertisedRecallSum += recall(searcher.search(advertisedQuery, k), truth, k);
        }
        double stockRecall = stockRecallSum / numQueries;
        double advertisedRecall = advertisedRecallSum / numQueries;
        assertTrue(
            "greediness 0 must neutralize even the tightest advertised bound: stock="
                + stockRecall
                + " advertised="
                + advertisedRecall,
            advertisedRecall >= stockRecall - 0.05);
      }
    }
  }

  /**
   * Below the activation threshold the manager creates plain collectors, so the search must be
   * stock search to the last bit: identical documents and scores, even when a hostile (invalid)
   * bound has been advertised. This is the policy layer that keeps small-k queries, where a floor
   * has little to save and the most recall to lose, entirely out of the mechanism.
   */
  public void testBelowActivationThresholdSearchIsExactlyStock() throws IOException {
    int dim = 16;
    int numDocs = 1000;
    int k = 10;
    float[][] vectors = new float[numDocs][];
    for (int i = 0; i < numDocs; i++) {
      vectors[i] = randomVector(dim);
    }
    assertTrue(
        "this test requires k below the default activation threshold",
        k < SharedFloorKnnCollectorManager.DEFAULT_FLOOR_ACTIVATION_K);
    try (Directory dir = newDirectory()) {
      indexInSegments(dir, vectors, 4);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        for (int i = 0; i < 5; i++) {
          float[] query = randomVector(dim);
          TopDocs stock = searcher.search(new KnnFloatVectorQuery(FIELD, query, k), k);

          SharedFloorKnnQuery flooredQuery =
              new SharedFloorKnnQuery(
                  FIELD,
                  query,
                  k,
                  FloorAwareKnnCollector.DEFAULT_GREEDINESS,
                  SharedFloorKnnCollectorManager.DEFAULT_FLOOR_ACTIVATION_K);
          // Deliberately invalid: far above any real similarity. Below the activation threshold
          // it must not matter, because no collector ever consults the floor.
          flooredQuery.manager.getGlobalFloor().advertise(Float.MAX_VALUE);
          TopDocs floored = searcher.search(flooredQuery, k);

          assertEquals(stock.scoreDocs.length, floored.scoreDocs.length);
          for (int j = 0; j < stock.scoreDocs.length; j++) {
            assertEquals("doc at rank " + j, stock.scoreDocs[j].doc, floored.scoreDocs[j].doc);
            assertEquals(
                "score at rank " + j, stock.scoreDocs[j].score, floored.scoreDocs[j].score, 0.0f);
          }
        }
      }
    }
  }

  /**
   * A {@link KnnFloatVectorQuery} routed through a {@link SharedFloorKnnCollectorManager}. The
   * manager is created with the query and returned for both collection passes, so both share one
   * floor; consequently a query instance carries single-execution state and must not be reused.
   * Unless a threshold is given, the floor activates at every k, because these tests exist to
   * exercise the floor path.
   */
  private static class SharedFloorKnnQuery extends KnnFloatVectorQuery {
    final SharedFloorKnnCollectorManager manager;

    SharedFloorKnnQuery(String field, float[] target, int k) {
      this(field, target, k, FloorAwareKnnCollector.DEFAULT_GREEDINESS);
    }

    SharedFloorKnnQuery(String field, float[] target, int k, float greediness) {
      this(field, target, k, greediness, 1);
    }

    SharedFloorKnnQuery(
        String field, float[] target, int k, float greediness, int floorActivationK) {
      super(field, target, k);
      this.manager =
          new SharedFloorKnnCollectorManager(
              k, new GlobalKnnFloor(k), greediness, floorActivationK);
    }

    @Override
    protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
      return manager;
    }
  }

  private float[] randomVector(int dim) {
    float[] vector = new float[dim];
    for (int i = 0; i < dim; i++) {
      vector[i] = (float) random().nextGaussian();
    }
    return vector;
  }

  private float[] randomUnitVector(int dim) {
    float[] vector = randomVector(dim);
    double norm = 0;
    for (float component : vector) {
      norm += component * component;
    }
    norm = Math.sqrt(norm);
    for (int i = 0; i < dim; i++) {
      vector[i] /= (float) norm;
    }
    return vector;
  }

  /**
   * Return {@code center + distance * direction + jitter}, where the jitter is a Gaussian
   * perturbation of the given magnitude in each dimension.
   */
  private float[] displaced(float[] center, float[] direction, float distance, float jitter) {
    float[] vector = new float[center.length];
    for (int i = 0; i < center.length; i++) {
      vector[i] = center[i] + distance * direction[i] + jitter * (float) random().nextGaussian();
    }
    return vector;
  }

  private void indexInSegments(Directory dir, float[][] vectors, int segments) throws IOException {
    try (IndexWriter writer =
        new IndexWriter(dir, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
      int docsPerSegment = (vectors.length + segments - 1) / segments;
      int written = 0;
      while (written < vectors.length) {
        List<float[]> segment = new ArrayList<>(docsPerSegment);
        for (int i = 0; i < docsPerSegment && written < vectors.length; i++, written++) {
          segment.add(vectors[written]);
        }
        addSegment(writer, segment);
      }
      writer.commit();
    }
  }

  private void addSegment(IndexWriter writer, List<float[]> vectors) throws IOException {
    for (float[] vector : vectors) {
      Document doc = new Document();
      doc.add(new KnnFloatVectorField(FIELD, vector, SIMILARITY));
      writer.addDocument(doc);
    }
    writer.flush();
  }

  private record DocScore(int doc, float score) {}

  private List<DocScore> exactSearch(IndexReader reader, float[] query) throws IOException {
    List<DocScore> scored = new ArrayList<>();
    for (LeafReaderContext ctx : reader.leaves()) {
      FloatVectorValues values = ctx.reader().getFloatVectorValues(FIELD);
      assertNotNull(values);
      KnnVectorValues.DocIndexIterator iterator = values.iterator();
      for (int doc = iterator.nextDoc();
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = iterator.nextDoc()) {
        float score = SIMILARITY.compare(query, values.vectorValue(iterator.index()));
        scored.add(new DocScore(ctx.docBase + doc, score));
      }
    }
    scored.sort(
        Comparator.comparingDouble(DocScore::score).reversed().thenComparingInt(DocScore::doc));
    return scored;
  }

  private Set<Integer> exactTopK(IndexReader reader, float[] query, int k) throws IOException {
    List<DocScore> scored = exactSearch(reader, query);
    Set<Integer> topK = new HashSet<>();
    for (int i = 0; i < k && i < scored.size(); i++) {
      topK.add(scored.get(i).doc());
    }
    return topK;
  }

  private float exactKthBestScore(IndexReader reader, float[] query, int k) throws IOException {
    List<DocScore> scored = exactSearch(reader, query);
    assertTrue(scored.size() >= k);
    return scored.get(k - 1).score();
  }

  private static double recall(TopDocs topDocs, Set<Integer> truth, int k) {
    int hits = 0;
    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
      if (truth.contains(scoreDoc.doc)) {
        hits++;
      }
    }
    return hits / (double) k;
  }
}
