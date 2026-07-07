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

/**
 * Unit tests of {@link SharedFloorKnnCollectorManager}'s collector-creation policy: which gate its
 * collectors get, which of them may publish into the shared floor, and how a shard's {@code
 * globalShare} shapes both. The end-to-end behavior of the collectors themselves is covered by
 * {@link TestSharedFloorKnnSearch}.
 */
public class TestSharedFloorKnnCollectorManager extends LuceneTestCase {

  private static final String FIELD = "vector";
  private static final VectorSimilarityFunction SIMILARITY = VectorSimilarityFunction.EUCLIDEAN;

  public void testPerShardGateMatchesOptimisticQuota() {
    // The gate must equal the quota the optimistic multi-segment strategy would give a segment of
    // the same proportion (perLeafTopKCalculation in AbstractKnnVectorQuery): k * share plus 16
    // standard deviations of the binomial, truncated. The two spot values below are the gates for
    // a 1-of-16 shard at the two k values of the benchmark this parameter exists for.
    assertEquals(1012, SharedFloorKnnCollectorManager.perShardGate(10000, 1 / 16.0));
    assertEquals(184, SharedFloorKnnCollectorManager.perShardGate(1000, 1 / 16.0));
    // A searcher holding the whole corpus is due all of k, with no padding to add.
    assertEquals(10000, SharedFloorKnnCollectorManager.perShardGate(10000, 1.0));
    // The gate never exceeds k, however much statistical padding the formula would add.
    assertEquals(100, SharedFloorKnnCollectorManager.perShardGate(100, 0.99));
    // The gate is at least 1, however small the share.
    assertEquals(1, SharedFloorKnnCollectorManager.perShardGate(1, 1e-9));

    expectThrows(
        IllegalArgumentException.class, () -> SharedFloorKnnCollectorManager.perShardGate(0, 0.5));
    expectThrows(
        IllegalArgumentException.class,
        () -> SharedFloorKnnCollectorManager.perShardGate(100, 0.0));
    expectThrows(
        IllegalArgumentException.class,
        () -> SharedFloorKnnCollectorManager.perShardGate(100, 1.1));
    expectThrows(
        IllegalArgumentException.class,
        () -> SharedFloorKnnCollectorManager.perShardGate(100, Double.NaN));
  }

  public void testInvalidGlobalShare() {
    int k = 1000;
    GlobalKnnFloor floor = new GlobalKnnFloor(k);
    for (float share : new float[] {0f, -0.5f, 1.1f, Float.NaN}) {
      expectThrows(
          IllegalArgumentException.class,
          () -> new SharedFloorKnnCollectorManager(k, floor, 0.9f, 1, 16, 256, share));
    }
  }

  public void testGlobalShareOpensTheGateAtTheExpectedShare() throws IOException {
    // A single-segment index declared to hold 1/16 of the corpus: collectors must open their
    // ascent gate at the shard's expected contribution to the merged top-k, not at the full local
    // queue — a shard's own index looks like its entire corpus, so without globalShare the gate
    // would sit at k and the floor could never end the fill.
    int k = 1000;
    try (Directory dir = newDirectory()) {
      indexInSegments(dir, randomVectors(20, 8), 1);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReaderContext context = reader.leaves().get(0);

        SharedFloorKnnCollectorManager sharded =
            new SharedFloorKnnCollectorManager(k, new GlobalKnnFloor(k), 0.9f, 1, 16, 256, 1f / 16);
        FloorAwareKnnCollector collector =
            (FloorAwareKnnCollector) sharded.newCollector(Integer.MAX_VALUE, null, context);
        assertEquals(
            "the gate must open at the shard's expected share of the merged top-k",
            SharedFloorKnnCollectorManager.perShardGate(k, 1.0 / 16),
            collector.gateK());

        SharedFloorKnnCollectorManager whole =
            new SharedFloorKnnCollectorManager(k, new GlobalKnnFloor(k), 0.9f, 1, 16, 256, 1f);
        FloorAwareKnnCollector wholeCollector =
            (FloorAwareKnnCollector) whole.newCollector(Integer.MAX_VALUE, null, context);
        assertEquals(
            "an index holding the whole corpus must gate at the full queue",
            k,
            wholeCollector.gateK());
      }
    }
  }

  public void testMultiSegmentGateIsProRataPerLeaf() throws IOException {
    // Full-k collectors over a multi-segment index (the optimistic strategy's second pass) must
    // gate at the leaf's pro-rata share, not at k: the floor established during the first pass is
    // otherwise barred from ending the second pass's re-fill until it has collected all of k.
    int k = 1000;
    int segments = 4;
    try (Directory dir = newDirectory()) {
      indexInSegments(dir, randomVectors(40, 8), segments);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals("test setup: segment count", segments, reader.leaves().size());
        SharedFloorKnnCollectorManager manager =
            new SharedFloorKnnCollectorManager(k, new GlobalKnnFloor(k), 0.9f, 1, 16, 256);
        int totalDocs = reader.maxDoc();
        for (LeafReaderContext context : reader.leaves()) {
          FloorAwareKnnCollector collector =
              (FloorAwareKnnCollector) manager.newCollector(Integer.MAX_VALUE, null, context);
          double leafShare = context.reader().maxDoc() / (double) totalDocs;
          assertEquals(
              "leaf " + context.ord + " must gate at its pro-rata share",
              SharedFloorKnnCollectorManager.perShardGate(k, leafShare),
              collector.gateK());
          assertTrue(collector.gateK() < k);
        }
      }
    }
  }

  public void testEachLeafPublishesIntoTheFloorAtMostOnce() throws IOException {
    // The optimistic strategy searches a competitive leaf twice, re-collecting the same
    // documents. Republishing them would put duplicate scores in the floor's heap, and a k-th
    // best over a multiset with duplicates can exceed the true merged cutoff — so the second
    // collector for the same leaf must read the floor without feeding it.
    int k = 1000;
    try (Directory dir = newDirectory()) {
      indexInSegments(dir, randomVectors(30, 8), 2);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        SharedFloorKnnCollectorManager manager =
            new SharedFloorKnnCollectorManager(k, new GlobalKnnFloor(k), 0.9f, 1, 16, 256);
        LeafReaderContext first = reader.leaves().get(0);
        LeafReaderContext second = reader.leaves().get(1);

        FloorAwareKnnCollector pass1 =
            (FloorAwareKnnCollector)
                manager.newOptimisticCollector(Integer.MAX_VALUE, null, first, 100);
        assertTrue("the first search of a leaf must publish", pass1.publishesToFloor());

        FloorAwareKnnCollector pass2 =
            (FloorAwareKnnCollector) manager.newCollector(Integer.MAX_VALUE, null, first);
        assertFalse("a second search of the same leaf must not publish", pass2.publishesToFloor());

        FloorAwareKnnCollector otherLeaf =
            (FloorAwareKnnCollector) manager.newCollector(Integer.MAX_VALUE, null, second);
        assertTrue(
            "the first search of a different leaf must publish", otherLeaf.publishesToFloor());
      }
    }
  }

  /**
   * The distributed wiring this manager exists for, in miniature: two disjoint indexes standing in
   * for two shards of one corpus, each searched through its own manager with its true {@code
   * globalShare}, both wired to one shared floor. The merged result must not lose recall against
   * stock search of the same two indexes.
   */
  public void testDisjointIndexesSharingOneFloorKeepMergedRecall() throws IOException {
    int dim = 16;
    int docsPerShard = 600;
    int k = 64;
    float[][] shardA = randomVectors(docsPerShard, dim);
    float[][] shardB = randomVectors(docsPerShard, dim);

    try (Directory dirA = newDirectory();
        Directory dirB = newDirectory()) {
      indexInSegments(dirA, shardA, 1);
      indexInSegments(dirB, shardB, 1);
      try (DirectoryReader readerA = DirectoryReader.open(dirA);
          DirectoryReader readerB = DirectoryReader.open(dirB)) {
        IndexSearcher searcherA = new IndexSearcher(readerA);
        IndexSearcher searcherB = new IndexSearcher(readerB);

        double stockRecallSum = 0;
        double flooredRecallSum = 0;
        int queries = 10;
        for (int i = 0; i < queries; i++) {
          float[] query = randomVector(dim);
          Set<Integer> truth = mergedExactTopK(readerA, readerB, query, k);

          TopDocs stockA = searcherA.search(new KnnFloatVectorQuery(FIELD, query, k), k);
          TopDocs stockB = searcherB.search(new KnnFloatVectorQuery(FIELD, query, k), k);
          stockRecallSum += mergedRecall(stockA, stockB, docsPerShard, truth, k);

          // One floor for the query; each shard's manager knows its share of the whole corpus.
          GlobalKnnFloor floor = new GlobalKnnFloor(k);
          TopDocs flooredA =
              searcherA.search(new SharedFloorShardQuery(FIELD, query, k, floor, 0.5f), k);
          TopDocs flooredB =
              searcherB.search(new SharedFloorShardQuery(FIELD, query, k, floor, 0.5f), k);
          flooredRecallSum += mergedRecall(flooredA, flooredB, docsPerShard, truth, k);
        }
        double stockRecall = stockRecallSum / queries;
        double flooredRecall = flooredRecallSum / queries;
        assertTrue(
            "cross-shard floor sharing lost recall: stock="
                + stockRecall
                + " floored="
                + flooredRecall,
            flooredRecall >= stockRecall - 0.05);
      }
    }
  }

  /**
   * A {@link KnnFloatVectorQuery} searching one shard of a two-shard corpus: its manager is wired
   * to the query's shared floor and declares this shard's share of the whole corpus. A query
   * instance carries single-execution state and must not be reused.
   */
  private static class SharedFloorShardQuery extends KnnFloatVectorQuery {
    private final SharedFloorKnnCollectorManager manager;

    SharedFloorShardQuery(String field, float[] target, int k, GlobalKnnFloor floor, float share) {
      super(field, target, k);
      this.manager = new SharedFloorKnnCollectorManager(k, floor, 0.9f, 1, 16, 256, share);
    }

    @Override
    protected KnnCollectorManager getKnnCollectorManager(int k, IndexSearcher searcher) {
      return manager;
    }
  }

  private float[][] randomVectors(int count, int dim) {
    float[][] vectors = new float[count][];
    for (int i = 0; i < count; i++) {
      vectors[i] = randomVector(dim);
    }
    return vectors;
  }

  private float[] randomVector(int dim) {
    float[] vector = new float[dim];
    for (int i = 0; i < dim; i++) {
      vector[i] = (float) random().nextGaussian();
    }
    return vector;
  }

  private void indexInSegments(Directory dir, float[][] vectors, int segments) throws IOException {
    try (IndexWriter writer =
        new IndexWriter(dir, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
      int docsPerSegment = (vectors.length + segments - 1) / segments;
      for (int i = 0; i < vectors.length; i++) {
        Document doc = new Document();
        doc.add(new KnnFloatVectorField(FIELD, vectors[i], SIMILARITY));
        writer.addDocument(doc);
        if ((i + 1) % docsPerSegment == 0) {
          writer.flush();
        }
      }
      writer.commit();
    }
  }

  private record DocScore(int globalDoc, float score) {}

  /**
   * Exact top-k over both shards, in a merged id space where shard A's docs keep their ids and
   * shard B's are offset by A's shard size.
   */
  private Set<Integer> mergedExactTopK(
      IndexReader readerA, IndexReader readerB, float[] query, int k) throws IOException {
    List<DocScore> scored = new ArrayList<>();
    collectExact(readerA, query, 0, scored);
    collectExact(readerB, query, readerA.maxDoc(), scored);
    scored.sort(
        Comparator.comparingDouble(DocScore::score)
            .reversed()
            .thenComparingInt(DocScore::globalDoc));
    Set<Integer> topK = new HashSet<>();
    for (int i = 0; i < k && i < scored.size(); i++) {
      topK.add(scored.get(i).globalDoc());
    }
    return topK;
  }

  private void collectExact(IndexReader reader, float[] query, int offset, List<DocScore> out)
      throws IOException {
    for (LeafReaderContext ctx : reader.leaves()) {
      FloatVectorValues values = ctx.reader().getFloatVectorValues(FIELD);
      assertNotNull(values);
      KnnVectorValues.DocIndexIterator iterator = values.iterator();
      for (int doc = iterator.nextDoc();
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = iterator.nextDoc()) {
        float score = SIMILARITY.compare(query, values.vectorValue(iterator.index()));
        out.add(new DocScore(offset + ctx.docBase + doc, score));
      }
    }
  }

  /** Merge the two shards' results by score, keep the top k, and score recall against truth. */
  private static double mergedRecall(
      TopDocs shardA, TopDocs shardB, int shardBOffset, Set<Integer> truth, int k) {
    List<DocScore> merged = new ArrayList<>();
    for (ScoreDoc scoreDoc : shardA.scoreDocs) {
      merged.add(new DocScore(scoreDoc.doc, scoreDoc.score));
    }
    for (ScoreDoc scoreDoc : shardB.scoreDocs) {
      merged.add(new DocScore(shardBOffset + scoreDoc.doc, scoreDoc.score));
    }
    merged.sort(
        Comparator.comparingDouble(DocScore::score)
            .reversed()
            .thenComparingInt(DocScore::globalDoc));
    int hits = 0;
    for (int i = 0; i < k && i < merged.size(); i++) {
      if (truth.contains(merged.get(i).globalDoc())) {
        hits++;
      }
    }
    return hits / (double) k;
  }
}
