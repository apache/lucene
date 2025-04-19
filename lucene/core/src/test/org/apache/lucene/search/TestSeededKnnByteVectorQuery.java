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
package org.apache.lucene.search;

import static org.apache.lucene.search.TestKnnByteVectorQuery.floatToBytes;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.knn.KnnCollectorManager;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.TestVectorUtil;

public class TestSeededKnnByteVectorQuery extends BaseKnnVectorQueryTestCase {

  private static final Query MATCH_NONE = new MatchNoDocsQuery();

  @Override
  AbstractKnnVectorQuery getKnnVectorQuery(String field, float[] query, int k, Query queryFilter) {
    KnnByteVectorQuery knnByteVectorQuery =
        new KnnByteVectorQuery(field, floatToBytes(query), k, queryFilter);
    return SeededKnnVectorQuery.fromByteQuery(knnByteVectorQuery, MATCH_NONE);
  }

  @Override
  AbstractKnnVectorQuery getThrowingKnnVectorQuery(String field, float[] vec, int k, Query query) {
    KnnByteVectorQuery knnByteVectorQuery =
        new TestKnnByteVectorQuery.ThrowingKnnVectorQuery(field, floatToBytes(vec), k, query);
    return SeededKnnVectorQuery.fromByteQuery(knnByteVectorQuery, MATCH_NONE);
  }

  @Override
  AbstractKnnVectorQuery getCappedResultsThrowingKnnVectorQuery(
      String field, float[] vec, int k, Query query, int maxResults) {
    KnnByteVectorQuery knnByteVectorQuery =
        new TestKnnByteVectorQuery.CappedResultsThrowingKnnVectorQuery(
            field, floatToBytes(vec), k, query, maxResults);
    return SeededKnnVectorQuery.fromByteQuery(knnByteVectorQuery, MATCH_NONE);
  }

  @Override
  float[] randomVector(int dim) {
    byte[] b = TestVectorUtil.randomVectorBytes(dim);
    float[] v = new float[b.length];
    int vi = 0;
    for (int i = 0; i < v.length; i++) {
      v[vi++] = b[i];
    }
    return v;
  }

  @Override
  Field getKnnVectorField(
      String name, float[] vector, VectorSimilarityFunction similarityFunction) {
    return new KnnByteVectorField(name, floatToBytes(vector), similarityFunction);
  }

  @Override
  Field getKnnVectorField(String name, float[] vector) {
    return new KnnByteVectorField(name, floatToBytes(vector), VectorSimilarityFunction.EUCLIDEAN);
  }

  public void testSeedWithTimeout() throws IOException {
    int numDocs = atLeast(50);
    int dimension = atLeast(5);
    int numIters = atLeast(5);
    try (Directory d = newDirectoryForTest()) {
      IndexWriterConfig iwc = new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec());
      RandomIndexWriter w = new RandomIndexWriter(random(), d, iwc);
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        doc.add(getKnnVectorField("field", randomVector(dimension)));
        doc.add(new NumericDocValuesField("tag", i));
        doc.add(new IntPoint("tag", i));
        w.addDocument(doc);
      }
      w.close();

      try (IndexReader reader = DirectoryReader.open(d)) {
        IndexSearcher searcher = newSearcher(reader);
        searcher.setTimeout(() -> true);
        int k = random().nextInt(80) + 1;
        for (int i = 0; i < numIters; i++) {
          // All documents as seeds
          Query seed =
              random().nextBoolean()
                  ? IntPoint.newRangeQuery("tag", 1, 6)
                  : new MatchAllDocsQuery();
          Query filter = random().nextBoolean() ? null : new MatchAllDocsQuery();
          KnnByteVectorQuery byteVectorQuery =
              new KnnByteVectorQuery("field", floatToBytes(randomVector(dimension)), k, filter);
          Query knnQuery = SeededKnnVectorQuery.fromByteQuery(byteVectorQuery, seed);
          assertEquals(0, searcher.count(knnQuery));
          // No seed documents -- falls back on full approx search
          seed = new MatchNoDocsQuery();
          knnQuery = SeededKnnVectorQuery.fromByteQuery(byteVectorQuery, seed);
          assertEquals(0, searcher.count(knnQuery));
        }
      }
    }
  }

  /** Tests with random vectors and a random seed. Uses RandomIndexWriter. */
  public void testRandomWithSeed() throws IOException {
    int numDocs = 1000;
    int dimension = atLeast(5);
    int numIters = atLeast(10);
    int numDocsWithVector = 0;
    try (Directory d = newDirectoryForTest()) {
      // Always use the default kNN format to have predictable behavior around when it hits
      // visitedLimit. This is fine since the test targets AbstractKnnVectorQuery logic, not the kNN
      // format
      // implementation.
      IndexWriterConfig iwc = new IndexWriterConfig().setCodec(TestUtil.getDefaultCodec());
      RandomIndexWriter w = new RandomIndexWriter(random(), d, iwc);
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        if (random().nextBoolean()) {
          // Randomly skip some vectors to test the mapping from docid to ordinals
          doc.add(getKnnVectorField("field", randomVector(dimension)));
          numDocsWithVector += 1;
        }
        doc.add(new NumericDocValuesField("tag", i));
        doc.add(new IntPoint("tag", i));
        w.addDocument(doc);
      }
      w.forceMerge(1);
      w.close();

      try (IndexReader reader = DirectoryReader.open(d)) {
        IndexSearcher searcher = newSearcher(reader);
        for (int i = 0; i < numIters; i++) {
          // verify timeout collector wrapping is used
          if (random().nextBoolean()) {
            searcher.setTimeout(() -> false);
          } else {
            searcher.setTimeout(null);
          }
          int k = random().nextInt(10) + 1;
          int n = random().nextInt(100) + 1;
          // we may get fewer results than requested if there are deletions, but this test doesn't
          // check that
          assert reader.hasDeletions() == false;

          // All documents as seeds
          AtomicInteger seedCalls = new AtomicInteger();
          Query seed1 = new MatchAllDocsQuery();
          Query filter = random().nextBoolean() ? null : new MatchAllDocsQuery();
          KnnByteVectorQuery byteVectorQuery =
              new KnnByteVectorQuery("field", floatToBytes(randomVector(dimension)), k, filter);
          AssertingSeededKnnVectorQuery query =
              new AssertingSeededKnnVectorQuery(byteVectorQuery, seed1, null, seedCalls);
          TopDocs results = searcher.search(query, n);
          assertEquals(seedCalls.get(), 1);
          int expected = Math.min(Math.min(n, k), numDocsWithVector);

          assertEquals(expected, results.scoreDocs.length);
          assertTrue(results.totalHits.value() >= results.scoreDocs.length);
          // verify the results are in descending score order
          float last = Float.MAX_VALUE;
          for (ScoreDoc scoreDoc : results.scoreDocs) {
            assertTrue(scoreDoc.score <= last);
            last = scoreDoc.score;
          }

          // Restrictive seed query -- 6 documents
          Query seed2 = IntPoint.newRangeQuery("tag", 1, 6);
          int seedCount =
              searcher.count(
                  new BooleanQuery.Builder()
                      .add(seed2, BooleanClause.Occur.MUST)
                      .add(new FieldExistsQuery("field"), BooleanClause.Occur.MUST)
                      .build());
          byteVectorQuery =
              new KnnByteVectorQuery("field", floatToBytes(randomVector(dimension)), k, null);
          query =
              new AssertingSeededKnnVectorQuery(
                  byteVectorQuery, seed2, null, seedCount > 0 ? seedCalls : null);
          results = searcher.search(query, n);
          assertEquals(seedCalls.get(), seedCount > 0 ? 2 : 1);
          expected = Math.min(Math.min(n, k), reader.numDocs());
          assertEquals(expected, results.scoreDocs.length);
          assertTrue(results.totalHits.value() >= results.scoreDocs.length);
          // verify the results are in descending score order
          last = Float.MAX_VALUE;
          for (ScoreDoc scoreDoc : results.scoreDocs) {
            assertTrue(scoreDoc.score <= last);
            last = scoreDoc.score;
          }

          // No seed documents -- falls back on full approx search
          Query seed3 = new MatchNoDocsQuery();
          query = new AssertingSeededKnnVectorQuery(byteVectorQuery, seed3, null, null);
          results = searcher.search(query, n);
          expected = Math.min(Math.min(n, k), reader.numDocs());
          assertEquals(expected, results.scoreDocs.length);
          assertTrue(results.totalHits.value() >= results.scoreDocs.length);
          // verify the results are in descending score order
          last = Float.MAX_VALUE;
          for (ScoreDoc scoreDoc : results.scoreDocs) {
            assertTrue(scoreDoc.score <= last);
            last = scoreDoc.score;
          }
        }
      }
    }
  }

  static class AssertingSeededKnnVectorQuery extends SeededKnnVectorQuery {
    private final AtomicInteger seedCalls;

    public AssertingSeededKnnVectorQuery(
        AbstractKnnVectorQuery query, Query seed, Weight seedWeight, AtomicInteger seedCalls) {
      super(query, seed, seedWeight);
      this.seedCalls = seedCalls;
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
      if (seedWeight != null) {
        return super.rewrite(indexSearcher);
      }
      AssertingSeededKnnVectorQuery rewritten =
          new AssertingSeededKnnVectorQuery(
              delegate, seed, createSeedWeight(indexSearcher), seedCalls);
      return rewritten.rewrite(indexSearcher);
    }

    @Override
    protected TopDocs approximateSearch(
        LeafReaderContext context,
        Bits acceptDocs,
        int visitedLimit,
        KnnCollectorManager knnCollectorManager)
        throws IOException {
      return delegate.approximateSearch(
          context,
          acceptDocs,
          visitedLimit,
          new AssertingSeededCollectorManager(new SeededCollectorManager(knnCollectorManager)));
    }

    class AssertingSeededCollectorManager extends SeededCollectorManager {

      public AssertingSeededCollectorManager(SeededCollectorManager delegate) {
        super(delegate);
      }

      @Override
      public KnnCollector newCollector(
          int numVisited, KnnSearchStrategy searchStrategy, LeafReaderContext context)
          throws IOException {
        KnnCollector knnCollector =
            knnCollectorManager.newCollector(numVisited, searchStrategy, context);
        if (knnCollector.getSearchStrategy() instanceof KnnSearchStrategy.Seeded seeded) {
          if (seedCalls == null && seeded.numberOfEntryPoints() > 0) {
            fail("Expected non-seeded collector but received: " + knnCollector);
          }
          return new AssertingKnnCollector(knnCollector);
        }
        if (seedCalls != null) {
          fail("Expected seeded collector but received: " + knnCollector);
        }
        return knnCollector;
      }
    }

    class AssertingKnnCollector extends KnnCollector.Decorator {
      public AssertingKnnCollector(KnnCollector collector) {
        super(collector);
      }

      @Override
      public KnnSearchStrategy getSearchStrategy() {
        KnnSearchStrategy searchStrategy = collector.getSearchStrategy();
        if (searchStrategy instanceof KnnSearchStrategy.Seeded seeded) {
          return new AssertingSeededStrategy(seeded);
        }
        return searchStrategy;
      }

      class AssertingSeededStrategy extends KnnSearchStrategy.Seeded {
        private final KnnSearchStrategy.Seeded seeded;

        public AssertingSeededStrategy(KnnSearchStrategy.Seeded seeded) {
          super(seeded.entryPoints(), seeded.numberOfEntryPoints(), seeded.originalStrategy());
          this.seeded = seeded;
        }

        @Override
        public int numberOfEntryPoints() {
          return seeded.numberOfEntryPoints();
        }

        @Override
        public DocIdSetIterator entryPoints() {
          DocIdSetIterator iterator = seeded.entryPoints();
          assert iterator.cost() > 0;
          seedCalls.incrementAndGet();
          return iterator;
        }
      }
    }
  }
}
