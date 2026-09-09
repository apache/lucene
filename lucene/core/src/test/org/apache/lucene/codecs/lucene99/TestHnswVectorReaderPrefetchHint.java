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
package org.apache.lucene.codecs.lucene99;

import java.io.IOException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene104.Lucene104HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.FloatVectorSimilarityQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.PatienceKnnVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryAccessHint;
import org.apache.lucene.search.QueryReadHint;
import org.apache.lucene.search.SeededKnnVectorQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Verifies that {@link Lucene99HnswVectorsReader} consumes {@link QueryAccessHint#POINT} by
 * wrapping the vector scorer so prefetch is called per neighbour batch before bulkScore.
 *
 * <p>The signal is observed via a counting {@link IndexInput} wrapper on the directory: with the
 * hint absent, the reader makes zero {@code prefetch} calls; with the hint present, it makes at
 * least one. Results are also compared to confirm correctness is unaffected.
 */
public class TestHnswVectorReaderPrefetchHint extends LuceneTestCase {

  private static final int DIM = 16;
  private static final int NUM_DOCS = 256;

  public void testPrefetchHintFloat32() throws Exception {
    // float32 HNSW: the graph scorer reads full-precision vectors from the .vec slice.
    assertPrefetchHintRespected(new Lucene99HnswVectorsFormat(), ".vec");
  }

  public void testPrefetchHintScalarQuantized() throws Exception {
    // int7 scalar-quantised HNSW: the graph scorer reads quantised vectors from the .veq slice.
    // Exercises OffHeapScalarQuantizedVectorValues#prefetch.
    assertPrefetchHintRespected(new Lucene104HnswScalarQuantizedVectorsFormat(), ".veq");
  }

  private void assertPrefetchHintRespected(KnnVectorsFormat format, String countedExt)
      throws Exception {
    final AtomicLong prefetchCount = new AtomicLong();

    try (Directory raw = newMMapDirectory();
        Directory dir = new PrefetchCountingDirectory(raw, prefetchCount, countedExt)) {
      buildIndex(dir, format, 42);
      try (IndexReader reader = DirectoryReader.open(dir)) {
        LeafReaderContext ctx = reader.leaves().get(0);
        Random rng = new Random(0xC0FFEE);
        float[] q = unit(DIM, rng);

        prefetchCount.set(0);
        TopDocs nohint = runSearch(ctx, q, /* hints= */ Set.of());
        long countNoHint = prefetchCount.get();

        prefetchCount.set(0);
        TopDocs hinted = runSearch(ctx, q, Set.of(QueryAccessHint.POINT));
        long countWithHint = prefetchCount.get();

        // Correctness: identical results regardless of hint (prefetch is read-only).
        assertEquals(nohint.scoreDocs.length, hinted.scoreDocs.length);
        for (int i = 0; i < nohint.scoreDocs.length; i++) {
          assertEquals(nohint.scoreDocs[i].doc, hinted.scoreDocs[i].doc);
          assertEquals(nohint.scoreDocs[i].score, hinted.scoreDocs[i].score, 0f);
        }
        // Consumer behaviour: no hint => no prefetch; POINT => at least one prefetch.
        assertEquals("no-hint baseline must not invoke prefetch", 0L, countNoHint);
        assertTrue(
            "POINT hint must invoke prefetch on the " + countedExt + " file; got=" + countWithHint,
            countWithHint > 0);
      }
    }
  }

  /**
   * End-to-end through the public API: {@link IndexSearcher#setReadHints} must reach the reader via
   * the production {@link org.apache.lucene.search.knn.TopKnnCollectorManager} (no hand-rolled
   * collector). With the hint set, a {@link KnnFloatVectorQuery} prefetches; without it, it does
   * not.
   */
  public void testReadHintReachesReaderThroughIndexSearcher() throws Exception {
    assertReadHintReachesReaderThroughIndexSearcher(new Lucene99HnswVectorsFormat(), ".vec");
  }

  /** Same end-to-end production path as above, but for the scalar-quantised format (.veq). */
  public void testReadHintReachesReaderThroughIndexSearcherScalarQuantized() throws Exception {
    assertReadHintReachesReaderThroughIndexSearcher(
        new Lucene104HnswScalarQuantizedVectorsFormat(), ".veq");
  }

  private void assertReadHintReachesReaderThroughIndexSearcher(
      KnnVectorsFormat format, String countedExt) throws Exception {
    final AtomicLong prefetchCount = new AtomicLong();

    try (Directory raw = newMMapDirectory();
        Directory dir = new PrefetchCountingDirectory(raw, prefetchCount, countedExt)) {
      buildIndex(dir, format, 7);
      try (IndexReader reader = DirectoryReader.open(dir)) {
        Random rng = new Random(0xBEEF);
        float[] q = unit(DIM, rng);
        KnnFloatVectorQuery query = new KnnFloatVectorQuery("vec", q, 10);

        // No hint on the searcher: production path must not prefetch.
        IndexSearcher s1 = new IndexSearcher(reader);
        s1.setQueryCache(null);
        prefetchCount.set(0);
        s1.search(query, 10);
        assertEquals("no read hint on the searcher must not prefetch", 0L, prefetchCount.get());

        // POINT hint on the searcher: TopKnnCollectorManager must carry it to the collector,
        // and Lucene99HnswVectorsReader must prefetch.
        IndexSearcher s2 = new IndexSearcher(reader);
        s2.setQueryCache(null);
        s2.setReadHints(Set.of(QueryAccessHint.POINT));
        prefetchCount.set(0);
        s2.search(query, 10);
        assertTrue(
            "IndexSearcher.setReadHints(POINT) must reach the reader and prefetch; got="
                + prefetchCount.get(),
            prefetchCount.get() > 0);
      }
    }
  }

  /**
   * Genuine per-query control: with a single shared {@link IndexSearcher} that has NO
   * searcher-level hint, a query built with {@link KnnFloatVectorQuery#withReadHints} prefetches
   * while a plain query on the same searcher does not. This proves the hint rides on the query, not
   * the searcher.
   */
  public void testReadHintIsPerQueryOnSharedSearcher() throws Exception {
    final AtomicLong prefetchCount = new AtomicLong();

    try (Directory raw = newMMapDirectory();
        Directory dir = new PrefetchCountingDirectory(raw, prefetchCount, ".vec")) {
      buildIndex(dir, new Lucene99HnswVectorsFormat(), 13);
      try (IndexReader reader = DirectoryReader.open(dir)) {
        float[] q = unit(DIM, new Random(0xCAFE));

        // ONE shared searcher, NO searcher-level hint.
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);
        assertEquals(Set.of(), searcher.getReadHints());

        KnnFloatVectorQuery plain = new KnnFloatVectorQuery("vec", q, 10);
        KnnFloatVectorQuery hinted = plain.withReadHints(Set.of(QueryAccessHint.POINT));

        // Plain query on the shared searcher: no prefetch.
        prefetchCount.set(0);
        searcher.search(plain, 10);
        assertEquals(
            "plain query on a searcher with no hint must not prefetch", 0L, prefetchCount.get());

        // Per-query hinted query on the SAME searcher: prefetch happens.
        prefetchCount.set(0);
        searcher.search(hinted, 10);
        assertTrue(
            "withReadHints(POINT) must prefetch even though the searcher has no hint; got="
                + prefetchCount.get(),
            prefetchCount.get() > 0);

        // The two queries are distinct (different hints => not equal, distinct cache keys).
        assertNotEquals(plain, hinted);

        // Same proof for the similarity-query family (different base/wiring): decay < 1 and no
        // filter select the approximate path. withReadHints on the SAME un-hinted searcher must
        // prefetch while the plain similarity query must not.
        FloatVectorSimilarityQuery simPlain =
            new FloatVectorSimilarityQuery(
                "vec", q, /* resultSimilarity= */ 0f, /* decay= */ 0.5f, /* filter= */ null);
        FloatVectorSimilarityQuery simHinted =
            simPlain.withReadHints(Set.of(QueryAccessHint.POINT));

        prefetchCount.set(0);
        searcher.search(simPlain, 50);
        assertEquals(
            "plain similarity query on a searcher with no hint must not prefetch",
            0L,
            prefetchCount.get());

        prefetchCount.set(0);
        searcher.search(simHinted, 50);
        assertTrue(
            "FloatVectorSimilarityQuery.withReadHints(POINT) must prefetch on an un-hinted searcher;"
                + " got="
                + prefetchCount.get(),
            prefetchCount.get() > 0);
        assertNotEquals(simPlain, simHinted);
      }
    }
  }

  /**
   * End-to-end through the public API for the similarity-threshold query family: {@link
   * FloatVectorSimilarityQuery}'s approximate (graph) search path runs through the reader's {@code
   * searchNearestVectors}, so {@link IndexSearcher#setReadHints} must reach the reader via {@code
   * AbstractVectorSimilarityQuery}'s collector wiring. (decay &lt; 1 and no filter select the
   * approximate path, not the exact-scan fallback.)
   */
  public void testReadHintReachesReaderThroughVectorSimilarityQuery() throws Exception {
    final AtomicLong prefetchCount = new AtomicLong();

    try (Directory raw = newMMapDirectory();
        Directory dir = new PrefetchCountingDirectory(raw, prefetchCount, ".vec")) {
      buildIndex(dir, new Lucene99HnswVectorsFormat(), 11);
      try (IndexReader reader = DirectoryReader.open(dir)) {
        float[] q = unit(DIM, new Random(0xABCD));
        // decay = 0.5 (< 1) and no filter => approximate graph search; low resultSimilarity so the
        // search actually traverses and reads vectors.
        Query query =
            new FloatVectorSimilarityQuery(
                "vec", q, /* resultSimilarity= */ 0f, /* decay= */ 0.5f, /* filter= */ null);

        IndexSearcher s1 = new IndexSearcher(reader);
        s1.setQueryCache(null);
        prefetchCount.set(0);
        s1.search(query, 50);
        assertEquals("no read hint on the searcher must not prefetch", 0L, prefetchCount.get());

        IndexSearcher s2 = new IndexSearcher(reader);
        s2.setQueryCache(null);
        s2.setReadHints(Set.of(QueryAccessHint.POINT));
        prefetchCount.set(0);
        s2.search(query, 50);
        assertTrue(
            "FloatVectorSimilarityQuery + setReadHints(POINT) must reach the reader and prefetch;"
                + " got="
                + prefetchCount.get(),
            prefetchCount.get() > 0);
      }
    }
  }

  /**
   * The hint must survive query wrapping. {@link SeededKnnVectorQuery} and {@link
   * PatienceKnnVectorQuery} both delegate collector creation to the inner query, so a per-query
   * hint attached to the inner {@link KnnFloatVectorQuery} via {@link
   * KnnFloatVectorQuery#withReadHints} must still reach the reader through the wrappers — including
   * when {@link PatienceKnnVectorQuery#rewrite} reconstructs its {@code Seeded} delegate. A plain
   * (un-hinted) inner query through the same wrappers must not prefetch. The searcher itself
   * carries no hint, so any prefetch observed here came through the wrapped query.
   */
  public void testReadHintPropagatesThroughSeededAndPatience() throws Exception {
    final AtomicLong prefetchCount = new AtomicLong();

    try (Directory raw = newMMapDirectory();
        Directory dir = new PrefetchCountingDirectory(raw, prefetchCount, ".vec")) {
      buildIndex(dir, new Lucene99HnswVectorsFormat(), 23);
      try (IndexReader reader = DirectoryReader.open(dir)) {
        float[] q = unit(DIM, new Random(0xD00D));
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);
        assertEquals(Set.of(), searcher.getReadHints());

        KnnFloatVectorQuery plain = new KnnFloatVectorQuery("vec", q, 10);
        KnnFloatVectorQuery hinted = plain.withReadHints(Set.of(QueryAccessHint.POINT));
        Query seed = new MatchAllDocsQuery();

        // Seeded wrapper: hint rides on the inner query.
        assertPrefetch(
            searcher, SeededKnnVectorQuery.fromFloatQuery(plain, seed), prefetchCount, false);
        assertPrefetch(
            searcher, SeededKnnVectorQuery.fromFloatQuery(hinted, seed), prefetchCount, true);

        // Patience over a float query.
        assertPrefetch(
            searcher, PatienceKnnVectorQuery.fromFloatQuery(plain), prefetchCount, false);
        assertPrefetch(
            searcher, PatienceKnnVectorQuery.fromFloatQuery(hinted), prefetchCount, true);

        // Patience over a Seeded query (exercises Patience.rewrite rebuilding the Seeded delegate).
        assertPrefetch(
            searcher,
            PatienceKnnVectorQuery.fromSeededQuery(
                SeededKnnVectorQuery.fromFloatQuery(plain, seed)),
            prefetchCount,
            false);
        assertPrefetch(
            searcher,
            PatienceKnnVectorQuery.fromSeededQuery(
                SeededKnnVectorQuery.fromFloatQuery(hinted, seed)),
            prefetchCount,
            true);
      }
    }
  }

  private static void assertPrefetch(
      IndexSearcher searcher, Query query, AtomicLong counter, boolean expectPrefetch)
      throws IOException {
    counter.set(0);
    searcher.search(query, 10);
    if (expectPrefetch) {
      assertTrue(
          "expected POINT prefetch for " + query + "; got=" + counter.get(), counter.get() > 0);
    } else {
      assertEquals("expected no prefetch for un-hinted " + query, 0L, counter.get());
    }
  }

  private static void buildIndex(Directory dir, KnnVectorsFormat format, long seed)
      throws IOException {
    try (IndexWriter w =
        new IndexWriter(
            dir,
            new IndexWriterConfig()
                .setUseCompoundFile(false)
                .setCodec(
                    new Lucene104Codec() {
                      @Override
                      public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                        return format;
                      }
                    }))) {
      Random rng = new Random(seed);
      for (int i = 0; i < NUM_DOCS; i++) {
        Document d = new Document();
        d.add(new KnnFloatVectorField("vec", unit(DIM, rng), VectorSimilarityFunction.DOT_PRODUCT));
        w.addDocument(d);
      }
      w.forceMerge(1);
    }
  }

  private static Directory newMMapDirectory() throws IOException {
    // Wrap in MockDirectoryWrapper (as TestKnnByteVectorQueryMMap does) so close-time leak
    // assertions run over the prefetch path's clone()/slice() wrappers. MockIndexInputWrapper
    // forwards prefetch(), so the CountingPrefetchInput layered on top still observes the calls.
    return new MockDirectoryWrapper(
        random(), new MMapDirectory(createTempDir("hnsw-prefetch-hint")));
  }

  private static TopDocs runSearch(LeafReaderContext ctx, float[] q, Set<QueryReadHint> hints)
      throws IOException {
    HintedTopKnnCollector collector = new HintedTopKnnCollector(10, hints);
    ctx.reader()
        .searchNearestVectors(
            "vec",
            q,
            collector,
            AcceptDocs.fromLiveDocs(ctx.reader().getLiveDocs(), ctx.reader().maxDoc()));
    return collector.topDocs();
  }

  /** TopKnnCollector that advertises a configurable hint set. */
  private static final class HintedTopKnnCollector extends TopKnnCollector {
    private final Set<QueryReadHint> hints;

    HintedTopKnnCollector(int k, Set<QueryReadHint> hints) {
      super(k, Integer.MAX_VALUE, null);
      this.hints = hints;
    }

    @Override
    public Set<QueryReadHint> readHints() {
      return hints;
    }
  }

  /** Directory wrapper that wraps each opened input with a prefetch-counting filter. */
  private static final class PrefetchCountingDirectory extends FilterDirectory {
    private final AtomicLong counter;
    private final String countedExt;

    PrefetchCountingDirectory(Directory in, AtomicLong counter, String countedExt) {
      super(in);
      this.counter = counter;
      this.countedExt = countedExt;
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      IndexInput delegate = in.openInput(name, context);
      // Only count prefetch on the file the consumer targets (the per-neighbour vector data:
      // .vec for float32, .veq for scalar-quantised). The HNSW graph file (.vex) is not what
      // this hint targets, and the meta files (.vem*) are small and only read at open time.
      if (name.endsWith(countedExt)) {
        return new CountingPrefetchInput(delegate, counter);
      }
      return delegate;
    }
  }

  private static final class CountingPrefetchInput extends FilterIndexInput {
    private final AtomicLong counter;

    CountingPrefetchInput(IndexInput in, AtomicLong counter) {
      super("CountingPrefetchInput(" + in + ")", in);
      this.counter = counter;
    }

    @Override
    public boolean prefetch(long offset, long length) throws IOException {
      counter.incrementAndGet();
      return in.prefetch(offset, length);
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      return new CountingPrefetchInput(in.slice(sliceDescription, offset, length), counter);
    }

    @Override
    public IndexInput clone() {
      return new CountingPrefetchInput(in.clone(), counter);
    }
  }

  private static float[] unit(int dim, Random rng) {
    float[] v = new float[dim];
    double norm = 0;
    for (int i = 0; i < dim; i++) {
      v[i] = rng.nextFloat() * 2f - 1f;
      norm += v[i] * v[i];
    }
    norm = Math.sqrt(norm);
    for (int i = 0; i < dim; i++) v[i] /= (float) norm;
    return v;
  }
}
