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
package org.apache.lucene.sandbox.codecs.ivfaster;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.IntPredicate;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

/**
 * Filtered search through the real codec: the filter is resolved first and walked doc-at-a-time
 * against the probed cells, and the probe widens until the shortlist is full.
 *
 * <p>Every test scores with the FP32 fine tier, so a missed neighbour is a cell the walk did not
 * reach or a document the intersection dropped, never a quantization artefact. The gate is recall
 * against a brute-force top-k over the SAME accepted set, since a filtered query that returns
 * plausible accepted documents rather than the nearest ones throws nothing.
 */
public class TestIVFasterFiltered extends LuceneTestCase {

  private static final String FIELD = "vector";

  /** Selectivity field: document {@code i} carries the value {@code i}. */
  private static final String SEL = "sel";

  private static Codec codec(int nlist, int nprobe, int spillBits) {
    return TestUtil.alwaysKnnVectorsFormat(
        new IVFasterVectorsFormat(
            nlist,
            nprobe,
            spillBits,
            IVFasterVectorsFormat.DEFAULT_SOAR_LAMBDA,
            IVFasterVectorsFormat.DEFAULT_LLOYD_ITERS,
            IVFasterVectorsFormat.CoarseTier.NITROX2,
            null,
            false));
  }

  /**
   * Recall across the filter regimes, at {@code nprobe = 1} so that every regime except the densest
   * depends on the walk widening the probe on its own.
   *
   * <p>90% and 50% leave the cells leading the intersection; 5% has the filter lead and takes
   * several doubling rounds; 0.5% falls under the exact-filter threshold and reranks every accepted
   * document; 0.05% is at or below {@code k}, where the query itself goes exact. Each must return
   * exactly {@code min(k, accepted)} hits.
   */
  public void testFilteredRecallAcrossSelectivities() throws Exception {
    final int dim = 16;
    final int count = 20_000;
    final int nlist = 128;
    final int k = 10;
    final float[][] vectors = clusteredCorpus(count, 24, dim);
    try (Directory dir = newDirectory()) {
      index(dir, vectors, cfg(codec(nlist, 1, IVFasterVectorsFormat.DEFAULT_SPILL_BITS)));
      try (IndexReader reader = DirectoryReader.open(dir)) {
        final IndexSearcher searcher = new IndexSearcher(reader);
        for (double sel : new double[] {0.9, 0.5, 0.05, 0.005, 0.0005}) {
          final int accepted = (int) Math.ceil(sel * count);
          final Query filter = IntPoint.newRangeQuery(SEL, 0, accepted - 1);
          final double recall =
              recall(searcher, vectors, i -> i < accepted, filter, k, 20, Math.min(k, accepted));
          if (VERBOSE) {
            System.out.println(
                "selectivity " + sel + " accepted " + accepted + " recall " + recall);
          }
          assertTrue(
              "selectivity " + sel + " recall " + recall, recall >= (sel <= 0.005 ? 0.99 : 0.9));
        }
      }
    }
  }

  /**
   * The band where the walk MUST widen: a filter too wide for the exact path, but too narrow for
   * the first round of cells to reach the gather target on its own.
   *
   * <p>WHY THIS BAND HAS ITS OWN TEST. Most selectivities never exercise widening at all. A filter
   * below {@code exactFilterBound} skips cell selection entirely and reranks everything it accepts;
   * a filter dense enough that one round of {@code nprobe} cells already holds the target stops
   * after that round. Only in between does the doubling loop run, which makes it the only band
   * where the probe ceiling — {@code ivfaster.filteredProbeMultiplier} times the query's own {@code
   * nprobe} — can change an answer. A ceiling set too tight shows up here and nowhere else, as
   * recall, with nothing thrown.
   *
   * <p>{@code nprobe = 4} over 128 cells puts the first round far short of the target at 10%
   * selectivity, and 10% of 20K documents is comfortably above the exact-path threshold, so this
   * query widens by construction.
   */
  public void testWideningBandRecall() throws Exception {
    final int dim = 16;
    final int count = 20_000;
    final int nlist = 128;
    final int k = 10;
    final double sel = 0.10;
    final int accepted = (int) Math.ceil(sel * count);
    final float[][] vectors = clusteredCorpus(count, 24, dim);
    try (Directory dir = newDirectory()) {
      index(dir, vectors, cfg(codec(nlist, 4, 1)));
      try (IndexReader reader = DirectoryReader.open(dir)) {
        final IndexSearcher searcher = new IndexSearcher(reader);
        final Query filter = IntPoint.newRangeQuery(SEL, 0, accepted - 1);
        final double recall = recall(searcher, vectors, i -> i < accepted, filter, k, 20, k);
        if (VERBOSE) {
          System.out.println("widening band: accepted " + accepted + " recall " + recall);
        }
        assertTrue("widening-band recall " + recall, recall >= 0.9);
      }
    }
  }

  /**
   * A filter at most {@code bruteN} wide is reranked whole, so the result must be the exact
   * brute-force top-k over the accepted set: nothing was left unprobed.
   */
  public void testExactPathMatchesBruteForce() throws Exception {
    final int dim = 16;
    final int count = 6000;
    final int accepted = 300;
    final float[][] vectors = clusteredCorpus(count, 12, dim);
    try (Directory dir = newDirectory()) {
      index(dir, vectors, cfg(codec(64, 1, 1)));
      try (IndexReader reader = DirectoryReader.open(dir)) {
        final IndexSearcher searcher = new IndexSearcher(reader);
        final Query filter = IntPoint.newRangeQuery(SEL, 0, accepted - 1);
        final double recall = recall(searcher, vectors, i -> i < accepted, filter, 10, 30, 10);
        assertTrue("exact filter path recall " + recall, recall >= 0.98);
      }
    }
  }

  /**
   * An index sort delivers documents to the writer out of doc order, at flush and at merge. The
   * writer must restore doc order: ordinals ascend by doc, each cell is doc-sorted (verified by the
   * reader at open, which would refuse the segment otherwise), and filtered recall holds.
   */
  public void testIndexSortSegment() throws Exception {
    final int dim = 16;
    final int count = 4000;
    final float[][] vectors = clusteredCorpus(count, 10, dim);
    try (Directory dir = newDirectory()) {
      final IndexWriterConfig cfg =
          cfg(codec(32, 2, IVFasterVectorsFormat.DEFAULT_SPILL_BITS))
              .setIndexSort(new Sort(new SortField("sortkey", SortField.Type.INT)))
              .setMaxBufferedDocs(500);
      index(dir, vectors, cfg);
      try (IndexReader reader = DirectoryReader.open(dir)) {
        assertEquals(1, reader.leaves().size());
        final LeafReaderContext ctx = reader.leaves().get(0);
        final FloatVectorValues values = ctx.reader().getFloatVectorValues(FIELD);
        final KnnVectorValues.DocIndexIterator it = values.iterator();
        int prev = -1;
        int seen = 0;
        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
          assertTrue("ordinals must ascend by doc", doc > prev);
          prev = doc;
          final int id = Integer.parseInt(ctx.reader().storedFields().document(doc).get("id"));
          final float[] got = values.vectorValue(it.index());
          assertEquals(
              "vector of doc " + doc + " is id " + id, 1.0, cosine(got, vectors[id]), 1e-4);
          seen++;
        }
        assertEquals(count, seen);
        final IndexSearcher searcher = new IndexSearcher(reader);
        final int accepted = count / 20;
        final Query filter = IntPoint.newRangeQuery(SEL, 0, accepted - 1);
        final double recall = recall(searcher, vectors, i -> i < accepted, filter, 10, 20, 10);
        assertTrue("index-sorted filtered recall " + recall, recall >= 0.9);
      }
    }
  }

  /**
   * Live docs alone are not a filter: an unfiltered query on a segment with deletions keeps the
   * bulk scan and drops deleted documents at the dedup, so it must return only live documents and
   * still find the nearest of them.
   */
  public void testDeletionsWithoutFilter() throws Exception {
    final int dim = 16;
    final int count = 3000;
    final int nlist = 16;
    final float[][] vectors = clusteredCorpus(count, 8, dim);
    final boolean[] deleted = new boolean[count];
    try (Directory dir = newDirectory()) {
      index(dir, vectors, cfg(codec(nlist, nlist, 1)));
      try (IndexWriter w = new IndexWriter(dir, cfg(codec(nlist, nlist, 1)))) {
        for (int i = 0; i < count; i++) {
          if (random().nextInt(10) < 3) {
            deleted[i] = true;
            w.deleteDocuments(new Term("key", Integer.toString(i)));
          }
        }
      }
      try (IndexReader reader = DirectoryReader.open(dir)) {
        final IndexSearcher searcher = new IndexSearcher(reader);
        final double recall = recall(searcher, vectors, i -> deleted[i] == false, null, 10, 20, 10);
        assertTrue("recall over live docs " + recall, recall >= 0.9);
      }
    }
  }

  /** Under spill and a filter, a document reached through several cells is reported once. */
  public void testSpillDedupUnderFilter() throws Exception {
    final int dim = 16;
    final int count = 5000;
    final float[][] vectors = clusteredCorpus(count, 10, dim);
    try (Directory dir = newDirectory()) {
      index(dir, vectors, cfg(codec(32, 2, 3)));
      try (IndexReader reader = DirectoryReader.open(dir)) {
        final IndexSearcher searcher = new IndexSearcher(reader);
        final int accepted = count / 5;
        final Query filter = IntPoint.newRangeQuery(SEL, 0, accepted - 1);
        for (int t = 0; t < 20; t++) {
          final float[] query = vectors[random().nextInt(count)];
          final TopDocs td = searcher.search(new KnnFloatVectorQuery(FIELD, query, 10, filter), 10);
          final Set<Integer> ids = new HashSet<>();
          for (var sd : td.scoreDocs) {
            final int id = Integer.parseInt(searcher.storedFields().document(sd.doc).get("id"));
            assertTrue("duplicate id " + id, ids.add(id));
            assertTrue("id " + id + " is outside the filter", id < accepted);
          }
          assertEquals(10, ids.size());
        }
      }
    }
  }

  /**
   * A filter that accepts everything walks the DAAT path with the cells leading; with every cell
   * probed it must find what the unfiltered bulk scan finds.
   */
  public void testMatchAllFilterEqualsUnfiltered() throws Exception {
    final int dim = 16;
    final int count = 4000;
    final int nlist = 16;
    final float[][] vectors = clusteredCorpus(count, 8, dim);
    try (Directory dir = newDirectory()) {
      index(dir, vectors, cfg(codec(nlist, nlist, 1)));
      try (IndexReader reader = DirectoryReader.open(dir)) {
        final IndexSearcher searcher = new IndexSearcher(reader);
        double agree = 0;
        final int trials = 20;
        for (int t = 0; t < trials; t++) {
          final float[] query = vectors[random().nextInt(count)];
          final Set<Integer> plain = ids(searcher, new KnnFloatVectorQuery(FIELD, query, 10));
          final Set<Integer> filtered =
              ids(searcher, new KnnFloatVectorQuery(FIELD, query, 10, new MatchAllDocsQuery()));
          assertEquals(10, filtered.size());
          final Set<Integer> both = new HashSet<>(plain);
          both.retainAll(filtered);
          agree += both.size() / 10.0;
        }
        assertTrue(
            "match-all filter agrees with unfiltered: " + agree / trials, agree / trials >= 0.9);
      }
    }
  }

  // --------------------------------------------------------------------------

  private static IndexWriterConfig cfg(Codec codec) {
    return new IndexWriterConfig().setCodec(codec).setMaxBufferedDocs(Integer.MAX_VALUE);
  }

  /** Indexes every vector with its id, its selectivity point, a delete key and a sort key. */
  private void index(Directory dir, float[][] vectors, IndexWriterConfig cfg) throws IOException {
    try (IndexWriter w = new IndexWriter(dir, cfg)) {
      for (int i = 0; i < vectors.length; i++) {
        final Document doc = new Document();
        doc.add(new KnnFloatVectorField(FIELD, vectors[i], VectorSimilarityFunction.DOT_PRODUCT));
        doc.add(new StoredField("id", Integer.toString(i)));
        doc.add(new IntPoint(SEL, i));
        doc.add(new StringField("key", Integer.toString(i), Field.Store.NO));
        doc.add(new NumericDocValuesField("sortkey", random().nextInt()));
        w.addDocument(doc);
      }
      w.forceMerge(1);
    }
  }

  private static Set<Integer> ids(IndexSearcher searcher, Query q) throws IOException {
    final TopDocs td = searcher.search(q, 10);
    final Set<Integer> out = new HashSet<>();
    for (var sd : td.scoreDocs) {
      out.add(Integer.parseInt(searcher.storedFields().document(sd.doc).get("id")));
    }
    return out;
  }

  /**
   * Mean recall of {@code trials} random queries against the brute-force top-{@code k} over the
   * accepted documents, asserting each query returns exactly {@code expectHits} results.
   */
  private double recall(
      IndexSearcher searcher,
      float[][] vectors,
      IntPredicate accepted,
      Query filter,
      int k,
      int trials,
      int expectHits)
      throws IOException {
    double total = 0;
    for (int t = 0; t < trials; t++) {
      final float[] query = vectors[random().nextInt(vectors.length)];
      final Set<Integer> truth = bruteForce(vectors, query, accepted, k);
      final Query knn =
          filter == null
              ? new KnnFloatVectorQuery(FIELD, query, k)
              : new KnnFloatVectorQuery(FIELD, query, k, filter);
      final TopDocs td = searcher.search(knn, k);
      assertEquals("hit count", expectHits, td.scoreDocs.length);
      int hit = 0;
      for (var sd : td.scoreDocs) {
        final int id = Integer.parseInt(searcher.storedFields().document(sd.doc).get("id"));
        assertTrue("id " + id + " is not accepted", accepted.test(id));
        if (truth.contains(id)) {
          hit++;
        }
      }
      total += (double) hit / truth.size();
    }
    return total / trials;
  }

  private static Set<Integer> bruteForce(
      float[][] vectors, float[] query, IntPredicate accepted, int k) {
    final int[] best = new int[k];
    final double[] bestDot = new double[k];
    int filled = 0;
    for (int i = 0; i < vectors.length; i++) {
      if (accepted.test(i) == false) {
        continue;
      }
      double dot = 0;
      for (int d = 0; d < query.length; d++) {
        dot += (double) query[d] * vectors[i][d];
      }
      if (filled == k && dot <= bestDot[k - 1]) {
        continue;
      }
      int pos = filled < k ? filled : k - 1;
      while (pos > 0 && bestDot[pos - 1] < dot) {
        bestDot[pos] = bestDot[pos - 1];
        best[pos] = best[pos - 1];
        pos--;
      }
      bestDot[pos] = dot;
      best[pos] = i;
      if (filled < k) {
        filled++;
      }
    }
    final Set<Integer> out = new HashSet<>();
    for (int i = 0; i < filled; i++) {
      out.add(best[i]);
    }
    return out;
  }

  private static double cosine(float[] a, float[] b) {
    double dot = 0;
    double na = 0;
    double nb = 0;
    for (int d = 0; d < a.length; d++) {
      dot += (double) a[d] * b[d];
      na += (double) a[d] * a[d];
      nb += (double) b[d] * b[d];
    }
    return dot / Math.sqrt(na * nb);
  }

  /** A corpus with real cluster structure, unit length. */
  private float[][] clusteredCorpus(int count, int clusters, int dim) {
    final float[][] centres = new float[clusters][];
    for (int c = 0; c < clusters; c++) {
      centres[c] = new float[dim];
      for (int d = 0; d < dim; d++) {
        centres[c][d] = (float) random().nextGaussian();
      }
    }
    final float[][] out = new float[count][];
    for (int i = 0; i < count; i++) {
      final float[] centre = centres[random().nextInt(clusters)];
      out[i] = new float[dim];
      double norm = 0;
      for (int d = 0; d < dim; d++) {
        out[i][d] = (float) (centre[d] + random().nextGaussian() * 0.3);
        norm += (double) out[i][d] * out[i][d];
      }
      norm = Math.sqrt(norm);
      for (int d = 0; d < dim; d++) {
        out[i][d] /= (float) norm;
      }
    }
    return out;
  }
}
