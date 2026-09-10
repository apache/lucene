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
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

/**
 * The kNN clause as a scorer: its documents arrive in doc order, so it conjoins with any other
 * clause, and the fine tier is paid only for documents the whole conjunction agrees on.
 */
public class TestIVFasterKnnQuery extends LuceneTestCase {

  private static final String FIELD = "vector";
  private static final String SEL = "sel";

  private static Codec codec(int nlist, int nprobe) {
    return TestUtil.alwaysKnnVectorsFormat(
        new IVFasterVectorsFormat(
            nlist,
            nprobe,
            IVFasterVectorsFormat.DEFAULT_SPILL_BITS,
            IVFasterVectorsFormat.DEFAULT_SOAR_LAMBDA,
            IVFasterVectorsFormat.DEFAULT_LLOYD_ITERS,
            IVFasterVectorsFormat.CoarseTier.NITROX2,
            null,
            false));
  }

  /** With every cell probed, the scorer's top-k must be the brute-force top-k. */
  public void testUnfilteredRecall() throws Exception {
    final int dim = 16;
    final int count = 8000;
    final int nlist = 32;
    final float[][] vectors = clusteredCorpus(count, 10, dim);
    try (Directory dir = newDirectory()) {
      index(dir, vectors, codec(nlist, nlist));
      try (IndexReader reader = DirectoryReader.open(dir)) {
        final IndexSearcher searcher = newSearcher(reader);
        final double recall =
            recall(
                searcher,
                vectors,
                _ -> true,
                q -> new IVFasterKnnQuery(FIELD, q, nlist, 0, false),
                10,
                20);
        assertTrue("unfiltered recall " + recall, recall >= 0.9);
      }
    }
  }

  /**
   * Conjoined with a filter clause, the query must return only accepted documents and find the
   * nearest of them. The configured probe is tiny, so recall depends on the scorer sizing its probe
   * from the leading clause's cost: a moderate filter widens the walk, and a narrow one switches to
   * reranking every accepted document.
   */
  public void testConjoinedWithFilter() throws Exception {
    final int dim = 16;
    final int count = 8000;
    final int nlist = 32;
    final float[][] vectors = clusteredCorpus(count, 10, dim);
    try (Directory dir = newDirectory()) {
      index(dir, vectors, codec(nlist, 2));
      try (IndexReader reader = DirectoryReader.open(dir)) {
        final IndexSearcher searcher = newSearcher(reader);
        for (double sel : new double[] {0.5, 0.2, 0.1, 0.02}) {
          final int accepted = (int) Math.ceil(sel * count);
          final Query filter = IntPoint.newRangeQuery(SEL, 0, accepted - 1);
          final double recall =
              recall(
                  searcher,
                  vectors,
                  i -> i < accepted,
                  q ->
                      new BooleanQuery.Builder()
                          .add(new IVFasterKnnQuery(FIELD, q, 2, 0, true), BooleanClause.Occur.MUST)
                          .add(filter, BooleanClause.Occur.FILTER)
                          .build(),
                  10,
                  20);
          assertTrue("selectivity " + sel + " recall " + recall, recall >= 0.9);
        }
      }
    }
  }

  /** At the segment's own nprobe, the scorer agrees with the codec's search on the same index. */
  public void testAgreesWithKnnFloatVectorQuery() throws Exception {
    final int dim = 16;
    final int count = 6000;
    final int nlist = 24;
    final float[][] vectors = clusteredCorpus(count, 8, dim);
    try (Directory dir = newDirectory()) {
      index(dir, vectors, codec(nlist, nlist));
      try (IndexReader reader = DirectoryReader.open(dir)) {
        final IndexSearcher searcher = newSearcher(reader);
        double agree = 0;
        final int trials = 20;
        for (int t = 0; t < trials; t++) {
          final float[] q = vectors[random().nextInt(count)];
          final Set<Integer> a = ids(searcher, new KnnFloatVectorQuery(FIELD, q, 10), 10);
          final Set<Integer> b = ids(searcher, new IVFasterKnnQuery(FIELD, q), 10);
          a.retainAll(b);
          agree += a.size() / 10.0;
        }
        assertTrue("agreement " + agree / trials, agree / trials >= 0.85);
      }
    }
  }

  public void testExplainAndContract() throws Exception {
    final int dim = 16;
    final int count = 500;
    final float[][] vectors = clusteredCorpus(count, 4, dim);
    try (Directory dir = newDirectory()) {
      index(dir, vectors, codec(4, 4));
      try (IndexReader reader = DirectoryReader.open(dir)) {
        final IndexSearcher searcher = newSearcher(reader);
        final float[] q = vectors[3];
        // Non-adaptive: the framework's consistency checks vary leadCost between scorers.
        final IVFasterKnnQuery query = new IVFasterKnnQuery(FIELD, q, 4, 0, false);
        QueryUtils.check(random(), query, searcher);
        final TopDocs td = searcher.search(query, 5);
        assertEquals(5, td.scoreDocs.length);
        final Explanation ex = searcher.explain(query, td.scoreDocs[0].doc);
        assertTrue(ex.isMatch());
        assertEquals(td.scoreDocs[0].score, ex.getValue().floatValue(), 1e-5f);
        assertEquals(query, new IVFasterKnnQuery(FIELD, q.clone(), 4, 0, false));
        assertEquals(
            query.hashCode(), new IVFasterKnnQuery(FIELD, q.clone(), 4, 0, false).hashCode());
        assertNotEquals(query, new IVFasterKnnQuery(FIELD, q, 3, 0, false));
        assertNotEquals(query, new IVFasterKnnQuery(FIELD, q, 4, 0, true));
        // A field the segment does not have matches nothing rather than failing.
        assertEquals(0, searcher.search(new IVFasterKnnQuery("nope", q), 5).scoreDocs.length);
      }
    }
  }

  // --------------------------------------------------------------------------

  interface QueryFactory {
    Query of(float[] q);
  }

  private void index(Directory dir, float[][] vectors, Codec codec) throws IOException {
    final IndexWriterConfig cfg =
        new IndexWriterConfig().setCodec(codec).setMaxBufferedDocs(Integer.MAX_VALUE);
    try (IndexWriter w = new IndexWriter(dir, cfg)) {
      for (int i = 0; i < vectors.length; i++) {
        final Document doc = new Document();
        doc.add(new KnnFloatVectorField(FIELD, vectors[i], VectorSimilarityFunction.DOT_PRODUCT));
        doc.add(new StoredField("id", Integer.toString(i)));
        doc.add(new IntPoint(SEL, i));
        w.addDocument(doc);
      }
      w.forceMerge(1);
    }
  }

  private static Set<Integer> ids(IndexSearcher searcher, Query q, int k) throws IOException {
    final TopDocs td = searcher.search(q, k);
    final Set<Integer> out = new HashSet<>();
    for (var sd : td.scoreDocs) {
      out.add(Integer.parseInt(searcher.storedFields().document(sd.doc).get("id")));
    }
    return out;
  }

  private double recall(
      IndexSearcher searcher,
      float[][] vectors,
      IntPredicate accepted,
      QueryFactory factory,
      int k,
      int trials)
      throws IOException {
    double total = 0;
    for (int t = 0; t < trials; t++) {
      final float[] q = vectors[random().nextInt(vectors.length)];
      final Set<Integer> truth = bruteForce(vectors, q, accepted, k);
      final TopDocs td = searcher.search(factory.of(q), k);
      assertEquals(k, td.scoreDocs.length);
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
