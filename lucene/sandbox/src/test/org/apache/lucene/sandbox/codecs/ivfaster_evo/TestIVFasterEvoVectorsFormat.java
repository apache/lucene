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

package org.apache.lucene.sandbox.codecs.ivfaster_evo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnByteVectorField;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.VectorUtil;

public class TestIVFasterEvoVectorsFormat extends LuceneTestCase {
  private IndexWriterConfig config(boolean sorted, int probes) {
    IndexWriterConfig config =
        newIndexWriterConfig()
            .setCodec(
                TestUtil.alwaysKnnVectorsFormat(
                    new IVFasterEvoVectorsFormat(
                        8, probes, 2, 1.4, IVFasterEvoVectorsFormat.Tier.FP32)));
    if (sorted) {
      config.setIndexSort(new Sort(new SortField("sort", SortField.Type.LONG)));
    }
    return config;
  }

  public void testRoundTripFiltersDeletesSortAndMerge() throws Exception {
    for (VectorSimilarityFunction similarity : VectorSimilarityFunction.values()) {
      try (Directory dir = newDirectory();
          IndexWriter writer = new IndexWriter(dir, config(true, 8))) {
        for (int i = 0; i < 160; i++) {
          Document doc = new Document();
          doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
          doc.add(new StringField("group", Integer.toString(i % 2), Field.Store.NO));
          doc.add(new NumericDocValuesField("sort", 160 - i));
          float[] vector =
              new float[] {random().nextFloat(), random().nextFloat(), random().nextFloat()};
          VectorUtil.l2normalize(vector);
          if (i % 7 != 0) {
            doc.add(new KnnFloatVectorField("v", vector, similarity));
          }
          if (i % 4 == 0) {
            doc.add(new KnnFloatVectorField("other", vector, similarity));
          }
          writer.addDocument(doc);
          if (i % 40 == 39) {
            writer.commit();
          }
        }
        for (int i = 0; i < 160; i += 13) {
          writer.deleteDocuments(new Term("id", Integer.toString(i)));
        }
        for (int stage = 0; stage < 2; stage++) {
          if (stage == 1) {
            writer.forceMerge(1);
          }
          try (DirectoryReader reader = DirectoryReader.open(writer)) {
            for (var context : reader.leaves()) {
              for (String field : List.of("v", "other")) {
                assertExact(context.reader(), field, similarity);
              }
            }
            IndexSearcher searcher = newSearcher(reader);
            float[] target = new float[] {1, 0, 0};
            var hits =
                searcher.search(
                    new KnnFloatVectorQuery("v", target, 10, new TermQuery(new Term("group", "1"))),
                    10);
            assertEquals(10, hits.scoreDocs.length);
          }
        }
        writer.commit();
        try (DirectoryReader reopened = DirectoryReader.open(dir)) {
          assertExact(reopened.leaves().get(0).reader(), "v", similarity);
        }
        TestUtil.checkIndex(dir);
      }
    }
  }

  static void assertExact(LeafReader reader, String field, VectorSimilarityFunction similarity)
      throws IOException {
    float[] target = new float[] {1, 0, 0};
    for (boolean filtered : List.of(false, true)) {
      FixedBitSet accepted = new FixedBitSet(reader.maxDoc());
      for (int doc = 0; doc < reader.maxDoc(); doc++) {
        if ((reader.getLiveDocs() == null || reader.getLiveDocs().get(doc))
            && (!filtered || doc % 3 == 0)) {
          accepted.set(doc);
        }
      }
      AcceptDocs accept = accepting(accepted);
      List<ScoreDoc> expected = new ArrayList<>();
      var values = reader.getFloatVectorValues(field);
      var quantized =
          values instanceof TieredVectors tiered
                  && tiered.fine.tier == IVFasterEvoVectorsFormat.Tier.U8
              ? tiered
              : null;
      var fineScorer = quantized == null ? null : quantized.fine.scorer(target, similarity);
      byte[] fineCode = quantized == null ? null : new byte[quantized.fine.bytes];
      var iterator = values.iterator();
      for (int doc = iterator.nextDoc();
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = iterator.nextDoc()) {
        if (accepted.get(doc)) {
          float score;
          if (quantized != null) {
            quantized.read(iterator.index(), false, fineCode);
            score = (float) fineScorer.score(fineCode);
          } else score = similarity.compare(target, values.vectorValue(iterator.index()));
          expected.add(new ScoreDoc(doc, score));
        }
      }
      expected.sort(
          Comparator.<ScoreDoc>comparingDouble(hit -> -hit.score).thenComparingInt(hit -> hit.doc));
      TopKnnCollector collector = new TopKnnCollector(10, Integer.MAX_VALUE);
      reader.searchNearestVectors(field, target, collector, accept);
      var actual = collector.topDocs();
      // Every cell is probed, and an AcceptDocs with bits takes the filtered scan, which
      // deduplicates spill copies at admission: visits are exactly the accepted documents.
      assertEquals(expected.size(), actual.totalHits.value());
      assertEquals(Math.min(10, expected.size()), actual.scoreDocs.length);
      for (int i = 0; i < actual.scoreDocs.length; i++) {
        assertEquals(expected.get(i).doc, actual.scoreDocs[i].doc);
        assertEquals(expected.get(i).score, actual.scoreDocs[i].score, 0f);
      }
      TopKnnCollector limited = new TopKnnCollector(10, 1);
      reader.searchNearestVectors(field, target, limited, accepting(accepted));
      assertEquals(Math.min(1, expected.size()), limited.visitedCount());
      if (expected.isEmpty() == false) {
        assertEquals(
            TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, limited.topDocs().totalHits.relation());
      }
      TopKnnCollector empty = new TopKnnCollector(10, Integer.MAX_VALUE);
      reader.searchNearestVectors(
          field, target, empty, accepting(new FixedBitSet(reader.maxDoc())));
      assertEquals(0, empty.visitedCount());
    }
  }

  static AcceptDocs accepting(FixedBitSet accepted) {
    return new AcceptDocs() {
      @Override
      public Bits bits() {
        return accepted;
      }

      @Override
      public int cost() {
        return accepted.cardinality();
      }

      @Override
      public DocIdSetIterator iterator() {
        return new BitSetIterator(accepted, accepted.cardinality());
      }
    };
  }

  public void testPartialProbingAndNullAcceptDocs() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, config(false, 1))) {
      // Cells are routed by an angular sketch, so use well-separated directions: eight noisy
      // clusters around distinct axes.
      float[] first = null;
      for (int i = 0; i < 80; i++) {
        float[] vector = new float[64];
        for (int d = 0; d < vector.length; d++) vector[d] = 0.05f * (random().nextFloat() - 0.5f);
        vector[i % 8] += 1;
        if (i == 0) first = vector;
        Document doc = new Document();
        doc.add(new KnnFloatVectorField("v", vector, VectorSimilarityFunction.EUCLIDEAN));
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        TopKnnCollector collector = new TopKnnCollector(1, Integer.MAX_VALUE);
        reader.leaves().get(0).reader().searchNearestVectors("v", first, collector, null);
        var results = collector.topDocs();
        assertEquals(0, results.scoreDocs[0].doc);
        assertEquals(1f, results.scoreDocs[0].score, 0f);
        assertTrue(results.totalHits.value() < 80);
        // The same segment can probe every cell without rebuilding its persisted default.
        var all =
            new TopKnnCollector(
                1, Integer.MAX_VALUE, new IVFasterEvoVectorsFormat.SearchStrategy(8));
        reader.leaves().get(0).reader().searchNearestVectors("v", first, all, null);
        var evo =
            (IVFasterEvoVectorsReader)
                ((org.apache.lucene.index.CodecReader) reader.leaves().get(0).reader())
                    .getVectorReader()
                    .unwrapReaderForField("v");
        assertEquals(evo.getFloatVectorValues("v").slotCount, all.topDocs().totalHits.value());
      }
    }
  }

  public void testFilterScalesProbesAndBulkScorerQueryMatchesStock() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, config(false, 1))) {
      float[] query = null;
      for (int i = 0; i < 400; i++) {
        float[] vector = new float[32];
        for (int d = 0; d < vector.length; d++) vector[d] = random().nextFloat() - 0.5f;
        if (i == 0) query = vector;
        Document doc = new Document();
        doc.add(new StringField("even", i % 2 == 0 ? "y" : "n", Field.Store.NO));
        doc.add(new StringField("mod4", "" + i % 4, Field.Store.NO));
        doc.add(new StringField("id", "" + i, Field.Store.NO));
        doc.add(new KnnFloatVectorField("v", vector, VectorSimilarityFunction.EUCLIDEAN));
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        var searcher = newSearcher(reader);
        // Two dense clauses whose conjunction accepts every fourth document.
        Query filter =
            new org.apache.lucene.search.BooleanQuery.Builder()
                .add(new TermQuery(new Term("even", "y")), BooleanClause.Occur.FILTER)
                .add(new TermQuery(new Term("mod4", "0")), BooleanClause.Occur.FILTER)
                .build();
        // One requested probe and a selectivity of 1/4 means four of the eight cells.
        var one = new IVFasterEvoVectorsFormat.SearchStrategy(1);
        var leaf = reader.leaves().get(0).reader();
        FixedBitSet accepted = new FixedBitSet(leaf.maxDoc());
        for (int doc = 0; doc < leaf.maxDoc(); doc += 4) accepted.set(doc);
        TopKnnCollector scaled = new TopKnnCollector(10, Integer.MAX_VALUE, one);
        leaf.searchNearestVectors("v", query, scaled, accepting(accepted));
        TopKnnCollector unscaled = new TopKnnCollector(10, Integer.MAX_VALUE, one);
        leaf.searchNearestVectors("v", query, unscaled, null);
        // A quarter of four cells' documents is about one cell's worth: far more than the
        // quarter of a single cell that an unscaled probe count would have admitted.
        assertTrue(scaled.visitedCount() > unscaled.visitedCount() / 2);
        assertTrue(scaled.visitedCount() <= accepted.cardinality());

        // The bulk-scorer query must return exactly what the stock query returns.
        for (int probes : new int[] {1, 2, 8}) {
          var strategy = new IVFasterEvoVectorsFormat.SearchStrategy(probes);
          TopDocs stock =
              searcher.search(new KnnFloatVectorQuery("v", query, 10, filter, strategy), 10);
          TopDocs bulk =
              searcher.search(new IVFasterEvoKnnQuery("v", query, 10, filter, probes), 10);
          assertEquals(stock.scoreDocs.length, bulk.scoreDocs.length);
          for (int i = 0; i < stock.scoreDocs.length; i++) {
            assertEquals(stock.scoreDocs[i].doc, bulk.scoreDocs[i].doc);
            assertEquals(stock.scoreDocs[i].score, bulk.scoreDocs[i].score, 0f);
            assertEquals(0, stock.scoreDocs[i].doc % 4);
          }
        }
        // A one-clause filter is delegated to the stock query rather than re-collected.
        Query single = new TermQuery(new Term("mod4", "0"));
        var two = new IVFasterEvoVectorsFormat.SearchStrategy(2);
        TopDocs viaStock =
            searcher.search(new KnnFloatVectorQuery("v", query, 10, single, two), 10);
        TopDocs viaEvo = searcher.search(new IVFasterEvoKnnQuery("v", query, 10, single, 2), 10);
        assertEquals(viaStock.scoreDocs.length, viaEvo.scoreDocs.length);
        for (int i = 0; i < viaStock.scoreDocs.length; i++) {
          assertEquals(viaStock.scoreDocs[i].doc, viaEvo.scoreDocs[i].doc);
          assertEquals(viaStock.scoreDocs[i].score, viaEvo.scoreDocs[i].score, 0f);
        }
        // Fewer matches than k: both fall back to scoring the matches exactly.
        Query three =
            new org.apache.lucene.search.BooleanQuery.Builder()
                .add(new TermQuery(new Term("id", "8")), BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term("id", "12")), BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term("id", "0")), BooleanClause.Occur.SHOULD)
                .build();
        TopDocs few = searcher.search(new IVFasterEvoKnnQuery("v", query, 10, three, 1), 10);
        assertEquals(3, few.scoreDocs.length);
        assertEquals(0, few.scoreDocs[0].doc);
        assertEquals(
            new IVFasterEvoKnnQuery("v", query, 10, filter, 2),
            new IVFasterEvoKnnQuery("v", query, 10, filter, 2));
        assertNotEquals(
            new IVFasterEvoKnnQuery("v", query, 10, filter, 2),
            new IVFasterEvoKnnQuery("v", query, 10, three, 2));
        assertNull(
            searcher.search(
                            new IVFasterEvoKnnQuery(
                                "v", query, 10, new TermQuery(new Term("id", "none")), 2),
                            10)
                        .scoreDocs
                        .length
                    == 0
                ? null
                : "hits");
      }
    }
  }

  public void testDenseFilterAcrossWindows() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, config(false, 8))) {
      // Documents span several 4096-doc conjunction windows, while the 693 vectors all fit the
      // 700-candidate coarse shortlist, so the fine ranking must be exact.
      for (int i = 0; i < 9000; i++) {
        Document doc = new Document();
        if (i % 13 == 0) {
          doc.add(new KnnFloatVectorField("v", new float[] {i % 64, 1, 0}));
        }
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        assertExact(reader.leaves().get(0).reader(), "v", VectorSimilarityFunction.EUCLIDEAN);
      }
    }
  }

  public void testEmptyFieldAfterMerge() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, config(false, 8))) {
      Document doc = new Document();
      doc.add(new StringField("id", "delete", Field.Store.NO));
      doc.add(new KnnFloatVectorField("v", new float[] {1, 0, 0}));
      writer.addDocument(doc);
      writer.addDocument(new Document());
      writer.commit();
      writer.deleteDocuments(new Term("id", "delete"));
      writer.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        assertExact(reader.leaves().get(0).reader(), "v", VectorSimilarityFunction.EUCLIDEAN);
        assertEquals(0, reader.leaves().get(0).reader().getFloatVectorValues("v").size());
      }
    }
  }

  public void testConfigurationAndUnsupportedEncoding() throws Exception {
    assertTrue(KnnVectorsFormat.forName("IVFasterEvo") instanceof IVFasterEvoVectorsFormat);
    expectThrows(IllegalArgumentException.class, () -> new IVFasterEvoVectorsFormat(0, 1));
    expectThrows(IllegalArgumentException.class, () -> new IVFasterEvoVectorsFormat(8, 0));
    expectThrows(IllegalArgumentException.class, () -> new IVFasterEvoVectorsFormat(8, 9));
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, config(false, 8))) {
      Document doc = new Document();
      doc.add(new KnnByteVectorField("v", new byte[] {1, 2}));
      expectThrows(IllegalArgumentException.class, () -> writer.addDocument(doc));
    }
  }
}
