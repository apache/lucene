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

package org.apache.lucene.sandbox.codecs.segmentivf;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.sandbox.codecs.segmentivf.Clustering.HotStart;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.InfoStream;

/** Tests SegmentIVF flush hot starts and the bulk-scorer filtered query. */
@ThreadLeakFilters(defaultFilters = true, filters = SegmentIVFBuildThreadsFilter.class)
public class TestHotStart extends LuceneTestCase {
  private final List<String> messages = Collections.synchronizedList(new ArrayList<>());

  private IndexWriterConfig config() {
    return newIndexWriterConfig()
        .setCodec(TestUtil.alwaysKnnVectorsFormat(new SegmentIVFVectorsFormat(8, 8)))
        .setMergePolicy(NoMergePolicy.INSTANCE)
        // Segment shapes are asserted below, so only explicit flushes may flush.
        .setRAMBufferSizeMB(64)
        .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
        .setInfoStream(
            new InfoStream() {
              @Override
              public void message(String component, String message) {
                messages.add(message);
              }

              @Override
              public boolean isEnabled(String component) {
                return component.equals("SIVF");
              }

              @Override
              public void close() {}
            });
  }

  private static void add(IndexWriter writer, String batch, int count, String field, float base)
      throws Exception {
    for (int i = 0; i < count; i++) {
      Document doc = new Document();
      doc.add(new StringField("batch", batch, Field.Store.NO));
      float[] vector = new float[16];
      for (int d = 0; d < vector.length; d++) vector[d] = base + random().nextFloat();
      vector[i % 16] += 4;
      doc.add(new KnnFloatVectorField(field, vector, VectorSimilarityFunction.DOT_PRODUCT));
      writer.addDocument(doc);
    }
  }

  private void assertLast(String... parts) {
    String last = messages.get(messages.size() - 1);
    for (String part : parts) assertTrue(last, last.contains(part));
  }

  public void testFlushSeedsFromLargestSegmentInFlight() throws Exception {
    HotStart.clear();
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, config())) {
      add(writer, "committed", 20, "v", 0);
      writer.commit();
      assertLast("docs=20", "source=cold");
      add(writer, "uncommitted", 40, "v", 0);
      writer.flush();
      assertLast("source=segment=_0", "vectors=20");
      // The largest segment seeds the next flush although no commit has published it.
      add(writer, "next", 10, "v", 0);
      writer.flush();
      assertLast("source=segment=_1", "vectors=40");
      // A flush smaller than the cell count still takes the full seed, leaving cells empty.
      add(writer, "tiny", 3, "v", 0);
      writer.flush();
      assertLast("docs=3", "source=segment=_1", "vectors=40");
      // Another field never borrows these centroids.
      add(writer, "other", 10, "w", 100);
      writer.flush();
      assertLast("field=w", "source=cold");
      // A segment that has left the index stops seeding once its files are gone.
      writer.deleteDocuments(new Term("batch", "uncommitted"));
      writer.commit();
      add(writer, "afterDelete", 10, "v", 0);
      writer.flush();
      assertLast("source=segment=_0", "vectors=20");
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        float[] query = new float[16];
        query[3] = 1;
        TopDocs hits = newSearcher(reader).search(new KnnFloatVectorQuery("v", query, 5), 5);
        assertEquals(5, hits.scoreDocs.length);
      }
    }
  }

  public void testSeedsOnlyFromSegmentsThisProcessWrote() throws Exception {
    HotStart.clear();
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, config())) {
        add(writer, "committed", 20, "v", 0);
        writer.commit();
        add(writer, "rolledBack", 60, "v", 0);
        writer.flush();
        writer.rollback();
      }
      // The rolled-back segment's name is reused by the next flush, which must not seed itself.
      try (IndexWriter writer = new IndexWriter(dir, config())) {
        add(writer, "afterRollback", 10, "v", 0);
        writer.flush();
        assertLast("source=segment=_0", "vectors=20");
        writer.rollback();
      }
      // A fresh process never seeds from an index it finds on disk, whether it appends to it or
      // replaces it: a CREATE writer keeps the old segments' files until its first commit.
      for (OpenMode mode : new OpenMode[] {OpenMode.APPEND, OpenMode.CREATE}) {
        HotStart.clear();
        try (IndexWriter writer = new IndexWriter(dir, config().setOpenMode(mode))) {
          add(writer, "fresh", 10, "v", 0);
          writer.flush();
          assertLast("docs=10", "source=cold");
          writer.rollback();
        }
      }
    }
  }

  public void testWeightedCentroidsUseSameLineageAndPopulation() {
    float[][][] centroids = {
      {{1, 0}, {0, 1}},
      {{0, 1}, {1, 0}},
      {{-1, 0}, {0, -1}}
    };
    int[][] members = {{3, 1}, {1, 3}, {100, 100}};
    String[] lineages = {"a", "a", "b"};
    HotStart.Source[] sources = new HotStart.Source[3];
    for (int i = 0; i < 3; i++) {
      sources[i] = new HotStart.Source(centroids[i], members[i], lineages[i], 4);
    }
    float[][] seed = HotStart.weightedCentroids(sources, 0);
    float scale = (float) (1.0 / Math.sqrt(10));
    assertArrayEquals(new float[] {3 * scale, scale}, seed[0], 0f);
    assertArrayEquals(new float[] {3 * scale, scale}, seed[1], 0f);
  }

  public void testFlushAndMergePickTheSameDonor() {
    float[][] two = new float[2][2], eight = new float[8][2], twelve = new float[12][2];
    HotStart.Source[] sources = {
      new HotStart.Source(two, null, "a", 1000), // another configuration's cell count
      new HotStart.Source(eight, null, "a", 50),
      new HotStart.Source(twelve, null, "a", 5000),
      new HotStart.Source(eight, null, "b", 80)
    };
    assertEquals(3, HotStart.donor(sources, 8)); // the largest segment with eight cells
    assertEquals(-1, HotStart.donor(sources, 4)); // no segment of that configuration
    assertTrue(HotStart.sameLineage(sources[1], sources[1]));
    assertFalse(HotStart.sameLineage(sources[1], sources[3]));
    assertFalse(HotStart.sameLineage(sources[0], sources[1])); // cells do not correspond
  }

  public void testBulkScorerQueryMatchesStockQuery() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter writer =
            new IndexWriter(
                dir,
                newIndexWriterConfig()
                    .setCodec(
                        TestUtil.alwaysKnnVectorsFormat(new SegmentIVFVectorsFormat(8, 2))))) {
      float[] query = null;
      for (int i = 0; i < 400; i++) {
        float[] vector = new float[32];
        for (int d = 0; d < vector.length; d++) vector[d] = random().nextFloat() - 0.5f;
        if (i == 0) query = vector;
        Document doc = new Document();
        doc.add(new StringField("even", i % 2 == 0 ? "y" : "n", Field.Store.NO));
        doc.add(new StringField("mod4", "" + i % 4, Field.Store.NO));
        doc.add(new StringField("id", "" + i, Field.Store.NO));
        doc.add(new KnnFloatVectorField("v", vector, VectorSimilarityFunction.DOT_PRODUCT));
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        var searcher = newSearcher(reader);
        // Two dense clauses whose conjunction accepts every fourth document.
        Query filter =
            new BooleanQuery.Builder()
                .add(new TermQuery(new Term("even", "y")), BooleanClause.Occur.FILTER)
                .add(new TermQuery(new Term("mod4", "0")), BooleanClause.Occur.FILTER)
                .build();
        // Unfiltered, the global rerank matches the per-segment one on a single segment.
        for (Query f : new Query[] {null, filter}) {
          for (int probes : new int[] {1, 2, 8}) {
            var strategy = new SegmentIVFVectorsFormat.SearchStrategy(probes);
            assertSame(
                searcher.search(new KnnFloatVectorQuery("v", query, 10, f, strategy), 10),
                searcher.search(new SegmentIVFKnnQuery("v", query, 10, f, probes), 10));
          }
        }
        // A one-clause filter is delegated to the stock query rather than re-collected.
        Query single = new TermQuery(new Term("mod4", "0"));
        assertSame(
            searcher.search(
                new KnnFloatVectorQuery(
                    "v", query, 10, single, new SegmentIVFVectorsFormat.SearchStrategy(2)),
                10),
            searcher.search(new SegmentIVFKnnQuery("v", query, 10, single, 2), 10));
        // Fewer matches than k: the matches are scored exactly.
        Query three =
            new BooleanQuery.Builder()
                .add(new TermQuery(new Term("id", "8")), BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term("id", "12")), BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term("id", "0")), BooleanClause.Occur.SHOULD)
                .build();
        TopDocs few = searcher.search(new SegmentIVFKnnQuery("v", query, 10, three, 1), 10);
        assertEquals(3, few.scoreDocs.length);
        int queryDoc = searcher.search(new TermQuery(new Term("id", "0")), 1).scoreDocs[0].doc;
        assertEquals(queryDoc, few.scoreDocs[0].doc);
        assertEquals(
            new SegmentIVFKnnQuery("v", query, 10, filter, 2),
            new SegmentIVFKnnQuery("v", query, 10, filter, 2));
      }
    }
  }

  private static void assertSame(TopDocs expected, TopDocs actual) {
    assertEquals(expected.scoreDocs.length, actual.scoreDocs.length);
    assertTrue(expected.scoreDocs.length > 0);
    for (int i = 0; i < expected.scoreDocs.length; i++) {
      assertEquals(expected.scoreDocs[i].doc, actual.scoreDocs[i].doc);
      assertEquals(expected.scoreDocs[i].score, actual.scoreDocs[i].score, 0f);
    }
  }
}
