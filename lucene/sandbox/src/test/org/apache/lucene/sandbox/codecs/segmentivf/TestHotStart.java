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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
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
        .setCodec(TestUtil.alwaysKnnVectorsFormat(SegmentIVFTestUtil.format(8, 16, 8)))
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
      for (int d = 0; d < vector.length; d++) {
        vector[d] = base + random().nextFloat();
      }
      vector[i % 16] += 4;
      doc.add(new KnnFloatVectorField(field, vector, VectorSimilarityFunction.DOT_PRODUCT));
      writer.addDocument(doc);
    }
  }

  /** Asserts the last flush's plan message contains every one of {@code parts}. */
  private void assertLast(String... parts) {
    assertLastMessage("flush ", parts);
  }

  /** Asserts the last message starting with {@code prefix} contains every one of {@code parts}. */
  private void assertLastMessage(String prefix, String... parts) {
    String last = null;
    synchronized (messages) {
      for (int i = messages.size() - 1; i >= 0 && last == null; i--) {
        if (messages.get(i).startsWith(prefix)) {
          last = messages.get(i);
        }
      }
    }
    assertNotNull("no message starting with " + prefix + " in " + messages, last);
    for (String part : parts) {
      assertTrue(last, last.contains(part));
    }
  }

  public void testFlushSeedsFromLargestSegmentInFlight() throws Exception {
    Clustering.HotStart.clear();
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, config())) {
      add(writer, "committed", 160, "v", 0);
      writer.commit();
      assertLast("docs=160", "source=cold");
      add(writer, "uncommitted", 320, "v", 0);
      writer.flush();
      assertLast("source=segment=_0", "vectors=160");
      // The largest segment seeds the next flush although no commit has published it.
      add(writer, "next", 140, "v", 0);
      writer.flush();
      assertLast("source=segment=_1", "vectors=320");
      // A flush with fewer than MIN_VECTORS_PER_CELL vectors per cell is one cold cell: no seed.
      add(writer, "tiny", 3, "v", 0);
      writer.flush();
      assertLast("docs=3", "source=cold");
      assertLastMessage("cluster ", "vectors=3", "nlist=1");
      // Another field never borrows these centroids.
      add(writer, "other", 140, "w", 100);
      writer.flush();
      assertLast("field=w", "source=cold");
      // A segment that has left the index stops seeding once its files are gone.
      writer.deleteDocuments(new Term("batch", "uncommitted"));
      writer.commit();
      add(writer, "afterDelete", 140, "v", 0);
      writer.flush();
      assertLast("source=segment=_0", "vectors=160");
      try (DirectoryReader reader = DirectoryReader.open(writer)) {
        float[] query = new float[16];
        query[3] = 1;
        TopDocs hits = newSearcher(reader).search(new KnnFloatVectorQuery("v", query, 5), 5);
        assertEquals(5, hits.scoreDocs.length);
      }
    }
  }

  /** Vectors per flush that config()'s 8 cells cluster rather than write as one cell. */
  private static final int CLUSTERED = SegmentIVFVectorsFormat.MIN_VECTORS_PER_CELL * 8;

  /** A merge forgets its sources' clusterings: no flush may follow to prune them by the files. */
  public void testMergeDropsItsSourcesSeeds() throws Exception {
    Clustering.HotStart.clear();
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, config().setMergePolicy(newLogMergePolicy()))) {
      for (int i = 0; i < 4; i++) {
        add(writer, "batch" + i, CLUSTERED, "v", 0);
        writer.flush();
      }
      assertEquals(Set.of("_0", "_1", "_2", "_3"), Clustering.HotStart.segments(dir));
      writer.forceMerge(1);
      writer.commit();
      Set<String> live = new HashSet<>();
      for (SegmentCommitInfo info : SegmentInfos.readLatestCommit(dir)) {
        live.add(info.info.name);
      }
      assertEquals(1, live.size());
      assertEquals(live, Clustering.HotStart.segments(dir));
    }
  }

  /**
   * Segments of another index merged in by addIndexes have names of their own index, which a
   * segment here may also have. Seeds are keyed by segment id, so the merge neither takes this
   * index's same-named segment for its source nor forgets that segment's clustering.
   */
  public void testAddIndexesKeepsSameNamedLocalSeed() throws Exception {
    Clustering.HotStart.clear();
    try (Directory other = newDirectory();
        Directory dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(other, config())) {
        add(writer, "other", CLUSTERED, "v", 0);
        writer.commit();
      }
      try (IndexWriter writer = new IndexWriter(dir, config());
          DirectoryReader source = DirectoryReader.open(other)) {
        add(writer, "local", CLUSTERED, "v", 0);
        writer.commit();
        assertEquals(Set.of("_0"), Clustering.HotStart.segments(dir));
        CodecReader[] leaves = new CodecReader[source.leaves().size()];
        for (int i = 0; i < leaves.length; i++) {
          leaves[i] = (CodecReader) source.leaves().get(i).reader();
        }
        writer.addIndexes(leaves);
        // This index's _0 is still remembered, beside the merged segment.
        assertTrue(
            Clustering.HotStart.segments(dir).toString(),
            Clustering.HotStart.segments(dir).contains("_0"));
        assertEquals(2, Clustering.HotStart.segments(dir).size());
      }
    }
  }

  /**
   * A single-cell segment is never remembered: a donor must have the configured cell count, so it
   * could not seed anything.
   */
  public void testSingleCellSegmentsAreNotRemembered() throws Exception {
    Clustering.HotStart.clear();
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, config())) {
      add(writer, "small", CLUSTERED - 1, "v", 0);
      writer.flush();
      assertEquals(Set.of(), Clustering.HotStart.segments(dir));
      add(writer, "large", CLUSTERED, "v", 0);
      writer.flush();
      assertEquals(Set.of("_1"), Clustering.HotStart.segments(dir));
    }
  }

  /**
   * A merge whose vectors were all deleted writes no clustering of its own, but still replaces its
   * sources, so it forgets theirs.
   */
  public void testMergeWithoutLiveVectorsForgetsItsSources() throws Exception {
    Clustering.HotStart.clear();
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, config().setMergePolicy(newLogMergePolicy()))) {
      for (int i = 0; i < 2; i++) {
        add(writer, "batch" + i, CLUSTERED, "v", 0);
        // A live document without a vector keeps the segment from being dropped whole.
        Document keep = new Document();
        keep.add(new StringField("batch", "keep", Field.Store.NO));
        writer.addDocument(keep);
        writer.flush();
      }
      assertEquals(Set.of("_0", "_1"), Clustering.HotStart.segments(dir));
      writer.deleteDocuments(new Term("batch", "batch0"), new Term("batch", "batch1"));
      writer.forceMerge(1);
      writer.commit();
      assertEquals(1, SegmentInfos.readLatestCommit(dir).size());
      assertEquals(Set.of(), Clustering.HotStart.segments(dir));
    }
  }

  /** A first flush with fewer vectors than cells must not seed later, larger ones. */
  public void testTinyLineageDoesNotSeed() throws Exception {
    Clustering.HotStart.clear();
    try (Directory dir = newDirectory();
        IndexWriter writer = new IndexWriter(dir, config())) {
      add(writer, "tiny", 3, "v", 0);
      writer.flush();
      assertLast("docs=3", "source=cold");
      add(writer, "bulk", 320, "v", 0);
      writer.flush();
      assertLast("docs=320", "source=cold");
      add(writer, "next", 140, "v", 0);
      writer.flush();
      assertLast("docs=140", "source=segment=_1", "vectors=320");
    }
  }

  public void testSeedsOnlyFromSegmentsThisProcessWrote() throws Exception {
    Clustering.HotStart.clear();
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, config())) {
        add(writer, "committed", 160, "v", 0);
        writer.commit();
        add(writer, "rolledBack", 320, "v", 0);
        writer.flush();
        writer.rollback();
      }
      // The rolled-back segment's name is reused by the next flush, which must not seed itself.
      try (IndexWriter writer = new IndexWriter(dir, config())) {
        add(writer, "afterRollback", 140, "v", 0);
        writer.flush();
        assertLast("source=segment=_0", "vectors=160");
        writer.rollback();
      }
      // A fresh process never seeds from an index it finds on disk, whether it appends to it or
      // replaces it: a CREATE writer keeps the old segments' files until its first commit.
      for (OpenMode mode : new OpenMode[] {OpenMode.APPEND, OpenMode.CREATE}) {
        Clustering.HotStart.clear();
        try (IndexWriter writer = new IndexWriter(dir, config().setOpenMode(mode))) {
          add(writer, "fresh", 140, "v", 0);
          writer.flush();
          assertLast("docs=140", "source=cold");
          writer.rollback();
        }
      }
    }
  }

  /** Only the largest segments are remembered, so the seeds of a long indexing run stay bounded. */
  public void testRemembersAtMostMaxSeedsLargestFirst() throws Exception {
    Clustering.HotStart.clear();
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer = new IndexWriter(dir, config())) {
        add(writer, "one", 1, "v", 0);
        writer.commit();
      }
      FieldInfo info;
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        info = reader.leaves().get(0).reader().getFieldInfos().fieldInfo("v");
      }
      Clustering.HotStart.clear();
      int published = Clustering.HotStart.MAX_SEEDS + 8;
      for (int i = 1; i <= published; i++) {
        float[][] centroids = new float[8][16];
        int[] assignment = new int[i];
        Clustering.Result clustering = new Clustering.Result(centroids, assignment, 1, assignment);
        Clustering.HotStart.publish(
            dir, "_s" + i, "id" + i, "lineage", info, clustering, i, List.of());
      }
      for (int i = 1; i <= published; i++) {
        boolean kept = i > published - Clustering.HotStart.MAX_SEEDS;
        assertEquals(
            "segment of " + i, kept, Clustering.HotStart.snapshot(dir, "id" + i, info) != null);
      }
      // Over the heap budget, the smallest are forgotten until the rest fit.
      Clustering.HotStart.Seed largest = Clustering.HotStart.snapshot(dir, "id" + published, info);
      Clustering.HotStart.evictToBudget(2 * largest.bytes());
      for (int i = 1; i <= published; i++) {
        boolean kept = i >= published - 1;
        assertEquals(
            "segment of " + i, kept, Clustering.HotStart.snapshot(dir, "id" + i, info) != null);
      }
      Clustering.HotStart.clear();
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
    Clustering.HotStart.Source[] sources = new Clustering.HotStart.Source[3];
    for (int i = 0; i < 3; i++) {
      sources[i] = new Clustering.HotStart.Source(centroids[i], members[i], lineages[i], 4);
    }
    float[][] seed = Clustering.HotStart.weightedCentroids(sources, 0);
    float scale = (float) (1.0 / Math.sqrt(10));
    assertArrayEquals(new float[] {3 * scale, scale}, seed[0], 0f);
    assertArrayEquals(new float[] {3 * scale, scale}, seed[1], 0f);
  }

  public void testFlushAndMergePickTheSameDonor() {
    float[][] two = new float[2][2];
    float[][] eight = new float[8][2];
    float[][] twelve = new float[12][2];
    Clustering.HotStart.Source[] sources = {
      new Clustering.HotStart.Source(two, null, "a", 1000), // another configuration's cell count
      new Clustering.HotStart.Source(eight, null, "a", 50),
      new Clustering.HotStart.Source(twelve, null, "a", 5000),
      new Clustering.HotStart.Source(eight, null, "b", 80)
    };
    // Lineage b holds 80 vectors with eight cells, lineage a only 50.
    assertEquals(3, Clustering.HotStart.donor(sources, 8));
    // No segment of that configuration.
    assertEquals(-1, Clustering.HotStart.donor(sources, 4));
    assertTrue(Clustering.HotStart.sameLineage(sources[1], sources[1]));
    assertFalse(Clustering.HotStart.sameLineage(sources[1], sources[3]));
    assertFalse(Clustering.HotStart.sameLineage(sources[0], sources[1])); // cells do not correspond
  }

  /** The lineage holding the most vectors donates, not the largest single segment. */
  public void testDonorIsTheLargestSegmentOfTheLargestLineage() {
    float[][] eight = new float[8][2];
    Clustering.HotStart.Source[] sources = {
      new Clustering.HotStart.Source(eight, null, "a", 60),
      new Clustering.HotStart.Source(eight, null, "b", 100),
      new Clustering.HotStart.Source(eight, null, "a", 70),
      new Clustering.HotStart.Source(eight, null, "a", 50),
      // Segments this process did not write have no lineage and stand alone.
      new Clustering.HotStart.Source(eight, null, null, 150)
    };
    // Lineage a holds 180 vectors, more than b's 100 or the lone 150; its largest segment donates.
    assertEquals(2, Clustering.HotStart.donor(sources, 8));
    sources[4] = new Clustering.HotStart.Source(eight, null, null, 200);
    assertEquals(4, Clustering.HotStart.donor(sources, 8));
    // A lineage with fewer vectors than cells never donates.
    Clustering.HotStart.Source[] small = {
      new Clustering.HotStart.Source(eight, null, "a", 3),
      new Clustering.HotStart.Source(eight, null, "a", 4),
      new Clustering.HotStart.Source(eight, null, null, 7)
    };
    assertEquals(-1, Clustering.HotStart.donor(small, 8));
    small[2] = new Clustering.HotStart.Source(eight, null, "a", 1);
    assertEquals(1, Clustering.HotStart.donor(small, 8));
  }

  public void testBulkScorerQueryMatchesStockQuery() throws Exception {
    try (Directory dir = newDirectory();
        IndexWriter writer =
            new IndexWriter(
                dir,
                newIndexWriterConfig()
                    .setCodec(
                        TestUtil.alwaysKnnVectorsFormat(SegmentIVFTestUtil.format(8, 32, 2))))) {
      float[] query = null;
      for (int i = 0; i < 400; i++) {
        float[] vector = new float[32];
        for (int d = 0; d < vector.length; d++) {
          vector[d] = random().nextFloat() - 0.5f;
        }
        if (i == 0) {
          query = vector;
        }
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
        // A one-clause filter gives the stock query's hits too.
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
