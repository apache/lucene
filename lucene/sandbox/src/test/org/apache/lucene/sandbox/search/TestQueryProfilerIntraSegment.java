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

package org.apache.lucene.sandbox.search;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NamedThreadFactory;
import org.hamcrest.MatcherAssert;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * Characterization test for the query profiler under <b>intra-segment</b> (partitioned) search.
 *
 * <p>Intra-segment search splits a single Lucene segment into multiple {@code
 * LeafReaderContextPartition}s (doc-id ranges), each searched independently via {@code searchLeaf}.
 * This is the seam OpenSearch and (upcoming) Lucene consumers use to parallelize work <i>within</i>
 * one large segment.
 *
 * <p><b>What this test guards (LUCENE concurrent-search profiling, cf. #14375 / #14413):</b> leaf
 * breakdowns are attributed per {@code LeafReaderContextPartition} — keyed by segment ordinal plus
 * doc-id range — rather than only by executing {@code Thread}. The partition identity is set at the
 * {@code searchLeaf} seam, the one place its {@code minDocId}/{@code maxDocId} bounds are in scope.
 * Consequently, when a single segment is searched as N partitions, the profiler emits N distinct
 * {@link AggregatedQueryLeafProfilerResult}s whose timings and counts are attributable to each
 * partition's doc-id range.
 *
 * <p>Before per-partition attribution was added, all N partitions collapsed into a single
 * thread-keyed breakdown; this test would then have seen {@code 1} breakdown with summed counts.
 */
public class TestQueryProfilerIntraSegment extends LuceneTestCase {

  private static final int NUM_DOCS = 200;

  private static Directory dir;
  private static IndexReader reader;

  @BeforeClass
  public static void setup() throws IOException {
    dir = newDirectory();
    RandomIndexWriter w = new RandomIndexWriter(random(), dir);
    for (int i = 0; i < NUM_DOCS; ++i) {
      Document doc = new Document();
      doc.add(new StringField("foo", "bar", Store.NO));
      w.addDocument(doc);
    }
    // Force a single segment so that partitioning below is genuinely *intra*-segment: the two
    // partitions are two doc-id ranges of the SAME leaf, not two different segments.
    w.forceMerge(1);
    reader = w.getReader();
    w.close();
  }

  @AfterClass
  public static void cleanup() throws IOException {
    IOUtils.close(reader, dir);
    dir = null;
    reader = null;
  }

  /**
   * Runs a match-everything {@link TermQuery} over a single segment split into two partitions and
   * asserts that the profiler produces one attributable breakdown per partition.
   */
  public void testSingleSegmentSplitIntoTwoPartitionsProducesPerPartitionBreakdowns()
      throws IOException {
    // Sanity: exactly one segment, so any partitioning is intra-segment.
    assertEquals("test requires a single forced-merged segment", 1, reader.leaves().size());
    final int maxDoc = reader.leaves().get(0).reader().maxDoc();
    assertEquals(NUM_DOCS, maxDoc);
    final int mid = maxDoc / 2;

    // An executor is required so that slices() is consulted (a non-concurrent searcher caches
    // whole-segment slices eagerly and never partitions).
    ExecutorService executor =
        Executors.newFixedThreadPool(2, new NamedThreadFactory("test-intra-segment"));
    try {
      // A profiling searcher whose slices() deterministically splits the one segment into two
      // partitions: [0, mid) and [mid, maxDoc). Lucene requires partitions of the same segment to
      // live in *separate* slices, so each partition gets its own slice (and thus its own thread).
      QueryProfilerIndexSearcher searcher =
          new QueryProfilerIndexSearcher(reader, executor) {
            @Override
            protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
              assertEquals(1, leaves.size());
              LeafReaderContext ctx = leaves.get(0);
              return new LeafSlice[] {
                new LeafSlice(
                    new ArrayList<>(
                        List.of(LeafReaderContextPartition.createFromAndTo(ctx, 0, mid)))),
                new LeafSlice(
                    new ArrayList<>(
                        List.of(LeafReaderContextPartition.createFromAndTo(ctx, mid, maxDoc))))
              };
            }
          };

      Query query = new TermQuery(new Term("foo", "bar"));
      // Use search() (not count()) so the top node stays a bare TermQuery and the scorer actually
      // iterates each partition's doc-id range, exercising NEXT_DOC/SCORE per partition.
      var topDocs = searcher.search(query, NUM_DOCS);
      assertEquals(
          "both partitions should have been searched", NUM_DOCS, topDocs.totalHits.value());

      List<QueryProfilerResult> results = searcher.getProfileResult();
      assertEquals("one query node (the TermQuery)", 1, results.size());
      QueryProfilerResult termQueryResult = results.get(0);
      assertEquals("TermQuery", termQueryResult.getQueryName());

      // ---- Nested slice -> partition attribution ---------------------------------------------
      // Each partition was searched as its own slice, so we get TWO slices, each holding exactly
      // ONE partition (its doc-id range).
      List<SliceProfilerResult> slices = termQueryResult.getSliceProfilerResults();
      assertEquals("two intra-segment partitions run as two slices", 2, slices.size());
      for (SliceProfilerResult slice : slices) {
        assertEquals("each slice searched exactly one partition", 1, slice.getPartitions().size());
        MatcherAssert.assertThat(slice.getSliceId(), greaterThanOrEqualTo(0));
        // Slice-level metadata is populated: a thread id (always present, even for unnamed virtual
        // threads), a thread name, a start time, and a positive total time derived from the slice's
        // partitions.
        MatcherAssert.assertThat(slice.getThreadId(), greaterThan(0L));
        assertNotNull(slice.getThreadName());
        MatcherAssert.assertThat(slice.getStartTime(), greaterThan(0L));
        MatcherAssert.assertThat(slice.getTotalTime(), greaterThan(0L));
      }
      // Distinct slice ids for the two independently-scheduled slices.
      assertNotEquals(
          "partitions in separate slices must have distinct slice ids",
          slices.get(0).getSliceId(),
          slices.get(1).getSliceId());

      // Locate each partition by its exposed doc-id range and verify counts are attributed to the
      // right range: [0, mid) sees mid docs, [mid, maxDoc) sees the rest.
      AggregatedQueryLeafProfilerResult first = findPartition(slices, 0, mid);
      AggregatedQueryLeafProfilerResult second = findPartition(slices, mid, maxDoc);

      // Both partitions are ranges of the same (single) segment, ordinal 0.
      assertEquals(0, first.getSegmentOrd());
      assertEquals(0, second.getSegmentOrd());

      long firstNextDoc =
          first.getTimeBreakdown().get(QueryProfilerTimingType.NEXT_DOC.toString() + "_count");
      long secondNextDoc =
          second.getTimeBreakdown().get(QueryProfilerTimingType.NEXT_DOC.toString() + "_count");
      long firstScore =
          first.getTimeBreakdown().get(QueryProfilerTimingType.SCORE.toString() + "_count");
      long secondScore =
          second.getTimeBreakdown().get(QueryProfilerTimingType.SCORE.toString() + "_count");

      // SCORE is invoked exactly once per matching doc, so it is a deterministic, per-partition
      // attribution: each partition scored exactly its own docs and the two cover the segment.
      assertEquals(mid, firstScore);
      assertEquals(maxDoc - mid, secondScore);
      assertEquals(NUM_DOCS, firstScore + secondScore);

      // NEXT_DOC is invoked once per matching doc, plus optionally one trailing advance to
      // NO_MORE_DOCS depending on the scorer/codec — so it is each partition's doc count, give or
      // take one. The key point is it is attributed to the partition, not summed across both.
      MatcherAssert.assertThat(firstNextDoc, anyOf(equalTo((long) mid), equalTo((long) mid + 1)));
      MatcherAssert.assertThat(
          secondNextDoc, anyOf(equalTo((long) (maxDoc - mid)), equalTo((long) (maxDoc - mid) + 1)));

      // Timings were actually recorded for each partition.
      MatcherAssert.assertThat(
          first.getTimeBreakdown().get(QueryProfilerTimingType.SCORE.toString()), greaterThan(0L));
      MatcherAssert.assertThat(
          second.getTimeBreakdown().get(QueryProfilerTimingType.SCORE.toString()), greaterThan(0L));

      // No child queries for a plain TermQuery.
      MatcherAssert.assertThat(termQueryResult.getProfiledChildren().size(), equalTo(0));
    } finally {
      executor.shutdown();
    }
  }

  /**
   * Multi-segment (non-intra) counterpart: two whole segments searched as two whole-segment
   * partitions in a single slice on a single thread.
   *
   * <p>This nails down the shape change introduced by keying breakdowns per partition rather than
   * per thread. With the original thread-keying, a single thread searching several whole segments
   * produced a <b>single</b> collapsed breakdown; here we assert the profiler instead produces one
   * breakdown <b>per segment</b>, each identified by its distinct {@code segmentOrd} and covering
   * that segment's whole doc-id range. This is per-segment attribution without any intra-segment
   * partitioning — segments are never split, so each partition spans its entire segment.
   */
  public void testMultipleWholeSegmentsProducePerSegmentBreakdowns() throws IOException {
    Directory multiSegmentDir = newDirectory();
    // NoMergePolicy + a commit between batches guarantees exactly two distinct segments.
    IndexWriterConfig config = new IndexWriterConfig(null).setMergePolicy(NoMergePolicy.INSTANCE);
    IndexWriter writer = new IndexWriter(multiSegmentDir, config);
    final int docsPerSegment = 50;
    for (int segment = 0; segment < 2; segment++) {
      for (int i = 0; i < docsPerSegment; i++) {
        Document doc = new Document();
        doc.add(new StringField("foo", "bar", Store.NO));
        writer.addDocument(doc);
      }
      writer.commit(); // seals the current segment
    }
    IndexReader multiSegmentReader = DirectoryReader.open(writer);
    writer.close();

    ExecutorService executor =
        Executors.newFixedThreadPool(2, new NamedThreadFactory("test-multi-segment"));
    try {
      assertEquals("test requires two segments", 2, multiSegmentReader.leaves().size());

      // Put both whole segments in a SINGLE slice, so a single thread searches both. Under the old
      // thread-keying this collapsed to one breakdown; per-segment keying keeps them separate.
      QueryProfilerIndexSearcher searcher =
          new QueryProfilerIndexSearcher(multiSegmentReader, executor) {
            @Override
            protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
              assertEquals(2, leaves.size());
              List<LeafReaderContextPartition> partitions = new ArrayList<>();
              for (LeafReaderContext ctx : leaves) {
                partitions.add(LeafReaderContextPartition.createForEntireSegment(ctx));
              }
              return new LeafSlice[] {new LeafSlice(partitions)};
            }
          };

      Query query = new TermQuery(new Term("foo", "bar"));
      var topDocs = searcher.search(query, 2 * docsPerSegment);
      assertEquals(2 * docsPerSegment, topDocs.totalHits.value());

      List<QueryProfilerResult> results = searcher.getProfileResult();
      assertEquals(1, results.size());
      QueryProfilerResult termQueryResult = results.get(0);

      // Both segments were searched within a SINGLE slice, so there is exactly ONE slice holding
      // TWO partitions (one per segment) — showing slice grouping is orthogonal to per-segment
      // attribution: multiple segments can share a slice.
      List<SliceProfilerResult> slices = termQueryResult.getSliceProfilerResults();
      assertEquals("both segments were searched in one slice", 1, slices.size());
      List<AggregatedQueryLeafProfilerResult> partitions = slices.get(0).getPartitions();
      assertEquals("the one slice must hold two per-segment partitions", 2, partitions.size());

      // Two distinct segment ordinals, each an entire-segment partition (min doc id 0).
      Set<Integer> segmentOrds = new HashSet<>();
      for (AggregatedQueryLeafProfilerResult p : partitions) {
        segmentOrds.add(p.getSegmentOrd());
        assertEquals("entire-segment partition starts at doc 0", 0, p.getMinDocId());
        // Each segment scored exactly its own docs.
        assertEquals(
            docsPerSegment,
            (long) p.getTimeBreakdown().get(QueryProfilerTimingType.SCORE.toString() + "_count"));
      }
      assertEquals("segments must have distinct ordinals", Set.of(0, 1), segmentOrds);
    } finally {
      executor.shutdown();
      IOUtils.close(multiSegmentReader, multiSegmentDir);
    }
  }

  /**
   * Combined case: multiple segments, each itself split into intra-segment partitions. Two segments
   * of {@code docsPerSegment} docs each are split into two doc-id-range partitions; because Lucene
   * requires partitions of the same segment to live in separate slices, this produces four slices
   * (2 segments × 2 partitions), each with a single partition.
   *
   * <p>This exercises both dimensions at once — {@code segmentOrd} distinguishing segments and the
   * doc-id range distinguishing partitions within a segment — verifying that each of the four
   * (segment, range) combinations is attributed independently and correctly.
   */
  public void testMultipleSegmentsEachPartitionedProducesPerPartitionBreakdowns()
      throws IOException {
    Directory multiSegmentDir = newDirectory();
    IndexWriterConfig config = new IndexWriterConfig(null).setMergePolicy(NoMergePolicy.INSTANCE);
    IndexWriter writer = new IndexWriter(multiSegmentDir, config);
    final int docsPerSegment = 60;
    final int mid = docsPerSegment / 2;
    for (int segment = 0; segment < 2; segment++) {
      for (int i = 0; i < docsPerSegment; i++) {
        Document doc = new Document();
        doc.add(new StringField("foo", "bar", Store.NO));
        writer.addDocument(doc);
      }
      writer.commit(); // seals the current segment
    }
    IndexReader multiSegmentReader = DirectoryReader.open(writer);
    writer.close();

    ExecutorService executor =
        Executors.newFixedThreadPool(4, new NamedThreadFactory("test-multi-seg-partitioned"));
    try {
      assertEquals("test requires two segments", 2, multiSegmentReader.leaves().size());

      // Split EACH segment into two partitions, each in its own slice (required for same-segment
      // partitions). Two segments × two partitions = four slices.
      QueryProfilerIndexSearcher searcher =
          new QueryProfilerIndexSearcher(multiSegmentReader, executor) {
            @Override
            protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
              assertEquals(2, leaves.size());
              List<LeafSlice> sliceList = new ArrayList<>();
              for (LeafReaderContext ctx : leaves) {
                sliceList.add(
                    new LeafSlice(
                        new ArrayList<>(
                            List.of(LeafReaderContextPartition.createFromAndTo(ctx, 0, mid)))));
                sliceList.add(
                    new LeafSlice(
                        new ArrayList<>(
                            List.of(
                                LeafReaderContextPartition.createFromAndTo(
                                    ctx, mid, docsPerSegment)))));
              }
              return sliceList.toArray(new LeafSlice[0]);
            }
          };

      Query query = new TermQuery(new Term("foo", "bar"));
      var topDocs = searcher.search(query, 2 * docsPerSegment);
      assertEquals(2 * docsPerSegment, topDocs.totalHits.value());

      List<QueryProfilerResult> results = searcher.getProfileResult();
      assertEquals(1, results.size());
      QueryProfilerResult termQueryResult = results.get(0);

      // Four slices, each holding exactly one partition, with four distinct slice ids.
      List<SliceProfilerResult> slices = termQueryResult.getSliceProfilerResults();
      assertEquals("2 segments x 2 partitions each => 4 slices", 4, slices.size());
      Set<Integer> sliceIds = new HashSet<>();
      for (SliceProfilerResult slice : slices) {
        assertEquals("each slice searched one partition", 1, slice.getPartitions().size());
        sliceIds.add(slice.getSliceId());
      }
      assertEquals("four distinct slice ids", 4, sliceIds.size());

      // Every (segment, doc-range) combination is present and correctly attributed.
      for (int seg = 0; seg < 2; seg++) {
        AggregatedQueryLeafProfilerResult lower = findPartition(slices, seg, 0, mid);
        AggregatedQueryLeafProfilerResult upper = findPartition(slices, seg, mid, docsPerSegment);
        assertEquals(
            "lower partition of segment " + seg + " scored its docs",
            mid,
            (long)
                lower.getTimeBreakdown().get(QueryProfilerTimingType.SCORE.toString() + "_count"));
        assertEquals(
            "upper partition of segment " + seg + " scored its docs",
            docsPerSegment - mid,
            (long)
                upper.getTimeBreakdown().get(QueryProfilerTimingType.SCORE.toString() + "_count"));
      }
    } finally {
      executor.shutdown();
      IOUtils.close(multiSegmentReader, multiSegmentDir);
    }
  }

  /**
   * Faithful production-shape test: drives Lucene's real {@link IndexSearcher#slices(List, int,
   * int, boolean) slicesWithSegmentPartitions} algorithm rather than a hand-built {@code slices()}
   * override. With a small {@code maxDocsPerSlice} threshold, large segments are split into
   * single-partition slices while small segments are grouped whole into one slice — the same mixed
   * layout a real workload (e.g. big5) produces.
   *
   * <p>Asserts that both shapes coexist in one profile: at least one slice holding a single
   * partition of a big segment, AND one slice holding multiple whole-segment partitions of
   * different small segments. This is the case where a slice genuinely holds more than one
   * partition.
   */
  public void testRealSlicerProducesMixedSingleAndGroupedSlices() throws IOException {
    // Small threshold so partitioning triggers without indexing millions of docs.
    final int maxDocsPerSlice = 20;
    final int maxSegmentsPerSlice = 5;

    Directory mixedDir = newDirectory();
    IndexWriterConfig config = new IndexWriterConfig(null).setMergePolicy(NoMergePolicy.INSTANCE);
    IndexWriter writer = new IndexWriter(mixedDir, config);
    // Two "big" segments (> maxDocsPerSlice, will be partitioned) and three "small" segments
    // (<= maxDocsPerSlice, will be grouped whole into a single slice).
    final int[] segmentSizes = {100, 100, 8, 8, 8};
    int totalDocs = 0;
    for (int size : segmentSizes) {
      for (int i = 0; i < size; i++) {
        Document doc = new Document();
        doc.add(new StringField("foo", "bar", Store.NO));
        writer.addDocument(doc);
      }
      writer.commit(); // seal each segment
      totalDocs += size;
    }
    IndexReader mixedReader = DirectoryReader.open(writer);
    writer.close();

    ExecutorService executor =
        Executors.newFixedThreadPool(4, new NamedThreadFactory("test-real-slicer"));
    try {
      assertEquals("test requires five segments", 5, mixedReader.leaves().size());

      QueryProfilerIndexSearcher searcher =
          new QueryProfilerIndexSearcher(mixedReader, executor) {
            @Override
            protected LeafSlice[] slices(List<LeafReaderContext> leaves) {
              // Delegate to Lucene's real production slicer with segment partitioning enabled.
              return slices(leaves, maxDocsPerSlice, maxSegmentsPerSlice, true);
            }
          };

      Query query = new TermQuery(new Term("foo", "bar"));
      var topDocs = searcher.search(query, totalDocs);
      assertEquals(totalDocs, topDocs.totalHits.value());

      List<QueryProfilerResult> results = searcher.getProfileResult();
      assertEquals(1, results.size());
      List<SliceProfilerResult> slices = results.get(0).getSliceProfilerResults();

      boolean sawSinglePartitionBigSegmentSlice = false;
      boolean sawGroupedMultiPartitionSlice = false;
      long totalScored = 0;
      Set<Integer> allSliceIds = new HashSet<>();

      for (SliceProfilerResult slice : slices) {
        allSliceIds.add(slice.getSliceId());
        List<AggregatedQueryLeafProfilerResult> partitions = slice.getPartitions();
        for (AggregatedQueryLeafProfilerResult p : partitions) {
          totalScored +=
              p.getTimeBreakdown().get(QueryProfilerTimingType.SCORE.toString() + "_count");
        }
        if (partitions.size() == 1) {
          // A big segment's partition covers only PART of its segment (its range does not start at
          // 0 and end at the segment's maxDoc for the interior partitions); either way it is one
          // partition in the slice.
          sawSinglePartitionBigSegmentSlice = true;
        } else if (partitions.size() > 1) {
          // The grouped small segments: each partition is a WHOLE different segment (min doc 0),
          // and their segment ordinals are distinct.
          sawGroupedMultiPartitionSlice = true;
          Set<Integer> groupedSegmentOrds = new HashSet<>();
          for (AggregatedQueryLeafProfilerResult p : partitions) {
            assertEquals("grouped partitions are whole segments", 0, p.getMinDocId());
            groupedSegmentOrds.add(p.getSegmentOrd());
          }
          assertEquals(
              "grouped slice holds partitions of distinct segments",
              partitions.size(),
              groupedSegmentOrds.size());
        }
      }

      assertTrue(
          "expected at least one slice holding a single big-segment partition",
          sawSinglePartitionBigSegmentSlice);
      assertTrue(
          "expected one slice holding multiple whole-segment partitions (grouped small segments)",
          sawGroupedMultiPartitionSlice);
      assertEquals("slice ids are unique", slices.size(), allSliceIds.size());
      assertEquals(
          "every matching doc is scored exactly once across all partitions",
          totalDocs,
          totalScored);
    } finally {
      executor.shutdown();
      IOUtils.close(mixedReader, mixedDir);
    }
  }

  /**
   * Sequential (no executor) multi-segment case: the searcher runs on the calling thread with no
   * concurrency, over multiple whole segments. This is the shape that {@code
   * TestQueryProfilerIndexSearcher} deliberately avoids via {@code forceMerge(1)}; here we assert
   * it directly.
   *
   * <p>With leaf timings keyed per partition, a sequential search over N whole segments produces a
   * single slice holding N whole-segment partitions (one per segment) — where the original
   * thread-level model produced a single merged breakdown. There is no executor, so all leaves run
   * on one thread within one implicit slice.
   */
  public void testSequentialMultiSegmentProducesPerSegmentPartitions() throws IOException {
    Directory multiSegmentDir = newDirectory();
    IndexWriterConfig config = new IndexWriterConfig(null).setMergePolicy(NoMergePolicy.INSTANCE);
    IndexWriter writer = new IndexWriter(multiSegmentDir, config);
    final int docsPerSegment = 30;
    final int numSegments = 3;
    for (int segment = 0; segment < numSegments; segment++) {
      for (int i = 0; i < docsPerSegment; i++) {
        Document doc = new Document();
        doc.add(new StringField("foo", "bar", Store.NO));
        writer.addDocument(doc);
      }
      writer.commit(); // seal each segment
    }
    IndexReader multiSegmentReader = DirectoryReader.open(writer);
    writer.close();

    try {
      assertEquals(numSegments, multiSegmentReader.leaves().size());

      // No executor -> sequential search on the calling thread, whole segments, no partitioning.
      QueryProfilerIndexSearcher searcher = new QueryProfilerIndexSearcher(multiSegmentReader);
      Query query = new TermQuery(new Term("foo", "bar"));
      var topDocs = searcher.search(query, numSegments * docsPerSegment);
      assertEquals(numSegments * docsPerSegment, topDocs.totalHits.value());

      List<SliceProfilerResult> slices =
          searcher.getProfileResult().get(0).getSliceProfilerResults();

      // Sequential search runs as a single implicit slice...
      assertEquals("sequential search is one implicit slice", 1, slices.size());
      // ...holding one whole-segment partition per segment.
      List<AggregatedQueryLeafProfilerResult> partitions = slices.get(0).getPartitions();
      assertEquals("one whole-segment partition per segment", numSegments, partitions.size());

      Set<Integer> segmentOrds = new HashSet<>();
      long totalScored = 0;
      for (AggregatedQueryLeafProfilerResult p : partitions) {
        segmentOrds.add(p.getSegmentOrd());
        assertEquals("whole-segment partition starts at doc 0", 0, p.getMinDocId());
        totalScored +=
            p.getTimeBreakdown().get(QueryProfilerTimingType.SCORE.toString() + "_count");
      }
      assertEquals("segments have distinct ordinals", numSegments, segmentOrds.size());
      assertEquals("every doc scored exactly once", numSegments * docsPerSegment, totalScored);
    } finally {
      IOUtils.close(multiSegmentReader, multiSegmentDir);
    }
  }

  /** Finds the partition covering the given doc-id range across all slices. */
  private static AggregatedQueryLeafProfilerResult findPartition(
      List<SliceProfilerResult> slices, int minDocId, int maxDocId) {
    for (SliceProfilerResult slice : slices) {
      for (AggregatedQueryLeafProfilerResult r : slice.getPartitions()) {
        if (r.getMinDocId() == minDocId && r.getMaxDocId() == maxDocId) {
          return r;
        }
      }
    }
    throw new AssertionError(
        "no partition breakdown found for doc-id range [" + minDocId + ", " + maxDocId + ")");
  }

  /** Finds the partition for a specific segment and doc-id range across all slices. */
  private static AggregatedQueryLeafProfilerResult findPartition(
      List<SliceProfilerResult> slices, int segmentOrd, int minDocId, int maxDocId) {
    for (SliceProfilerResult slice : slices) {
      for (AggregatedQueryLeafProfilerResult r : slice.getPartitions()) {
        if (r.getSegmentOrd() == segmentOrd
            && r.getMinDocId() == minDocId
            && r.getMaxDocId() == maxDocId) {
          return r;
        }
      }
    }
    throw new AssertionError(
        "no partition breakdown found for segment "
            + segmentOrd
            + " doc-id range ["
            + minDocId
            + ", "
            + maxDocId
            + ")");
  }
}
