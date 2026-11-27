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
package org.apache.lucene.facet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.TreeSet;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.DrillSidewaysScorer.DocsAndCost;
import org.apache.lucene.facet.DrillSidewaysScorer.Strategy;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.search.AssertingBulkScorer;
import org.apache.lucene.tests.search.RandomApproximationQuery;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;

/**
 * Tests that each strategy of {@link DrillSidewaysScorer} collects the expected hits and
 * near-misses, and returns accurate next docs, when a leaf is scored at once or in ranges like with
 * intra-segment concurrency and timeouts.
 */
public class TestDrillSidewaysScorer extends LuceneTestCase {

  private static final int CHUNK = 2048;

  public void testQueryFirstSingleDim() throws IOException {
    doTestStrategy(0.02, 0.5, 1, false, Strategy.QUERY_FIRST);
  }

  public void testQueryFirstMultipleDims() throws IOException {
    doTestStrategy(0.02, 0.5, TestUtil.nextInt(random(), 2, 4), false, Strategy.QUERY_FIRST);
  }

  public void testQueryFirstForcedByScoreSubDocsAtOnce() throws IOException {
    // the costs alone would choose UNION
    doTestStrategy(0.3, 0.3, TestUtil.nextInt(random(), 1, 4), true, Strategy.QUERY_FIRST);
  }

  public void testDrillDownAdvance() throws IOException {
    doTestStrategy(0.9, 0.05, TestUtil.nextInt(random(), 2, 3), false, Strategy.DRILL_DOWN_ADVANCE);
  }

  public void testUnion() throws IOException {
    doTestStrategy(0.3, 0.3, TestUtil.nextInt(random(), 1, 4), false, Strategy.UNION);
  }

  private void doTestStrategy(
      double baseProbability,
      double dimProbability,
      int numDims,
      boolean scoreSubDocsAtOnce,
      Strategy expectedStrategy)
      throws IOException {
    // an empty dim leaves no hits, only its near-misses, and needs another dim for the costs to
    // still choose the expected strategy
    final int emptyDim = numDims > 1 && random().nextBoolean() ? random().nextInt(numDims) : -1;
    final double deletedProbability = random().nextInt(3) * 0.25;
    try (Directory dir = newDirectory()) {
      // a single segment covering several windows of the windowed strategies
      int numDocs = TestUtil.nextInt(random(), 5_000, 20_000);
      // computed from the indexed docs, which keep their doc IDs in the single segment
      List<Integer> expectedHits = new ArrayList<>();
      List<List<Integer>> expectedSideways = new ArrayList<>();
      for (int dim = 0; dim < numDims; dim++) {
        expectedSideways.add(new ArrayList<>());
      }
      IndexWriterConfig iwc =
          new IndexWriterConfig()
              .setMergePolicy(NoMergePolicy.INSTANCE)
              .setMaxBufferedDocs(IndexWriterConfig.DISABLE_AUTO_FLUSH)
              .setRAMBufferSizeMB(256);
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numDocs; i++) {
          Document doc = new Document();
          // doc 0 is a hit or a near-miss of the empty dim, so that there is always something to
          // collect, and windows starting at doc 0 fill their first slot
          boolean firstDoc = i == 0;
          boolean matchesBase = firstDoc || random().nextDouble() < baseProbability;
          if (matchesBase) {
            doc.add(new StringField("base", "yes", Field.Store.NO));
          }
          int numMissingDims = 0;
          int missingDim = -1;
          for (int dim = 0; dim < numDims; dim++) {
            if (dim != emptyDim && (firstDoc || random().nextDouble() < dimProbability)) {
              doc.add(new StringField("dim" + dim, "yes", Field.Store.NO));
            } else {
              numMissingDims++;
              missingDim = dim;
            }
          }
          boolean deleted = firstDoc == false && random().nextDouble() < deletedProbability;
          if (deleted) {
            doc.add(new StringField("deleted", "yes", Field.Store.NO));
          }
          w.addDocument(doc);

          if (matchesBase && deleted == false) {
            if (numMissingDims == 0) {
              expectedHits.add(i);
              for (List<Integer> sideways : expectedSideways) {
                sideways.add(i);
              }
            } else if (numMissingDims == 1) {
              expectedSideways.get(missingDim).add(i);
            }
          }
        }
        w.commit();
        w.deleteDocuments(new Term("deleted", "yes"));
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(1, reader.leaves().size());
        LeafReaderContext context = reader.leaves().getFirst();
        IndexSearcher searcher = new IndexSearcher(reader);
        Weight base = createWeight(searcher, new TermQuery(new Term("base", "yes")));
        Weight[] drillDowns = new Weight[numDims];
        for (int dim = 0; dim < numDims; dim++) {
          drillDowns[dim] = createWeight(searcher, new TermQuery(new Term("dim" + dim, "yes")));
        }
        Bits liveDocs = context.reader().getLiveDocs();
        assertEquals(deletedProbability == 0, liveDocs == null);
        int maxDoc = context.reader().maxDoc();

        Recorder expected = new Recorder(numDims);
        DrillSidewaysScorer scorer =
            newScorer(context, base, drillDowns, expected, scoreSubDocsAtOnce);
        assertEquals(expectedStrategy, scorer.strategy);
        assertEquals(
            DocIdSetIterator.NO_MORE_DOCS,
            scorer.score(expected.main, liveDocs, 0, DocIdSetIterator.NO_MORE_DOCS));
        expected.assertDocs(expectedHits, expectedSideways);
        TreeSet<Integer> expectedDocs = expected.allDocs();

        // split the leaf in partitions that are each scored by a new scorer like when searching
        // with intra-segment concurrency, and score each partition in random windows
        Recorder actual = new Recorder(numDims);
        int partitionMin = 0;
        for (int partitionMax : partitionBounds(maxDoc, new ArrayList<>(expectedDocs))) {
          BulkScorer partitionScorer =
              AssertingBulkScorer.wrap(
                  random(),
                  new CheckingBulkScorer(
                      newScorer(context, base, drillDowns, actual, scoreSubDocsAtOnce),
                      expectedDocs,
                      maxDoc),
                  maxDoc);
          partitionScorer.score(actual.main, liveDocs, partitionMin, partitionMax);
          partitionMin = partitionMax;
        }

        actual.assertDocs(expectedHits, expectedSideways);
        assertEquals(expected.main.scores, actual.main.scores);

        // nothing to score after the last doc
        Recorder afterLastDoc = new Recorder(numDims);
        assertEquals(
            DocIdSetIterator.NO_MORE_DOCS,
            newScorer(context, base, drillDowns, afterLastDoc, scoreSubDocsAtOnce)
                .score(afterLastDoc.main, liveDocs, maxDoc, DocIdSetIterator.NO_MORE_DOCS));
        assertTrue(afterLastDoc.allDocs().isEmpty());
      }
    }
  }

  /**
   * Returns the upper bounds of random partitions of the leaf, which may start or end on the bounds
   * of the windows of the windowed strategies or on matches, or be empty.
   */
  private static List<Integer> partitionBounds(int maxDoc, List<Integer> matches) {
    TreeSet<Integer> bounds = new TreeSet<>();
    int numBounds = random().nextInt(5);
    for (int i = 0; i < numBounds; i++) {
      int bound =
          switch (random().nextInt(4)) {
            case 0 -> random().nextInt(maxDoc);
            case 1 -> random().nextInt(maxDoc / CHUNK + 1) * CHUNK;
            case 2 -> random().nextInt(maxDoc / CHUNK + 1) * CHUNK + random().nextInt(3) - 1;
            default -> matches.get(random().nextInt(matches.size())) + random().nextInt(2);
          };
      bounds.add(Math.clamp(bound, 0, maxDoc));
    }
    List<Integer> partitionMaxs = new ArrayList<>(bounds);
    if (random().nextInt(10) == 0 && partitionMaxs.isEmpty() == false) {
      // an empty partition
      partitionMaxs.add(partitionMaxs.getLast());
    }
    partitionMaxs.add(DocIdSetIterator.NO_MORE_DOCS);
    return partitionMaxs;
  }

  private Weight createWeight(IndexSearcher searcher, Query query) throws IOException {
    if (random().nextBoolean()) {
      query = new RandomApproximationQuery(query, random());
    }
    return searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE, 1);
  }

  /** Creates a scorer the same way as {@link DrillSidewaysQuery}. */
  private static DrillSidewaysScorer newScorer(
      LeafReaderContext context,
      Weight base,
      Weight[] drillDowns,
      Recorder recorder,
      boolean scoreSubDocsAtOnce)
      throws IOException {
    DocsAndCost[] dims = new DocsAndCost[drillDowns.length];
    for (int dim = 0; dim < dims.length; dim++) {
      Scorer scorer = drillDowns[dim].scorer(context);
      if (scorer == null) {
        scorer =
            new ConstantScoreScorer(0f, ScoreMode.COMPLETE_NO_SCORES, DocIdSetIterator.empty());
      }
      dims[dim] = new DocsAndCost(scorer, recorder.sideways.get(dim));
    }
    Arrays.sort(dims, Comparator.comparingLong(o -> o.approximation.cost()));
    return new DrillSidewaysScorer(context, base.scorer(context), dims, scoreSubDocsAtOnce);
  }

  /**
   * Checks the values returned by {@link DrillSidewaysScorer#score}, and sometimes scores an empty
   * range first.
   */
  private static class CheckingBulkScorer extends BulkScorer {
    private final BulkScorer in;
    private final TreeSet<Integer> expectedDocs;
    private final int maxDoc;

    CheckingBulkScorer(BulkScorer in, TreeSet<Integer> expectedDocs, int maxDoc) {
      this.in = in;
      this.expectedDocs = expectedDocs;
      this.maxDoc = maxDoc;
    }

    @Override
    public int score(LeafCollector collector, Bits acceptDocs, int min, int max)
        throws IOException {
      if (random().nextInt(10) == 0) {
        checkNext(min, in.score(collector, acceptDocs, min, min));
      }
      return checkNext(max, in.score(collector, acceptDocs, min, max));
    }

    private int checkNext(int max, int next) {
      assertTrue("next=" + next + " is before max=" + max, next >= max);
      if (max >= maxDoc) {
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, next);
      }
      // the next hit or near-miss must not be skipped
      Integer expectedNext = expectedDocs.ceiling(max);
      if (expectedNext != null) {
        assertTrue(
            "next=" + next + " is after the next match=" + expectedNext, next <= expectedNext);
      }
      return next;
    }

    @Override
    public long cost() {
      return in.cost();
    }
  }

  private static class Recorder {
    final RecordingLeafCollector main = new RecordingLeafCollector();
    final List<RecordingLeafCollector> sideways = new ArrayList<>();

    Recorder(int numDims) {
      for (int dim = 0; dim < numDims; dim++) {
        sideways.add(new RecordingLeafCollector());
      }
    }

    void assertDocs(List<Integer> hits, List<List<Integer>> sidewaysDocs) {
      assertEquals(hits, main.docs);
      for (int dim = 0; dim < sideways.size(); dim++) {
        assertEquals("dim " + dim, sidewaysDocs.get(dim), sideways.get(dim).docs);
      }
    }

    /** Returns the docs of all hits and near-misses. */
    TreeSet<Integer> allDocs() {
      TreeSet<Integer> docs = new TreeSet<>(main.docs);
      for (RecordingLeafCollector collector : sideways) {
        docs.addAll(collector.docs);
      }
      return docs;
    }
  }

  private static class RecordingLeafCollector implements LeafCollector {
    final List<Integer> docs = new ArrayList<>();
    final List<Float> scores = new ArrayList<>();
    private Scorable scorer;

    @Override
    public void setScorer(Scorable scorer) {
      this.scorer = scorer;
    }

    @Override
    public void collect(int doc) throws IOException {
      if (docs.isEmpty() == false) {
        assertTrue("out of order: " + doc + " after " + docs.getLast(), doc > docs.getLast());
      }
      docs.add(doc);
      scores.add(scorer.score());
    }
  }
}
