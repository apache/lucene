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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.LongUnaryOperator;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FixedBitSet;

/**
 * Tests the {@code intoBitSet()} and {@code applyMask()} bulk paths of {@link
 * DocValuesRangeIterator} over single-valued numeric doc values, including YES, YES_IF_PRESENT, and
 * MAYBE block states.
 */
public class TestSkipBlockRangeIteratorBitSetOperations extends BaseDocValuesSkipperTests {

  // Use enough docs to span at least 4 skip blocks (default skip block size = 4096 docs).
  private static final int DOC_COUNT = 4096 * 4;

  private Directory dir;
  private DirectoryReader reader;
  private IndexSearcher searcher;

  private long[] values;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    dir = newDirectory();
    IndexWriterConfig iwc = new IndexWriterConfig();
    iwc.setCodec(new Lucene104Codec());
    IndexWriter w = new IndexWriter(dir, iwc);
    values = new long[DOC_COUNT];
    for (int i = 0; i < DOC_COUNT; i++) {
      values[i] = i % 100; // deterministic: 0..99 repeating
      Document doc = new Document();
      doc.add(NumericDocValuesField.indexedField("age", values[i]));
      doc.add(SortedNumericDocValuesField.indexedField("multi_age", values[i] - 1000));
      doc.add(SortedNumericDocValuesField.indexedField("multi_age", values[i]));
      doc.add(NumericDocValuesField.indexedField("score", (i * 7L) % 1000));
      w.addDocument(doc);
    }
    w.forceMerge(1);
    reader = DirectoryReader.open(w);
    w.close();
    searcher = new IndexSearcher(reader);
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    dir.close();
    super.tearDown();
  }

  /**
   * Verifies that SortedNumericDocValuesRangeQuery wires a DocValuesRangeIterator two-phase when
   * the field has a skip index and is single-valued.
   */
  public void testTwoPhaseIteratorIsWired() throws Exception {
    LeafReaderContext ctx = reader.leaves().get(0);
    // Use a range that won't be rewritten to MatchAll or MatchNone
    Query q = SortedNumericDocValuesField.newSlowRangeQuery("age", 20, 40);
    Weight weight = q.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
    ScorerSupplier ss = weight.scorerSupplier(ctx);
    assertNotNull("ScorerSupplier must not be null", ss);

    assertTrue(
        "ScorerSupplier must be a ConstantScoreScorerSupplier",
        ss instanceof ConstantScoreScorerSupplier);
    DocIdSetIterator iter = ((ConstantScoreScorerSupplier) ss).iterator(Long.MAX_VALUE);
    TwoPhaseIterator twoPhase = TwoPhaseIterator.unwrap(iter);
    assertTrue(
        "Range query on single-valued field with skip index must use a DocValuesRangeIterator,"
            + " but got: "
            + (twoPhase == null ? iter.getClass() : twoPhase.getClass()).getSimpleName(),
        twoPhase instanceof DocValuesRangeIterator);
  }

  public void testMultiValuedTwoPhaseIteratorIsWired() throws Exception {
    LeafReaderContext ctx = reader.leaves().get(0);
    Query q = SortedNumericDocValuesField.newSlowRangeQuery("multi_age", 20, 40);
    Weight weight = q.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f);
    ScorerSupplier ss = weight.scorerSupplier(ctx);
    assertNotNull("ScorerSupplier must not be null", ss);

    assertTrue(
        "ScorerSupplier must be a ConstantScoreScorerSupplier",
        ss instanceof ConstantScoreScorerSupplier);
    DocIdSetIterator iter = ((ConstantScoreScorerSupplier) ss).iterator(Long.MAX_VALUE);
    TwoPhaseIterator twoPhase = TwoPhaseIterator.unwrap(iter);
    assertTrue(
        "Range query on multi-valued field with skip index must use a DocValuesRangeIterator,"
            + " but got: "
            + (twoPhase == null ? iter.getClass() : twoPhase.getClass()).getSimpleName(),
        twoPhase instanceof DocValuesRangeIterator);
    assertEquals(
        searcher.count(SortedNumericDocValuesField.newSlowRangeQuery("age", 20, 40)),
        searcher.count(q));
  }

  public void testSingleFieldRangeCorrectness() throws Exception {
    // Count expected matches by scanning values[] directly
    int expected = 0;
    for (long v : values) {
      if (v >= 20 && v <= 40) expected++;
    }
    assertTrue("Should have some matches with controlled data", expected > 0);

    Query q = SortedNumericDocValuesField.newSlowRangeQuery("age", 20, 40);
    assertEquals("Single-field count must match linear scan", expected, searcher.count(q));
  }

  public void testMultiFieldRangeCorrectness() throws Exception {
    int expectedBoth = 0;
    for (int i = 0; i < DOC_COUNT; i++) {
      long age = values[i];
      long score = (i * 7L) % 1000;
      if (age >= 20 && age <= 40 && score >= 100 && score <= 500) expectedBoth++;
    }

    Query ageOnly = SortedNumericDocValuesField.newSlowRangeQuery("age", 20, 40);
    Query scoreOnly = SortedNumericDocValuesField.newSlowRangeQuery("score", 100, 500);
    Query both =
        new BooleanQuery.Builder().add(ageOnly, Occur.FILTER).add(scoreOnly, Occur.FILTER).build();

    assertEquals("Multi-field count must match linear scan", expectedBoth, searcher.count(both));
    assertTrue(
        "Conjunction must be <= age-only count", searcher.count(both) <= searcher.count(ageOnly));
    assertTrue(
        "Conjunction must be <= score-only count",
        searcher.count(both) <= searcher.count(scoreOnly));
  }

  public void testResultsAreRepeatable() throws Exception {
    Query q =
        new BooleanQuery.Builder()
            .add(SortedNumericDocValuesField.newSlowRangeQuery("age", 20, 40), Occur.FILTER)
            .add(SortedNumericDocValuesField.newSlowRangeQuery("score", 100, 500), Occur.FILTER)
            .build();

    int count1 = searcher.count(q);
    int count2 = searcher.count(q);
    assertEquals("Results must be deterministic", count1, count2);
  }

  /**
   * Single-field range with a restrictive second clause exercises the single-field bitset path via
   * DenseConjunctionBulkScorer. Uses a second range query on a different field as the restrictive
   * clause — MatchAllDocsQuery is not used because BooleanQuery rewrites it away.
   */
  public void testSingleFieldWithRestrictiveSecondClause() throws Exception {
    // age in [20,40] AND score in [0,0]..score=0 only for docs where (i*7)%1000==0, i.e., i=0
    Query ageQ = SortedNumericDocValuesField.newSlowRangeQuery("age", 0, 99); // matches all
    Query scoreQ = SortedNumericDocValuesField.newSlowRangeQuery("score", 0, 0); // very restrictive

    int expectedScore0 = 0;
    for (int i = 0; i < DOC_COUNT; i++) {
      if ((i * 7L) % 1000 == 0) expectedScore0++;
    }

    Query combined =
        new BooleanQuery.Builder().add(ageQ, Occur.FILTER).add(scoreQ, Occur.FILTER).build();

    assertEquals("Should match only docs where score=0", expectedScore0, searcher.count(combined));
  }

  public void testIntoBitSetAdvancesWhenUpToIsBlockBoundary() throws Exception {
    long queryMin = 10;
    long queryMax = 20;

    NumericDocValues values = docValues(queryMin, queryMax);
    DocValuesSkipper skipper = docValuesSkipper(queryMin, queryMax, true);
    DocValuesRangeIterator iter =
        DocValuesRangeIterator.forRange(values, skipper, queryMin, queryMax);
    DocIdSetIterator approximation = iter.approximation();
    assertEquals(0, approximation.nextDoc());

    FixedBitSet bitSet = new FixedBitSet(2048);
    iter.intoBitSet(128, bitSet, 0);

    assertEquals("All docs in the YES block must match", 128, bitSet.cardinality());
    assertTrue(
        "approximation must skip the NO block that starts at the block boundary, but was "
            + approximation.docID(),
        approximation.docID() >= 512);

    // Continue filling the bitset in a second window, as DenseConjunctionBulkScorer does.
    iter.intoBitSet(2048, bitSet, 0);
    FixedBitSet expected = new FixedBitSet(2048);
    values = docValues(queryMin, queryMax);
    for (int d = values.nextDoc();
        d != DocIdSetIterator.NO_MORE_DOCS && d < 2048;
        d = values.nextDoc()) {
      if (values.longValue() >= queryMin && values.longValue() <= queryMax) {
        expected.set(d);
      }
    }
    FixedBitSet diff = expected.clone();
    diff.andNot(bitSet);
    assertEquals("Second intoBitSet window must not miss matching docs", 0, diff.cardinality());
  }

  /** Directly tests intoBitSet() against a linear scan reference. */
  public void testIntoBitSetMatchesLinearScan() throws Exception {
    doTestIntoBitSetMatchesLinearScan(reader);
  }

  /** Large-scale version — 100 skip blocks */
  @Nightly
  public void testIntoBitSetMatchesLinearScanHuge() throws Exception {
    try (Directory hugeDir = newDirectory()) {
      buildIndex(hugeDir, 4096 * 100);
      try (DirectoryReader r = DirectoryReader.open(hugeDir)) {
        doTestIntoBitSetMatchesLinearScan(r);
      }
    }
  }

  private void doTestIntoBitSetMatchesLinearScan(DirectoryReader r) throws Exception {
    LeafReaderContext ctx = r.leaves().get(0);
    int maxDoc = ctx.reader().maxDoc();
    int windowSize = DenseConjunctionBulkScorer.WINDOW_SIZE; // 4096

    FixedBitSet expected = new FixedBitSet(windowSize);
    NumericDocValues refValues = ctx.reader().getNumericDocValues("age");
    for (int d = 0; d < Math.min(maxDoc, windowSize); d++) {
      if (refValues.advanceExact(d) && refValues.longValue() >= 20 && refValues.longValue() <= 40) {
        expected.set(d);
      }
    }

    DocValuesSkipper skipper = ctx.reader().getDocValuesSkipper("age");
    NumericDocValues dv = ctx.reader().getNumericDocValues("age");
    assertNotNull("Field must have a skip index", skipper);

    DocValuesRangeIterator iter = DocValuesRangeIterator.forRange(dv, skipper, 20, 40);
    iter.approximation().nextDoc();

    FixedBitSet actual = new FixedBitSet(windowSize);
    iter.intoBitSet(Math.min(maxDoc, windowSize), actual, 0);

    assertEquals(
        "intoBitSet must set exactly the same bits as linear scan",
        expected.cardinality(),
        actual.cardinality());
    FixedBitSet diff = expected.clone();
    diff.xor(actual);
    assertEquals("No bits should differ", 0, diff.cardinality());
  }

  /** Tests YES block path — range covers all values, all docs match. */
  public void testIntoBitSetAllMatchRange() throws Exception {
    doTestIntoBitSetAllMatchRange(reader);
  }

  @Nightly
  public void testIntoBitSetAllMatchRangeHuge() throws Exception {
    try (Directory hugeDir = newDirectory()) {
      buildIndex(hugeDir, 4096 * 100);
      try (DirectoryReader r = DirectoryReader.open(hugeDir)) {
        doTestIntoBitSetAllMatchRange(r);
      }
    }
  }

  private void doTestIntoBitSetAllMatchRange(DirectoryReader r) throws Exception {
    LeafReaderContext ctx = r.leaves().get(0);
    int maxDoc = ctx.reader().maxDoc();

    DocValuesSkipper skipper = ctx.reader().getDocValuesSkipper("age");
    NumericDocValues dv = ctx.reader().getNumericDocValues("age");
    assertNotNull(skipper);

    DocValuesRangeIterator iter = DocValuesRangeIterator.forRange(dv, skipper, 0, 99);
    iter.approximation().nextDoc();

    FixedBitSet bitSet = new FixedBitSet(maxDoc);
    iter.intoBitSet(maxDoc, bitSet, 0);

    assertEquals("All docs should match range [0,99]", maxDoc, bitSet.cardinality());
  }

  /** Tests NO block path — range matches nothing. */
  public void testIntoBitSetNoMatchRange() throws Exception {
    doTestIntoBitSetNoMatchRange(reader);
  }

  @Nightly
  public void testIntoBitSetNoMatchRangeHuge() throws Exception {
    try (Directory hugeDir = newDirectory()) {
      buildIndex(hugeDir, 4096 * 100);
      try (DirectoryReader r = DirectoryReader.open(hugeDir)) {
        doTestIntoBitSetNoMatchRange(r);
      }
    }
  }

  private void doTestIntoBitSetNoMatchRange(DirectoryReader r) throws Exception {
    LeafReaderContext ctx = r.leaves().get(0);
    int maxDoc = ctx.reader().maxDoc();

    DocValuesSkipper skipper = ctx.reader().getDocValuesSkipper("age");
    NumericDocValues dv = ctx.reader().getNumericDocValues("age");
    assertNotNull(skipper);

    DocValuesRangeIterator iter = DocValuesRangeIterator.forRange(dv, skipper, 200, 300);
    iter.approximation().nextDoc();

    FixedBitSet bitSet = new FixedBitSet(maxDoc);
    iter.intoBitSet(maxDoc, bitSet, 0);

    assertEquals("No docs should match out-of-range query", 0, bitSet.cardinality());
  }

  /** Tests YES_IF_PRESENT block path — sparse field, only even docs have a value. */
  public void testIntoBitSetSparseField() throws Exception {
    doTestIntoBitSetSparseField(1000);
  }

  @Nightly
  public void testIntoBitSetSparseFieldHuge() throws Exception {
    doTestIntoBitSetSparseField(4096 * 20);
  }

  private void doTestIntoBitSetSparseField(int numDocs) throws Exception {
    try (Directory sparseDir = newDirectory()) {
      IndexWriterConfig iwc = new IndexWriterConfig().setCodec(new Lucene104Codec());
      IndexWriter w = new IndexWriter(sparseDir, iwc);
      List<Integer> expectedDocs = new ArrayList<>();
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        if (i % 2 == 0) {
          // All values within [20, 40] so blocks are YES_IF_PRESENT (not MAYBE)
          long val = 20 + (i % 21);
          doc.add(NumericDocValuesField.indexedField("sparse", val));
          expectedDocs.add(i);
        }
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (DirectoryReader sparseReader = DirectoryReader.open(w)) {
        w.close();
        LeafReaderContext ctx = sparseReader.leaves().get(0);

        DocValuesSkipper skipper = ctx.reader().getDocValuesSkipper("sparse");
        NumericDocValues dv = ctx.reader().getNumericDocValues("sparse");
        if (skipper == null) return;

        DocValuesRangeIterator iter = DocValuesRangeIterator.forRange(dv, skipper, 20, 40);
        iter.approximation().nextDoc();

        FixedBitSet bitSet = new FixedBitSet(numDocs);
        iter.intoBitSet(numDocs, bitSet, 0);

        assertEquals(
            "Sparse field intoBitSet must match expected count",
            expectedDocs.size(),
            bitSet.cardinality());
        for (int i = 1; i < numDocs; i += 2) {
          assertFalse("Odd doc " + i + " has no value and must not be set", bitSet.get(i));
        }
      }
    }
  }

  /** Tests nextDoc() and advance() return the same docs as a linear scan. */
  public void testIterationCorrectness() throws Exception {
    doTestIterationCorrectness(reader);
  }

  @Nightly
  public void testIterationCorrectnessHuge() throws Exception {
    try (Directory hugeDir = newDirectory()) {
      buildIndex(hugeDir, 4096 * 100);
      try (DirectoryReader r = DirectoryReader.open(hugeDir)) {
        doTestIterationCorrectness(r);
      }
    }
  }

  private void doTestIterationCorrectness(DirectoryReader r) throws Exception {
    LeafReaderContext ctx = r.leaves().get(0);

    List<Integer> expected = new ArrayList<>();
    NumericDocValues refValues = ctx.reader().getNumericDocValues("age");
    for (int d = 0; d < ctx.reader().maxDoc(); d++) {
      if (refValues.advanceExact(d) && refValues.longValue() >= 20 && refValues.longValue() <= 40) {
        expected.add(d);
      }
    }
    assertTrue("Test requires some matching docs", expected.size() > 0);

    // Test 1: nextDoc() returns all matching docs in order
    DocValuesSkipper skipper = ctx.reader().getDocValuesSkipper("age");
    NumericDocValues dv = ctx.reader().getNumericDocValues("age");
    assertNotNull(skipper);

    DocIdSetIterator iter =
        TwoPhaseIterator.asDocIdSetIterator(DocValuesRangeIterator.forRange(dv, skipper, 20, 40));
    List<Integer> actual = new ArrayList<>();
    for (int d = iter.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = iter.nextDoc()) {
      actual.add(d);
    }
    assertEquals("nextDoc() must return same docs as expected", expected, actual);

    // Test 2: advance() forward through the iterator with random skips.
    // Uses a single iterator instance and advances forward multiple times,
    // verifying each advance lands on the correct next matching doc.
    skipper = ctx.reader().getDocValuesSkipper("age");
    dv = ctx.reader().getNumericDocValues("age");
    iter =
        TwoPhaseIterator.asDocIdSetIterator(DocValuesRangeIterator.forRange(dv, skipper, 20, 40));
    Random rng = random();
    int idx = 0;
    while (idx < expected.size()) {
      int target = expected.get(idx);
      int doc = iter.advance(target);
      assertEquals("advance(" + target + ") must return the target", target, doc);
      // Skip forward by a random amount (1-5 matching docs)
      idx += rng.nextInt(5) + 1;
    }

    // Test 3: advance() to a non-matching doc lands on the next match.
    // Picks random targets between consecutive matches and verifies
    // advance returns the next matching doc after the target.
    skipper = ctx.reader().getDocValuesSkipper("age");
    dv = ctx.reader().getNumericDocValues("age");
    iter =
        TwoPhaseIterator.asDocIdSetIterator(DocValuesRangeIterator.forRange(dv, skipper, 20, 40));
    int prevDoc = -1;
    for (int i = 0; i < expected.size() && i < 20; i++) {
      int matchDoc = expected.get(i);
      if (matchDoc > prevDoc + 1) {
        // Target a gap between previous position and this match
        int target = prevDoc + 1 + rng.nextInt(matchDoc - prevDoc - 1);
        int doc = iter.advance(target);
        assertEquals(
            "advance(" + target + ") must skip to next match at " + matchDoc, matchDoc, doc);
        prevDoc = doc;
      } else {
        prevDoc = matchDoc;
      }
    }

    // Test 4: advance() past all docs returns NO_MORE_DOCS
    skipper = ctx.reader().getDocValuesSkipper("age");
    dv = ctx.reader().getNumericDocValues("age");
    iter =
        TwoPhaseIterator.asDocIdSetIterator(DocValuesRangeIterator.forRange(dv, skipper, 20, 40));
    int doc = iter.advance(ctx.reader().maxDoc());
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, doc);
  }

  /** Helper: builds an index with {@code numDocs} docs, values = i % 100. */
  private void buildIndex(Directory dir, int numDocs) throws Exception {
    IndexWriterConfig iwc = new IndexWriterConfig().setCodec(new Lucene104Codec());
    try (IndexWriter w = new IndexWriter(dir, iwc)) {
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        doc.add(NumericDocValuesField.indexedField("age", i % 100));
        w.addDocument(doc);
      }
      w.forceMerge(1);
    }
  }

  /**
   * Tests the DocValuesRangeIterator iteration and intoBitSet paths using the fake NumericDocValues
   * and DocValuesSkipper from BaseDocValuesSkipperTests, which exercises all block types (YES, NO,
   * MAYBE, YES_IF_PRESENT) across both dense and sparse regions.
   */
  public void testAllBlockTypesWithFakeSkipper() throws Exception {
    long queryMin = 10;
    long queryMax = 20;

    // Collect expected matching docs by brute-force scan (up to 2048, the skipper's range)
    List<Integer> expected = new ArrayList<>();
    NumericDocValues refValues = docValues(queryMin, queryMax);
    for (int d = refValues.nextDoc();
        d != DocIdSetIterator.NO_MORE_DOCS && d < 2048;
        d = refValues.nextDoc()) {
      long v = refValues.longValue();
      if (v >= queryMin && v <= queryMax) {
        expected.add(d);
      }
    }
    assertTrue("Should have matching docs", expected.size() > 0);

    // Test nextDoc() through all block types
    NumericDocValues values = docValues(queryMin, queryMax);
    DocValuesSkipper skipper = docValuesSkipper(queryMin, queryMax, true);
    DocIdSetIterator iter =
        TwoPhaseIterator.asDocIdSetIterator(
            DocValuesRangeIterator.forRange(values, skipper, queryMin, queryMax));
    List<Integer> actual = new ArrayList<>();
    for (int d = iter.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = iter.nextDoc()) {
      actual.add(d);
    }
    assertEquals("Must return same docs across all block types", expected, actual);

    // Test advance() across block types:
    // - advance into YES block (docs 0-127)
    // - advance into NO block, should skip to MAYBE (docs 512+)
    // - advance into MAYBE block
    // - advance into YES_IF_PRESENT block (docs 1024+, sparse)
    // - advance past end
    values = docValues(queryMin, queryMax);
    skipper = docValuesSkipper(queryMin, queryMax, true);
    iter =
        TwoPhaseIterator.asDocIdSetIterator(
            DocValuesRangeIterator.forRange(values, skipper, queryMin, queryMax));

    // advance into YES block — should land exactly on target
    assertEquals(50, iter.advance(50));
    // advance within same YES block
    assertEquals(100, iter.advance(100));
    // advance into NO block (128-511) — should skip to first MAYBE match (doc 514)
    int doc = iter.advance(200);
    assertTrue("advance(200) should skip NO blocks, got " + doc, doc >= 512);
    assertTrue("advance(200) result must be in expected", expected.contains(doc));
    // advance into sparse region (YES_IF_PRESENT, docs 1024+)
    doc = iter.advance(1024);
    assertTrue("advance(1024) should find a doc >= 1024, got " + doc, doc >= 1024);
    assertTrue("advance(1024) result must be in expected", expected.contains(doc));
    // advance past all docs
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, iter.advance(2048));

    // Test intoBitSet() across all block types
    values = docValues(queryMin, queryMax);
    skipper = docValuesSkipper(queryMin, queryMax, true);
    DocValuesRangeIterator rangeIter =
        DocValuesRangeIterator.forRange(values, skipper, queryMin, queryMax);
    int firstDoc = rangeIter.approximation().nextDoc();

    FixedBitSet bitSet = new FixedBitSet(2048);
    rangeIter.intoBitSet(2048, bitSet, 0);

    // All expected docs after firstDoc should be set
    for (int expectedDoc : expected) {
      if (expectedDoc > firstDoc) {
        assertTrue("Doc " + expectedDoc + " should be set in bitset", bitSet.get(expectedDoc));
      }
    }
    // No unexpected docs should be set
    for (int d = bitSet.nextSetBit(0);
        d != DocIdSetIterator.NO_MORE_DOCS;
        d = d + 1 < bitSet.length() ? bitSet.nextSetBit(d + 1) : DocIdSetIterator.NO_MORE_DOCS) {
      assertTrue("Doc " + d + " set in bitset but not in expected", expected.contains(d));
    }
  }

  /**
   * With every doc a candidate, {@code applyMask} must confirm exactly the same docs as {@code
   * intoBitSet} would set from scratch.
   */
  public void testApplyMaskWithFullCandidateSetMatchesIntoBitSet() throws Exception {
    long queryMin = 10;
    long queryMax = 20;

    NumericDocValues values = docValues(queryMin, queryMax);
    DocValuesSkipper skipper = docValuesSkipper(queryMin, queryMax, true);
    DocValuesRangeIterator intoBitSetIter =
        DocValuesRangeIterator.forRange(values, skipper, queryMin, queryMax);
    intoBitSetIter.approximation().nextDoc();
    FixedBitSet expected = new FixedBitSet(2048);
    intoBitSetIter.intoBitSet(2048, expected, 0);

    values = docValues(queryMin, queryMax);
    skipper = docValuesSkipper(queryMin, queryMax, true);
    DocValuesRangeIterator maskIter =
        DocValuesRangeIterator.forRange(values, skipper, queryMin, queryMax);
    maskIter.approximation().nextDoc();
    FixedBitSet allCandidates = new FixedBitSet(2048);
    allCandidates.set(0, 2048);
    maskIter.applyMask(2048, allCandidates, 0);

    assertEquals(
        "applyMask with every doc a candidate must match intoBitSet", expected, allCandidates);
  }

  /** {@code applyMask} must only ever clear candidate bits, never set new ones. */
  public void testApplyMaskNeverSetsBitsOutsideInitialCandidates() throws Exception {
    long queryMin = 10;
    long queryMax = 20;

    NumericDocValues values = docValues(queryMin, queryMax);
    DocValuesSkipper skipper = docValuesSkipper(queryMin, queryMax, true);
    DocValuesRangeIterator iter =
        DocValuesRangeIterator.forRange(values, skipper, queryMin, queryMax);
    iter.approximation().nextDoc();

    Random rng = random();
    FixedBitSet candidates = new FixedBitSet(2048);
    FixedBitSet originalCandidates = new FixedBitSet(2048);
    for (int d = 0; d < 2048; d++) {
      if (rng.nextInt(5) == 0) {
        candidates.set(d);
        originalCandidates.set(d);
      }
    }

    iter.applyMask(2048, candidates, 0);

    FixedBitSet extra = candidates.clone();
    extra.andNot(originalCandidates);
    assertEquals(
        "applyMask must never set a bit that wasn't already a candidate", 0, extra.cardinality());
  }

  /**
   * Directly tests applyMask() against a linear scan restricted to a random candidate set, across
   * all block types (YES, NO, MAYBE, YES_IF_PRESENT).
   */
  public void testApplyMaskMatchesLinearScanRestrictedToCandidates() throws Exception {
    long queryMin = 10;
    long queryMax = 20;

    Random rng = random();
    FixedBitSet candidates = new FixedBitSet(2048);
    for (int d = 0; d < 2048; d++) {
      if (rng.nextBoolean()) {
        candidates.set(d);
      }
    }

    NumericDocValues refValues = docValues(queryMin, queryMax);
    FixedBitSet expected = new FixedBitSet(2048);
    for (int d = refValues.nextDoc();
        d != DocIdSetIterator.NO_MORE_DOCS && d < 2048;
        d = refValues.nextDoc()) {
      if (candidates.get(d)
          && refValues.longValue() >= queryMin
          && refValues.longValue() <= queryMax) {
        expected.set(d);
      }
    }

    NumericDocValues values = docValues(queryMin, queryMax);
    DocValuesSkipper skipper = docValuesSkipper(queryMin, queryMax, true);
    DocValuesRangeIterator iter =
        DocValuesRangeIterator.forRange(values, skipper, queryMin, queryMax);
    iter.approximation().nextDoc();

    FixedBitSet actual = candidates.clone();
    iter.applyMask(2048, actual, 0);

    assertEquals("applyMask must match linear scan restricted to candidates", expected, actual);
  }

  /** A YES block means every doc in it matches, so its candidate bits must remain untouched. */
  public void testApplyMaskLeavesYesBlockCandidatesSet() throws Exception {
    long queryMin = 10;
    long queryMax = 20;

    NumericDocValues values = docValues(queryMin, queryMax);
    DocValuesSkipper skipper = docValuesSkipper(queryMin, queryMax, true);
    DocValuesRangeIterator iter =
        DocValuesRangeIterator.forRange(values, skipper, queryMin, queryMax);
    iter.approximation().nextDoc();

    // Docs [0, 128) are dense and within [queryMin, queryMax]: a YES block.
    FixedBitSet candidates = new FixedBitSet(2048);
    candidates.set(0, 128);

    iter.applyMask(128, candidates, 0);

    assertEquals("Every candidate in a YES block must remain set", 128, candidates.cardinality());
  }

  /**
   * A YES_IF_PRESENT block means every doc that has a value matches; applyMask must clear
   * candidates whose doc has no value while leaving present candidates set.
   */
  public void testApplyMaskClearsAbsentCandidatesInYesIfPresentBlock() throws Exception {
    long queryMin = 10;
    long queryMax = 20;

    NumericDocValues values = docValues(queryMin, queryMax);
    DocValuesSkipper skipper = docValuesSkipper(queryMin, queryMax, true);
    DocValuesRangeIterator iter =
        DocValuesRangeIterator.forRange(values, skipper, queryMin, queryMax);
    // Docs [1088, 1152) are sparse (only even docs have a value) but every present value is
    // within [queryMin, queryMax]: a YES_IF_PRESENT block.
    assertEquals(1088, iter.approximation().advance(1088));

    FixedBitSet candidates = new FixedBitSet(2048);
    candidates.set(1088, 1152);

    iter.applyMask(1152, candidates, 0);

    for (int d = 1088; d < 1152; d++) {
      if (d % 2 == 0) {
        assertTrue(
            "Present doc " + d + " in a YES_IF_PRESENT block must remain a candidate",
            candidates.get(d));
      } else {
        assertFalse("Absent doc " + d + " must be cleared", candidates.get(d));
      }
    }
  }

  /**
   * A MAYBE block has no block-level shortcut; applyMask must confirm each candidate doc
   * individually against the predicate.
   */
  public void testApplyMaskConfirmsIndividualCandidatesInMaybeBlock() throws Exception {
    long queryMin = 10;
    long queryMax = 20;

    // Docs [512, 1024) mix in-range and out-of-range values depending on (d/2)%3: a MAYBE block.
    NumericDocValues refValues = docValues(queryMin, queryMax);
    FixedBitSet expected = new FixedBitSet(2048);
    for (int d = 512; d < 1024; d++) {
      refValues.advanceExact(d);
      if (refValues.longValue() >= queryMin && refValues.longValue() <= queryMax) {
        expected.set(d);
      }
    }
    assertTrue(
        "Test requires a genuine mix of matches and non-matches",
        expected.cardinality() > 0 && expected.cardinality() < 512);

    NumericDocValues values = docValues(queryMin, queryMax);
    DocValuesSkipper skipper = docValuesSkipper(queryMin, queryMax, true);
    DocValuesRangeIterator iter =
        DocValuesRangeIterator.forRange(values, skipper, queryMin, queryMax);
    iter.approximation().advance(512);

    FixedBitSet candidates = new FixedBitSet(2048);
    candidates.set(512, 1024);
    iter.applyMask(1024, candidates, 0);

    assertEquals(
        "applyMask must confirm each candidate individually in a MAYBE block",
        expected,
        candidates);
  }

  /**
   * When there are no candidates anywhere in the requested range, applyMask must never touch doc
   * values -- not even to bulk-scan presence for a YES_IF_PRESENT block or evaluate the predicate
   * for a MAYBE block.
   */
  public void testApplyMaskTouchesNoDocValuesWhenNoCandidates() throws Exception {
    long queryMin = 10;
    long queryMax = 20;

    int[] callCount = {0};
    NumericDocValues delegate = docValues(queryMin, queryMax);
    NumericDocValues counting =
        new NumericDocValues() {
          @Override
          public long longValue() throws IOException {
            callCount[0]++;
            return delegate.longValue();
          }

          @Override
          public boolean advanceExact(int target) throws IOException {
            callCount[0]++;
            return delegate.advanceExact(target);
          }

          @Override
          public int docID() {
            return delegate.docID();
          }

          @Override
          public int nextDoc() throws IOException {
            callCount[0]++;
            return delegate.nextDoc();
          }

          @Override
          public int advance(int target) throws IOException {
            callCount[0]++;
            return delegate.advance(target);
          }

          @Override
          public long cost() {
            return delegate.cost();
          }
        };

    DocValuesSkipper skipper = docValuesSkipper(queryMin, queryMax, true);
    DocValuesRangeIterator iter =
        DocValuesRangeIterator.forRange(counting, skipper, queryMin, queryMax);
    // approximation() is the block iterator over the skipper, not `counting`, so positioning it
    // never touches doc values.
    iter.approximation().nextDoc();
    assertEquals(0, callCount[0]);

    FixedBitSet noCandidates = new FixedBitSet(2048); // nothing is a candidate anywhere
    iter.applyMask(2048, noCandidates, 0);

    assertEquals(
        "applyMask must not touch doc values when there are no candidates", 0, callCount[0]);
    assertEquals(0, noCandidates.cardinality());
  }

  /** applyMask must leave the approximation positioned at or beyond upTo, mirroring intoBitSet. */
  public void testApplyMaskAdvancesApproximationToUpTo() throws Exception {
    long queryMin = 10;
    long queryMax = 20;

    NumericDocValues values = docValues(queryMin, queryMax);
    DocValuesSkipper skipper = docValuesSkipper(queryMin, queryMax, true);
    DocValuesRangeIterator iter =
        DocValuesRangeIterator.forRange(values, skipper, queryMin, queryMax);
    DocIdSetIterator approximation = iter.approximation();
    assertEquals(0, approximation.nextDoc());

    FixedBitSet candidates = new FixedBitSet(2048);
    candidates.set(0, 128);
    iter.applyMask(128, candidates, 0);

    assertTrue(
        "approximation must be positioned at or beyond upTo after applyMask, but was "
            + approximation.docID(),
        approximation.docID() >= 128);
  }

  /**
   * Regression test for a leading NO-gap: if the caller has already advanced the approximation past
   * {@code offset} into a {@code [offset, blockStart)} span that doesn't intersect the query range
   * at all -- as DenseConjunctionBulkScorer does when a middle clause's block covering windowBase
   * is a NO block -- applyMask must still clear any candidates sitting in that gap rather than
   * leaving them untouched because no block ever "lands" on them.
   */
  public void testApplyMaskClearsLeadingNoGap() throws Exception {
    long queryMin = 10;
    long queryMax = 20;

    NumericDocValues values = docValues(queryMin, queryMax);
    DocValuesSkipper skipper = docValuesSkipper(queryMin, queryMax, true);
    DocValuesRangeIterator iter =
        DocValuesRangeIterator.forRange(values, skipper, queryMin, queryMax);

    // 300 is inside the [256, 512) NO region (values queryMin-1, entirely out of range), so
    // advancing from there jumps straight past it -- exactly what DenseConjunctionBulkScorer's
    // own advance(windowBase) does for a middle clause.
    int windowBase = 300;
    int landedAt = iter.approximation().advance(windowBase);
    assertTrue(
        "test requires advancing from within a NO region to jump forward", landedAt > windowBase);

    FixedBitSet candidates = new FixedBitSet(2048 - windowBase);
    candidates.set(0, candidates.length()); // every doc from windowBase onward is a candidate

    iter.applyMask(2048, candidates, windowBase);

    for (int d = windowBase; d < landedAt; d++) {
      assertFalse(
          "doc " + d + " is in the leading NO gap and must be cleared",
          candidates.get(d - windowBase));
    }
  }

  /**
   * Regression test for the degenerate case where the approximation is already positioned at or
   * beyond {@code upTo} when applyMask is called, because the whole requested window falls inside a
   * NO gap. The while loop body never runs, so applyMask must still clear every candidate via the
   * trailing gap check rather than leaving the loop having done nothing.
   */
  public void testApplyMaskWhenApproximationStartsAtOrPastUpTo() throws Exception {
    long queryMin = 10;
    long queryMax = 20;

    NumericDocValues values = docValues(queryMin, queryMax);
    DocValuesSkipper skipper = docValuesSkipper(queryMin, queryMax, true);
    DocValuesRangeIterator iter =
        DocValuesRangeIterator.forRange(values, skipper, queryMin, queryMax);

    int windowBase = 300;
    int landedAt = iter.approximation().advance(windowBase);
    int upTo = windowBase + 50;
    assertTrue("test requires the approximation to land beyond a small window", landedAt >= upTo);

    FixedBitSet candidates = new FixedBitSet(upTo - windowBase);
    candidates.set(0, candidates.length());

    iter.applyMask(upTo, candidates, windowBase);

    assertEquals(
        "the whole window is a NO gap; every candidate must be cleared",
        0,
        candidates.cardinality());
  }

  /**
   * Regression test ensuring the MAYBE branch of applyMask keeps confirming via the bulk (SIMD-
   * capable) {@link NumericDocValues#rangeIntoBitSet} path instead of falling back to a scalar
   * per-doc {@code matches()} loop, which would silently lose vectorization once a range clause
   * becomes a trailing clause in a conjunction rather than the sole/leading clause.
   */
  public void testApplyMaskMaybeBlockUsesVectorizedPath() throws Exception {
    long queryMin = 10;
    long queryMax = 20;

    int[] rangeIntoBitSetCalls = {0};
    NumericDocValues delegate = docValues(queryMin, queryMax);
    NumericDocValues counting =
        new NumericDocValues() {
          @Override
          public long longValue() throws IOException {
            return delegate.longValue();
          }

          @Override
          public boolean advanceExact(int target) throws IOException {
            return delegate.advanceExact(target);
          }

          @Override
          public int docID() {
            return delegate.docID();
          }

          @Override
          public int nextDoc() throws IOException {
            return delegate.nextDoc();
          }

          @Override
          public int advance(int target) throws IOException {
            return delegate.advance(target);
          }

          @Override
          public long cost() {
            return delegate.cost();
          }

          @Override
          public void rangeIntoBitSet(
              int fromDoc, int toDoc, long minValue, long maxValue, FixedBitSet bitSet, int offset)
              throws IOException {
            rangeIntoBitSetCalls[0]++;
            super.rangeIntoBitSet(fromDoc, toDoc, minValue, maxValue, bitSet, offset);
          }
        };

    DocValuesSkipper skipper = docValuesSkipper(queryMin, queryMax, true);
    DocValuesRangeIterator iter =
        DocValuesRangeIterator.forRange(counting, skipper, queryMin, queryMax);
    // Docs [512, 1024) mix in-range and out-of-range values: a MAYBE region.
    iter.approximation().advance(512);

    FixedBitSet candidates = new FixedBitSet(2048);
    candidates.set(512, 1024);

    iter.applyMask(1024, candidates, 0);

    assertTrue(
        "MAYBE block confirmation must use the vectorized rangeIntoBitSet path, not a scalar"
            + " per-doc fallback",
        rangeIntoBitSetCalls[0] > 0);
  }

  /**
   * Randomized end-to-end check on a real index: applyMask must match per-doc evaluation restricted
   * to a random candidate set, across random data and range selectivities.
   */
  public void testApplyMaskMatchesPerDocEvaluationOnRealIndex() throws Exception {
    Random rng = random();
    for (int iter = 0; iter < 10; iter++) {
      int numDocs = rng.nextInt(4096, 4096 * 3);
      long maxValue = rng.nextInt(100, 10000);
      long rangeMin = rng.nextLong(0, maxValue / 2);
      long rangeMax = rangeMin + rng.nextLong(1, maxValue / 2);

      try (Directory dir = newDirectory()) {
        IndexWriterConfig iwc = new IndexWriterConfig().setCodec(new Lucene104Codec());
        long[] values = new long[numDocs];
        try (IndexWriter w = new IndexWriter(dir, iwc)) {
          for (int i = 0; i < numDocs; i++) {
            values[i] = rng.nextLong(0, maxValue);
            Document doc = new Document();
            doc.add(NumericDocValuesField.indexedField("val", values[i]));
            w.addDocument(doc);
          }
          w.forceMerge(1);
        }

        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          LeafReaderContext ctx = reader.leaves().get(0);
          DocValuesSkipper skipper = ctx.reader().getDocValuesSkipper("val");
          assertNotNull("Field must have a skip index", skipper);

          FixedBitSet candidates = new FixedBitSet(numDocs);
          for (int d = 0; d < numDocs; d++) {
            if (rng.nextInt(3) != 0) {
              candidates.set(d);
            }
          }

          FixedBitSet expected = candidates.clone();
          for (int d = 0; d < numDocs; d++) {
            if (expected.get(d) && (values[d] < rangeMin || values[d] > rangeMax)) {
              expected.clear(d);
            }
          }

          NumericDocValues dv = ctx.reader().getNumericDocValues("val");
          DocValuesRangeIterator rangeIter =
              DocValuesRangeIterator.forRange(dv, skipper, rangeMin, rangeMax);
          rangeIter.approximation().nextDoc();

          FixedBitSet actual = candidates.clone();
          rangeIter.applyMask(numDocs, actual, 0);

          assertEquals(
              "applyMask must match per-doc evaluation restricted to candidates (numDocs="
                  + numDocs
                  + ", range=["
                  + rangeMin
                  + ","
                  + rangeMax
                  + "])",
              expected,
              actual);
        }
      }
    }
  }

  /**
   * Tests that rangeIntoBitSet (SIMD/fast path) produces the exact same results as per-doc
   * evaluation (slow path) across random data with various densities and range selectivities.
   */
  public void testRangeIntoBitSetMatchesPerDocEvaluation() throws Exception {
    Random rng = random();
    for (int iter = 0; iter < 10; iter++) {
      int numDocs = rng.nextInt(4096, 4096 * 5);
      long maxValue = rng.nextInt(100, 10000);
      // Random range selectivity
      long rangeMin = rng.nextLong(0, maxValue / 2);
      long rangeMax = rangeMin + rng.nextLong(1, maxValue / 2);

      try (Directory dir = newDirectory()) {
        IndexWriterConfig iwc = new IndexWriterConfig().setCodec(new Lucene104Codec());
        try (IndexWriter w = new IndexWriter(dir, iwc)) {
          for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            doc.add(NumericDocValuesField.indexedField("val", rng.nextLong(0, maxValue)));
            w.addDocument(doc);
          }
          w.forceMerge(1);
        }

        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          LeafReaderContext ctx = reader.leaves().get(0);

          // Slow path: per-doc evaluation
          FixedBitSet expected = new FixedBitSet(numDocs);
          NumericDocValues slowDv = ctx.reader().getNumericDocValues("val");
          for (int d = 0; d < numDocs; d++) {
            if (slowDv.advanceExact(d)) {
              long v = slowDv.longValue();
              if (v >= rangeMin && v <= rangeMax) {
                expected.set(d);
              }
            }
          }

          // Fast path: rangeIntoBitSet
          FixedBitSet actual = new FixedBitSet(numDocs);
          NumericDocValues fastDv = ctx.reader().getNumericDocValues("val");
          fastDv.rangeIntoBitSet(0, numDocs, rangeMin, rangeMax, actual, 0);

          assertEquals(
              "rangeIntoBitSet must match per-doc evaluation (numDocs="
                  + numDocs
                  + ", range=["
                  + rangeMin
                  + ","
                  + rangeMax
                  + "])",
              expected,
              actual);
        }
      }
    }
  }

  public void testRangeIntoBitSetMatchesPerDocEvaluationWithDeltaEncoding() throws Exception {
    long delta = 1_000_000L;
    long[] values = rangeValues(DOC_COUNT, doc -> delta + doc);
    assertRangeIntoBitSetMatchesPerDocEvaluation(
        "delta-only encoded range must match decoded evaluation",
        values,
        delta + 127,
        delta + 4097);
  }

  public void testRangeIntoBitSetMatchesPerDocEvaluationWithGcdEncoding() throws Exception {
    long[] values = rangeValues(DOC_COUNT, doc -> doc * 1_000L);
    assertRangeIntoBitSetMatchesPerDocEvaluation(
        "gcd encoded range must match decoded evaluation", values, 123_456L, 4_567_890L);
  }

  public void testRangeIntoBitSetMatchesPerDocEvaluationWithGcdAndDeltaEncoding() throws Exception {
    long delta = 1_000_000L;
    long[] values = rangeValues(DOC_COUNT, doc -> delta + doc * 100L);
    assertRangeIntoBitSetMatchesPerDocEvaluation(
        "gcd+delta encoded range must match decoded evaluation",
        values,
        delta + 123,
        delta + 456_789);
  }

  public void testRangeIntoBitSetMatchesPerDocEvaluationWhenGcdRangeFallsBetweenValues()
      throws Exception {
    long[] values = rangeValues(DOC_COUNT, doc -> 10L + doc * 5L);
    assertRangeIntoBitSetMatchesPerDocEvaluation(
        "gcd encoded gap range must match no docs", values, 11, 14);
  }

  public void testRangeIntoBitSetMatchesPerDocEvaluationWithOpenLowerBound() throws Exception {
    long delta = 1_000_000L;
    long[] values = rangeValues(DOC_COUNT, doc -> delta + doc);
    assertRangeIntoBitSetMatchesPerDocEvaluation(
        "open Long.MIN_VALUE lower bound must saturate and match all docs up to the upper bound",
        values,
        Long.MIN_VALUE,
        delta + 127);
  }

  public void testRangeIntoBitSetMatchesPerDocEvaluationWithOpenUpperBound() throws Exception {
    long delta = 1_000_000L;
    long[] values = rangeValues(DOC_COUNT, doc -> delta + doc * 100L);
    assertRangeIntoBitSetMatchesPerDocEvaluation(
        "open Long.MAX_VALUE upper bound must saturate and match all docs from the lower bound",
        values,
        delta + 50L * 100L,
        Long.MAX_VALUE);
  }

  public void testRangeIntoBitSetMatchesPerDocEvaluationWithBothOpenBounds() throws Exception {
    long delta = 1_000_000L;
    long[] values = rangeValues(DOC_COUNT, doc -> delta + doc * 100L);
    assertRangeIntoBitSetMatchesPerDocEvaluation(
        "fully open range must match every doc", values, Long.MIN_VALUE, Long.MAX_VALUE);
  }

  public void testRangeIntoBitSetMatchesPerDocEvaluationWithNegativeEncodedLowerBound()
      throws Exception {
    // Stored values are non-negative (min == delta), but the query lower bound is below delta so
    // (minValue - delta) is negative even though Math.subtractExact succeeds. This exercises the
    // Math.max(0, encodedMin) clamp.
    long delta = 1_000_000L;
    long[] values = rangeValues(DOC_COUNT, doc -> delta + doc);
    assertRangeIntoBitSetMatchesPerDocEvaluation(
        "negative encoded lower bound must clamp to 0 and match docs up to the upper bound",
        values,
        delta - 50,
        delta + 100);
  }

  public void testRangeIntoBitSetMatchesPerDocEvaluationWithRangeBelowStoredValues()
      throws Exception {
    // The whole query range is below the stored minimum, so encodedMin > encodedMax after the
    // bound transformation and the SIMD path is skipped without iterating.
    long delta = 1_000_000L;
    long[] values = rangeValues(DOC_COUNT, doc -> delta + doc * 100L);
    assertRangeIntoBitSetMatchesPerDocEvaluation(
        "query range below stored values must match no docs", values, delta - 1_000L, delta - 1L);
  }

  public void testRangeIntoBitSetMatchesPerDocEvaluationWithRangeAboveStoredValues()
      throws Exception {
    long delta = 1_000_000L;
    long[] values = rangeValues(DOC_COUNT, doc -> delta + doc * 100L);
    long max = delta + (DOC_COUNT - 1L) * 100L;
    assertRangeIntoBitSetMatchesPerDocEvaluation(
        "query range above stored values must match no docs", values, max + 1L, max + 1_000L);
  }

  public void testRangeIntoBitSetMatchesPerDocEvaluationWithFullGcdDeltaRange() throws Exception {
    long delta = 1_000_000L;
    long[] values = rangeValues(DOC_COUNT, doc -> delta + doc * 100L);
    assertRangeIntoBitSetMatchesPerDocEvaluation(
        "full gcd+delta encoded range must match all docs",
        values,
        delta,
        delta + (DOC_COUNT - 1L) * 100L);
  }

  public void testRangeIntoBitSetMatchesPerDocEvaluationWithSingleGcdDeltaValue() throws Exception {
    long delta = 1_000_000L;
    long value = delta + 123L * 100L;
    long[] values = rangeValues(DOC_COUNT, doc -> delta + doc * 100L);
    assertRangeIntoBitSetMatchesPerDocEvaluation(
        "single gcd+delta encoded value range must match one doc", values, value, value);
  }

  public void testSortedNumericRangeIntoBitSetDenseFixedCardinality() throws Exception {
    doTestSortedNumericRangeIntoBitSet(true, true);
  }

  public void testSortedNumericRangeIntoBitSetDenseVariableCardinality() throws Exception {
    doTestSortedNumericRangeIntoBitSet(true, false);
  }

  public void testSortedNumericRangeIntoBitSetSparseFixedCardinality() throws Exception {
    doTestSortedNumericRangeIntoBitSet(false, true);
  }

  public void testSortedNumericRangeIntoBitSetSparseVariableCardinality() throws Exception {
    doTestSortedNumericRangeIntoBitSet(false, false);
  }

  private void doTestSortedNumericRangeIntoBitSet(boolean dense, boolean fixedCardinality)
      throws Exception {
    doTestSortedNumericRangeIntoBitSet(dense, fixedCardinality, 4);
  }

  public void testSortedNumericRangeIntoBitSetVaryingCardinality() throws Exception {
    for (int cardinality : new int[] {2, 3, 4, 5, 7, 8}) {
      doTestSortedNumericRangeIntoBitSet(true, true, cardinality);
    }
  }

  private void doTestSortedNumericRangeIntoBitSet(
      boolean dense, boolean fixedCardinality, int fixedCardinalityValue) throws Exception {
    int numDocs = 4096 * 2;
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = new IndexWriterConfig().setCodec(new Lucene104Codec());
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (int docID = 0; docID < numDocs; docID++) {
          Document doc = new Document();
          if (dense || docID % 3 != 0) {
            int valueCount = fixedCardinality ? fixedCardinalityValue : 1 + (docID & 3);
            long firstValue = (docID * 13L) % 100;
            for (int i = 0; i < valueCount; i++) {
              doc.add(SortedNumericDocValuesField.indexedField("sn", firstValue + i * 3L));
            }
          }
          w.addDocument(doc);
        }
        w.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReaderContext ctx = reader.leaves().get(0);
        FixedBitSet expected = new FixedBitSet(numDocs);
        var expectedValues = ctx.reader().getSortedNumericDocValues("sn");
        for (int docID = 0; docID < numDocs; docID++) {
          if (expectedValues.advanceExact(docID)) {
            for (int i = 0, count = expectedValues.docValueCount(); i < count; i++) {
              long value = expectedValues.nextValue();
              if (value >= 20) {
                if (value <= 40) {
                  expected.set(docID);
                }
                break;
              }
            }
          }
        }

        FixedBitSet actual = new FixedBitSet(numDocs);
        ctx.reader().getSortedNumericDocValues("sn").rangeIntoBitSet(0, numDocs, 20, 40, actual, 0);
        assertEquals(expected, actual);
      }
    }
  }

  public void testSingletonDelegatesRangeIntoBitSet() throws Exception {
    int maxDoc = 100;
    boolean[] delegated = {false};
    NumericDocValues spy =
        new NumericDocValues() {
          private int doc = -1;

          @Override
          public long longValue() {
            return doc;
          }

          @Override
          public boolean advanceExact(int target) {
            doc = target;
            return true;
          }

          @Override
          public int docID() {
            return doc;
          }

          @Override
          public int nextDoc() {
            return ++doc < maxDoc ? doc : NO_MORE_DOCS;
          }

          @Override
          public int advance(int target) {
            doc = target;
            return doc < maxDoc ? doc : NO_MORE_DOCS;
          }

          @Override
          public long cost() {
            return maxDoc;
          }

          @Override
          public void rangeIntoBitSet(
              int fromDoc, int toDoc, long minValue, long maxValue, FixedBitSet bitSet, int offset)
              throws IOException {
            delegated[0] = true;
            super.rangeIntoBitSet(fromDoc, toDoc, minValue, maxValue, bitSet, offset);
          }
        };

    SortedNumericDocValues singleton = DocValues.singleton(spy);
    FixedBitSet bitSet = new FixedBitSet(maxDoc);
    singleton.rangeIntoBitSet(0, maxDoc, 20, 40, bitSet, 0);

    assertTrue("Expected delegation to NumericDocValues.rangeIntoBitSet", delegated[0]);
    assertTrue("Expected some bits set", bitSet.cardinality() > 0);
  }

  private static long[] rangeValues(int numDocs, LongUnaryOperator valueFunction) {
    long[] values = new long[numDocs];
    for (int i = 0; i < numDocs; i++) {
      values[i] = valueFunction.applyAsLong(i);
    }
    return values;
  }

  private void assertRangeIntoBitSetMatchesPerDocEvaluation(
      String message, long[] values, long rangeMin, long rangeMax) throws Exception {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = new IndexWriterConfig().setCodec(new Lucene104Codec());
      try (IndexWriter w = new IndexWriter(dir, iwc)) {
        for (long value : values) {
          Document doc = new Document();
          doc.add(NumericDocValuesField.indexedField("val", value));
          w.addDocument(doc);
        }
        w.forceMerge(1);
      }

      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReaderContext ctx = reader.leaves().get(0);
        FixedBitSet expected = new FixedBitSet(values.length);
        NumericDocValues slowDv = ctx.reader().getNumericDocValues("val");
        for (int d = 0; d < values.length; d++) {
          if (slowDv.advanceExact(d)) {
            long value = slowDv.longValue();
            if (value >= rangeMin && value <= rangeMax) {
              expected.set(d);
            }
          }
        }

        FixedBitSet actual = new FixedBitSet(values.length);
        NumericDocValues fastDv = ctx.reader().getNumericDocValues("val");
        fastDv.rangeIntoBitSet(0, values.length, rangeMin, rangeMax, actual, 0);

        assertEquals(message, expected, actual);
      }
    }
  }
}
