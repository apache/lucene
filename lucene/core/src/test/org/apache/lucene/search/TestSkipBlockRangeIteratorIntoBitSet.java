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

import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.FixedBitSet;

/**
 * Tests correctness of {@link BatchDocValuesRangeIterator} and its {@code intoBitSet()} path,
 * including YES, YES_IF_PRESENT, and MAYBE block states.
 */
public class TestSkipBlockRangeIteratorIntoBitSet extends LuceneTestCase {

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
   * Verifies that SortedNumericDocValuesRangeQuery wires BatchDocValuesRangeIterator when the field
   * has a skip index and is single-valued.
   */
  public void testBatchIteratorIsWired() throws Exception {
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
    assertTrue(
        "Range query on single-valued field with skip index must use BatchDocValuesRangeIterator"
            + " but got: "
            + iter.getClass().getSimpleName(),
        iter instanceof BatchDocValuesRangeIterator);
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

  public void testIntoBitSetMatchesBruteForce() throws Exception {
    LeafReaderContext ctx = reader.leaves().get(0);
    int maxDoc = ctx.reader().maxDoc();
    int windowSize = DenseConjunctionBulkScorer.WINDOW_SIZE; // 4096

    // Scan all docs manually and record which match range [20, 40]
    FixedBitSet expected = new FixedBitSet(windowSize);
    NumericDocValues refValues = ctx.reader().getNumericDocValues("age");
    for (int d = 0; d < Math.min(maxDoc, windowSize); d++) {
      if (refValues.advanceExact(d) && refValues.longValue() >= 20 && refValues.longValue() <= 40) {
        expected.set(d);
      }
    }

    // BatchDocValuesRangeIterator path
    DocValuesSkipper skipper = ctx.reader().getDocValuesSkipper("age");
    NumericDocValues dv = ctx.reader().getNumericDocValues("age");
    assertNotNull("Field must have a skip index", skipper);

    BatchDocValuesRangeIterator iter = new BatchDocValuesRangeIterator(dv, skipper, 20, 40);
    iter.nextDoc(); // position to first matching doc

    FixedBitSet actual = new FixedBitSet(windowSize);
    iter.intoBitSet(Math.min(maxDoc, windowSize), actual, 0);

    assertEquals(
        "intoBitSet must set exactly the same bits as linear scan",
        expected.cardinality(),
        actual.cardinality());
    // Verify bit-by-bit
    FixedBitSet diff = expected.clone();
    diff.xor(actual);
    assertEquals("No bits should differ", 0, diff.cardinality());
  }

  public void testIntoBitSetAllMatchRange() throws Exception {
    LeafReaderContext ctx = reader.leaves().get(0);
    int maxDoc = ctx.reader().maxDoc();

    DocValuesSkipper skipper = ctx.reader().getDocValuesSkipper("age");
    NumericDocValues dv = ctx.reader().getNumericDocValues("age");
    assertNotNull(skipper);

    // Range [0, 99] covers all values — all docs should match
    BatchDocValuesRangeIterator iter = new BatchDocValuesRangeIterator(dv, skipper, 0, 99);
    iter.nextDoc();

    FixedBitSet bitSet = new FixedBitSet(maxDoc);
    iter.intoBitSet(maxDoc, bitSet, 0);

    assertEquals("All docs should match range [0,99]", maxDoc, bitSet.cardinality());
  }

  public void testIntoBitSetNoMatchRange() throws Exception {
    LeafReaderContext ctx = reader.leaves().get(0);
    int maxDoc = ctx.reader().maxDoc();

    DocValuesSkipper skipper = ctx.reader().getDocValuesSkipper("age");
    NumericDocValues dv = ctx.reader().getNumericDocValues("age");
    assertNotNull(skipper);

    // Range [200, 300] — no values exist in this range
    BatchDocValuesRangeIterator iter = new BatchDocValuesRangeIterator(dv, skipper, 200, 300);
    iter.nextDoc();

    FixedBitSet bitSet = new FixedBitSet(maxDoc);
    iter.intoBitSet(maxDoc, bitSet, 0);

    assertEquals("No docs should match out-of-range query", 0, bitSet.cardinality());
  }

  public void testIntoBitSetSparseField() throws Exception {
    try (Directory sparseDir = newDirectory()) {
      IndexWriterConfig iwc = new IndexWriterConfig().setCodec(new Lucene104Codec());
      IndexWriter w = new IndexWriter(sparseDir, iwc);
      int numDocs = 1000;
      List<Integer> expectedDocs = new ArrayList<>();
      for (int i = 0; i < numDocs; i++) {
        Document doc = new Document();
        if (i % 2 == 0) { // only even docs have the field
          long val = i % 100;
          doc.add(NumericDocValuesField.indexedField("sparse", val));
          if (val >= 20 && val <= 40) expectedDocs.add(i);
        }
        w.addDocument(doc);
      }
      w.forceMerge(1);
      try (DirectoryReader sparseReader = DirectoryReader.open(w)) {
        w.close();
        LeafReaderContext ctx = sparseReader.leaves().get(0);

        DocValuesSkipper skipper = ctx.reader().getDocValuesSkipper("sparse");
        NumericDocValues dv = ctx.reader().getNumericDocValues("sparse");
        if (skipper == null) return; // codec doesn't support skip index

        BatchDocValuesRangeIterator iter = new BatchDocValuesRangeIterator(dv, skipper, 20, 40);
        iter.nextDoc();

        FixedBitSet bitSet = new FixedBitSet(numDocs);
        iter.intoBitSet(numDocs, bitSet, 0);

        // Verify count matches expected
        assertEquals(
            "Sparse field intoBitSet must match expected count",
            expectedDocs.size(),
            bitSet.cardinality());

        // Verify no odd docs (which have no value) are set
        for (int i = 1; i < numDocs; i += 2) {
          assertFalse("Odd doc " + i + " has no value and must not be set", bitSet.get(i));
        }
      }
    }
  }

  public void testAdvanceCorrectness() throws Exception {
    LeafReaderContext ctx = reader.leaves().get(0);

    // Collect all matching doc IDs beforehand
    List<Integer> expected = new ArrayList<>();
    NumericDocValues refValues = ctx.reader().getNumericDocValues("age");
    for (int d = 0; d < ctx.reader().maxDoc(); d++) {
      if (refValues.advanceExact(d) && refValues.longValue() >= 20 && refValues.longValue() <= 40) {
        expected.add(d);
      }
    }

    DocValuesSkipper skipper = ctx.reader().getDocValuesSkipper("age");
    NumericDocValues dv = ctx.reader().getNumericDocValues("age");
    assertNotNull(skipper);

    BatchDocValuesRangeIterator iter = new BatchDocValuesRangeIterator(dv, skipper, 20, 40);
    List<Integer> actual = new ArrayList<>();
    for (int d = iter.nextDoc(); d != DocIdSetIterator.NO_MORE_DOCS; d = iter.nextDoc()) {
      actual.add(d);
    }

    assertEquals("advance() must return same docs as expected", expected, actual);
  }
}
