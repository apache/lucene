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
package org.apache.lucene.search.comparators;

import java.io.IOException;
import java.util.function.IntFunction;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;

/**
 * Tests that the adaptive skipping logic in TermOrdValComparator's SkipperBasedCompetitiveState
 * preserves correctness across data distributions, and that it disables skipping when the skip
 * index is ineffective.
 */
public class TestTermOrdValComparatorAdaptiveSkipping extends LuceneTestCase {

  private static final String SORT_FIELD_SKIP = "sort.skip";
  private static final String SORT_FIELD_NO_SKIP = "sort.noskip";
  private static final String QUERY_FIELD = "match";
  private static final String QUERY_VALUE = "yes";
  private static final int NUM_DOCS = 200_000;
  private static final int NUM_HITS = 10;
  private static final int TOTAL_HITS_THRESHOLD = 1;

  /**
   * For interleaved data (alternating values uncorrelated with doc order), the adaptive logic may
   * disable skipping. Verify that top-N results are identical to the non-skip baseline.
   */
  public void testResultsCorrectForInterleavedData() throws Exception {
    BytesRef a = new BytesRef("a");
    BytesRef z = new BytesRef("z");
    try (Directory dir = newDirectory()) {
      buildIndex(dir, i -> (i & 1) == 0 ? a : z);
      try (IndexReader reader = DirectoryReader.open(dir)) {
        assertTopNMatch(new IndexSearcher(reader));
      }
    }
  }

  /**
   * For clustered data (values sorted with doc order), skipping should remain active and prune
   * non-competitive blocks. Verify results are correct AND that totalHits is significantly lower
   * with skipping, showing that the adaptive logic did not incorrectly disable.
   */
  public void testSkippingEffectiveForClusteredData() throws Exception {
    BytesRef a = new BytesRef("a");
    BytesRef z = new BytesRef("z");
    try (Directory dir = newDirectory()) {
      buildIndex(dir, i -> i < NUM_DOCS / 2 ? a : z);
      try (IndexReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);

        assertTopNMatch(searcher);

        TopFieldDocs withSkip = searchSort(searcher, SORT_FIELD_SKIP);
        TopFieldDocs withoutSkip = searchSort(searcher, SORT_FIELD_NO_SKIP);

        assertTrue(
            "Expected skip-enabled totalHits ("
                + withSkip.totalHits.value()
                + ") to be significantly lower than non-skip ("
                + withoutSkip.totalHits.value()
                + "), proving skipping was not incorrectly disabled",
            withSkip.totalHits.value() < withoutSkip.totalHits.value() * 3 / 4);
      }
    }
  }

  /**
   * For random data, results must still be correct regardless of whether skipping is active or
   * adaptively disabled.
   */
  public void testResultsCorrectForRandomData() throws Exception {
    BytesRef[] values = new BytesRef[26];
    for (int i = 0; i < 26; i++) {
      values[i] = new BytesRef(Character.toString((char) ('a' + i)));
    }
    try (Directory dir = newDirectory()) {
      buildIndex(dir, _ -> values[random().nextInt(values.length)]);
      try (IndexReader reader = DirectoryReader.open(dir)) {
        assertTopNMatch(new IndexSearcher(reader));
      }
    }
  }

  /**
   * Directly exercises the comparator with interleaved data and verifies that adaptive disabling
   * fires via {@link TermOrdValComparator.TermOrdValLeafComparator#isSkippingDisabled()}.
   */
  public void testAdaptiveDisablingFiresForInterleavedData() throws Exception {
    BytesRef a = new BytesRef("a");
    BytesRef z = new BytesRef("z");
    try (Directory dir = newDirectory()) {
      buildIndex(dir, i -> (i & 1) == 0 ? a : z);
      try (IndexReader reader = DirectoryReader.open(dir)) {
        assertEquals(1, reader.leaves().size());
        LeafReaderContext leafContext = reader.leaves().get(0);
        int maxDoc = leafContext.reader().maxDoc();

        TermOrdValComparator comparator =
            new TermOrdValComparator(1, SORT_FIELD_SKIP, false, false, Pruning.GREATER_THAN);
        LeafFieldComparator leafComp = comparator.getLeafComparator(leafContext);
        assertTrue(leafComp instanceof TermOrdValComparator.TermOrdValLeafComparator);
        var typedLeafComp = (TermOrdValComparator.TermOrdValLeafComparator) leafComp;

        leafComp.copy(0, 0);
        leafComp.setBottom(0);
        leafComp.setHitsThresholdReached();

        DocIdSetIterator competitiveIter = leafComp.competitiveIterator();
        assumeTrue(
            "Competitive iterator not available (codec may not support skip index)",
            competitiveIter != null);

        assertFalse(
            "Skipping should not be disabled before iteration", typedLeafComp.isSkippingDisabled());

        // Advance through the entire segment
        int doc = 0;
        while (doc < maxDoc) {
          int next = competitiveIter.advance(doc);
          if (next == DocIdSetIterator.NO_MORE_DOCS) break;
          doc = next + 1;
        }

        assertTrue(
            "Skipping should be adaptively disabled after traversing interleaved data",
            typedLeafComp.isSkippingDisabled());
      }
    }
  }

  /**
   * Directly exercises the comparator with clustered data and verifies that adaptive disabling does
   * NOT fire, because the skip iterator successfully skips non-competitive blocks.
   */
  public void testAdaptiveDisablingDoesNotFireForClusteredData() throws Exception {
    BytesRef a = new BytesRef("a");
    BytesRef z = new BytesRef("z");
    try (Directory dir = newDirectory()) {
      buildIndex(dir, i -> i < NUM_DOCS / 2 ? a : z);
      try (IndexReader reader = DirectoryReader.open(dir)) {
        assertEquals(1, reader.leaves().size());
        LeafReaderContext leafContext = reader.leaves().get(0);
        int maxDoc = leafContext.reader().maxDoc();

        TermOrdValComparator comparator =
            new TermOrdValComparator(1, SORT_FIELD_SKIP, false, false, Pruning.GREATER_THAN);
        LeafFieldComparator leafComp = comparator.getLeafComparator(leafContext);
        assertTrue(leafComp instanceof TermOrdValComparator.TermOrdValLeafComparator);
        var typedLeafComp = (TermOrdValComparator.TermOrdValLeafComparator) leafComp;

        leafComp.copy(0, 0);
        leafComp.setBottom(0);
        leafComp.setHitsThresholdReached();

        DocIdSetIterator competitiveIter = leafComp.competitiveIterator();
        assumeTrue(
            "Competitive iterator not available (codec may not support skip index)",
            competitiveIter != null);

        // Advance through the entire segment
        int doc = 0;
        while (doc < maxDoc) {
          int next = competitiveIter.advance(doc);
          if (next == DocIdSetIterator.NO_MORE_DOCS) break;
          doc = next + 1;
        }

        assertFalse(
            "Skipping should remain active for clustered data", typedLeafComp.isSkippingDisabled());
      }
    }
  }

  private void buildIndex(Directory dir, IntFunction<BytesRef> valueSupplier) throws IOException {
    IndexWriterConfig config = newIndexWriterConfig();
    // Use a LogMergePolicy to ensure segments are merged in order, preserving the document
    // layout. Random merge policies like MockRandomMergePolicy can reorder segments during
    // forceMerge, which would interleave clustered values and break test assertions.
    config.setMergePolicy(newLogMergePolicy());
    try (IndexWriter writer = new IndexWriter(dir, config)) {
      for (int i = 0; i < NUM_DOCS; i++) {
        Document doc = new Document();
        doc.add(new StringField(QUERY_FIELD, QUERY_VALUE, Field.Store.NO));
        BytesRef value = valueSupplier.apply(i);
        doc.add(new SortedDocValuesField(SORT_FIELD_NO_SKIP, value));
        doc.add(SortedDocValuesField.indexedField(SORT_FIELD_SKIP, value));
        writer.addDocument(doc);
      }
      writer.forceMerge(1);
    }
  }

  private void assertTopNMatch(IndexSearcher searcher) throws IOException {
    TopFieldDocs withSkip = searchSort(searcher, SORT_FIELD_SKIP);
    TopFieldDocs withoutSkip = searchSort(searcher, SORT_FIELD_NO_SKIP);

    assertEquals(
        "Top-N result count should match", withSkip.scoreDocs.length, withoutSkip.scoreDocs.length);
    for (int i = 0; i < withSkip.scoreDocs.length; i++) {
      FieldDoc skipDoc = (FieldDoc) withSkip.scoreDocs[i];
      FieldDoc noSkipDoc = (FieldDoc) withoutSkip.scoreDocs[i];
      assertEquals(
          "Sort value at position " + i + " should match", skipDoc.fields[0], noSkipDoc.fields[0]);
    }
  }

  private TopFieldDocs searchSort(IndexSearcher searcher, String field) throws IOException {
    Query query = new TermQuery(new Term(QUERY_FIELD, QUERY_VALUE));
    SortField sortField = new SortField(field, SortField.Type.STRING);
    Sort sort = new Sort(sortField, SortField.FIELD_SCORE);
    TopFieldCollectorManager manager =
        new TopFieldCollectorManager(sort, NUM_HITS, null, TOTAL_HITS_THRESHOLD);
    return searcher.search(query, manager);
  }
}
