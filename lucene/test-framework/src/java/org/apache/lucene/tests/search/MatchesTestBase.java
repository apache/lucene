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

package org.apache.lucene.tests.search;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.NamedMatches;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

/**
 * Base class for tests checking the {@link Weight#matches(LeafReaderContext, int)} implementations
 */
public abstract class MatchesTestBase extends LuceneTestCase {

  /** @return an array of documents to be indexed */
  protected abstract String[] getDocuments();

  protected IndexSearcher searcher;
  protected Directory directory;
  protected IndexReader reader = null;

  protected static final String FIELD_WITH_OFFSETS = "field_offsets";
  protected static final String FIELD_NO_OFFSETS = "field_no_offsets";
  protected static final String FIELD_DOCS_ONLY = "field_docs_only";
  protected static final String FIELD_FREQS = "field_freqs";
  protected static final String FIELD_POINT = "field_point";

  private static final FieldType OFFSETS = new FieldType(TextField.TYPE_STORED);

  static {
    OFFSETS.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
  }

  private static final FieldType DOCS = new FieldType(TextField.TYPE_STORED);

  static {
    DOCS.setIndexOptions(IndexOptions.DOCS);
  }

  private static final FieldType DOCS_AND_FREQS = new FieldType(TextField.TYPE_STORED);

  static {
    DOCS_AND_FREQS.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
  }

  @Override
  public void tearDown() throws Exception {
    reader.close();
    directory.close();
    super.tearDown();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    RandomIndexWriter writer =
        new RandomIndexWriter(
            random(),
            directory,
            newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));
    String[] docFields = getDocuments();
    for (int i = 0; i < docFields.length; i++) {
      Document doc = new Document();
      doc.add(newField(FIELD_WITH_OFFSETS, docFields[i], OFFSETS));
      doc.add(newField(FIELD_NO_OFFSETS, docFields[i], TextField.TYPE_STORED));
      doc.add(newField(FIELD_DOCS_ONLY, docFields[i], DOCS));
      doc.add(newField(FIELD_FREQS, docFields[i], DOCS_AND_FREQS));
      doc.add(new IntPoint(FIELD_POINT, 10));
      doc.add(new NumericDocValuesField(FIELD_POINT, 10));
      doc.add(new NumericDocValuesField("id", i));
      doc.add(newField("id", Integer.toString(i), TextField.TYPE_STORED));
      writer.addDocument(doc);
    }
    writer.forceMerge(1);
    reader = writer.getReader();
    writer.close();
    searcher = newSearcher(getOnlyLeafReader(reader));
  }

  /**
   * For a given query and field, check that expected matches are retrieved
   *
   * @param q the query
   * @param field the field to pull matches from
   * @param expected an array of arrays of ints; for each entry, the first int is the expected
   *     docid, followed by pairs of start and end positions
   */
  protected void checkMatches(Query q, String field, int[][] expected) throws IOException {
    Weight w = searcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE, 1);
    for (int i = 0; i < expected.length; i++) {
      List<LeafReaderContext> leafContexts = searcher.getLeafContexts();
      LeafReaderContext ctx = leafContexts.get(ReaderUtil.subIndex(expected[i][0], leafContexts));
      int doc = expected[i][0] - ctx.docBase;
      Matches matches = w.matches(ctx, doc);
      if (matches == null) {
        assertEquals(expected[i].length, 1);
        continue;
      }
      MatchesIterator it = matches.getMatches(field);
      if (expected[i].length == 1) {
        assertNull(it);
        continue;
      }
      checkFieldMatches(it, expected[i]);
      checkFieldMatches(matches.getMatches(field), expected[i]); // test multiple calls
    }
  }

  /**
   * For a given query and field, check that the expected numbers of query labels were found
   *
   * @param q the query
   * @param field the field to pull matches from
   * @param expected an array of expected label counts, one entry per document
   */
  protected void checkLabelCount(Query q, String field, int[] expected) throws IOException {
    Weight w = searcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE, 1);
    for (int i = 0; i < expected.length; i++) {
      List<LeafReaderContext> leafContexts = searcher.getLeafContexts();
      LeafReaderContext ctx = leafContexts.get(ReaderUtil.subIndex(i, leafContexts));
      int doc = i - ctx.docBase;
      Matches matches = w.matches(ctx, doc);
      if (matches == null) {
        assertEquals("Expected to get matches on document " + i, 0, expected[i]);
        continue;
      }
      MatchesIterator it = matches.getMatches(field);
      if (expected[i] == 0) {
        assertNull(it);
        continue;
      } else {
        assertNotNull(it);
      }
      IdentityHashMap<Query, Integer> labels = new IdentityHashMap<>();
      while (it.next()) {
        labels.put(it.getQuery(), 1);
      }
      assertEquals(expected[i], labels.size());
    }
  }

  /**
   * Given a MatchesIterator, check that it has the expected set of start and end positions
   *
   * @param it an iterator
   * @param expected an array of expected start and end pairs and start and end offsets; the entry
   *     at position 0 is ignored
   */
  protected void checkFieldMatches(MatchesIterator it, int[] expected) throws IOException {
    int pos = 1;
    while (it.next()) {
      // System.out.println(expected[i][pos] + "->" + expected[i][pos + 1] + "[" + expected[i][pos +
      // 2] + "->" + expected[i][pos + 3] + "]");
      assertEquals("Wrong start position", expected[pos], it.startPosition());
      assertEquals("Wrong end position", expected[pos + 1], it.endPosition());
      assertEquals("Wrong start offset", expected[pos + 2], it.startOffset());
      assertEquals("Wrong end offset", expected[pos + 3], it.endOffset());
      pos += 4;
    }
    assertEquals(expected.length, pos);
  }

  /**
   * Given a query and field, check that matches are returned from expected documents and that they
   * return -1 for start and end positions
   *
   * @param q the query
   * @param field the field to pull matches from
   * @param expected an array of booleans indicating if matches are expected from each document
   */
  protected void checkNoPositionsMatches(Query q, String field, boolean[] expected)
      throws IOException {
    Weight w = searcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE, 1);
    for (int i = 0; i < expected.length; i++) {
      List<LeafReaderContext> leafContexts = searcher.getLeafContexts();
      LeafReaderContext ctx = leafContexts.get(ReaderUtil.subIndex(i, leafContexts));
      int doc = i - ctx.docBase;
      Matches matches = w.matches(ctx, doc);
      if (expected[i]) {
        MatchesIterator mi = matches.getMatches(field);
        assertTrue(mi.next());
        assertEquals(-1, mi.startPosition());
        while (mi.next()) {
          assertEquals(-1, mi.startPosition());
        }
      } else {
        assertNull(matches);
      }
    }
  }

  /**
   * Given a query, check that matches contain the expected NamedQuery wrapper names
   *
   * @param q the query
   * @param expectedNames an array of arrays of Strings; for each document, an array of expected
   *     query names that match
   */
  protected void checkSubMatches(Query q, String[][] expectedNames) throws IOException {
    Weight w = searcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE_NO_SCORES, 1);
    for (int i = 0; i < expectedNames.length; i++) {
      List<LeafReaderContext> leafContexts = searcher.getLeafContexts();
      LeafReaderContext ctx = leafContexts.get(ReaderUtil.subIndex(i, leafContexts));
      int doc = i - ctx.docBase;
      Matches matches = w.matches(ctx, doc);
      if (matches == null) {
        assertEquals("Expected to get no matches on document " + i, 0, expectedNames[i].length);
        continue;
      }
      Set<String> expectedQueries = new HashSet<>(Arrays.asList(expectedNames[i]));
      Set<String> actualQueries =
          NamedMatches.findNamedMatches(matches).stream()
              .map(NamedMatches::getName)
              .collect(Collectors.toSet());

      Set<String> unexpected = new HashSet<>(actualQueries);
      unexpected.removeAll(expectedQueries);
      assertEquals("Unexpected matching leaf queries: " + unexpected, 0, unexpected.size());
      Set<String> missing = new HashSet<>(expectedQueries);
      missing.removeAll(actualQueries);
      assertEquals("Missing matching leaf queries: " + missing, 0, missing.size());
    }
  }

  /**
   * Assert that query matches from a field are all leaf matches and do not contain sub matches
   *
   * @param q the query
   * @param field the field
   */
  protected void assertIsLeafMatch(Query q, String field) throws IOException {
    Weight w = searcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE, 1);
    for (int i = 0; i < searcher.getIndexReader().maxDoc(); i++) {
      List<LeafReaderContext> leafContexts = searcher.getLeafContexts();
      LeafReaderContext ctx = leafContexts.get(ReaderUtil.subIndex(i, leafContexts));
      int doc = i - ctx.docBase;
      Matches matches = w.matches(ctx, doc);
      if (matches == null) {
        return;
      }
      MatchesIterator mi = matches.getMatches(field);
      if (mi == null) {
        return;
      }
      while (mi.next()) {
        assertNull(mi.getSubMatches());
      }
    }
  }

  /**
   * For a query and field, check that each document's submatches conform to an expected TermMatch
   *
   * @param q the query
   * @param field the field to pull matches for
   * @param expected an array per doc of arrays per match of an array of expected submatches
   */
  protected void checkTermMatches(Query q, String field, TermMatch[][][] expected)
      throws IOException {
    Weight w = searcher.createWeight(searcher.rewrite(q), ScoreMode.COMPLETE, 1);
    for (int i = 0; i < expected.length; i++) {
      List<LeafReaderContext> leafContexts = searcher.getLeafContexts();
      LeafReaderContext ctx = leafContexts.get(ReaderUtil.subIndex(i, leafContexts));
      int doc = i - ctx.docBase;
      Matches matches = w.matches(ctx, doc);
      if (matches == null) {
        assertEquals(expected[i].length, 0);
        continue;
      }
      MatchesIterator it = matches.getMatches(field);
      if (expected[i].length == 0) {
        assertNull(it);
        continue;
      }
      checkTerms(expected[i], it);
    }
  }

  private void checkTerms(TermMatch[][] expected, MatchesIterator it) throws IOException {
    int upTo = 0;
    while (it.next()) {
      Set<TermMatch> expectedMatches = new HashSet<>(Arrays.asList(expected[upTo]));
      MatchesIterator submatches = it.getSubMatches();
      while (submatches.next()) {
        TermMatch tm =
            new TermMatch(
                submatches.startPosition(), submatches.startOffset(), submatches.endOffset());
        if (expectedMatches.remove(tm) == false) {
          fail("Unexpected term match: " + tm);
        }
      }
      if (expectedMatches.size() != 0) {
        fail(
            "Missing term matches: "
                + expectedMatches.stream().map(Object::toString).collect(Collectors.joining(", ")));
      }
      upTo++;
    }
    if (upTo < expected.length - 1) {
      fail("Missing expected match");
    }
  }

  /** Encapsulates a term position, start and end offset */
  protected static class TermMatch {

    /** The position */
    public final int position;

    /** The start offset */
    public final int startOffset;

    /** The end offset */
    public final int endOffset;

    /** Create a new TermMatch */
    public TermMatch(int position, int startOffset, int endOffset) {
      this.position = position;
      this.startOffset = startOffset;
      this.endOffset = endOffset;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      TermMatch termMatch = (TermMatch) o;
      return position == termMatch.position
          && startOffset == termMatch.startOffset
          && endOffset == termMatch.endOffset;
    }

    @Override
    public int hashCode() {
      return Objects.hash(position, startOffset, endOffset);
    }

    @Override
    public String toString() {
      return position + "[" + startOffset + "->" + endOffset + "]";
    }
  }
}
