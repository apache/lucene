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
package org.apache.lucene.document;

import java.io.IOException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;

public class TestSortedNumericDocValuesRangeQuery extends LuceneTestCase {

  public void testRangeQueryDoesNotUseMismatchedDoublePointStats() throws IOException {
    assertNotEquals(
        "test requires encodings that actually differ",
        Double.doubleToLongBits(-1.5),
        NumericUtils.doubleToSortableLong(-1.5));
    assertHitsForIeeeBitsAndPoints(-1.5, false);
    assertHitsForIeeeBitsAndPoints(-1.5, true);
  }

  public void testRangeQueryDoesNotUseMismatchedFloatPointStats() throws IOException {
    assertNotEquals(
        "test requires encodings that actually differ",
        Float.floatToIntBits(-1.5f),
        NumericUtils.floatToSortableInt(-1.5f));
    assertHitsForIeeeFloatBitsAndPoints(-1.5f, false);
    assertHitsForIeeeFloatBitsAndPoints(-1.5f, true);
  }

  public void testIndexOrDocValuesQueryDoesNotDropHitsOnMismatchedDoubleEncodings()
      throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      // An empty document keeps the points query from rewriting to MatchAllDocsQuery,
      // which would hide a MatchNoDocs rewrite of the doc-values side.
      final double value = -2.25;
      final Document doc = new Document();
      doc.add(new DoublePoint("field", value));
      doc.add(new NumericDocValuesField("field", Double.doubleToLongBits(value)));
      w.addDocument(doc);
      w.addDocument(new Document());
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        final IndexSearcher searcher = newSearcher(reader);
        final long bits = Double.doubleToLongBits(value);
        final Query query =
            new IndexOrDocValuesQuery(
                DoublePoint.newRangeQuery("field", value, value),
                NumericDocValuesField.newSlowRangeQuery("field", bits, bits));
        assertEquals(1, searcher.count(query));
        assertFalse(searcher.rewrite(query) instanceof MatchNoDocsQuery);
      }
    }
  }

  private void assertHitsForIeeeBitsAndPoints(double value, boolean skipIndex) throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      final Document doc = new Document();
      doc.add(new DoublePoint("field", value));
      final long bits = Double.doubleToLongBits(value);
      if (skipIndex) {
        doc.add(NumericDocValuesField.indexedField("field", bits));
      } else {
        doc.add(new NumericDocValuesField("field", bits));
      }
      w.addDocument(doc);
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        final IndexSearcher searcher = newSearcher(reader);
        final Query query = NumericDocValuesField.newSlowRangeQuery("field", bits, bits);
        assertEquals("skipIndex=" + skipIndex, 1, searcher.count(query));
        assertFalse("skipIndex=" + skipIndex, searcher.rewrite(query) instanceof MatchNoDocsQuery);
      }
    }
  }

  private void assertHitsForIeeeFloatBitsAndPoints(float value, boolean skipIndex)
      throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      final Document doc = new Document();
      doc.add(new FloatPoint("field", value));
      final int bits = Float.floatToIntBits(value);
      if (skipIndex) {
        doc.add(NumericDocValuesField.indexedField("field", bits));
      } else {
        doc.add(new NumericDocValuesField("field", bits));
      }
      w.addDocument(doc);
      w.commit();
      try (IndexReader reader = DirectoryReader.open(w)) {
        final IndexSearcher searcher = newSearcher(reader);
        final Query query = NumericDocValuesField.newSlowRangeQuery("field", bits, bits);
        assertEquals("skipIndex=" + skipIndex, 1, searcher.count(query));
        assertFalse("skipIndex=" + skipIndex, searcher.rewrite(query) instanceof MatchNoDocsQuery);
      }
    }
  }
}
