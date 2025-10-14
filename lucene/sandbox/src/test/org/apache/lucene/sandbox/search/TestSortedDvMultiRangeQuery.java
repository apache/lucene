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

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.sandbox.document.LongPointMultiRangeBuilder;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

public class TestSortedDvMultiRangeQuery extends LuceneTestCase {
  private Codec getCodec() {
    // small interval size to test with many intervals
    return TestUtil.alwaysDocValuesFormat(new Lucene90DocValuesFormat(random().nextInt(4, 16)));
  }

  public void testDuelWithStandardDisjunction() throws IOException {
    int iterations = LuceneTestCase.TEST_NIGHTLY ? atLeast(100) : 10;
    for (int iter = 0; iter < iterations; iter++) {
      Directory dir = newDirectory();
      final RandomIndexWriter w;

      int dims = 1;
      boolean singleton = random().nextBoolean();
      boolean sortedIndex = random().nextBoolean();
      if (!sortedIndex) {
        w = new RandomIndexWriter(random(), dir);
      } else {
        IndexWriterConfig config = new IndexWriterConfig().setCodec(getCodec());
        SortField sortField =
            random().nextBoolean()
                ? new SortField("docVal", SortField.Type.STRING, random().nextBoolean())
                : new SortField("numVal", SortField.Type.LONG, random().nextBoolean());
        config.setIndexSort(new Sort(sortField));
        w = new RandomIndexWriter(random(), dir);
      }

      long[] scratch = new long[dims];
      int maxDocs = LuceneTestCase.TEST_NIGHTLY ? atLeast(1000) : 100;
      for (int i = 0; i < maxDocs; i++) {
        int numPoints = singleton ? 1 : RandomNumbers.randomIntBetween(random(), 1, 10);
        Document doc = new Document();
        for (int j = 0; j < numPoints; j++) {
          for (int v = 0; v < dims; v++) {
            scratch[v] = RandomNumbers.randomLongBetween(random(), 0, atLeast(100));
          }
          doc.add(new LongPoint("point", scratch));
          assert scratch.length == 1;
          if (singleton) {
            if (sortedIndex) {
              doc.add(SortedDocValuesField.indexedField("docVal", LongPoint.pack(scratch)));
              doc.add(NumericDocValuesField.indexedField("numVal", scratch[0]));
            } else {
              doc.add(new SortedDocValuesField("docVal", LongPoint.pack(scratch)));
              doc.add(new NumericDocValuesField("numVal", scratch[0]));
            }
          } else {
            if (sortedIndex) {
              doc.add(SortedSetDocValuesField.indexedField("docVal", LongPoint.pack(scratch)));
              doc.add(SortedNumericDocValuesField.indexedField("numVal", scratch[0]));
            } else {
              doc.add(new SortedSetDocValuesField("docVal", LongPoint.pack(scratch)));
              doc.add(new SortedNumericDocValuesField("numVal", scratch[0]));
            }
          }
        }
        w.addDocument(doc);
        if (rarely()) {
          w.commit(); // segmenting to check index sorter.
        }
      }

      IndexReader reader = w.getReader();
      IndexSearcher searcher = newSearcher(reader);

      int numRanges = RandomNumbers.randomIntBetween(random(), 1, 20);
      LongPointMultiRangeBuilder builder1 = new LongPointMultiRangeBuilder("point", dims);
      BooleanQuery.Builder builder2 = new BooleanQuery.Builder();
      DocValuesMultiRangeQuery.SortedSetStabbingBuilder builder3 =
          new DocValuesMultiRangeQuery.SortedSetStabbingBuilder("docVal");
      DocValuesMultiRangeQuery.SortedNumericStabbingBuilder builderNumeric =
          new DocValuesMultiRangeQuery.SortedNumericStabbingBuilder("numVal");

      for (int i = 0; i < numRanges; i++) {
        long[] lower = new long[dims];
        long[] upper = new long[dims];
        for (int j = 0; j < dims; j++) {
          lower[j] = RandomNumbers.randomLongBetween(random(), -100, 200);
          upper[j] = lower[j] + RandomNumbers.randomLongBetween(random(), 0, 100);
        }
        builder1.add(lower, upper);
        builder2.add(LongPoint.newRangeQuery("point", lower, upper), BooleanClause.Occur.SHOULD);
        if (Arrays.equals(lower, upper) && random().nextBoolean()) {
          builder3.add(LongPoint.pack(lower));
        } else {
          builder3.add(LongPoint.pack(lower), LongPoint.pack(upper));
        }
        builderNumeric.add(lower[0], upper[0]);
      }

      Query query1 = builder1.build();
      Query query2 = builder2.build();
      Query query3 = builder3.build();
      Query queryNumeric = builderNumeric.build();
      TopDocs result1 = searcher.search(query1, reader.maxDoc(), Sort.INDEXORDER);
      TopDocs result2 = searcher.search(query2, reader.maxDoc(), Sort.INDEXORDER);
      TopDocs result3 = searcher.search(query3, reader.maxDoc(), Sort.INDEXORDER);
      TopDocs resultNumeric = searcher.search(queryNumeric, reader.maxDoc(), Sort.INDEXORDER);
      assertEquals(result2.totalHits, result1.totalHits);
      assertEquals(result2.totalHits, result3.totalHits);
      assertEquals(resultNumeric.totalHits, result3.totalHits);
      assertEquals(result2.scoreDocs.length, result1.scoreDocs.length);
      assertEquals(result2.scoreDocs.length, result3.scoreDocs.length);
      assertEquals(resultNumeric.scoreDocs.length, result3.scoreDocs.length);
      for (int i = 0; i < result2.scoreDocs.length; i++) {
        assertEquals(result2.scoreDocs[i].doc, result1.scoreDocs[i].doc);
        assertEquals(result2.scoreDocs[i].doc, result3.scoreDocs[i].doc);
        assertEquals(result3.scoreDocs[i].doc, resultNumeric.scoreDocs[i].doc);
      }

      IOUtils.close(reader, w, dir);
    }
  }

  public void testEquals() {
    Query q1 = mrSsDvQ("foo", 3, 5, 7, 9);
    QueryUtils.checkEqual(q1, mrSsDvQ("foo", 3, 5, 7, 9));
    QueryUtils.checkEqual(q1, mrSsDvQ("foo", 7, 9, 3, 5));
    QueryUtils.checkUnequal(q1, mrSsDvQ("foo", 7, 9, 5, 3));
    QueryUtils.checkUnequal(q1, mrSsDvQ("foo", 3, 5 + 1, 7, 9));
    QueryUtils.checkUnequal(q1, mrSsDvQ("foo", 3, 5, 7 + 1, 9));
    QueryUtils.checkUnequal(q1, mrSsDvQ("bar", 3, 5, 7, 9));
  }

  public void testNumericEquals() {
    Query q1 = mrSNumDvQ("foo", 3, 5, 7, 9);
    QueryUtils.checkEqual(q1, mrSNumDvQ("foo", 3, 5, 7, 9));
    QueryUtils.checkEqual(q1, mrSNumDvQ("foo", 7, 9, 3, 5));
    QueryUtils.checkUnequal(q1, mrSNumDvQ("foo", 7, 9, 5, 3));
    QueryUtils.checkUnequal(q1, mrSNumDvQ("foo", 3, 5 + 1, 7, 9));
    QueryUtils.checkUnequal(q1, mrSNumDvQ("foo", 3, 5, 7 + 1, 9));
    QueryUtils.checkUnequal(q1, mrSNumDvQ("bar", 3, 5, 7, 9));
  }

  private Query mrSsDvQ(String field, int... ends) {
    DocValuesMultiRangeQuery.SortedSetStabbingBuilder b = mrSsDvBuilder(field, ends);
    return b.build();
  }

  private Query mrSNumDvQ(String field, int... ends) {
    DocValuesMultiRangeQuery.SortedNumericStabbingBuilder b = mrSNumDvBuilder(field, ends);
    return b.build();
  }

  private static DocValuesMultiRangeQuery.SortedSetStabbingBuilder mrSsDvBuilder(
      String field, int... ends) {
    DocValuesMultiRangeQuery.SortedSetStabbingBuilder b =
        new DocValuesMultiRangeQuery.SortedSetStabbingBuilder(field);
    List<Integer> posns =
        IntStream.range(0, ends.length / 2).map(i -> i * 2).boxed().collect(Collectors.toList());
    Collections.shuffle(posns, random());
    for (Integer pos : posns) {
      int lower = ends[pos];
      int upper = ends[pos + 1];
      if (lower == upper && random().nextBoolean()) {
        b.add(IntPoint.pack(lower));
      } else {
        b.add(IntPoint.pack(lower), IntPoint.pack(upper));
      }
    }
    return b;
  }

  private static DocValuesMultiRangeQuery.SortedNumericStabbingBuilder mrSNumDvBuilder(
      String field, int... ends) {
    DocValuesMultiRangeQuery.SortedNumericStabbingBuilder b =
        new DocValuesMultiRangeQuery.SortedNumericStabbingBuilder(field);
    List<Integer> posns =
        IntStream.range(0, ends.length / 2).map(i -> i * 2).boxed().collect(Collectors.toList());
    Collections.shuffle(posns, random());
    for (Integer pos : posns) {
      int lower = ends[pos];
      int upper = ends[pos + 1];
      b.add(lower, upper);
      for (int repeat = 0; repeat < random().nextInt(3); repeat++) {
        if (rarely()) {
          b.add(lower, upper); // plain repeat
        } else {
          if (random().nextBoolean()) {
            b.add(lower, lower); // lower point repeat
          } else {
            b.add(upper, upper); // upper point repeat
          }
        }
      }
    }
    return b;
  }

  public void testToString() {
    Query q1 = mrSsDvQ("foo", 3, 5, 7, 9);
    assertEquals("foo:[[80 0 0 3]..[80 0 0 5], [80 0 0 7]..[80 0 0 9]]", q1.toString());
    assertEquals("[[80 0 0 3]..[80 0 0 5], [80 0 0 7]..[80 0 0 9]]", q1.toString("foo"));
    assertEquals("foo:[[80 0 0 3]..[80 0 0 5], [80 0 0 7]..[80 0 0 9]]", q1.toString("bar"));
  }

  public void testNumericToString() {
    Query q1 = mrSNumDvQ("foo", 3, 5, 7, 9);
    assertEquals("foo:[3..5, 7..9]", q1.toString());
    assertEquals("[3..5, 7..9]", q1.toString("foo"));
    assertEquals("foo:[3..5, 7..9]", q1.toString("bar"));
  }

  public void testOverrideToString() {
    DocValuesMultiRangeQuery.SortedSetStabbingBuilder b =
        new DocValuesMultiRangeQuery.SortedSetStabbingBuilder("foo") {
          @Override
          protected Query createSortedSetDocValuesMultiRangeQuery() {
            return new SortedSetDocValuesMultiRangeQuery(fieldName, clauses) {
              @Override
              public String toString(String fld) {
                return fieldName + " " + rangeClauses.size();
              }
            };
          }
        };
    b.add(IntPoint.pack(1), IntPoint.pack(2));
    b.add(IntPoint.pack(3), IntPoint.pack(4));
    assertEquals("foo 2", b.build().toString());

    DocValuesMultiRangeQuery.ByteRange myrange =
        new DocValuesMultiRangeQuery.ByteRange(IntPoint.pack(1), IntPoint.pack(2)) {
          @Override
          public String toString() {
            return IntPoint.decodeDimension(lower.bytes, 0)
                + " "
                + IntPoint.decodeDimension(upper.bytes, 0);
          }
        };
    assertEquals("1 2", myrange.toString());
  }

  public void testOverrideNumericsToString() {
    DocValuesMultiRangeQuery.SortedNumericStabbingBuilder b =
        new DocValuesMultiRangeQuery.SortedNumericStabbingBuilder("foo") {
          @Override
          protected Query createSortedNumericDocValuesMultiRangeQuery() {
            return new SortedNumericDocValuesMultiRangeQuery(fieldName, clauses) {
              @Override
              public String toString(String fld) {
                return fieldName + " " + sortedClauses.size();
              }
            };
          }
        };
    b.add(1, 2);
    b.add(3, 4);
    assertEquals("foo 2", b.build().toString());

    DocValuesMultiRangeQuery.LongRange myrange =
        new DocValuesMultiRangeQuery.LongRange(1, 2) {
          @Override
          public String toString() {
            return lower + " " + upper;
          }
        };
    assertEquals("1 2", myrange.toString());
  }

  public void testMissingField() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    iw.addDocument(new Document());
    IndexReader reader = iw.getReader();
    iw.close();
    IndexSearcher searcher = newSearcher(reader);
    for (Query query : List.of(mrSsDvQ("foo", 1, 2), mrSNumDvQ("foo", 1, 2))) {
      Weight w = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE, 1);
      assertNull(w.scorer(searcher.getIndexReader().leaves().getFirst()));
    }
    reader.close();
    dir.close();
  }

  public void testEdgeCases() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    final Document doc1 = new Document();
    doc1.add(new SortedSetDocValuesField("foo", IntPoint.pack(1)));
    iw.addDocument(doc1);
    final Document doc2 = new Document();
    doc2.add(new SortedSetDocValuesField("foo", IntPoint.pack(10)));
    iw.addDocument(doc2);

    IndexReader reader = iw.getReader();
    iw.close();
    IndexSearcher searcher = newSearcher(reader);
    for (DocValuesMultiRangeQuery.SortedSetStabbingBuilder builder :
        List.of(
            mrSsDvBuilder("foo", 2, 3, 4, 5, -5, -2), mrSsDvBuilder("foo", 2, 3, 4, 5, 12, 15))) {
      assertEquals("no match", 0, searcher.search(builder.build(), 1).totalHits.value());
      BytesRef lower;
      BytesRef upper;
      builder.add(lower = IntPoint.pack(100), upper = IntPoint.pack(200));
      assertEquals("no match", 0, searcher.search(builder.build(), 1).totalHits.value());
      lower.bytes = IntPoint.pack(1).bytes;
      upper.bytes = IntPoint.pack(10).bytes;
      assertEquals(
          "updating bytes changes nothing",
          0,
          searcher.search(builder.build(), 1).totalHits.value());
      builder.add(lower, upper);
      assertEquals(
          "sanity check for potential match",
          2,
          searcher.search(builder.build(), 1).totalHits.value());
    }
    // hit by value as a range upper==lower
    TopDocs hit1 = searcher.search(mrSsDvQ("foo", 2, 3, 4, 5, -5, -2, 1, 1), 1);
    TopDocs hit10 = searcher.search(mrSsDvQ("foo", 2, 3, 4, 5, -5, -2, 10, 10), 1);
    assertEquals(1, hit1.totalHits.value());
    assertEquals(1, hit10.totalHits.value());
    assertNotEquals(hit1.scoreDocs[0].doc, hit10.scoreDocs[0].doc);
    reader.close();
    dir.close();
  }

  public void testNumericCases() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    final Document doc1 = new Document();
    doc1.add(new SortedNumericDocValuesField("foo", 1L));
    iw.addDocument(doc1);
    final Document doc2 = new Document();
    doc2.add(new SortedNumericDocValuesField("foo", 10L));
    iw.addDocument(doc2);

    IndexReader reader = iw.getReader();
    iw.close();
    IndexSearcher searcher = newSearcher(reader);
    for (DocValuesMultiRangeQuery.SortedNumericStabbingBuilder builder :
        List.of(
            mrSNumDvBuilder("foo", 2, 3, 4, 5, -5, -2),
            mrSNumDvBuilder("foo", 2, 3, 4, 5, 12, 15))) {
      assertEquals("no match", 0, searcher.search(builder.build(), 1).totalHits.value());
      long lower;
      long upper;
      builder.add(lower = 100, 200);
      assertEquals("no match", 0, searcher.search(builder.build(), 1).totalHits.value());
      lower = 1;
      upper = 10;
      builder.add(lower, upper);
      Query query = builder.build();
      assertEquals(
          "sanity check for potential match " + query,
          2,
          searcher.search(query, 100).totalHits.value());
    }
    // hit by value as a range upper==lower
    TopDocs hit1 = searcher.search(mrSNumDvQ("foo", 2, 3, 4, 5, -5, -2, 1, 1), 1);
    TopDocs hit10 = searcher.search(mrSNumDvQ("foo", 2, 3, 4, 5, -5, -2, 10, 10), 1);
    assertEquals(1, hit1.totalHits.value());
    assertEquals(1, hit10.totalHits.value());
    assertNotEquals(hit1.scoreDocs[0].doc, hit10.scoreDocs[0].doc);
    reader.close();
    dir.close();
  }
}
