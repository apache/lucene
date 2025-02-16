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
package org.apache.lucene.sandbox;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.sandbox.search.DocValuesMultiRangeQuery;
import org.apache.lucene.sandbox.search.SortedSetDocValuesMultiRangeQuery;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

public class TestMultirangeDocValuesQueries extends LuceneTestCase {

  private Codec getCodec() {
    // small interval size to test with many intervals
    return TestUtil.alwaysDocValuesFormat(new Lucene90DocValuesFormat(random().nextInt(4, 16)));
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

  private Query mrSsDvQ(String field, int... ends) {
    DocValuesMultiRangeQuery.SortedSetStabbingBuilder b = mrSsDvBuilder(field, ends);
    return b.build();
  }

  private static DocValuesMultiRangeQuery.SortedSetStabbingBuilder mrSsDvBuilder(
      String field, int... ends) {
    DocValuesMultiRangeQuery.SortedSetStabbingBuilder b =
        new DocValuesMultiRangeQuery.SortedSetStabbingBuilder(field);
    for (int j = 0; j < ends.length; j += 2) {
      b.add(IntPoint.pack(ends[j]), IntPoint.pack(ends[j + 1]));
    }
    return b;
  }

  public void testToString() {
    Query q1 = mrSsDvQ("foo", 3, 5, 7, 9);
    assertEquals("foo:[[80 0 0 3]..[80 0 0 5], [80 0 0 7]..[80 0 0 9]]", q1.toString());
    assertEquals("[[80 0 0 3]..[80 0 0 5], [80 0 0 7]..[80 0 0 9]]", q1.toString("foo"));
    assertEquals("foo:[[80 0 0 3]..[80 0 0 5], [80 0 0 7]..[80 0 0 9]]", q1.toString("bar"));
  }

  public void testOverrideToString() {
    int[] ends = new int[] {3, 5, 7, 9};
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

  public void testMissingField() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    iw.addDocument(new Document());
    IndexReader reader = iw.getReader();
    iw.close();
    IndexSearcher searcher = newSearcher(reader);
    for (Query query : Collections.singletonList(mrSsDvQ("foo", 1, 2))) {
      Weight w = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE, 1);
      assertNull(w.scorer(searcher.getIndexReader().leaves().get(0)));
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
      Query frozen;
      assertEquals("no match", 0, searcher.search(frozen = builder.build(), 1).totalHits.value());
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
}
