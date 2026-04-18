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
package org.apache.lucene.queries.spans;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.CheckHits;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/** Basic tests for SpanWithinQuery */
public class TestSpanWithinQuery extends LuceneTestCase {
  protected static IndexSearcher searcher;
  protected static Directory directory;
  protected static IndexReader reader;

  @BeforeClass
  public static void beforeClass() throws Exception {
    directory = newDirectory();
    RandomIndexWriter writer =
        new RandomIndexWriter(
            random(),
            directory,
            newIndexWriterConfig(new MockAnalyzer(random())).setMergePolicy(newLogMergePolicy()));

    addDoc(writer, "one two three four five");
    addDoc(writer, "one two three four five six");
    addDoc(writer, "one three five seven nine");
    addDoc(writer, "two four six eight ten");
    addDoc(writer, "one three five seven ten");
    addDoc(writer, "one four seven eight nine");
    addDoc(writer, "two one three five seven");
    addDoc(writer, "three five seven nine ten");
    addDoc(writer, "ten eight six four two");
    addDoc(writer, "one two two three four");
    addDoc(writer, "two four six six eight");
    addDoc(writer, "one three five five seven");
    addDoc(writer, "one two three four five six seven");
    addDoc(writer, "ten nine eight seven six five four");
    addDoc(writer, "one three five seven nine ten eight");
    addDoc(writer, "two four six eight ten nine seven");

    reader = writer.getReader();
    searcher = newSearcher(reader);
    writer.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    directory.close();
  }

  private static void addDoc(RandomIndexWriter writer, String content) throws Exception {
    Document doc = new Document();
    doc.add(newTextField("field", content, Field.Store.YES));
    writer.addDocument(doc);
  }

  protected void check(SpanQuery q, int[] docs) throws Exception {
    CheckHits.checkHitCollector(random(), q, null, searcher, docs);
  }

  public void testHashcodeEquals() {
    SpanQuery q1 = spanQuery("field", "one", "two");
    SpanQuery q2 = spanQuery("field", "one", "thre");
    SpanQuery q3 = spanQuery("field", "one", "two");
    SpanQuery q4 = spanQuery("field", "one", "four");

    SpanWithinQuery query1 = new SpanWithinQuery(q1, q2);
    SpanWithinQuery query2 = new SpanWithinQuery(q3, q4);

    QueryUtils.check(query1);
    QueryUtils.check(query2);
    QueryUtils.checkUnequal(query1, query2);
  }

  public void testBasicWithin() throws Exception {
    SpanQuery big = spanQuery("field", "one", "five");
    SpanQuery little = spanQuery("field", "two", "three");

    SpanWithinQuery query = new SpanWithinQuery(big, little);

    int[] expectedDocs = {0, 1, 12};
    check(query, expectedDocs);
  }

  public void testNoMatches() throws Exception {
    SpanQuery big = spanQuery("field", "ten", "one");
    SpanQuery little = spanQuery("field", "two", "three");

    SpanWithinQuery query = new SpanWithinQuery(big, little);

    int[] expectedDocs = {};
    check(query, expectedDocs);
  }

  @Test
  public void testDifferentFieldsThrowsIllegalArgumentException() {
    SpanQuery big = new SpanTermQuery(new Term("field1", "one"));
    SpanQuery little = new SpanTermQuery(new Term("field2", "two"));

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new SpanWithinQuery(big, little);
        });
  }

  public void testToString() {
    SpanQuery big = spanQuery("field", "one", "four");
    SpanQuery little = spanQuery("field", "two", "three");

    SpanWithinQuery query = new SpanWithinQuery(big, little);
    String queryString = query.toString("field");

    assertTrue(queryString.contains("SpanWithin"));
    assertTrue(queryString.contains("one"));
    assertTrue(queryString.contains("two"));
    assertTrue(queryString.contains("three"));
    assertTrue(queryString.contains("four"));
  }

  public void testBasicNear() throws Exception {
    SpanQuery big = spanNearQuery("field", new String[] {"one", "two", "three", "four"});
    SpanQuery little = spanNearQuery("field", new String[] {"two", "three"});
    SpanWithinQuery query = new SpanWithinQuery(big, little);

    int[] expectedDocs = {0, 1, 9, 12};
    CheckHits.checkHitCollector(random(), query, null, searcher, expectedDocs);
  }

  public void testDescendingSequence() throws Exception {
    SpanQuery big = spanNearQuery("field", new String[] {"ten", "nine", "eight"});
    SpanQuery little = spanNearQuery("field", new String[] {"nine", "eight"});
    SpanWithinQuery query = new SpanWithinQuery(big, little);

    int[] expectedDocs = {13};
    CheckHits.checkHitCollector(random(), query, null, searcher, expectedDocs);
  }

  private SpanNearQuery spanQuery(String field, String term1, String term2) {
    SpanTermQuery t1 = new SpanTermQuery(new Term(field, term1));
    SpanTermQuery t2 = new SpanTermQuery(new Term(field, term2));
    return new SpanNearQuery(new SpanQuery[] {t1, t2}, 5, true);
  }

  private SpanNearQuery spanNearQuery(String field, String[] terms) {
    SpanQuery[] clauses = new SpanQuery[terms.length];
    for (int i = 0; i < terms.length; i++) {
      clauses[i] = new SpanTermQuery(new Term(field, terms[i]));
    }
    return new SpanNearQuery(clauses, 5, true);
  }
}
