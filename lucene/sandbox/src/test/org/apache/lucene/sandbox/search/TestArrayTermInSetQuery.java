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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.ByteRunnable;

public class TestArrayTermInSetQuery extends LuceneTestCase {

  private static final String FIELD = "group";

  public void testMatchesSameDocsAsTermInSetQuery() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

    addDoc(writer, "alpha");
    addDoc(writer, "beta");
    addDoc(writer, "gamma");
    addDoc(writer, "delta");
    writer.commit();
    addDoc(writer, "epsilon");
    addDoc(writer, "zeta");
    writer.commit();

    DirectoryReader reader = DirectoryReader.open(writer);
    IndexSearcher searcher = newSearcher(reader);
    writer.close();

    List<BytesRef> queryTerms =
        Arrays.asList(new BytesRef("alpha"), new BytesRef("gamma"), new BytesRef("zeta"));

    TopDocs vanillaResults = searcher.search(new TermInSetQuery(FIELD, queryTerms), 100);
    TopDocs arrayResults = searcher.search(new ArrayTermInSetQuery(FIELD, queryTerms), 100);

    assertEquals(vanillaResults.scoreDocs.length, arrayResults.scoreDocs.length);
    for (int i = 0; i < vanillaResults.scoreDocs.length; i++) {
      assertEquals(vanillaResults.scoreDocs[i].doc, arrayResults.scoreDocs[i].doc);
      assertEquals(vanillaResults.scoreDocs[i].score, arrayResults.scoreDocs[i].score, 0.0f);
    }

    reader.close();
    dir.close();
  }

  public void testReturnsNoResultsForNonMatchingTerms() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
    addDoc(writer, "alpha");
    addDoc(writer, "beta");
    writer.commit();

    DirectoryReader reader = DirectoryReader.open(writer);
    IndexSearcher searcher = newSearcher(reader);
    writer.close();

    TopDocs results =
        searcher.search(
            new ArrayTermInSetQuery(
                FIELD, Arrays.asList(new BytesRef("nonexistent"), new BytesRef("bogus"))),
            100);
    assertEquals(0, results.scoreDocs.length);

    reader.close();
    dir.close();
  }

  public void testWorksWithSingleTerm() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
    addDoc(writer, "alpha");
    addDoc(writer, "beta");
    addDoc(writer, "alpha");
    writer.commit();

    DirectoryReader reader = DirectoryReader.open(writer);
    IndexSearcher searcher = newSearcher(reader);
    writer.close();

    TopDocs results =
        searcher.search(new ArrayTermInSetQuery(FIELD, Arrays.asList(new BytesRef("alpha"))), 100);
    assertEquals(2, results.scoreDocs.length);

    reader.close();
    dir.close();
  }

  public void testWorksAcrossMultipleSegments() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());

    for (int seg = 0; seg < 5; seg++) {
      for (int doc = 0; doc < 20; doc++) {
        addDoc(writer, "term_" + (seg * 20 + doc));
      }
      writer.commit();
    }

    DirectoryReader reader = DirectoryReader.open(writer);
    IndexSearcher searcher = newSearcher(reader);
    writer.close();

    List<BytesRef> queryTerms =
        Arrays.asList(
            new BytesRef("term_0"),
            new BytesRef("term_25"),
            new BytesRef("term_50"),
            new BytesRef("term_75"),
            new BytesRef("term_99"));

    TopDocs vanillaResults = searcher.search(new TermInSetQuery(FIELD, queryTerms), 100);
    TopDocs arrayResults = searcher.search(new ArrayTermInSetQuery(FIELD, queryTerms), 100);

    assertEquals(vanillaResults.scoreDocs.length, arrayResults.scoreDocs.length);

    reader.close();
    dir.close();
  }

  public void testEqualQueriesHaveSameHashCode() {
    ArrayTermInSetQuery a =
        new ArrayTermInSetQuery(FIELD, Arrays.asList(new BytesRef("alpha"), new BytesRef("beta")));
    ArrayTermInSetQuery b =
        new ArrayTermInSetQuery(FIELD, Arrays.asList(new BytesRef("alpha"), new BytesRef("beta")));

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
  }

  public void testSortIsInternal() {
    // Same set of terms in different input orders must produce equal queries.
    ArrayTermInSetQuery sorted =
        new ArrayTermInSetQuery(
            FIELD,
            Arrays.asList(new BytesRef("alpha"), new BytesRef("beta"), new BytesRef("gamma")));
    ArrayTermInSetQuery reversed =
        new ArrayTermInSetQuery(
            FIELD,
            Arrays.asList(new BytesRef("gamma"), new BytesRef("beta"), new BytesRef("alpha")));

    assertEquals(sorted, reversed);
    assertEquals(sorted.hashCode(), reversed.hashCode());
  }

  public void testDuplicatesInInputAreDeduplicated() {
    ArrayTermInSetQuery deduped =
        new ArrayTermInSetQuery(FIELD, Arrays.asList(new BytesRef("alpha"), new BytesRef("beta")));
    ArrayTermInSetQuery withDupes =
        new ArrayTermInSetQuery(
            FIELD,
            Arrays.asList(
                new BytesRef("alpha"),
                new BytesRef("alpha"),
                new BytesRef("beta"),
                new BytesRef("beta"),
                new BytesRef("beta")));

    assertEquals(deduped, withDupes);
    assertEquals(2, deduped.getTermsCount());
    assertEquals(2, withDupes.getTermsCount());
  }

  public void testDifferentTermsAreNotEqual() {
    ArrayTermInSetQuery a =
        new ArrayTermInSetQuery(FIELD, Arrays.asList(new BytesRef("alpha"), new BytesRef("beta")));
    ArrayTermInSetQuery b =
        new ArrayTermInSetQuery(FIELD, Arrays.asList(new BytesRef("alpha"), new BytesRef("gamma")));

    assertNotEquals(a, b);
  }

  public void testDifferentFieldsAreNotEqual() {
    ArrayTermInSetQuery a =
        new ArrayTermInSetQuery("field_a", Arrays.asList(new BytesRef("alpha")));
    ArrayTermInSetQuery b =
        new ArrayTermInSetQuery("field_b", Arrays.asList(new BytesRef("alpha")));

    assertNotEquals(a, b);
  }

  public void testToStringContainsFieldAndTerms() {
    ArrayTermInSetQuery query =
        new ArrayTermInSetQuery(FIELD, Arrays.asList(new BytesRef("alpha"), new BytesRef("beta")));
    String str = query.toString();
    assertTrue("toString should contain field name, got: " + str, str.contains(FIELD));
    assertTrue("toString should contain term 'alpha', got: " + str, str.contains("alpha"));
    assertTrue("toString should contain term 'beta', got: " + str, str.contains("beta"));
  }

  public void testVisitCallsConsumeTermsForSingleTerm() {
    ArrayTermInSetQuery query = new ArrayTermInSetQuery(FIELD, Arrays.asList(new BytesRef("only")));
    AtomicBoolean consumed = new AtomicBoolean(false);

    query.visit(
        new QueryVisitor() {
          @Override
          public void consumeTerms(Query query, Term... terms) {
            consumed.set(true);
            assertEquals(1, terms.length);
            assertEquals(FIELD, terms[0].field());
            assertEquals("only", terms[0].text());
          }
        });

    assertTrue("consumeTerms should have been called for single-term query", consumed.get());
  }

  public void testVisitCallsConsumeTermsMatchingForMultipleTerms() {
    ArrayTermInSetQuery query =
        new ArrayTermInSetQuery(FIELD, Arrays.asList(new BytesRef("alpha"), new BytesRef("beta")));
    AtomicBoolean consumed = new AtomicBoolean(false);

    query.visit(
        new QueryVisitor() {
          @Override
          public void consumeTermsMatching(
              Query query, String field, Supplier<ByteRunnable> automaton) {
            consumed.set(true);
            assertEquals(FIELD, field);
            ByteRunnable bra = automaton.get();
            assertTrue(
                "automaton should accept 'alpha'", bra.run(new BytesRef("alpha").bytes, 0, 5));
            assertTrue("automaton should accept 'beta'", bra.run(new BytesRef("beta").bytes, 0, 4));
          }
        });

    assertTrue("consumeTermsMatching should have been called for multi-term query", consumed.get());
  }

  public void testRamBytesUsedIsPositiveAndScalesWithTermCount() {
    ArrayTermInSetQuery small = new ArrayTermInSetQuery(FIELD, Arrays.asList(new BytesRef("a")));
    ArrayTermInSetQuery large = new ArrayTermInSetQuery(FIELD, Arrays.asList(buildTerms(100)));

    assertTrue("ramBytesUsed should be positive", small.ramBytesUsed() > 0);
    assertTrue("more terms should use more RAM", large.ramBytesUsed() > small.ramBytesUsed());
  }

  public void testGetTermsCountReturnsNumberOfTerms() {
    ArrayTermInSetQuery query =
        new ArrayTermInSetQuery(
            FIELD, Arrays.asList(new BytesRef("a"), new BytesRef("b"), new BytesRef("c")));
    assertEquals(3, query.getTermsCount());
  }

  public void testDifferentTermBoundariesWithSameConcatenationAreNotEqual() {
    // ["a","bc"] and ["ab","c"] have identical concatenated bytes "abc" but different term
    // boundaries. VInt-length prefixes in the packed representation must distinguish them.
    ArrayTermInSetQuery q1 =
        new ArrayTermInSetQuery(FIELD, Arrays.asList(new BytesRef("a"), new BytesRef("bc")));
    ArrayTermInSetQuery q2 =
        new ArrayTermInSetQuery(FIELD, Arrays.asList(new BytesRef("ab"), new BytesRef("c")));

    assertNotEquals(q1, q2);
  }

  public void testDifferentTermCountWithSameConcatenationAreNotEqual() {
    // ["abc"] vs ["a","bc"] — same bytes, different term count
    ArrayTermInSetQuery q1 = new ArrayTermInSetQuery(FIELD, Arrays.asList(new BytesRef("abc")));
    ArrayTermInSetQuery q2 =
        new ArrayTermInSetQuery(FIELD, Arrays.asList(new BytesRef("a"), new BytesRef("bc")));

    assertNotEquals(q1, q2);
  }

  public void testPackedBackingWorksWithNonZeroOffsetBytesRefs() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig());
    addDoc(writer, "alpha");
    addDoc(writer, "beta");
    writer.commit();

    DirectoryReader reader = DirectoryReader.open(writer);
    IndexSearcher searcher = newSearcher(reader);
    writer.close();

    // BytesRef with non-zero offset into a larger backing array
    byte[] buf = "XXXalphaYYY".getBytes(StandardCharsets.UTF_8);
    BytesRef offsetRef = new BytesRef(buf, 3, 5);
    List<BytesRef> terms = Arrays.asList(offsetRef, new BytesRef("beta"));

    TopDocs results = searcher.search(new ArrayTermInSetQuery(FIELD, terms), 100);
    assertEquals(2, results.scoreDocs.length);

    // Equality still works across independently constructed queries that happen to have one
    // input with a non-zero-offset BytesRef.
    List<BytesRef> terms2 = Arrays.asList(new BytesRef("alpha"), new BytesRef("beta"));
    ArrayTermInSetQuery q1 = new ArrayTermInSetQuery(FIELD, terms);
    ArrayTermInSetQuery q2 = new ArrayTermInSetQuery(FIELD, terms2);
    assertEquals(q1, q2);
    assertEquals(q1.hashCode(), q2.hashCode());

    reader.close();
    dir.close();
  }

  private static BytesRef[] buildTerms(int count) {
    BytesRef[] terms = new BytesRef[count];
    for (int i = 0; i < count; i++) {
      terms[i] = new BytesRef("term_" + String.format(Locale.ROOT, "%04d", i));
    }
    return terms;
  }

  private static void addDoc(IndexWriter writer, String value) throws IOException {
    Document doc = new Document();
    doc.add(new StringField(FIELD, value, Field.Store.YES));
    writer.addDocument(doc);
  }
}
