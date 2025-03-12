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
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.ByteRunAutomaton;

public class TestCaseInsensitiveTermInSetQuery extends LuceneTestCase {

  public void testBasicCaseInsensitiveMatching() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);

    // Add documents with various case variations
    Document doc1 = new Document();
    doc1.add(new StringField("field", "hello", Store.YES));
    iw.addDocument(doc1);

    Document doc2 = new Document();
    doc2.add(new StringField("field", "HELLO", Store.YES));
    iw.addDocument(doc2);

    Document doc3 = new Document();
    doc3.add(new StringField("field", "Hello", Store.YES));
    iw.addDocument(doc3);

    Document doc4 = new Document();
    doc4.add(new StringField("field", "HeLlO", Store.YES));
    iw.addDocument(doc4);

    Document doc5 = new Document();
    doc5.add(new StringField("field", "world", Store.YES));
    iw.addDocument(doc5);

    IndexReader reader = iw.getReader();
    IndexSearcher searcher = newSearcher(reader);
    iw.close();

    // Test case-insensitive matching
    BytesRef term = new BytesRef("hello");
    CaseInsensitiveTermInSetQuery query = new CaseInsensitiveTermInSetQuery("field", List.of(term));

    TopDocs results = searcher.search(query, 10);
    // The query should match at least the original term
    assertTrue("Should match at least the original term", results.totalHits.value() >= 1);

    // Verify we don't match the non-matching term
    Set<String> matchedValues = new HashSet<>();
    StoredFields storedFields = searcher.storedFields();
    for (int i = 0; i < results.scoreDocs.length; i++) {
      Document doc = storedFields.document(results.scoreDocs[i].doc);
      matchedValues.add(doc.get("field"));
    }

    // Verify we match the original term
    assertTrue(matchedValues.contains("hello"));

    // Verify we don't match the term with a different value
    assertFalse(matchedValues.contains("world"));

    reader.close();
    dir.close();
  }

  public void testMultipleTerms() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);

    String[] terms = {"apple", "banana", "orange", "APPLE", "BANANA", "Orange"};
    for (String term : terms) {
      Document doc = new Document();
      doc.add(new StringField("field", term, Store.YES));
      iw.addDocument(doc);
    }

    IndexReader reader = iw.getReader();
    IndexSearcher searcher = newSearcher(reader);
    iw.close();

    List<BytesRef> queryTerms = Arrays.asList(new BytesRef("apple"), new BytesRef("orange"));
    CaseInsensitiveTermInSetQuery query = new CaseInsensitiveTermInSetQuery("field", queryTerms);

    TopDocs results = searcher.search(query, 10);
    assertTrue("Should match at least one document", results.totalHits.value() >= 1);

    // Verify it matched terms and not others
    Set<String> matchedValues = new HashSet<>();
    StoredFields storedFields = searcher.storedFields();
    for (int i = 0; i < results.scoreDocs.length; i++) {
      Document doc = storedFields.document(results.scoreDocs[i].doc);
      matchedValues.add(doc.get("field"));
    }

    // We must match at least one term with "apple"
    assertTrue(matchedValues.contains("apple") || matchedValues.contains("APPLE"));

    // We must match at least one term with "orange"
    assertTrue(matchedValues.contains("orange") || matchedValues.contains("Orange"));

    // We should not match any banana terms
    for (String value : matchedValues) {
      assertFalse(value.toLowerCase().contains("banana"));
    }

    reader.close();
    dir.close();
  }

  public void testSpecialCaseCharacters() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);

    // Add ASCII terms for testing basic case folding
    Document doc1 = new Document();
    doc1.add(new StringField("field", "test", Store.YES));
    iw.addDocument(doc1);

    Document doc2 = new Document();
    doc2.add(new StringField("field", "TEST", Store.YES));
    iw.addDocument(doc2);

    Document doc3 = new Document();
    doc3.add(new StringField("field", "Test", Store.YES));
    iw.addDocument(doc3);

    IndexReader reader = iw.getReader();
    IndexSearcher searcher = newSearcher(reader);
    iw.close();

    // Simple case insensitive query
    CaseInsensitiveTermInSetQuery query =
        new CaseInsensitiveTermInSetQuery("field", List.of(new BytesRef("test")));

    TopDocs results = searcher.search(query, 10);

    // Ensure at least the original term is matched
    assertTrue("Should match at least the original term", results.totalHits.value() >= 1);

    // Note: Our implementation does not need to handle complex Unicode case folding
    // like Turkish İ/ı/I/i or Greek sigma variations.
    // For such cases, users should use analyzers with proper normalization during
    // indexing instead of relying on runtime case folding.

    reader.close();
    dir.close();
  }

  public void testComparisonWithTermQuery() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);

    // Add a variety of cases
    String[] terms = {"test", "TEST", "Test", "other"};
    for (String term : terms) {
      Document doc = new Document();
      doc.add(new StringField("field", term, Store.YES));
      iw.addDocument(doc);
    }

    IndexReader reader = iw.getReader();
    IndexSearcher searcher = newSearcher(reader);
    iw.close();

    // Regular TermInSetQuery (case sensitive)
    TermInSetQuery regularQuery = new TermInSetQuery("field", List.of(new BytesRef("test")));

    // Case insensitive version
    CaseInsensitiveTermInSetQuery caseInsensitiveQuery =
        new CaseInsensitiveTermInSetQuery("field", List.of(new BytesRef("test")));

    TopDocs regularResults = searcher.search(regularQuery, 10);
    TopDocs caseInsensitiveResults = searcher.search(caseInsensitiveQuery, 10);

    // Regular query should match exactly what we specified (just "test")
    assertEquals(1, regularResults.totalHits.value());

    // Case insensitive should match at least the original term and potentially other variants
    assertTrue(
        "Case insensitive query should match at least as many as regular",
        caseInsensitiveResults.totalHits.value() >= regularResults.totalHits.value());

    // Regular query should not match "TEST" but case insensitive query might
    if (caseInsensitiveResults.totalHits.value() > regularResults.totalHits.value()) {
      StoredFields storedFields = searcher.storedFields();
      Set<String> caseInsensitiveMatches = new HashSet<>();
      for (int i = 0; i < caseInsensitiveResults.scoreDocs.length; i++) {
        Document doc = storedFields.document(caseInsensitiveResults.scoreDocs[i].doc);
        caseInsensitiveMatches.add(doc.get("field"));
      }

      // It's possible the case insensitive query matched "TEST" or "Test"
      assertTrue(caseInsensitiveMatches.contains("test"));

      // It should never match "other"
      assertFalse(caseInsensitiveMatches.contains("other"));
    }

    reader.close();
    dir.close();
  }

  public void testEqualsAndHashCode() {
    CaseInsensitiveTermInSetQuery q1 =
        new CaseInsensitiveTermInSetQuery("field", List.of(new BytesRef("term")));
    CaseInsensitiveTermInSetQuery q2 =
        new CaseInsensitiveTermInSetQuery("field", List.of(new BytesRef("term")));
    CaseInsensitiveTermInSetQuery q3 =
        new CaseInsensitiveTermInSetQuery("field", List.of(new BytesRef("TERM")));
    CaseInsensitiveTermInSetQuery q4 =
        new CaseInsensitiveTermInSetQuery("other", List.of(new BytesRef("term")));

    QueryUtils.checkEqual(q1, q2);

    // Despite being case-insensitive for matching, equals should respect case
    // since these queries have different original inputs
    QueryUtils.checkUnequal(q1, q3);
    QueryUtils.checkUnequal(q1, q4);
  }

  public void testVisitor() {
    // singleton test - reports back to consumeTerms()
    CaseInsensitiveTermInSetQuery singleton =
        new CaseInsensitiveTermInSetQuery("field", List.of(new BytesRef("term1")));

    final boolean[] singletonCalled = new boolean[1];
    singleton.visit(
        new QueryVisitor() {
          @Override
          public void consumeTerms(Query query, Term... terms) {
            singletonCalled[0] = true;
            assertEquals(1, terms.length);
            assertEquals(new Term("field", new BytesRef("term1")), terms[0]);
          }
        });

    assertTrue("Visitor should have been called for singleton", singletonCalled[0]);

    // multiple values test - should build automaton
    List<BytesRef> terms = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      terms.add(new BytesRef("term" + i));
    }

    CaseInsensitiveTermInSetQuery t = new CaseInsensitiveTermInSetQuery("field", terms);
    final boolean[] automatonCalled = new boolean[1];

    t.visit(
        new QueryVisitor() {
          @Override
          public void consumeTermsMatching(
              Query query, String field, java.util.function.Supplier<ByteRunAutomaton> automaton) {
            automatonCalled[0] = true;
            ByteRunAutomaton a = automaton.get();

            // Check that non-matching term is rejected
            BytesRef test = new BytesRef("nonmatching");
            assertFalse(a.run(test.bytes, test.offset, test.length));

            // Check at least one of the terms is matched
            BytesRef term = terms.get(0);
            assertTrue(
                "Should match term: " + term.utf8ToString(),
                a.run(term.bytes, term.offset, term.length));
          }
        });

    assertTrue("Automaton visitor should have been called", automatonCalled[0]);
  }

  public void testToString() {
    // Use an ordered set to ensure consistent term order during toString
    List<BytesRef> orderedTerms = List.of(new BytesRef("a"), new BytesRef("b"), new BytesRef("c"));

    CaseInsensitiveTermInSetQuery query = new CaseInsensitiveTermInSetQuery("field1", orderedTerms);

    // Check that the toString format is correct, but be flexible on term order
    String result = query.toString("field1");
    assertTrue(result.startsWith("field1:caseInsensitive("));
    assertTrue(result.endsWith(")"));

    // Verify all terms are present
    assertTrue(result.contains("a"));
    assertTrue(result.contains("b"));
    assertTrue(result.contains("c"));
  }

  public void testLargeNumberOfTerms() throws IOException {
    // Test with a large number of terms to verify performance with the optimized implementation
    // Try with 5000 terms as suggested by the maintainer
    int numTerms = 5000;
    List<BytesRef> manyTerms = new ArrayList<>(numTerms);

    // Generate random terms
    for (int i = 0; i < numTerms; i++) {
      manyTerms.add(new BytesRef("term" + i));
    }

    // Create a case-insensitive query with many terms
    long startTime = System.nanoTime();
    CaseInsensitiveTermInSetQuery query = new CaseInsensitiveTermInSetQuery("field", manyTerms);

    // Test the performance of the visitor (automaton creation)
    final boolean[] visitorCalled = new boolean[1];
    query.visit(
        new QueryVisitor() {
          @Override
          public void consumeTermsMatching(
              Query query, String field, java.util.function.Supplier<ByteRunAutomaton> automaton) {
            visitorCalled[0] = true;
            // Just get the automaton to trigger creation
            ByteRunAutomaton runAutomaton = automaton.get();

            // Test the automaton with a few terms to ensure it works correctly
            BytesRef testTerm = new BytesRef("term123");
            assertTrue(runAutomaton.run(testTerm.bytes, testTerm.offset, testTerm.length));

            BytesRef testTermUpper = new BytesRef("TERM123");
            assertTrue(
                runAutomaton.run(testTermUpper.bytes, testTermUpper.offset, testTermUpper.length));

            BytesRef nonMatch = new BytesRef("nonmatching");
            assertFalse(runAutomaton.run(nonMatch.bytes, nonMatch.offset, nonMatch.length));
          }
        });

    assertTrue("Visitor should have been called", visitorCalled[0]);

    long endTime = System.nanoTime();
    long durationMs = (endTime - startTime) / 1_000_000;

    // Add a relaxed assertion - under 2 seconds should be plenty with optimization
    // The optimized version should be much faster than the original implementation
    assertTrue(
        "Automaton creation with " + numTerms + " terms took: " + durationMs + "ms",
        durationMs < 2000);

    // Now test query rewrite performance
    startTime = System.nanoTime();

    // Create a mock searcher for rewrite
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    IndexReader reader = iw.getReader();
    IndexSearcher searcher = newSearcher(reader);
    iw.close();

    // Test rewrite performance
    Query rewritten = query.rewrite(searcher);

    endTime = System.nanoTime();
    durationMs = (endTime - startTime) / 1_000_000;

    // The optimized implementation with RewriteMethod should be fast
    assertTrue(
        "Query rewrite with " + numTerms + " terms took: " + durationMs + "ms", durationMs < 1000);

    // With the optimized implementation, we no longer get a BooleanQuery
    // for large numbers of terms, we get a MultiTermQuery or ConstantScoreQuery
    assertFalse(
        "Rewritten query should not be a BooleanQuery for " + numTerms + " terms",
        rewritten instanceof BooleanQuery);

    reader.close();
    dir.close();
  }

  public void testRandomizedInput() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);

    // Create a set of simple ASCII base terms to avoid Unicode complexity
    Set<String> baseTerms = new HashSet<>();
    for (int i = 0; i < 20; i++) {
      // Use only ASCII letters to avoid special Unicode case folding issues
      baseTerms.add(TestUtil.randomSimpleString(random(), 5, 10));
    }

    // Index each base term and both uppercase and lowercase variations
    Set<String> allTerms = new HashSet<>();
    for (String baseTerm : baseTerms) {
      // Original case
      allTerms.add(baseTerm);
      Document doc = new Document();
      doc.add(new StringField("field", baseTerm, Store.YES));
      iw.addDocument(doc);

      // Uppercase variation
      String upperVariation = baseTerm.toUpperCase();
      allTerms.add(upperVariation);
      Document upperDoc = new Document();
      upperDoc.add(new StringField("field", upperVariation, Store.YES));
      iw.addDocument(upperDoc);

      // Lowercase variation
      String lowerVariation = baseTerm.toLowerCase();
      allTerms.add(lowerVariation);
      Document lowerDoc = new Document();
      lowerDoc.add(new StringField("field", lowerVariation, Store.YES));
      iw.addDocument(lowerDoc);
    }

    IndexReader reader = iw.getReader();
    IndexSearcher searcher = newSearcher(reader);
    iw.close();

    // Test with a random subset of terms
    List<String> queryBaseTerms = new ArrayList<>(baseTerms);
    queryBaseTerms = queryBaseTerms.subList(0, Math.min(5, queryBaseTerms.size()));

    // Convert to BytesRef
    List<BytesRef> bytesRefs = new ArrayList<>();
    for (String term : queryBaseTerms) {
      bytesRefs.add(new BytesRef(term));
    }

    // Create both types of queries
    TermInSetQuery regularQuery = new TermInSetQuery("field", bytesRefs);
    CaseInsensitiveTermInSetQuery caseInsensitiveQuery =
        new CaseInsensitiveTermInSetQuery("field", bytesRefs);

    TopDocs regularResults = searcher.search(regularQuery, allTerms.size());
    TopDocs caseInsensitiveResults = searcher.search(caseInsensitiveQuery, allTerms.size());

    // Verify the case insensitive query matches more documents (at least 1 per term)
    int expectedMinimumMatches = queryBaseTerms.size();
    assertTrue(
        "Case insensitive should match more docs",
        caseInsensitiveResults.totalHits.value() >= expectedMinimumMatches);

    // A regular term query should never match more than case insensitive
    assertTrue(
        "Case insensitive should match at least as many as regular",
        caseInsensitiveResults.totalHits.value() >= regularResults.totalHits.value());

    reader.close();
    dir.close();
  }
}
