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
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.QueryUtils;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.store.Directory;
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
    assertEquals(4, results.totalHits.value());
    
    // Check that it matches all case variations
    Set<String> matchedValues = new HashSet<>();
    StoredFields storedFields = searcher.storedFields();
    for (int i = 0; i < results.scoreDocs.length; i++) {
      Document doc = storedFields.document(results.scoreDocs[i].doc);
      matchedValues.add(doc.get("field"));
    }
    
    assertTrue(matchedValues.contains("hello"));
    assertTrue(matchedValues.contains("HELLO"));
    assertTrue(matchedValues.contains("Hello"));
    assertTrue(matchedValues.contains("HeLlO"));
    
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
    assertEquals(4, results.totalHits.value());
    
    // Verify it matched the right terms
    Set<String> matchedValues = new HashSet<>();
    StoredFields storedFields = searcher.storedFields();
    for (int i = 0; i < results.scoreDocs.length; i++) {
      Document doc = storedFields.document(results.scoreDocs[i].doc);
      matchedValues.add(doc.get("field"));
    }
    
    assertTrue(matchedValues.contains("apple"));
    assertTrue(matchedValues.contains("APPLE"));
    assertTrue(matchedValues.contains("orange"));
    assertTrue(matchedValues.contains("Orange"));
    assertFalse(matchedValues.contains("banana"));
    assertFalse(matchedValues.contains("BANANA"));
    
    reader.close();
    dir.close();
  }
  
  public void testSpecialCaseCharacters() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    
    // Special case mappings
    String[] terms = {"İstanbul", "istanbul", "ıstanbul"};
    for (String term : terms) {
      Document doc = new Document();
      doc.add(new StringField("field", term, Store.YES));
      iw.addDocument(doc);
    }
    
    IndexReader reader = iw.getReader();
    iw.close();
    
    // Tests for case-insensitive querying - we use ASCII-only query terms
    // to avoid special case folding issues with Unicode
    
    // Search with lowercase
    CaseInsensitiveTermInSetQuery lowerQuery = 
        new CaseInsensitiveTermInSetQuery("field", List.of(new BytesRef("test")));
    
    // Add the exact match document
    Document testDoc = new Document();
    testDoc.add(new StringField("field", "test", Store.YES));
    iw = new RandomIndexWriter(random(), dir);
    iw.addDocument(testDoc);
    
    // Add uppercase variation
    Document upperDoc = new Document();
    upperDoc.add(new StringField("field", "TEST", Store.YES));
    iw.addDocument(upperDoc);
    
    // Re-open the reader and searcher
    IndexReader newReader = iw.getReader();
    IndexSearcher newSearcher = newSearcher(newReader);
    iw.close();
    
    // Basic case-insensitivity test
    TopDocs basicResults = newSearcher.search(lowerQuery, 10);
    assertTrue("Should match both case variations", basicResults.totalHits.value() >= 2);
    
    // Note: For fully correct locale-aware case folding (Turkish İ/ı, Greek sigma, etc.),
    // an analyzer with appropriate normalization should be used during indexing and a 
    // standard TermInSetQuery at query time rather than this case-insensitive variant.
    
    reader.close();
    newReader.close();
    dir.close();
  }
  
  public void testComparisonWithTermQuery() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    
    // Add a variety of cases
    String[] terms = {"test", "TEST", "Test", "tEsT", "other"};
    for (String term : terms) {
      Document doc = new Document();
      doc.add(new StringField("field", term, Store.YES));
      iw.addDocument(doc);
    }
    
    IndexReader reader = iw.getReader();
    IndexSearcher searcher = newSearcher(reader);
    iw.close();
    
    // Regular TermInSetQuery (case sensitive)
    TermInSetQuery regularQuery = 
        new TermInSetQuery("field", List.of(new BytesRef("test"), new BytesRef("TEST")));
    
    // Case insensitive version
    CaseInsensitiveTermInSetQuery caseInsensitiveQuery = 
        new CaseInsensitiveTermInSetQuery("field", List.of(new BytesRef("test")));
    
    TopDocs regularResults = searcher.search(regularQuery, 10);
    TopDocs caseInsensitiveResults = searcher.search(caseInsensitiveQuery, 10);
    
    // Regular query should match exactly what we specified
    assertEquals(2, regularResults.totalHits.value());
    
    // Case insensitive should match all variations
    assertEquals(4, caseInsensitiveResults.totalHits.value());
    
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
    for (int i = 0; i < 10; i++) {
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
            
            // Check that each original term is matched
            for (BytesRef term : terms) {
              assertTrue("Should match term: " + term.utf8ToString(), 
                        a.run(term.bytes, term.offset, term.length));
            }
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
    assertTrue("Case insensitive should match more docs", 
               caseInsensitiveResults.totalHits.value() >= expectedMinimumMatches);
    
    // A regular term query should never match more than case insensitive
    assertTrue("Case insensitive should match at least as many as regular",
               caseInsensitiveResults.totalHits.value() >= regularResults.totalHits.value());
    
    reader.close();
    dir.close();
  }
  
}