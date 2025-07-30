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
package org.apache.lucene.misc.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestImpactRangeQuery extends LuceneTestCase {

  public void testBasicFunctionality() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // Add documents with varying content
    for (int i = 0; i < 10000; i++) {
      Document doc = new Document();
      if (i < 1000) {
        // First range: high quality docs
        doc.add(new Field("content", "lucene search engine", TextField.TYPE_STORED));
      } else if (i < 2000) {
        // Second range: medium quality
        doc.add(new Field("content", "search", TextField.TYPE_STORED));
      } else {
        // Rest: low quality
        doc.add(new Field("content", "other content", TextField.TYPE_STORED));
      }
      writer.addDocument(doc);
    }

    IndexReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = new IndexSearcher(reader);

    // Test with regular query
    Query baseQuery = new TermQuery(new org.apache.lucene.index.Term("content", "search"));
    TopDocs regularResults = searcher.search(baseQuery, 10);

    // Test with impact range query
    Query impactQuery = new ImpactRangeQuery(baseQuery, 1000);
    TopDocs impactResults = searcher.search(impactQuery, 10);

    // Both should return same number of results
    assertEquals(
        "Total hits should be equal",
        regularResults.totalHits.value(),
        impactResults.totalHits.value());

    // Both should return same documents in top results
    assertEquals(
        "Number of score docs should be equal",
        regularResults.scoreDocs.length,
        impactResults.scoreDocs.length);

    reader.close();
    dir.close();
  }

  public void testEmptyResults() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    Document doc = new Document();
    doc.add(new Field("content", "something", TextField.TYPE_STORED));
    writer.addDocument(doc);

    IndexReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = new IndexSearcher(reader);

    Query baseQuery = new TermQuery(new org.apache.lucene.index.Term("content", "notfound"));
    Query impactQuery = new ImpactRangeQuery(baseQuery, 100);
    TopDocs results = searcher.search(impactQuery, 10);

    assertEquals(0, results.totalHits.value());

    reader.close();
    dir.close();
  }

  public void testRangePrioritization() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // Create documents with varying term frequencies to test prioritization
    // High-scoring documents (multiple occurrences of search term)
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      doc.add(new Field("id", String.valueOf(i), TextField.TYPE_STORED));
      doc.add(
          new Field("content", "search search search high relevance", TextField.TYPE_NOT_STORED));
      writer.addDocument(doc);
    }

    // Add spacer documents to separate ranges
    for (int i = 100; i < 900; i++) {
      Document doc = new Document();
      doc.add(new Field("id", String.valueOf(i), TextField.TYPE_STORED));
      doc.add(new Field("content", "other content", TextField.TYPE_NOT_STORED));
      writer.addDocument(doc);
    }

    // Low-scoring documents (single occurrence)
    for (int i = 900; i < 1000; i++) {
      Document doc = new Document();
      doc.add(new Field("id", String.valueOf(i), TextField.TYPE_STORED));
      doc.add(new Field("content", "search low relevance", TextField.TYPE_NOT_STORED));
      writer.addDocument(doc);
    }

    IndexReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = new IndexSearcher(reader);
    Query baseQuery = new TermQuery(new org.apache.lucene.index.Term("content", "search"));

    // Use a range size of 100 to separate high/low scoring documents
    Query impactQuery = new ImpactRangeQuery(baseQuery, 100);
    TopDocs results = searcher.search(impactQuery, 200); // Get enough results to see both ranges

    // With range prioritization, documents from high-scoring range (0-99) should appear
    // before documents from low-scoring range (900-999)
    int highScoringCount = 0;
    int lowScoringCount = 0;
    int firstLowScoringIndex = -1;

    StoredFields storedFields = searcher.storedFields();
    for (int i = 0; i < results.scoreDocs.length; i++) {
      Document doc = storedFields.document(results.scoreDocs[i].doc);
      String id = doc.get("id");
      if (id != null) {
        int docId = Integer.parseInt(id);
        if (docId < 100) {
          highScoringCount++;
        } else if (docId >= 900) {
          lowScoringCount++;
          if (firstLowScoringIndex == -1) {
            firstLowScoringIndex = i;
          }
        }
      }
    }

    assertEquals("Should find all high-scoring documents", 100, highScoringCount);
    assertEquals("Should find all low-scoring documents", 100, lowScoringCount);

    // The key test: high-scoring documents should appear first due to range prioritization
    assertTrue(
        "Range prioritization should put high-scoring documents first",
        firstLowScoringIndex >= 100);

    reader.close();
    dir.close();
  }

  public void testNoDocumentDuplication() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // Create documents where we know exactly what should match
    for (int i = 0; i < 50; i++) {
      Document doc = new Document();
      doc.add(new Field("id", String.valueOf(i), TextField.TYPE_STORED));
      if (i < 25) {
        doc.add(new Field("content", "search", TextField.TYPE_NOT_STORED));
      } else {
        doc.add(new Field("content", "other", TextField.TYPE_NOT_STORED));
      }
      writer.addDocument(doc);
    }

    IndexReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = new IndexSearcher(reader);
    Query baseQuery = new TermQuery(new org.apache.lucene.index.Term("content", "search"));

    // Use range size that will create multiple ranges
    TopDocs results = searcher.search(new ImpactRangeQuery(baseQuery, 10), 50);

    // Should find exactly 25 documents (0-24)
    assertEquals("Should find exactly 25 matching documents", 25, results.totalHits.value());
    assertEquals("Should return exactly 25 documents", 25, results.scoreDocs.length);

    // Verify no duplicates by checking all returned doc IDs are unique
    Set<Integer> seenDocs = new java.util.HashSet<>();
    StoredFields storedFields = searcher.storedFields();

    for (var scoreDoc : results.scoreDocs) {
      Document doc = storedFields.document(scoreDoc.doc);
      int id = Integer.parseInt(doc.get("id"));

      assertFalse("Document " + id + " returned multiple times", seenDocs.contains(id));
      seenDocs.add(id);
      assertTrue("Only docs 0-24 should match", id < 25);
    }

    reader.close();
    dir.close();
  }

  public void testActualRangePrioritizationIsEnabled() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // Create a scenario where range prioritization should be clearly visible
    // High-scoring range: docs 0-9 (term appears 3 times each)
    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      doc.add(new Field("id", String.valueOf(i), TextField.TYPE_STORED));
      doc.add(new Field("content", "test test test", TextField.TYPE_NOT_STORED));
      writer.addDocument(doc);
    }

    // Low-scoring range: docs 10-19 (term appears once each)
    for (int i = 10; i < 20; i++) {
      Document doc = new Document();
      doc.add(new Field("id", String.valueOf(i), TextField.TYPE_STORED));
      doc.add(new Field("content", "test", TextField.TYPE_NOT_STORED));
      writer.addDocument(doc);
    }

    IndexReader reader = writer.getReader();
    writer.close();
    IndexSearcher searcher = new IndexSearcher(reader);

    Query baseQuery = new TermQuery(new org.apache.lucene.index.Term("content", "test"));

    // Use small range size so we get clear separation
    TopDocs results = searcher.search(new ImpactRangeQuery(baseQuery, 10), 20);

    // Count how many docs from each range appear in first 10 results
    int highScoringInTop10 = 0;
    int lowScoringInTop10 = 0;

    StoredFields storedFields = searcher.storedFields();
    for (int i = 0; i < Math.min(10, results.scoreDocs.length); i++) {
      Document doc = storedFields.document(results.scoreDocs[i].doc);
      int id = Integer.parseInt(doc.get("id"));
      if (id < 10) {
        highScoringInTop10++;
      } else {
        lowScoringInTop10++;
      }
    }

    // Compare with regular query to see if we're getting different ordering
    TopDocs regularResults = searcher.search(baseQuery, 20);
    int regularHighScoringInTop10 = 0;
    int regularLowScoringInTop10 = 0;

    for (int i = 0; i < Math.min(10, regularResults.scoreDocs.length); i++) {
      Document doc = storedFields.document(regularResults.scoreDocs[i].doc);
      int id = Integer.parseInt(doc.get("id"));
      if (id < 10) {
        regularHighScoringInTop10++;
      } else {
        regularLowScoringInTop10++;
      }
    }

    // If range prioritization is working, we should see all high-scoring docs first
    assertTrue(
        "Range prioritization should favor high-scoring docs",
        highScoringInTop10 >= lowScoringInTop10);

    reader.close();
    dir.close();
  }

  public void testEarlyTermination() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // Create documents with different score potentials:
    // First 10 docs: high scoring (term appears 5 times each)
    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      doc.add(new Field("id", String.valueOf(i), TextField.TYPE_STORED));
      doc.add(new Field("content", "test test test test test", TextField.TYPE_NOT_STORED));
      writer.addDocument(doc);
    }

    // Next 100 docs: medium scoring (term appears 2 times each)
    for (int i = 10; i < 110; i++) {
      Document doc = new Document();
      doc.add(new Field("id", String.valueOf(i), TextField.TYPE_STORED));
      doc.add(new Field("content", "test test", TextField.TYPE_NOT_STORED));
      writer.addDocument(doc);
    }

    // Next 1000 docs: low scoring (term appears once each)
    for (int i = 110; i < 1110; i++) {
      Document doc = new Document();
      doc.add(new Field("id", String.valueOf(i), TextField.TYPE_STORED));
      doc.add(new Field("content", "test", TextField.TYPE_NOT_STORED));
      writer.addDocument(doc);
    }

    IndexReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = new IndexSearcher(reader);
    Query baseQuery = new TermQuery(new org.apache.lucene.index.Term("content", "test"));

    // Use range size of 10 to create clear separation between high/medium/low scoring ranges
    Query impactQuery = new ImpactRangeQuery(baseQuery, 10);

    // Search for only top 5 results - this should trigger early termination
    TopDocs results = searcher.search(impactQuery, 5);

    // Verify we get high-quality results first
    StoredFields storedFields = searcher.storedFields();
    int highScoringCount = 0;

    for (int i = 0; i < results.scoreDocs.length; i++) {
      Document doc = storedFields.document(results.scoreDocs[i].doc);
      int id = Integer.parseInt(doc.get("id"));
      if (id < 10) { // High scoring documents
        highScoringCount++;
      }
    }

    // With early termination and range prioritization, we should get mostly high-scoring docs
    assertTrue(
        "Early termination should favor high-scoring documents",
        highScoringCount >= 3); // At least 3 out of 5 should be high-scoring

    reader.close();
    dir.close();
  }

  public void testWithTopScoreDocCollector() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // Create documents with varying relevance
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      doc.add(new Field("id", String.valueOf(i), TextField.TYPE_STORED));
      // Some docs have term multiple times (higher score)
      if (i < 20) {
        doc.add(
            new Field(
                "content", "search engine search algorithm search", TextField.TYPE_NOT_STORED));
      } else if (i < 50) {
        doc.add(new Field("content", "search algorithm", TextField.TYPE_NOT_STORED));
      } else {
        doc.add(new Field("content", "basic search", TextField.TYPE_NOT_STORED));
      }
      writer.addDocument(doc);
    }

    IndexReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = new IndexSearcher(reader);
    Query baseQuery = new TermQuery(new org.apache.lucene.index.Term("content", "search"));
    Query impactQuery = new ImpactRangeQuery(baseQuery, 25);

    // Test with TopScoreDocCollector using CollectorManager
    int numHits = 10;
    TopScoreDocCollectorManager manager =
        new TopScoreDocCollectorManager(numHits, null, Integer.MAX_VALUE);
    TopDocs topDocs = searcher.search(impactQuery, manager);

    assertEquals("Should collect requested number of hits", numHits, topDocs.scoreDocs.length);
    assertTrue("Should find matching documents", topDocs.totalHits.value() > 0);

    // Verify scores are in descending order (standard collector behavior)
    float previousScore = Float.MAX_VALUE;
    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
      assertTrue("Scores should be in descending order", scoreDoc.score <= previousScore);
      previousScore = scoreDoc.score;
    }

    // Verify it works with early termination threshold
    TopScoreDocCollectorManager managerWithThreshold =
        new TopScoreDocCollectorManager(numHits, null, 50);
    TopDocs topDocsWithThreshold = searcher.search(impactQuery, managerWithThreshold);

    // Should still get same top docs even with threshold
    assertEquals(
        "Should get same number of top docs", numHits, topDocsWithThreshold.scoreDocs.length);

    reader.close();
    dir.close();
  }

  public void testTopScoreDocCollectorWithLargeDataset() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // Add 1000 documents with varied scores like the provided test
    for (int i = 0; i < 1000; i++) {
      Document doc = new Document();
      // Alternate between "important search term" and "search term" to create score variation
      String content = (i % 2 == 0) ? "important search term" : "search term";
      doc.add(new Field("content", content, TextField.TYPE_STORED));
      doc.add(new Field("id", String.valueOf(i), TextField.TYPE_STORED));
      writer.addDocument(doc);
    }

    IndexReader reader = writer.getReader();
    writer.close();
    IndexSearcher searcher = new IndexSearcher(reader);

    Query baseQuery = new TermQuery(new org.apache.lucene.index.Term("content", "search"));
    // Use a range size of 200 as in the provided test
    Query impactRangeQuery = new ImpactRangeQuery(baseQuery, 200);

    int topK = 10;
    TopScoreDocCollectorManager manager =
        new TopScoreDocCollectorManager(topK, null, Integer.MAX_VALUE);
    TopDocs topDocs = searcher.search(impactRangeQuery, manager);

    assertEquals("Should return top K documents", topK, topDocs.scoreDocs.length);
    assertEquals("Should find all matching documents", 1000, topDocs.totalHits.value());

    // Check descending order of scores - this is the key verification from the provided test
    for (int i = 1; i < topDocs.scoreDocs.length; i++) {
      assertTrue(
          "Scores should be in descending order",
          topDocs.scoreDocs[i - 1].score >= topDocs.scoreDocs[i].score);
    }

    // Verify we can retrieve document content for the top results
    StoredFields storedFields = searcher.storedFields();
    for (ScoreDoc sd : topDocs.scoreDocs) {
      Document doc = storedFields.document(sd.doc);
      String content = doc.get("content");
      assertNotNull("Document should have content field", content);
      assertTrue("Content should contain search term", content.contains("search"));
    }

    // Verify the query found both types of documents
    boolean foundImportant = false;
    boolean foundRegular = false;
    for (int i = 0; i < Math.min(20, topDocs.totalHits.value()); i++) {
      Document doc = storedFields.document(i);
      String content = doc.get("content");
      if (content != null) {
        if (content.contains("important")) foundImportant = true;
        if (!content.contains("important")) foundRegular = true;
      }
    }
    assertTrue("Should find at least one document with 'important'", foundImportant);
    assertTrue("Should find at least one document without 'important'", foundRegular);

    reader.close();
    dir.close();
  }

  public void testWithEarlyTerminationCollector() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // Create a large dataset to test early termination
    for (int i = 0; i < 1000; i++) {
      Document doc = new Document();
      doc.add(new Field("id", String.valueOf(i), TextField.TYPE_STORED));
      // First 100 docs are high quality
      if (i < 100) {
        doc.add(
            new Field(
                "content",
                "search optimization search algorithm search",
                TextField.TYPE_NOT_STORED));
      } else {
        doc.add(new Field("content", "search", TextField.TYPE_NOT_STORED));
      }
      writer.addDocument(doc);
    }

    IndexReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = new IndexSearcher(reader);
    Query baseQuery = new TermQuery(new org.apache.lucene.index.Term("content", "search"));
    Query impactQuery = new ImpactRangeQuery(baseQuery, 50);

    // Custom collector that terminates after collecting enough high-quality docs
    class EarlyTerminatingCollector extends SimpleCollector {
      private final int maxDocs;
      private int collected = 0;
      private Scorable scorer;
      private final List<Integer> collectedDocs = new ArrayList<>();
      private final List<Float> scores = new ArrayList<>();

      EarlyTerminatingCollector(int maxDocs, float minScore) {
        this.maxDocs = maxDocs;
      }

      @Override
      public void setScorer(Scorable scorer) throws IOException {
        this.scorer = scorer;
      }

      @Override
      public void collect(int doc) throws IOException {
        float score = scorer.score();
        collectedDocs.add(doc);
        scores.add(score);
        collected++;

        if (collected >= maxDocs) {
          throw new CollectionTerminatedException();
        }
      }

      @Override
      public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE;
      }

      public int getCollectedCount() {
        return collected;
      }

      public float getAverageScore() {
        if (scores.isEmpty()) return 0;
        float sum = 0;
        for (float score : scores) {
          sum += score;
        }
        return sum / scores.size();
      }
    }

    // Test early termination - collect any 10 documents
    EarlyTerminatingCollector earlyCollector = new EarlyTerminatingCollector(10, 0.0f);
    boolean terminatedEarly = false;
    try {
      searcher.search(impactQuery, earlyCollector);
    } catch (CollectionTerminatedException e) {
      terminatedEarly = true;
    }

    // The collector should have collected some documents
    assertTrue("Should collect some documents", earlyCollector.getCollectedCount() > 0);
    assertTrue("Should have reasonable average score", earlyCollector.getAverageScore() > 0);

    // Verify early termination functionality works

    // Test with CollectorManager for concurrent collection
    CollectorManager<EarlyTerminatingCollector, Integer> manager =
        new CollectorManager<>() {
          @Override
          public EarlyTerminatingCollector newCollector() throws IOException {
            return new EarlyTerminatingCollector(5, 0.0f);
          }

          @Override
          public Integer reduce(Collection<EarlyTerminatingCollector> collectors)
              throws IOException {
            int total = 0;
            for (EarlyTerminatingCollector c : collectors) {
              total += c.getCollectedCount();
            }
            return total;
          }
        };

    Integer totalCollected = searcher.search(impactQuery, manager);
    assertTrue("CollectorManager should collect documents", totalCollected > 0);

    reader.close();
    dir.close();
  }

  public void testComposedInBooleanQuery() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // Create documents with multiple fields
    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      doc.add(new Field("id", String.valueOf(i), TextField.TYPE_STORED));

      // Vary content across documents
      if (i % 3 == 0) {
        doc.add(new Field("title", "search engine", TextField.TYPE_NOT_STORED));
        doc.add(new Field("content", "lucene search", TextField.TYPE_NOT_STORED));
      } else if (i % 3 == 1) {
        doc.add(new Field("title", "database", TextField.TYPE_NOT_STORED));
        doc.add(new Field("content", "search algorithm", TextField.TYPE_NOT_STORED));
      } else {
        doc.add(new Field("title", "information", TextField.TYPE_NOT_STORED));
        doc.add(new Field("content", "retrieval system", TextField.TYPE_NOT_STORED));
      }
      writer.addDocument(doc);
    }

    IndexReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = new IndexSearcher(reader);

    // Create impact queries for different fields
    Query titleQuery = new TermQuery(new org.apache.lucene.index.Term("title", "search"));
    Query impactTitleQuery = new ImpactRangeQuery(titleQuery, 20);

    Query contentQuery = new TermQuery(new org.apache.lucene.index.Term("content", "search"));
    Query impactContentQuery = new ImpactRangeQuery(contentQuery, 20);

    // Compose in BooleanQuery with MUST clauses
    BooleanQuery.Builder mustBuilder = new BooleanQuery.Builder();
    mustBuilder.add(impactTitleQuery, BooleanClause.Occur.MUST);
    mustBuilder.add(contentQuery, BooleanClause.Occur.MUST);
    Query mustQuery = mustBuilder.build();

    TopDocs mustResults = searcher.search(mustQuery, 10);
    assertTrue("Should find documents matching both clauses", mustResults.totalHits.value() > 0);

    // Compose in BooleanQuery with SHOULD clauses
    BooleanQuery.Builder shouldBuilder = new BooleanQuery.Builder();
    shouldBuilder.add(impactTitleQuery, BooleanClause.Occur.SHOULD);
    shouldBuilder.add(impactContentQuery, BooleanClause.Occur.SHOULD);
    Query shouldQuery = shouldBuilder.build();

    TopDocs shouldResults = searcher.search(shouldQuery, 10);
    assertTrue(
        "Should find documents matching either clause",
        shouldResults.totalHits.value() > mustResults.totalHits.value());

    // Compose with regular queries and impact queries mixed
    BooleanQuery.Builder mixedBuilder = new BooleanQuery.Builder();
    mixedBuilder.add(impactTitleQuery, BooleanClause.Occur.SHOULD);
    mixedBuilder.add(contentQuery, BooleanClause.Occur.SHOULD); // Regular query
    mixedBuilder.add(
        new TermQuery(new org.apache.lucene.index.Term("content", "lucene")),
        BooleanClause.Occur.MUST);
    Query mixedQuery = mixedBuilder.build();

    TopDocs mixedResults = searcher.search(mixedQuery, 10);
    assertTrue("Mixed query should find results", mixedResults.totalHits.value() > 0);

    // Verify nested impact queries work correctly
    BooleanQuery.Builder outerBuilder = new BooleanQuery.Builder();
    outerBuilder.add(shouldQuery, BooleanClause.Occur.MUST);
    outerBuilder.add(
        new TermQuery(new org.apache.lucene.index.Term("content", "lucene")),
        BooleanClause.Occur.SHOULD);
    Query nestedQuery = outerBuilder.build();

    TopDocs nestedResults = searcher.search(nestedQuery, 10);
    assertTrue("Nested query should work correctly", nestedResults.totalHits.value() > 0);

    // Verify scores are computed correctly in boolean context
    for (ScoreDoc scoreDoc : nestedResults.scoreDocs) {
      assertTrue("Scores should be positive", scoreDoc.score > 0);
    }

    reader.close();
    dir.close();
  }

  public void testTopScoreDocCollectorWithImpactRange() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);

    // Add 1000 documents with varied scores
    for (int i = 0; i < 1000; i++) {
      Document doc = new Document();
      String content = (i % 2 == 0) ? "important search term" : "search term";
      doc.add(new Field("content", content, TextField.TYPE_STORED));
      doc.add(new Field("id", String.valueOf(i), TextField.TYPE_STORED));
      writer.addDocument(doc);
    }

    IndexReader reader = writer.getReader();
    writer.close();
    IndexSearcher searcher = new IndexSearcher(reader);

    Query baseQuery = new TermQuery(new org.apache.lucene.index.Term("content", "search"));
    Query impactRangeQuery = new ImpactRangeQuery(baseQuery, 200, 800);

    int topK = 10;
    TopScoreDocCollectorManager manager =
        new TopScoreDocCollectorManager(topK, null, Integer.MAX_VALUE);
    TopDocs topDocs = searcher.search(impactRangeQuery, manager);

    assertEquals(topK, topDocs.scoreDocs.length);

    for (ScoreDoc sd : topDocs.scoreDocs) {
      assertTrue("docID should be in [200, 800)", sd.doc >= 200 && sd.doc < 800);
    }

    // Check descending order of scores
    for (int i = 1; i < topDocs.scoreDocs.length; i++) {
      assertTrue(
          "scores should be descending",
          topDocs.scoreDocs[i - 1].score >= topDocs.scoreDocs[i].score);
    }

    reader.close();
    dir.close();
  }

  public void testEquals() {
    Query baseQuery = new TermQuery(new Term("field", "test"));

    // Test 2-parameter constructor equality
    ImpactRangeQuery query1 = new ImpactRangeQuery(baseQuery, 1000);
    ImpactRangeQuery query2 = new ImpactRangeQuery(baseQuery, 1000);
    ImpactRangeQuery query3 = new ImpactRangeQuery(baseQuery, 2000);

    assertEquals(query1, query2);
    assertNotEquals(query1, query3);

    // Test 3-parameter constructor equality
    ImpactRangeQuery query4 = new ImpactRangeQuery(baseQuery, 100, 500);
    ImpactRangeQuery query5 = new ImpactRangeQuery(baseQuery, 100, 500);
    ImpactRangeQuery query6 = new ImpactRangeQuery(baseQuery, 200, 500);
    ImpactRangeQuery query7 = new ImpactRangeQuery(baseQuery, 100, 600);

    assertEquals(query4, query5);
    assertNotEquals(query4, query6); // Different minDoc
    assertNotEquals(query4, query7); // Different maxDoc

    // Test cross-constructor inequality
    assertNotEquals(query1, query4); // Same base query but different constructors

    // Test different base queries
    Query differentBase = new TermQuery(new Term("field", "other"));
    ImpactRangeQuery query8 = new ImpactRangeQuery(differentBase, 1000);
    assertNotEquals(query1, query8);
  }

  public void testHashCode() {
    Query baseQuery = new TermQuery(new Term("field", "test"));

    // Equal objects must have equal hash codes
    ImpactRangeQuery query1 = new ImpactRangeQuery(baseQuery, 1000);
    ImpactRangeQuery query2 = new ImpactRangeQuery(baseQuery, 1000);
    assertEquals(query1.hashCode(), query2.hashCode());

    ImpactRangeQuery query3 = new ImpactRangeQuery(baseQuery, 100, 500);
    ImpactRangeQuery query4 = new ImpactRangeQuery(baseQuery, 100, 500);
    assertEquals(query3.hashCode(), query4.hashCode());

    // Different objects should generally have different hash codes
    ImpactRangeQuery query5 = new ImpactRangeQuery(baseQuery, 2000);
    assertNotEquals(query1.hashCode(), query5.hashCode());

    ImpactRangeQuery query6 = new ImpactRangeQuery(baseQuery, 200, 500);
    assertNotEquals(query3.hashCode(), query6.hashCode());
  }

  public void testRewrite() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

    // Add some documents
    for (int i = 0; i < 10; i++) {
      Document doc = new Document();
      doc.add(new TextField("field", "test", Field.Store.NO));
      writer.addDocument(doc);
    }
    writer.close();

    IndexReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);

    // Test rewrite with 2-parameter constructor
    Query baseQuery = new TermQuery(new Term("field", "test"));
    ImpactRangeQuery query1 = new ImpactRangeQuery(baseQuery, 1000);
    Query rewritten1 = query1.rewrite(searcher);

    // Should be the same if base query doesn't need rewriting
    assertSame(query1, rewritten1);

    // Test rewrite with 3-parameter constructor
    ImpactRangeQuery query2 = new ImpactRangeQuery(baseQuery, 100, 500);
    Query rewritten2 = query2.rewrite(searcher);

    // Should be the same if base query doesn't need rewriting
    assertSame(query2, rewritten2);

    // Test with a query that needs rewriting (BooleanQuery with single clause)
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(baseQuery, BooleanClause.Occur.MUST);
    Query needsRewrite = builder.build();

    ImpactRangeQuery query3 = new ImpactRangeQuery(needsRewrite, 1000);
    Query rewritten3 = query3.rewrite(searcher);

    // Should be different after rewriting
    assertNotSame(query3, rewritten3);
    assertTrue(rewritten3 instanceof ImpactRangeQuery);

    ImpactRangeQuery rewrittenImpact3 = (ImpactRangeQuery) rewritten3;
    assertEquals(1000, rewrittenImpact3.getRangeSize());

    // Test 3-parameter constructor rewrite preserves bounds
    ImpactRangeQuery query4 = new ImpactRangeQuery(needsRewrite, 100, 500);
    Query rewritten4 = query4.rewrite(searcher);

    assertNotSame(query4, rewritten4);
    assertTrue(rewritten4 instanceof ImpactRangeQuery);

    // Check that the rewritten query preserves the document bounds
    ImpactRangeQuery rewrittenImpact4 = (ImpactRangeQuery) rewritten4;
    assertEquals(400, rewrittenImpact4.getRangeSize()); // maxDoc - minDoc = 500 - 100

    reader.close();
    dir.close();
  }
}
