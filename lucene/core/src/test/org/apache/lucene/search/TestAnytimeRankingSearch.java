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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.NamedThreadFactory;
import org.junit.Test;

public class TestAnytimeRankingSearch extends LuceneTestCase {
  public void testAnytimeRanking() throws Exception {
    String indexPath = Files.createTempDirectory("testindex1").toAbsolutePath().toString();

    try (Directory directory = FSDirectory.open(Paths.get(indexPath));
        IndexWriter writer =
            new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))) {

      for (int i = 1; i <= 10000; i++) {
        String content =
            "Lucene document "
                + i
                + " ranking relevance retrieval performance precision recall diversity feedback tuning"
                    .split(" ")[i % 10];
        addDocument(writer, content, i);
      }

      writer.commit();
    }

    try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {
      IndexSearcher searcher = newSearcher(reader);
      searcher.setSimilarity(new BM25Similarity());
      Query query = new TermQuery(new Term("content", "lucene"));

      Map<Integer, Double> rangeScores = new HashMap<>();
      for (int i = 1; i <= 10; i++) {
        rangeScores.put(i, random().nextDouble());
      }

      AnytimeRankingSearcher anytimeSearcher =
          new AnytimeRankingSearcher(searcher, 10, 50, "content");
      int cpus = Runtime.getRuntime().availableProcessors();
      ExecutorService executor =
          Executors.newFixedThreadPool(cpus, new NamedThreadFactory("hunspellStemming-"));
      List<Future<TopDocs>> futures = new ArrayList<>();

      searcher.setSimilarity(new BM25Similarity(5.0f, 0.2f)); // Stronger term frequency weighting

      long startTime = System.nanoTime();
      for (int i = 0; i < 10; i++) {
        futures.add(
            executor.submit(() -> anytimeSearcher.search(query, rangeScores, reader.maxDoc())));
      }

      for (Future<TopDocs> future : futures) {
        TopDocs results = future.get();
        assertNotNull("Results should not be null", results);
        assertTrue("At least one result should be retrieved", results.scoreDocs.length > 0);
      }
      executor.shutdown();
      long duration = (System.nanoTime() - startTime) / 1_000_000;
      System.out.println("Total query execution time: " + duration + " ms");
    }
  }

  public void testDifferentQueries() throws Exception {
    String indexPath = Files.createTempDirectory("testindex2").toAbsolutePath().toString();

    try (Directory directory = FSDirectory.open(Paths.get(indexPath));
        IndexWriter writer =
            new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))) {

      for (int i = 1; i <= 10000; i++) {
        String content =
            "Lucene document "
                + i
                + " "
                + " ranking relevance "
                + TestUtil.randomSimpleString(random());
        addDocument(writer, content, i);
      }

      writer.commit();
    }

    try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {
      IndexSearcher searcher = newSearcher(reader);
      searcher.setSimilarity(new BM25Similarity());

      Query[] queries = {
        new TermQuery(new Term("content", "lucene")),
        new TermQuery(new Term("content", "document")),
        new TermQuery(new Term("content", "relevance"))
      };

      for (Query query : queries) {
        AnytimeRankingSearcher anytimeSearcher =
            new AnytimeRankingSearcher(searcher, 10, 100, "content");
        TopDocs results = anytimeSearcher.search(query, new HashMap<>(), reader.maxDoc());
        assertNotNull("Results should not be null", results);
        assertTrue("At least one result should be retrieved", results.scoreDocs.length > 0);
      }
    }
  }

  public void testEmptyIndex() throws Exception {
    String indexPath = Files.createTempDirectory("testindex3").toAbsolutePath().toString();

    try (Directory directory = FSDirectory.open(Paths.get(indexPath));
        IndexWriter writer =
            new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))) {
      writer.commit(); // Commit empty index
    }

    try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {
      IndexSearcher searcher = newSearcher(reader);
      searcher.setSimilarity(new BM25Similarity());
      Query query = new TermQuery(new Term("content", "lucene"));

      AnytimeRankingSearcher anytimeSearcher =
          new AnytimeRankingSearcher(searcher, 10, 50, "content");
      TopDocs results = anytimeSearcher.search(query, new HashMap<>(), reader.maxDoc());
      assertNotNull("Results should not be null", results);
      assertEquals("No results should be found in an empty index", 0, results.totalHits.value());
    }
  }

  public void testConcurrentQueries() throws Exception {
    String indexPath = Files.createTempDirectory("testindex4").toAbsolutePath().toString();

    try (Directory directory = FSDirectory.open(Paths.get(indexPath));
        IndexWriter writer =
            new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))) {

      for (int i = 1; i <= 10000; i++) {
        String content =
            "Lucene document "
                + i
                + " "
                + " ranking relevance "
                + " "
                + TestUtil.randomSimpleString(random());
        addDocument(writer, content, i);
      }

      writer.commit();
    }

    try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {
      IndexSearcher searcher = newSearcher(reader);
      searcher.setSimilarity(new BM25Similarity());

      Query[] queries = {
        new TermQuery(new Term("content", "lucene")),
        new TermQuery(new Term("content", "document")),
        new TermQuery(new Term("content", "ranking"))
      };

      AnytimeRankingSearcher anytimeSearcher =
          new AnytimeRankingSearcher(searcher, 10, 50, "content");
      int cpus = Runtime.getRuntime().availableProcessors();
      ExecutorService executor =
          Executors.newFixedThreadPool(cpus, new NamedThreadFactory("hunspellStemming-"));
      List<Future<TopDocs>> futures = new ArrayList<>();

      for (Query query : queries) {
        futures.add(
            executor.submit(() -> anytimeSearcher.search(query, new HashMap<>(), reader.maxDoc())));
      }

      for (Future<TopDocs> future : futures) {
        TopDocs results = future.get();
        assertNotNull("Results should not be null", results);
        System.out.println(
            "Concurrent query executed with " + results.totalHits.value() + " results.");
      }
      executor.shutdown();
    }
  }

  @Test
  public void testSlaCutoffTriggersEarlyTermination() throws Exception {
    String indexPath = Files.createTempDirectory("testindex5").toAbsolutePath().toString();

    try (Directory directory = FSDirectory.open(Paths.get(indexPath));
        IndexWriter writer =
            new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))) {

      for (int i = 1; i <= 10000; i++) {
        String content =
            "Lucene document "
                + i
                + " "
                + " ranking relevance "
                + " "
                + TestUtil.randomSimpleString(random());
        addDocument(writer, content, i);
      }

      writer.commit();
    }

    try (IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {
      IndexSearcher searcher = newSearcher(reader);
      searcher.setSimilarity(new BM25Similarity());

      Query query = new TermQuery(new Term("content", "lucene"));
      AnytimeRankingSearcher anytimeSearcher =
          new AnytimeRankingSearcher(searcher, 10, 1, "content"); // Intentionally tight SLA
      TopDocs results = anytimeSearcher.search(query, new HashMap<>(), reader.maxDoc());

      assertNotNull(results);
      assertTrue("Expect partial results due to SLA cutoff", results.scoreDocs.length > 0);
    }
  }

  private void addDocument(IndexWriter writer, String content, int docID) throws IOException {
    Document doc = new Document();
    doc.add(new IntPoint("docID", docID)); // Enables range queries
    doc.add(new StoredField("docID", docID)); // Allows retrieval
    doc.add(new TextField("content", content, Field.Store.YES));
    writer.addDocument(doc);
  }
}
