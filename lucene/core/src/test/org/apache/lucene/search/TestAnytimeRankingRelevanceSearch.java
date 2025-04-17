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
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Test;

public class TestAnytimeRankingRelevanceSearch extends LuceneTestCase {

  private String indexPath;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    indexPath = Files.createTempDirectory("testindex_relevance").toAbsolutePath().toString();

    try (Directory directory = FSDirectory.open(Paths.get(indexPath));
        IndexWriter writer =
            new IndexWriter(directory, new IndexWriterConfig(new StandardAnalyzer()))) {

      for (int i = 1; i <= 50; i++) {
        addDocument(
            writer,
            "Lucene test document "
                + i
                + " containing ranking relevance efficiency performance scoring",
            i);
        if (i % 5 == 0) {
          addDocument(writer, "Lucene boosted search efficiency query expansion", i + 50);
        }
        if (i % 3 == 0) {
          addDocument(writer, "Lucene document test ranking optimization BM25 tuning", i + 100);
        }
        if (i % 7 == 0) {
          addDocument(writer, "Lucene scalable indexing techniques for optimal retrieval", i + 150);
        }
      }

      writer.commit();
    }
  }

  @Test
  public void testRelevanceAcrossDiverseDocuments() throws Exception {
    try (DirectoryReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {
      IndexSearcher searcher = newSearcher(reader);
      searcher.setSimilarity(new BM25Similarity());

      Query query = new TermQuery(new Term("content", "lucene"));
      try (AnytimeRankingSearcher anytimeSearcher =
          new AnytimeRankingSearcher(reader, 10, 20, "content")) {
        TopDocs results = anytimeSearcher.search(query);

        assertNotNull("Results should not be null", results);
        assertTrue("Results should be greater than zero", results.scoreDocs.length > 0);
      }
    }
  }

  public void testPerformanceQueries() throws Exception {
    try (DirectoryReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {
      IndexSearcher searcher = newSearcher(reader);
      searcher.setSimilarity(new BM25Similarity());

      try (AnytimeRankingSearcher anytimeSearcher =
          new AnytimeRankingSearcher(reader, 15, 30, "content")) {
        Query query = new TermQuery(new Term("content", "performance"));
        TopDocs results = anytimeSearcher.search(query);

        assertNotNull("Results should not be null", results);
        assertTrue("Results should be greater than zero", results.scoreDocs.length > 0);
      }
    }
  }

  @Test
  public void testBM25ScoringDiversity() throws Exception {
    try (DirectoryReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {
      IndexSearcher searcher = newSearcher(reader);
      searcher.setSimilarity(new BM25Similarity());

      try (AnytimeRankingSearcher anytimeSearcher =
          new AnytimeRankingSearcher(reader, 10, 50, "content")) {
        Query query = new TermQuery(new Term("content", "scoring"));
        TopDocs results = anytimeSearcher.search(query);

        assertNotNull("Results should not be null", results);
        assertTrue("Results should be greater than zero", results.scoreDocs.length > 0);
      }
    }
  }

  @Test
  public void testEmptyQuery() throws Exception {
    try (DirectoryReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {
      IndexSearcher searcher = newSearcher(reader);
      searcher.setSimilarity(new BM25Similarity());

      try (AnytimeRankingSearcher anytimeSearcher =
          new AnytimeRankingSearcher(reader, 5, 20, "content")) {
        Query query = new TermQuery(new Term("content", " "));
        TopDocs results = anytimeSearcher.search(query);

        assertNotNull("Empty queries should return zero results", results);
        assertEquals("Results should be zero for an empty query", 0, results.scoreDocs.length);
      }
    }
  }

  @Test
  public void testBinAwareScoringImpact() throws Exception {
    String path = Files.createTempDirectory("test-bin-aware").toString();
    try (Directory dir = FSDirectory.open(Paths.get(path));
        IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {
      for (int i = 0; i < 100; i++) {
        Document doc = new Document();
        doc.add(new TextField("content", "lucene bin test", Field.Store.NO));
        doc.add(new IntPoint("docID", i));
        writer.addDocument(doc);
      }
      writer.commit();
    }

    try (DirectoryReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(indexPath)))) {
      try (AnytimeRankingSearcher ars = new AnytimeRankingSearcher(reader, 5, 20, "content")) {
        TopDocs results = ars.search(new TermQuery(new Term("content", "lucene")));
        assertTrue(results.scoreDocs.length > 0);
      }
    }
  }

  @Test
  public void testMultiSegmentIndexScoring() throws Exception {
    Directory dir = newDirectory();
    IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
    config.setMaxBufferedDocs(10); // force small segments
    try (IndexWriter writer = new IndexWriter(dir, config)) {
      for (int i = 0; i < 100; i++) {
        Document doc = new Document();
        doc.add(new TextField("content", "lucene segment scoring", Field.Store.NO));
        doc.add(new IntPoint("docID", i));
        writer.addDocument(doc);
      }
      writer.commit(); // multiple segments
    }

    IndexReader reader = DirectoryReader.open(dir);
      assertTrue("Should have multiple segments", reader.leaves().size() > 1);

      try (AnytimeRankingSearcher anytimeSearcher =
          new AnytimeRankingSearcher(reader, 10, 50, "content")) {
        TopDocs topDocs = anytimeSearcher.search(new TermQuery(new Term("content", "lucene")));

        assertNotNull("TopDocs should not be null", topDocs);
        assertTrue("Should return some results", topDocs.scoreDocs.length > 0);
      }
    dir.close();
  }

  private void addDocument(IndexWriter writer, String content, int docID) throws IOException {
    FieldType type = new FieldType();
    type.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    type.setTokenized(true);
    type.setStored(true);
    type.setStoreTermVectors(true);
    type.putAttribute("doBinning", "true");
    type.freeze();

    Document doc = new Document();
    doc.add(new IntPoint("docID", docID));
    doc.add(new StoredField("docID", docID));
    doc.add(new Field("content", content, type));

    writer.addDocument(doc);
  }
}
