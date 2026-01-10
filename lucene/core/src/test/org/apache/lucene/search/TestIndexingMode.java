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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestIndexingMode extends LuceneTestCase {

  public void testIndexingModePassedToWeight() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

    Document doc = new Document();
    doc.add(new TextField("field", "test", Field.Store.YES));
    writer.addDocument(doc);
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);

    // Test COLD mode
    searcher.setIndexingMode(IndexingMode.COLD);
    TermQuery query = new TermQuery(new Term("field", "test"));
    Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1.0f);
    assertEquals(IndexingMode.COLD, weight.indexingMode);

    // Test HOT mode
    searcher.setIndexingMode(IndexingMode.HOT);
    weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1.0f);
    assertEquals(IndexingMode.HOT, weight.indexingMode);

    // Test ADAPTIVE mode (default)
    searcher.setIndexingMode(IndexingMode.ADAPTIVE);
    weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1.0f);
    assertEquals(IndexingMode.ADAPTIVE, weight.indexingMode);

    reader.close();
    dir.close();
  }

  public void testDefaultIndexingModeIsAdaptive() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

    Document doc = new Document();
    doc.add(new TextField("field", "test", Field.Store.YES));
    writer.addDocument(doc);
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);

    // Default should be ADAPTIVE
    assertEquals(IndexingMode.ADAPTIVE, searcher.getIndexingMode());

    TermQuery query = new TermQuery(new Term("field", "test"));
    Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1.0f);
    assertEquals(IndexingMode.ADAPTIVE, weight.indexingMode);

    reader.close();
    dir.close();
  }

  public void testIndexingModeWithDifferentQueryTypes() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

    Document doc = new Document();
    doc.add(new TextField("field", "hello world", Field.Store.YES));
    writer.addDocument(doc);
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setIndexingMode(IndexingMode.HOT);

    // Test with TermQuery
    TermQuery termQuery = new TermQuery(new Term("field", "hello"));
    Weight termWeight = searcher.createWeight(termQuery, ScoreMode.COMPLETE, 1.0f);
    assertEquals(IndexingMode.HOT, termWeight.indexingMode);

    // Test with BooleanQuery
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(new TermQuery(new Term("field", "hello")), BooleanClause.Occur.SHOULD);
    builder.add(new TermQuery(new Term("field", "world")), BooleanClause.Occur.SHOULD);
    BooleanQuery boolQuery = builder.build();
    Weight boolWeight = searcher.createWeight(boolQuery, ScoreMode.COMPLETE, 1.0f);
    assertEquals(IndexingMode.HOT, boolWeight.indexingMode);

    // Test with ConstantScoreQuery
    ConstantScoreQuery csQuery = new ConstantScoreQuery(termQuery);
    Weight csWeight = searcher.createWeight(csQuery, ScoreMode.COMPLETE, 1.0f);
    assertEquals(IndexingMode.HOT, csWeight.indexingMode);

    // Test with BoostQuery
    BoostQuery boostQuery = new BoostQuery(termQuery, 2.0f);
    Weight boostWeight = searcher.createWeight(boostQuery, ScoreMode.COMPLETE, 1.0f);
    assertEquals(IndexingMode.HOT, boostWeight.indexingMode);

    // Test with DisjunctionMaxQuery
    DisjunctionMaxQuery dmQuery =
        new DisjunctionMaxQuery(
            java.util.Arrays.asList(
                new TermQuery(new Term("field", "hello")),
                new TermQuery(new Term("field", "world"))),
            0.1f);
    Weight dmWeight = searcher.createWeight(dmQuery, ScoreMode.COMPLETE, 1.0f);
    assertEquals(IndexingMode.HOT, dmWeight.indexingMode);

    reader.close();
    dir.close();
  }

  public void testIndexingModePreservedAcrossRewrite() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

    Document doc = new Document();
    doc.add(new TextField("field", "test", Field.Store.YES));
    writer.addDocument(doc);
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);
    searcher.setIndexingMode(IndexingMode.COLD);

    // Create a query that needs rewriting
    Query query = new TermQuery(new Term("field", "test"));
    Query rewritten = searcher.rewrite(query);

    // Create weight from rewritten query
    Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1.0f);
    assertEquals(IndexingMode.COLD, weight.indexingMode);

    reader.close();
    dir.close();
  }

  public void testWeightDefaultConstructorUsesAdaptive() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

    Document doc = new Document();
    doc.add(new TextField("field", "test", Field.Store.YES));
    writer.addDocument(doc);
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);

    // Use the 3-parameter createWeight (backward compatibility)
    TermQuery query = new TermQuery(new Term("field", "test"));
    Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE, 1.0f);

    // Should default to ADAPTIVE
    assertEquals(IndexingMode.ADAPTIVE, weight.indexingMode);

    reader.close();
    dir.close();
  }

  public void testIndexingModeAccessibleFromWeight() throws IOException {
    Directory dir = newDirectory();
    IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig());

    Document doc = new Document();
    doc.add(new TextField("field", "test", Field.Store.YES));
    writer.addDocument(doc);
    writer.close();

    DirectoryReader reader = DirectoryReader.open(dir);
    IndexSearcher searcher = new IndexSearcher(reader);

    for (IndexingMode mode : IndexingMode.values()) {
      searcher.setIndexingMode(mode);
      TermQuery query = new TermQuery(new Term("field", "test"));
      Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE, 1.0f);

      // Verify IndexingMode is accessible and correct
      assertEquals(mode, weight.indexingMode);

      // Verify search still works correctly
      TopDocs results = searcher.search(query, 10);
      assertEquals(1, results.totalHits.value());
    }

    reader.close();
    dir.close();
  }
}
