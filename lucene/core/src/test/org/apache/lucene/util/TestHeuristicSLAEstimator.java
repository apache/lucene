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
package org.apache.lucene.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.Before;
import org.junit.Test;

public class TestHeuristicSLAEstimator extends LuceneTestCase {

  private static final String FIELD = "content";
  private IndexReader reader;
  private HeuristicSLAEstimator estimator;

  @Before
  public void setup() throws Exception {
    String indexPath = Files.createTempDirectory("testindex1").toAbsolutePath().toString();
    Directory dir = FSDirectory.open(Paths.get(indexPath));
    IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer());
    IndexWriter writer = new IndexWriter(dir, config);

    for (int i = 0; i < 100; i++) {
      Document doc = new Document();
      doc.add(
          new TextField(
              FIELD, "lucene search relevance ranking performance document", Field.Store.NO));
      writer.addDocument(doc);
    }

    writer.commit();
    writer.close();

    reader = DirectoryReader.open(dir);
    estimator = new HeuristicSLAEstimator(FIELD);
  }

  @Test
  public void testTermQueryEstimate() throws IOException {
    Query query = new TermQuery(new Term(FIELD, "lucene"));
    double estimate = estimator.estimate(query, reader, 100);
    assertTrue("Estimate should be >= base SLA", estimate >= 100);
  }

  @Test
  public void testPhraseQueryEstimate() throws IOException {
    PhraseQuery query = new PhraseQuery(FIELD, "lucene", "search", "ranking");
    double estimate = estimator.estimate(query, reader, 100);
    assertTrue("Phrase query should increase SLA", estimate > 100);
  }

  @Test
  public void testBooleanQueryEstimate() throws IOException {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    builder.add(new TermQuery(new Term(FIELD, "lucene")), BooleanClause.Occur.MUST);
    builder.add(new TermQuery(new Term(FIELD, "search")), BooleanClause.Occur.SHOULD);
    Query query = builder.build();
    double estimate = estimator.estimate(query, reader, 100);
    assertTrue("Boolean query should increase SLA", estimate > 100);
  }

  @Test
  public void testDisjunctionMaxQueryEstimate() throws IOException {
    DisjunctionMaxQuery query =
        new DisjunctionMaxQuery(
            java.util.Arrays.asList(
                new TermQuery(new Term(FIELD, "lucene")),
                new TermQuery(new Term(FIELD, "ranking"))),
            0.1f);
    double estimate = estimator.estimate(query, reader, 100);
    assertTrue("DisjunctionMaxQuery should increase SLA", estimate > 100);
  }

  @Test
  public void testNestedBooleanQueryEstimate() throws IOException {
    BooleanQuery.Builder inner = new BooleanQuery.Builder();
    inner.add(new TermQuery(new Term(FIELD, "lucene")), BooleanClause.Occur.MUST);
    inner.add(new TermQuery(new Term(FIELD, "search")), BooleanClause.Occur.MUST);

    BooleanQuery.Builder outer = new BooleanQuery.Builder();
    outer.add(inner.build(), BooleanClause.Occur.MUST);
    outer.add(new TermQuery(new Term(FIELD, "ranking")), BooleanClause.Occur.SHOULD);

    Query query = outer.build();
    double estimate = estimator.estimate(query, reader, 100);
    assertTrue("Nested Boolean query should increase SLA", estimate > 100);
  }

  @Test
  public void testMatchAllDocsQueryEstimate() throws IOException {
    Query query = new MatchAllDocsQuery();
    double estimate = estimator.estimate(query, reader, 100);
    assertTrue("MatchAllDocsQuery should be close to base SLA", estimate >= 100);
  }
}
