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
package org.apache.lucene.search.similarities;

import java.io.IOException;
import java.util.Random;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.search.similarities.BaseSimilarityTestCase;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.IOUtils;

public class TestRawTFSimilarity extends BaseSimilarityTestCase {

  private Directory directory;
  private IndexReader indexReader;
  private IndexSearcher indexSearcher;

  @Override
  protected Similarity getSimilarity(Random random) {
    return new RawTFSimilarity();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    directory = newDirectory();
    try (IndexWriter indexWriter = new IndexWriter(directory, newIndexWriterConfig())) {
      final Document document1 = new Document();
      final Document document2 = new Document();
      final Document document3 = new Document();
      document1.add(LuceneTestCase.newTextField("test", "one", Field.Store.YES));
      document2.add(LuceneTestCase.newTextField("test", "two two", Field.Store.YES));
      document3.add(LuceneTestCase.newTextField("test", "three three three", Field.Store.YES));
      indexWriter.addDocument(document1);
      indexWriter.addDocument(document2);
      indexWriter.addDocument(document3);
      indexWriter.commit();
    }
    indexReader = DirectoryReader.open(directory);
    indexSearcher = newSearcher(indexReader);
    indexSearcher.setSimilarity(new RawTFSimilarity());
  }

  @Override
  public void tearDown() throws Exception {
    IOUtils.close(indexReader, directory);
    super.tearDown();
  }

  public void testOne() throws IOException {
    implTest("one", 1f);
  }

  public void testTwo() throws IOException {
    implTest("two", 2f);
  }

  public void testThree() throws IOException {
    implTest("three", 3f);
  }

  private void implTest(String text, float expectedScore) throws IOException {
    Query query = new TermQuery(new Term("test", text));
    TopDocs topDocs = indexSearcher.search(query, 1);
    assertEquals(1, topDocs.totalHits.value());
    assertEquals(1, topDocs.scoreDocs.length);
    assertEquals(expectedScore, topDocs.scoreDocs[0].score, 0.0);
  }

  public void testBoostQuery() throws IOException {
    Query query = new TermQuery(new Term("test", "three"));
    float boost = 14f;
    TopDocs topDocs = indexSearcher.search(new BoostQuery(query, boost), 1);
    assertEquals(1, topDocs.totalHits.value());
    assertEquals(1, topDocs.scoreDocs.length);
    assertEquals(42f, topDocs.scoreDocs[0].score, 0.0);
  }
}
