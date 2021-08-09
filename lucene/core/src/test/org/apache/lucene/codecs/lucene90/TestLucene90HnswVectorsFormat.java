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
package org.apache.lucene.codecs.lucene90;

import static com.carrotsearch.randomizedtesting.RandomizedTest.frequently;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.TestUtil;

public class TestLucene90HnswVectorsFormat extends BaseKnnVectorsFormatTestCase {
  @Override
  protected Codec getCodec() {
    return TestUtil.getDefaultCodec();
  }

  public void testSearchWithDeletes() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      final int numDocs = atLeast(100);
      final int dim = 30;
      int docIndex = 0;
      for (int i = 0; i < numDocs; ++i) {
        Document d = new Document();
        if (frequently()) {
          d.add(new StringField("id", String.valueOf(docIndex), Field.Store.YES));
          d.add(new KnnVectorField("vector", randomVector(dim)));
          docIndex++;
        } else {
          d.add(new StringField("other", "value", Field.Store.NO));
        }
        w.addDocument(d);
      }
      w.commit();

      Set<Term> toDelete = new HashSet<>();
      for (int i = 0; i < 20; i++) {
        int index = random().nextInt(docIndex);
        toDelete.add(new Term("id", String.valueOf(index)));
      }
      w.deleteDocuments(toDelete.toArray(new Term[0]));
      w.commit();

      try (IndexReader reader = DirectoryReader.open(dir)) {
        Set<String> allIds = new HashSet<>();
        for (LeafReaderContext context : reader.leaves()) {
          TopDocs topDocs =
              context.reader().searchNearestVectors("vector", randomVector(30), numDocs);
          for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            Document doc = reader.document(scoreDoc.doc, Set.of("id"));
            String id = doc.get("id");
            assertFalse(
                "search returned a deleted document: " + id, toDelete.contains(new Term("id", id)));
            allIds.add(id);
          }
        }
        assertEquals("search missed some documents", docIndex - toDelete.size(), allIds.size());
      }
    }
  }

  public void testSearchWithAllDeletes() throws IOException {
    try (Directory dir = newDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
      final int numDocs = atLeast(100);
      final int dim = 30;
      for (int i = 0; i < numDocs; ++i) {
        Document d = new Document();
        d.add(new KnnVectorField("vector", randomVector(dim)));
        w.addDocument(d);
      }
      w.commit();

      w.deleteDocuments(new MatchAllDocsQuery());
      w.commit();

      try (IndexReader reader = DirectoryReader.open(dir)) {
        for (LeafReaderContext context : reader.leaves()) {
          TopDocs topDocs =
              context.reader().searchNearestVectors("vector", randomVector(30), numDocs);
          assertEquals(0, topDocs.scoreDocs.length);
          assertTrue(topDocs.totalHits.value > 0L);
        }
      }
    }
  }
}
