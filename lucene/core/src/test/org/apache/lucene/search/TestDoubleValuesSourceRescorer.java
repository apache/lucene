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

import java.util.Arrays;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestDoubleValuesSourceRescorer extends LuceneTestCase {

  private static final String ID_FIELD = "id";
  private static final String DOC_VAL_FIELD = "docVal";
  private static final String DOC_VAL_STORED_FIELD = "storedDocVal";

  private final DoubleValuesSource doubleValuesSource =
      DoubleValuesSource.fromIntField(DOC_VAL_FIELD);

  private final DoubleValuesSourceRescorer rescorer =
      new DoubleValuesSourceRescorer(doubleValuesSource) {
        @Override
        protected float combine(float firstPassScore, boolean valuePresent, double sourceValue) {
          return valuePresent ? (float) sourceValue : 0f;
        }
      };

  private static final List<String> dictionary =
      Arrays.asList(
          "river", "quick", "brown", "fox", "jumped", "lazy", "fence", "wizard", "of", "a", "an",
          "the", "cookie", "golf", "golden", "tennis", "boy", "plays", "likes", "wants");

  String randomSentence() {
    final int length = random().nextInt(3, 10);
    StringBuilder sentence = new StringBuilder();
    for (int i = 0; i < length; i++) {
      sentence.append(dictionary.get(random().nextInt(dictionary.size() - 1)) + " ");
    }
    return sentence.toString();
  }

  private void publishDocs(int numDocs, String fieldName, boolean indexDocValues, Directory dir)
      throws Exception {
    RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
    for (int i = 0; i < numDocs; i++) {
      Document d = new Document();
      d.add(newStringField(ID_FIELD, Integer.toString(i), Field.Store.YES));
      d.add(newTextField(fieldName, randomSentence(), Field.Store.NO));
      if (indexDocValues) {
        int val = i + 100;
        d.add(new NumericDocValuesField(DOC_VAL_FIELD, val));
        d.add(newStringField(DOC_VAL_STORED_FIELD, Integer.toString(val), Field.Store.YES));
      }
      w.addDocument(d);
    }
    w.close();
  }

  public void testBasic() throws Exception {
    try (Directory dir = newDirectory()) {
      publishDocs(random().nextInt(50, 100), "title", true, dir);
      try (IndexReader r = DirectoryReader.open(dir)) {
        IndexSearcher s = new IndexSearcher(r);
        TermQuery query =
            new TermQuery(
                new Term("title", dictionary.get(random().nextInt(dictionary.size() - 1))));
        TopDocs queryHits = s.search(query, 50);
        TopDocs rescoredHits = rescorer.rescore(s, queryHits, 15);
        assertTrue(rescoredHits.scoreDocs.length <= 15);
        assertEquals(queryHits.totalHits, rescoredHits.totalHits);
        for (int i = 1; i < rescoredHits.scoreDocs.length; i++) {
          assertTrue(rescoredHits.scoreDocs[i - 1].score > rescoredHits.scoreDocs[i].score);
        }
        for (ScoreDoc hit : rescoredHits.scoreDocs) {
          assertEquals(
              s.storedFields().document(hit.doc).get(DOC_VAL_STORED_FIELD),
              Integer.toString((int) hit.score));
        }
        int doc = rescoredHits.scoreDocs[0].doc;
        Explanation e = rescorer.explain(s, s.explain(query, doc), doc);
        String msg = e.toString();
        assertTrue(msg.contains("combined score from firstPass and DoubleValuesSource"));
        assertTrue(msg.contains(getClass().toString()));
        assertTrue(msg.contains("first pass score"));
        assertTrue(msg.contains("value from DoubleValuesSource"));
      }
    }
  }

  public void testSubsetAndIdempotency() throws Exception {
    try (Directory dir = newDirectory()) {
      publishDocs(random().nextInt(60, 200), "title", true, dir);
      try (IndexReader r = DirectoryReader.open(dir)) {
        IndexSearcher s = new IndexSearcher(r);
        TermQuery query =
            new TermQuery(
                new Term("title", dictionary.get(random().nextInt(dictionary.size() - 1))));
        TopDocs queryHits = s.search(query, 50);
        TopDocs rescoredHits1 = rescorer.rescore(s, queryHits, 15);

        int hits1Len = rescoredHits1.scoreDocs.length;
        int hit2N = Math.max(hits1Len / 2, 1);
        TopDocs rescoredHits2 = rescorer.rescore(s, queryHits, hit2N);
        assertEquals(hit2N, rescoredHits2.scoreDocs.length);
        for (int i = 0; i < hit2N; i++) {
          assertEquals(rescoredHits1.scoreDocs[i].doc, rescoredHits2.scoreDocs[i].doc);
          assertEquals(rescoredHits1.scoreDocs[i].score, rescoredHits2.scoreDocs[i].score, 1e-5);
        }
      }
    }
  }

  public void testMissingValues() throws Exception {
    try (Directory dir = newDirectory()) {
      publishDocs(random().nextInt(60, 200), "title", false, dir);
      try (IndexReader r = DirectoryReader.open(dir)) {
        IndexSearcher s = new IndexSearcher(r);
        TermQuery query =
            new TermQuery(
                new Term("title", dictionary.get(random().nextInt(dictionary.size() - 1))));
        TopDocs queryHits = s.search(query, 50);
        TopDocs rescoredHits = rescorer.rescore(s, queryHits, 15);
        assertTrue(rescoredHits.scoreDocs.length <= 15);
        assertEquals(queryHits.totalHits, rescoredHits.totalHits);
        for (int i = 0; i < rescoredHits.scoreDocs.length; i++) {
          assertEquals(rescoredHits.scoreDocs[i].score, 0f, 1e-5);
          if (i > 0) {
            assertTrue(rescoredHits.scoreDocs[i - 1].doc < rescoredHits.scoreDocs[i].doc);
          }
        }
        int doc = rescoredHits.scoreDocs[0].doc;
        Explanation e = rescorer.explain(s, s.explain(query, doc), doc);
        String msg = e.toString();
        assertTrue(msg.contains("combined score from firstPass and DoubleValuesSource"));
        assertTrue(msg.contains(getClass().toString()));
        assertTrue(msg.contains("first pass score"));
        assertTrue(msg.contains("no value in DoubleValuesSource"));
      }
    }
  }
}
