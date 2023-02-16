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

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomFloat;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestDocAndScoreQuery extends LuceneTestCase {
  public void testBasics() throws IOException {
    try (Directory directory = newDirectory()) {
      final DirectoryReader reader;
      try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
        for (int i = 0; i < 50; i++) {
          Document doc = new Document();
          doc.add(new StringField("field", "value" + i, Field.Store.NO));
          iw.addDocument(doc);
          if (i % 10 == 0) {
            iw.flush();
          }
        }
        reader = iw.getReader();
      }
      try (reader) {
        IndexSearcher searcher = LuceneTestCase.newSearcher(reader);
        List<ScoreDoc> scoreDocsList = new ArrayList<>();
        for (int doc = 0; doc < 50; doc += 1 + random().nextInt(5)) {
          scoreDocsList.add(new ScoreDoc(doc, randomFloat()));
        }
        ScoreDoc[] scoreDocs = scoreDocsList.toArray(new ScoreDoc[0]);
        int[] docs = new int[scoreDocs.length];
        float[] scores = new float[scoreDocs.length];
        for (int i = 0; i < scoreDocs.length; i++) {
          docs[i] = scoreDocs[i].doc;
          scores[i] = scoreDocs[i].score;
        }
        int[] segments = AbstractKnnVectorQuery.findSegmentStarts(reader, docs);

        AbstractKnnVectorQuery.DocAndScoreQuery query =
            new AbstractKnnVectorQuery.DocAndScoreQuery(
                docs, scores, segments, reader.getContext().id());
        final Weight w = query.createWeight(searcher, ScoreMode.TOP_SCORES, 1.0f);
        TopDocs topDocs = searcher.search(query, 100);
        assertEquals(scoreDocs.length, topDocs.totalHits.value);
        assertEquals(TotalHits.Relation.EQUAL_TO, topDocs.totalHits.relation);
        Arrays.sort(topDocs.scoreDocs, Comparator.comparingInt(scoreDoc -> scoreDoc.doc));
        assertEquals(scoreDocs.length, topDocs.scoreDocs.length);
        for (int i = 0; i < scoreDocs.length; i++) {
          assertEquals(scoreDocs[i].doc, topDocs.scoreDocs[i].doc);
          assertEquals(scoreDocs[i].score, topDocs.scoreDocs[i].score, 0.0001f);
          assertTrue(searcher.explain(query, scoreDocs[i].doc).isMatch());
        }

        for (LeafReaderContext leafReaderContext : searcher.getLeafContexts()) {
          Scorer scorer = w.scorer(leafReaderContext);
          // If we have matching docs, the score should always be greater than 0 for that segment
          if (scorer != null) {
            assertTrue(leafReaderContext.toString(), scorer.getMaxScore(NO_MORE_DOCS) > 0.0f);
          }
        }
      }
    }
  }
}
