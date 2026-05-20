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
package org.apache.lucene.queries.function;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queries.function.valuesource.NormValueSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.CheckHits;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestLongNormValueSource extends LuceneTestCase {

  public void testNorm() throws Exception {
    Similarity sim = new ClassicSimilarity();
    final var field = "text";
    var analyzer = new MockAnalyzer(random());
    var dir = newDirectory();
    IndexWriterConfig iwConfig = newIndexWriterConfig(analyzer);
    iwConfig.setMergePolicy(newLogMergePolicy());
    iwConfig.setSimilarity(sim);

    try (RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwConfig)) {

      // the leading number is the number of terms / positions for that value
      final var store = Store.YES;
      Document doc = new Document();
      doc.add(new StringField("id", "6", store));
      doc.add(new TextField(field, "this is a test test test", store));
      iw.addDocument(doc);

      doc = new Document();
      doc.add(new StringField("id", "2", store));
      doc.add(new TextField(field, "second test", store));
      iw.addDocument(doc);

      IndexSearcher searcher;
      try (var reader = iw.getReader()) {
        searcher = newSearcher(reader);
        searcher.setSimilarity(sim);

        Query q = new FunctionQuery(new NormValueSource(field));
        TopDocs docs = searcher.search(q, searcher.getIndexReader().numDocs(), new Sort(), true);
        final var expectedDocIds = new int[] {1, 0};
        final var expectedScoreDocs =
            new ScoreDoc[] {new ScoreDoc(1, 0.70710677f), new ScoreDoc(0, 0.4082483f)};
        CheckHits.checkHitsQuery(q, expectedScoreDocs, docs.scoreDocs, expectedDocIds);
        CheckHits.checkExplanations(q, "", searcher);
        final var storedFields = searcher.getIndexReader().storedFields();
        assertEquals("2", storedFields.document(expectedDocIds[0]).get("id"));
        assertEquals("6", storedFields.document(expectedDocIds[1]).get("id"));
      }
    } finally {
      dir.close();
      analyzer.close();
    }
  }
}
