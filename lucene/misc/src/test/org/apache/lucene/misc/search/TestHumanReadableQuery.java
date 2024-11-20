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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.analysis.MockTokenizer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.search.CheckHits;
import org.apache.lucene.tests.util.English;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class TestHumanReadableQuery extends LuceneTestCase {
  private static IndexSearcher searcher;
  private static IndexReader reader;
  private static Directory directory;
  private static final String field = "field";

  @BeforeClass
  public static void beforeClass() throws Exception {
    directory = newDirectory();
    RandomIndexWriter writer =
        new RandomIndexWriter(
            random(),
            directory,
            newIndexWriterConfig(new MockAnalyzer(random(), MockTokenizer.SIMPLE, true))
                .setMaxBufferedDocs(TestUtil.nextInt(random(), 100, 1000))
                .setMergePolicy(newLogMergePolicy()));
    // writer.infoStream = System.out;
    for (int i = 0; i < 1000; i++) {
      Document doc = new Document();
      doc.add(newTextField(field, English.intToEnglish(i), Field.Store.YES));
      writer.addDocument(doc);
    }
    reader = writer.getReader();
    searcher = newSearcher(reader);
    writer.close();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    reader.close();
    directory.close();
    searcher = null;
    reader = null;
    directory = null;
  }

  public void testHitsEqualPhraseQuery() throws Exception {
    PhraseQuery pQuery = new PhraseQuery(field, "seventy", "seven");
    CheckHits.checkHits(
        random(),
        pQuery,
        field,
        searcher,
        new int[] {77, 177, 277, 377, 477, 577, 677, 777, 877, 977});

    String description = "TestPhraseQuery";
    HumanReadableQuery hQuery = new HumanReadableQuery(pQuery, description);
    CheckHits.checkHits(
        random(),
        hQuery,
        field,
        searcher,
        new int[] {77, 177, 277, 377, 477, 577, 677, 777, 877, 977});
    assertEquals(description + ":" + pQuery, hQuery.toString());
  }

  public void testHitsEqualBooleanQuery() throws Exception {
    BooleanQuery.Builder query = new BooleanQuery.Builder();
    query.add(new TermQuery(new Term(field, "seventy")), BooleanClause.Occur.MUST);
    query.add(new TermQuery(new Term(field, "seven")), BooleanClause.Occur.MUST);
    BooleanQuery bQuery = query.build();
    CheckHits.checkHits(
        random(),
        bQuery,
        field,
        searcher,
        new int[] {
          77, 177, 277, 377, 477, 577, 677, 770, 771, 772, 773, 774, 775, 776, 777, 778, 779, 877,
          977
        });

    String description = "TestBooleanQuery";
    HumanReadableQuery hQuery = new HumanReadableQuery(bQuery, description);
    CheckHits.checkHits(
        random(),
        hQuery,
        field,
        searcher,
        new int[] {
          77, 177, 277, 377, 477, 577, 677, 770, 771, 772, 773, 774, 775, 776, 777, 778, 779, 877,
          977
        });
    assertEquals(description + ":" + bQuery, hQuery.toString());
  }

  public void testKnnVectorQuery() {
    String description = "TestingKnnVectorQuery";
    KnnFloatVectorQuery kFVQ = new KnnFloatVectorQuery("f1", new float[] {0, 1}, 6);
    HumanReadableQuery hQuery = new HumanReadableQuery(kFVQ, description);
    assertEquals(description + ":" + kFVQ, hQuery.toString());
    assertEquals(kFVQ, hQuery.getWrappedQuery());
  }
}
