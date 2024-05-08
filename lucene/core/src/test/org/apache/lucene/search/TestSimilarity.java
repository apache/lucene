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
import java.util.Collection;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

/** Similarity unit test. */
public class TestSimilarity extends LuceneTestCase {

  public static class SimpleSimilarity extends ClassicSimilarity {
    @Override
    public float lengthNorm(int length) {
      return 1;
    }

    @Override
    public float tf(float freq) {
      return freq;
    }

    @Override
    public float idf(long docFreq, long docCount) {
      return 1.0f;
    }

    @Override
    public Explanation idfExplain(CollectionStatistics collectionStats, TermStatistics[] stats) {
      return Explanation.match(1.0f, "Inexplicable");
    }
  }

  public void testSimilarity() throws Exception {
    Directory store = newDirectory();
    RandomIndexWriter writer =
        new RandomIndexWriter(
            random(),
            store,
            newIndexWriterConfig(new MockAnalyzer(random()))
                .setSimilarity(new SimpleSimilarity())
                .setMergePolicy(newMergePolicy(random(), false)));

    Document d1 = new Document();
    d1.add(newTextField("field", "a c", Field.Store.YES));

    Document d2 = new Document();
    d2.add(newTextField("field", "a c b", Field.Store.YES));

    writer.addDocument(d1);
    writer.addDocument(d2);
    IndexReader reader = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(reader);
    searcher.setSimilarity(new SimpleSimilarity());

    Term a = new Term("field", "a");
    Term b = new Term("field", "b");
    Term c = new Term("field", "c");

    assertScore(searcher, new TermQuery(b), 1.0f);

    BooleanQuery.Builder bq = new BooleanQuery.Builder();
    bq.add(new TermQuery(a), BooleanClause.Occur.SHOULD);
    bq.add(new TermQuery(b), BooleanClause.Occur.SHOULD);
    // System.out.println(bq.toString("field"));
    searcher.search(
        bq.build(),
        new CollectorManager<SimpleCollector, Void>() {
          @Override
          public SimpleCollector newCollector() {
            return new ScoreAssertingCollector() {
              private int base = 0;

              @Override
              public void collect(int doc) throws IOException {
                // System.out.println("Doc=" + doc + " score=" + score);
                assertEquals((float) doc + base + 1, scorer.score(), 0);
              }

              @Override
              protected void doSetNextReader(LeafReaderContext context) {
                base = context.docBase;
              }
            };
          }

          @Override
          public Void reduce(Collection<SimpleCollector> collectors) {
            return null;
          }
        });

    PhraseQuery pq = new PhraseQuery(a.field(), a.bytes(), c.bytes());
    // System.out.println(pq.toString("field"));
    assertScore(searcher, pq, 1.0f);

    pq = new PhraseQuery(2, a.field(), a.bytes(), b.bytes());
    // System.out.println(pq.toString("field"));
    assertScore(searcher, pq, 0.5f);

    reader.close();
    store.close();
  }

  private static void assertScore(IndexSearcher searcher, Query query, float score)
      throws IOException {
    searcher.search(
        query,
        new CollectorManager<SimpleCollector, Void>() {
          @Override
          public SimpleCollector newCollector() {
            return new ScoreAssertingCollector() {
              @Override
              public final void collect(int doc) throws IOException {
                // System.out.println("Doc=" + doc + " score=" + score);
                assertEquals(score, scorer.score(), 0);
              }
            };
          }

          @Override
          public Void reduce(Collection<SimpleCollector> collectors) {
            return null;
          }
        });
  }

  private abstract static class ScoreAssertingCollector extends SimpleCollector {
    Scorable scorer;

    @Override
    public final void setScorer(Scorable scorer) {
      this.scorer = scorer;
    }

    @Override
    public final ScoreMode scoreMode() {
      return ScoreMode.COMPLETE;
    }
  }
}
