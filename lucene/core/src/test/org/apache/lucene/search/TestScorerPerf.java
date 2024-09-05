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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;

public class TestScorerPerf extends LuceneTestCase {
  private final boolean validate = true; // set to false when doing performance testing

  private static FixedBitSet randBitSet(int sz, int numBitsToSet) {
    FixedBitSet set = new FixedBitSet(sz);
    for (int i = 0; i < numBitsToSet; i++) {
      set.set(random().nextInt(sz));
    }
    return set;
  }

  private static FixedBitSet[] randBitSets(int numSets, int setSize) {
    FixedBitSet[] sets = new FixedBitSet[numSets];
    for (int i = 0; i < sets.length; i++) {
      sets[i] = randBitSet(setSize, random().nextInt(setSize));
    }
    return sets;
  }

  private static final class CountingHitCollectorManager
      implements CollectorManager<CountingHitCollector, CountingHitCollector> {

    @Override
    public CountingHitCollector newCollector() {
      return new CountingHitCollector();
    }

    @Override
    public CountingHitCollector reduce(Collection<CountingHitCollector> collectors) {
      CountingHitCollector result = new CountingHitCollector();
      for (CountingHitCollector collector : collectors) {
        result.count += collector.count;
        result.sum += collector.sum;
      }
      return result;
    }
  }

  private static class CountingHitCollector extends SimpleCollector {
    int count = 0;
    int sum = 0;
    protected int docBase = 0;

    @Override
    public void collect(int doc) {
      count++;
      sum += docBase + doc; // use it to avoid any possibility of being eliminated by hotspot
    }

    public int getCount() {
      return count;
    }

    @Override
    protected void doSetNextReader(LeafReaderContext context) {
      docBase = context.docBase;
    }

    @Override
    public ScoreMode scoreMode() {
      return ScoreMode.COMPLETE_NO_SCORES;
    }
  }

  private static class BitSetQuery extends Query {

    private final FixedBitSet docs;

    BitSetQuery(FixedBitSet docs) {
      this.docs = docs;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
      return new ConstantScoreWeight(this, boost) {
        @Override
        public Scorer scorer(LeafReaderContext context) throws IOException {
          return new ConstantScoreScorer(
              this, score(), scoreMode, new BitSetIterator(docs, docs.approximateCardinality()));
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
          return false;
        }
      };
    }

    @Override
    public void visit(QueryVisitor visitor) {}

    @Override
    public String toString(String field) {
      return "randomBitSetFilter";
    }

    @Override
    public boolean equals(Object other) {
      return sameClassAs(other) && docs.equals(((BitSetQuery) other).docs);
    }

    @Override
    public int hashCode() {
      return 31 * classHash() + docs.hashCode();
    }
  }

  private FixedBitSet addClause(FixedBitSet[] sets, BooleanQuery.Builder bq, FixedBitSet result) {
    final FixedBitSet rnd = sets[random().nextInt(sets.length)];
    Query q = new BitSetQuery(rnd);
    bq.add(q, BooleanClause.Occur.MUST);
    if (validate) {
      if (result == null) {
        result = rnd.clone();
      } else {
        result.and(rnd);
      }
    }
    return result;
  }

  private void doConjunctions(IndexSearcher s, FixedBitSet[] sets, int iter, int maxClauses)
      throws IOException {

    for (int i = 0; i < iter; i++) {
      int nClauses = random().nextInt(maxClauses - 1) + 2; // min 2 clauses
      BooleanQuery.Builder bq = new BooleanQuery.Builder();
      FixedBitSet result = null;
      for (int j = 0; j < nClauses; j++) {
        result = addClause(sets, bq, result);
      }
      CountingHitCollector hc = s.search(bq.build(), new CountingHitCollectorManager());

      if (validate) {
        assertEquals(result.cardinality(), hc.getCount());
      }
    }
  }

  private void doNestedConjunctions(
      IndexSearcher s, FixedBitSet[] sets, int iter, int maxOuterClauses, int maxClauses)
      throws IOException {
    long nMatches = 0;

    for (int i = 0; i < iter; i++) {
      int oClauses = random().nextInt(maxOuterClauses - 1) + 2;
      BooleanQuery.Builder oq = new BooleanQuery.Builder();
      FixedBitSet result = null;

      for (int o = 0; o < oClauses; o++) {

        int nClauses = random().nextInt(maxClauses - 1) + 2; // min 2 clauses
        BooleanQuery.Builder bq = new BooleanQuery.Builder();
        for (int j = 0; j < nClauses; j++) {
          result = addClause(sets, bq, result);
        }

        oq.add(bq.build(), BooleanClause.Occur.MUST);
      } // outer

      CountingHitCollector hc = s.search(oq.build(), new CountingHitCollectorManager());
      nMatches += hc.getCount();
      if (validate) {
        assertEquals(result.cardinality(), hc.getCount());
      }
    }
    if (VERBOSE) {
      System.out.println("Average number of matches=" + (nMatches / iter));
    }
  }

  public void testConjunctions() throws Exception {
    // test many small sets... the bugs will be found on boundary conditions
    try (Directory d = newDirectory()) {
      IndexWriter iw = new IndexWriter(d, newIndexWriterConfig(new MockAnalyzer(random())));
      iw.addDocument(new Document());
      iw.close();

      try (DirectoryReader r = DirectoryReader.open(d)) {
        IndexSearcher s = newSearcher(r);
        s.setQueryCache(null);

        FixedBitSet[] sets = randBitSets(atLeast(1000), atLeast(10));

        int iterations = TEST_NIGHTLY ? atLeast(10000) : atLeast(500);
        doConjunctions(s, sets, iterations, atLeast(5));
        doNestedConjunctions(s, sets, iterations, atLeast(3), atLeast(3));
      }
    }
  }
}
