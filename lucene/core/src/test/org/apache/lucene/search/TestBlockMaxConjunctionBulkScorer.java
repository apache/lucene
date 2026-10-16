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
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestBlockMaxConjunctionBulkScorer extends LuceneTestCase {

  /**
   * When the windows at the end of a segment are not competitive, the lead is not advanced, and
   * scoring must still stop at maxDoc rather than iterating windows up to the provided max
   * (NO_MORE_DOCS for whole-segment partitions).
   */
  public void testDoesNotIterateWindowsBeyondMaxDoc() throws IOException {
    try (Directory dir = newDirectory()) {
      final int numDocs = 200_000;
      final String longTail = " w w w w w w w w w w w w w w w w w w w w";
      // impacts-based max scores are needed to reproduce, so use the default codec rather than a
      // random one
      try (IndexWriter w =
          new IndexWriter(dir, newIndexWriterConfig().setCodec(TestUtil.getDefaultCodec()))) {
        for (int i = 0; i < numDocs; i++) {
          // f1 is a short field: these docs fill the top-k with high scores
          String f1 = i < 1000 ? "a b" : "x";
          String f2;
          if (i < 1000) {
            f2 = "z";
          } else if (i < 3000) {
            f2 = "b" + longTail; // makes the "b" clause costlier, so the "a" clause leads
          } else if (i == numDocs - 1) {
            f2 = "a b" + longTail; // last match, only through the low-scoring f2 field
          } else {
            f2 = (i % 2 == 0 ? "a" : "b") + longTail; // frequent in f2, never both
          }
          Document doc = new Document();
          doc.add(new TextField("f1", f1, Store.NO));
          doc.add(new TextField("f2", f2, Store.NO));
          w.addDocument(doc);
        }
        w.forceMerge(1);
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        IndexSearcher searcher = new IndexSearcher(reader);
        Query query =
            new BooleanQuery.Builder()
                .add(fieldDisjunction("a"), Occur.MUST)
                .add(fieldDisjunction("b"), Occur.MUST)
                .build();
        BooleanWeight weight =
            (BooleanWeight)
                searcher.createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1f);
        LeafReaderContext ctx = reader.leaves().get(0);
        List<CountingScorer> scorers = new ArrayList<>();
        for (BooleanWeight.WeightedBooleanClause clause : weight.weightedClauses) {
          scorers.add(new CountingScorer(clause.weight.scorerSupplier(ctx).get(Long.MAX_VALUE)));
        }
        BulkScorer bulkScorer =
            new BlockMaxConjunctionBulkScorer(ctx.reader().maxDoc(), new ArrayList<>(scorers));

        int next =
            bulkScorer.score(new TopKLeafCollector(10), null, 0, DocIdSetIterator.NO_MORE_DOCS);

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, next);
        long advanceShallowCalls = scorers.stream().mapToLong(s -> s.advanceShallowCalls).sum();
        // Every window calls advanceShallow on each clause; there are at most a few windows per
        // postings block, nowhere near the ~32K windows needed to reach Integer.MAX_VALUE.
        assertTrue(
            "too many advanceShallow calls: " + advanceShallowCalls, advanceShallowCalls < 10_000);
      }
    }
  }

  private static Query fieldDisjunction(String term) {
    return new BooleanQuery.Builder()
        .add(new TermQuery(new Term("f1", term)), Occur.SHOULD)
        .add(new TermQuery(new Term("f2", term)), Occur.SHOULD)
        .build();
  }

  /** Keeps the top-k scores and sets the min competitive score like TopScoreDocCollector. */
  private static class TopKLeafCollector implements LeafCollector {
    private final float[] topK;
    private int size;
    private Scorable scorer;

    TopKLeafCollector(int k) {
      topK = new float[k];
    }

    @Override
    public void setScorer(Scorable scorer) {
      this.scorer = scorer;
    }

    @Override
    public void collect(int doc) throws IOException {
      float score = scorer.score();
      if (size < topK.length) {
        topK[size++] = score;
      } else if (score > topK[0]) {
        topK[0] = score;
      } else {
        return;
      }
      if (size == topK.length) {
        java.util.Arrays.sort(topK);
        scorer.setMinCompetitiveScore(Math.nextUp(topK[0]));
      }
    }
  }

  private static class CountingScorer extends Scorer {
    private final Scorer in;
    long advanceShallowCalls;

    CountingScorer(Scorer in) {
      this.in = in;
    }

    @Override
    public int docID() {
      return in.docID();
    }

    @Override
    public DocIdSetIterator iterator() {
      return in.iterator();
    }

    @Override
    public TwoPhaseIterator twoPhaseIterator() {
      return in.twoPhaseIterator();
    }

    @Override
    public float score() throws IOException {
      return in.score();
    }

    @Override
    public int advanceShallow(int target) throws IOException {
      advanceShallowCalls++;
      return in.advanceShallow(target);
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
      return in.getMaxScore(upTo);
    }

    @Override
    public void setMinCompetitiveScore(float minScore) throws IOException {
      in.setMinCompetitiveScore(minScore);
    }
  }
}
