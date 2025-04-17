/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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
import org.apache.lucene.index.BinScoreLeafReader;
import org.apache.lucene.index.BinScoreReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;

/** SLA-aware collector that boosts score based on bin distribution. */
public final class AnytimeRankingCollector extends TopDocsCollector<ScoreDoc> {

  private static final int DOCS_PER_CHECK = 100;
  private static final int MIN_CHECK_INTERVAL = 32;
  private static final int MAX_CHECK_INTERVAL = 512;

  private final long cutoffNanos;
  private final long start;
  private final float[] binBoosts;

  private int count = 0;

  AnytimeRankingCollector(int topK, long cutoffNanos, float[] binBoosts) {
    super(new HitQueue(topK, false));
    this.cutoffNanos = cutoffNanos;
    this.start = System.nanoTime();
    this.binBoosts = binBoosts;
  }

  @Override
  protected int topDocsSize() {
    return pq.size();
  }

  @Override
  protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
    return new TopDocs(
        new TotalHits(totalHits, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
        results != null ? results : EMPTY_TOPDOCS.scoreDocs);
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext ctx) throws IOException {
    final int base = ctx.docBase;
    final LeafReader reader = ctx.reader();
    final BinScoreReader binReader =
        reader instanceof BinScoreLeafReader
            ? ((BinScoreLeafReader) reader).getBinScoreReader()
            : null;

    int interval =
        Math.max(
            MIN_CHECK_INTERVAL, Math.min(MAX_CHECK_INTERVAL, reader.numDocs() / DOCS_PER_CHECK));

    return new LeafCollector() {
      private Scorable scorer;

      @Override
      public void setScorer(Scorable scorer) {
        this.scorer = scorer;
      }

      @Override
      public void collect(int doc) throws IOException {
        float score = scorer.score();
        if (score <= 0 || Float.isNaN(score)) {
          return;
        }

        int globalDoc = doc + base;
        if (binReader != null && binBoosts != null) {
          int bin = binReader.getBinForDoc(doc);
          if (bin >= 0 && bin < binBoosts.length) {
            score *= binBoosts[bin];
          }
        }

        if (pq.insertWithOverflow(new ScoreDoc(globalDoc, score)) != null) {
          totalHits++;
        }

        if (++count % interval == 0 && System.nanoTime() - start > cutoffNanos) {
          throw new CollectionTerminatedException();
        }
      }
    };
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.TOP_SCORES;
  }
}
