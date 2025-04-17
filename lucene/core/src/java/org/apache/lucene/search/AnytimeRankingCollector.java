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
import java.util.Locale;

import org.apache.lucene.index.BinScoreLeafReader;
import org.apache.lucene.index.BinScoreReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;

/**
 * SLA-aware collector that boosts score based on bin distribution.
 *
 * <p>This collector integrates bin-based scoring boosts with SLA-aware early termination. It wraps
 * segment-level LeafReaders that support bin lookups using {@link BinScoreReader}.
 *
 * <p>The caller must ensure the IndexReader is wrapped with {@code BinScoreUtil.wrap(...)},
 * otherwise bin information will not be available and boosting will be skipped.
 */
public final class AnytimeRankingCollector extends TopDocsCollector<ScoreDoc> {

  private static final int DOCS_PER_CHECK = 100;
  private static final int MIN_CHECK_INTERVAL = 32;
  private static final int MAX_CHECK_INTERVAL = 512;

  private final long cutoffNanos;
  private final long start;
  private final float[] binBoosts;

  private int count = 0;

  /**
   * Constructs a ranking collector with SLA cutoff and per-bin boosting.
   *
   * @param topK maximum number of documents to return
   * @param cutoffNanos SLA budget in nanoseconds
   * @param binBoosts array of boost multipliers by bin index
   */
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
        (reader instanceof BinScoreLeafReader)
            ? ((BinScoreLeafReader) reader).getBinScoreReader()
            : null;

    final int interval =
        Math.max(MIN_CHECK_INTERVAL, Math.min(MAX_CHECK_INTERVAL, reader.numDocs() / DOCS_PER_CHECK));

    return new LeafCollector() {
      private Scorable scorer;

      @Override
      public void setScorer(Scorable scorer) {
        this.scorer = scorer;
      }

      @Override
      public void collect(int doc) throws IOException {
        float rawScore = scorer.score();

        float boost = 1.0f;
        int bin = -1;
        if (binReader != null && binBoosts != null) {
          bin = binReader.getBinForDoc(doc);
          if (bin >= 0 && bin < binBoosts.length) {
            boost = binBoosts[bin];
          }
        }

        float finalScore = rawScore * boost;

        System.out.printf(
            Locale.ROOT,
            "doc=%d raw=%.4f boost=%.2f bin=%d final=%.4f%n",
            doc + base, rawScore, boost, bin, finalScore
        );

        if (finalScore <= 0 || Float.isNaN(finalScore)) {
          return;
        }

        if (pq.insertWithOverflow(new ScoreDoc(doc + base, finalScore)) != null) {
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