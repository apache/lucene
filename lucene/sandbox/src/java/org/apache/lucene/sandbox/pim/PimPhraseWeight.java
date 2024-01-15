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

package org.apache.lucene.sandbox.pim;

import org.apache.lucene.sandbox.sdk.DpuException;
import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.LeafSimScorer;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/** Weight class for phrase search on PIM system */
public class PimPhraseWeight extends Weight {

  private final PimPhraseScoreStats scoreStats;

  protected PimPhraseWeight(PimPhraseQuery query, PimPhraseScoreStats scoreStats) {
    super(query);
    this.scoreStats = scoreStats;
  }

  @Override
  public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
    Scorer scorer = scorer(context);
    if (scorer == null) return null;
    return new PimBulkScorer(scorer);
  }

  @Override
  public Scorer scorer(LeafReaderContext context) throws IOException {
    return matchPhrase(context);
  }

  @Override
  public Explanation explain(LeafReaderContext context, int doc) throws IOException {
    return explainWithPhraseQuery(context, doc);
  }

  private Explanation explainWithPhraseQuery(LeafReaderContext context, int doc)
      throws IOException {
    PhraseQuery query = buildPhraseQuery();
    Weight weight = query.createWeight(scoreStats.searcher, scoreStats.scoreMode, scoreStats.boost);
    return weight.explain(context, doc);
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return true;
  }

  private Scorer matchPhrase(LeafReaderContext context) throws IOException {

    // The score can be computed in the DPUs, but it needs to store the table doc->norm (see
    // LeafSimScorer.getNormValue())
    // this table can be an array: n bits per norm value, with n determined based on [min,max]
    // value.
    try {
      // if the PIM system is ready to answer queries for this context use it
      if (PimSystemManager.get().isReady(context)
          && PimSystemManager.get().isQuerySupported(getQuery())) {

        /*System.out.println(
        ">> Query is offloaded to PIM system "
            + (PimSystemManager.USE_SOFTWARE_MODEL ? "simulator" : "hardware")
            + " for segment "
            + context.ord);*/
        PimPhraseQuery pimQuery = (PimPhraseQuery) getQuery();
        LeafSimScorer simScorer =
            new LeafSimScorer(
                scoreStats.similarity.scorer(
                    scoreStats.boost, scoreStats.collectionStats, scoreStats.termStats),
                context.reader(),
                pimQuery.getField(),
                scoreStats.scoreMode.needsScores());
        try {
          return new PimScorer(this, PimSystemManager.get().search(context, pimQuery, simScorer));
        } catch (
            @SuppressWarnings("unused")
            PimSystemManager.PimQueryQueueFullException e) {
          // PimSystemManager queue is full, handle the query on CPU
          return matchWithPhraseQuery(context);
        } catch (
            @SuppressWarnings("unused")
            InterruptedException e) {
          Thread.currentThread().interrupt();
          return null;
        }
      } else {
        return matchWithPhraseQuery(context);
      }
    } catch (
        @SuppressWarnings("unused")
        DpuException e) {
      // Error in PimSystemManager instantiation due to failing DPU allocation
      // Note: this should not happen as at this stage the PimSystemManager
      // instantiation should already be done if a PIM index was loaded in the system
      // Run the query on CPU
      return matchWithPhraseQuery(context);
    }
  }

  private Scorer matchWithPhraseQuery(LeafReaderContext context) throws IOException {
    PhraseQuery query = buildPhraseQuery();
    Weight weight = query.createWeight(scoreStats.searcher, scoreStats.scoreMode, scoreStats.boost);
    return weight.scorer(context);
  }

  private PhraseQuery buildPhraseQuery() {
    PimPhraseQuery query = (PimPhraseQuery) getQuery();
    PhraseQuery.Builder builder = new PhraseQuery.Builder().setSlop(query.getSlop());
    for (int i = 0; i < query.getTerms().length; i++) {
      builder.add(query.getTerms()[i], query.getPositions()[i]);
    }
    return builder.build();
  }
}
