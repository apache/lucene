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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.lucene.internal.hppc.LongIntHashMap;
import org.apache.lucene.misc.search.DiversifiedTopDocsCollector.ScoreDocKey;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;

/**
 * A {@link CollectorManager} for {@link DiversifiedTopDocsCollector}. Subclasses implement {@link
 * #newCollector()} to supply concrete collector instances; this class provides the {@link
 * #reduce(Collection)} that merges per-slice results while preserving the diversity constraint
 * across the whole result set.
 *
 * @lucene.experimental
 */
public abstract class DiversifiedTopDocsCollectorManager<C extends DiversifiedTopDocsCollector>
    implements CollectorManager<C, TopDocs> {

  private final int numHits;
  private final int maxHitsPerKey;

  /**
   * @param numHits maximum number of results to return
   * @param maxHitsPerKey maximum number of results per key allowed in the final results
   */
  protected DiversifiedTopDocsCollectorManager(int numHits, int maxHitsPerKey) {
    this.numHits = numHits;
    this.maxHitsPerKey = maxHitsPerKey;
  }

  /**
   * Merges per-slice results into a single {@link TopDocs} while re-applying the diversity
   * constraint globally. Each per-slice collector may contain up to {@code numHits} results, but
   * the same key can appear in multiple slices; {@code reduce} enforces {@code maxHitsPerKey}
   * across all slices.
   */
  @Override
  public TopDocs reduce(Collection<C> collectors) throws IOException {
    long totalHits = 0;
    List<ScoreDocKey> allDocs = new ArrayList<>();
    for (C collector : collectors) {
      TopDocs topDocs = collector.topDocs();
      totalHits += topDocs.totalHits.value();
      for (ScoreDoc sd : topDocs.scoreDocs) {
        allDocs.add((ScoreDocKey) sd);
      }
    }
    // Sort descending by score; break ties ascending by doc for determinism
    allDocs.sort(
        (a, b) -> {
          int cmp = Float.compare(b.score, a.score);
          return cmp != 0 ? cmp : Integer.compare(a.doc, b.doc);
        });
    // Greedy pick: take up to numHits while honouring maxHitsPerKey per key
    LongIntHashMap keyCounts = new LongIntHashMap();
    List<ScoreDoc> result = new ArrayList<>();
    for (ScoreDocKey sdk : allDocs) {
      assert sdk.key != null;
      int count = keyCounts.getOrDefault(sdk.key, 0);
      if (count < maxHitsPerKey) {
        result.add(sdk);
        keyCounts.put(sdk.key, count + 1);
        if (result.size() == numHits) {
          break;
        }
      }
    }
    return new TopDocs(
        new TotalHits(totalHits, TotalHits.Relation.EQUAL_TO), result.toArray(ScoreDoc[]::new));
  }
}
