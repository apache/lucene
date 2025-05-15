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
package org.apache.lucene.sandbox.facet;

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.sandbox.facet.cutters.FacetCutter;
import org.apache.lucene.sandbox.facet.recorders.FacetRecorder;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;

/**
 * {@link Collector} that brings together {@link FacetCutter} and {@link FacetRecorder} to compute
 * facets during collection phase.
 *
 * @lucene.experimental
 */
public final class FacetFieldCollector implements Collector {
  private final FacetCutter facetCutter;
  private final FacetRecorder facetRecorder;

  /** Collector for cutter+recorder pair. */
  public FacetFieldCollector(FacetCutter facetCutter, FacetRecorder facetRecorder) {
    this.facetCutter = facetCutter;
    this.facetRecorder = facetRecorder;
  }

  @Override
  public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
    return new FacetFieldLeafCollector(context, facetCutter, facetRecorder);
  }

  @Override
  public ScoreMode scoreMode() {
    // TODO: Some FacetRecorders might need scores, e.g. to get associated numeric values, see for
    // example TaxonomyFacetFloatAssociations. Not sure if anyone actually uses it, because
    // FacetsCollectorManager creates FacetsCollector with keepScores: false. But if someone needs
    // it, we can add boolean needScores method to FacetRecorder interface, return
    // ScoreMode.COMPLETE here when the method returns true. FacetRecorders#needScores should be
    // implemented on case by case basis, e.g. LongAggregationsFacetRecorder can take it as a
    // constuctor argument, and when it's true call LongValues#getValues with the scores.
    return ScoreMode.COMPLETE_NO_SCORES;
  }
}
