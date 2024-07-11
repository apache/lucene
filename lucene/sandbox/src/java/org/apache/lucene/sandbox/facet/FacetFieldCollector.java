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
import org.apache.lucene.sandbox.facet.abstracts.FacetCutter;
import org.apache.lucene.sandbox.facet.abstracts.FacetRecorder;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;

/**
 * {@link Collector} that brings together {@link FacetCutter} and {@link FacetRecorder} to compute
 * facets during collection phase.
 */
public class FacetFieldCollector implements Collector {
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
    // TODO: We don't need to ever keep scores, do we?
    // return keepScores ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    return ScoreMode.COMPLETE_NO_SCORES;
  }
}
