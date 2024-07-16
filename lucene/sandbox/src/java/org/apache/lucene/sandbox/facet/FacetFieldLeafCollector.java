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
import org.apache.lucene.sandbox.facet.abstracts.LeafFacetCutter;
import org.apache.lucene.sandbox.facet.abstracts.LeafFacetRecorder;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;

/** LeafCollector that collects and records data for all facet ordinals for each doc. */
public class FacetFieldLeafCollector implements LeafCollector {

  private final LeafReaderContext context;
  private final FacetCutter cutter;
  private final FacetRecorder recorder;
  private LeafFacetCutter leafCutter;

  private LeafFacetRecorder leafRecorder;

  /** Constructor */
  public FacetFieldLeafCollector(
      LeafReaderContext context, FacetCutter cutter, FacetRecorder recorder) {
    this.context = context;
    this.cutter = cutter;
    this.recorder = recorder;
  }

  @Override
  public void setScorer(Scorable scorer) throws IOException {
    // TODO: do we need scorer for anything here?
  }

  @Override
  public void collect(int doc) throws IOException {
    if (leafCutter == null) {
      leafCutter = cutter.createLeafCutter(context);
      assert leafRecorder == null;
      leafRecorder = recorder.getLeafRecorder(context);
    }
    if (leafCutter.advanceExact(doc)) {
      for (int curOrd = leafCutter.nextOrd();
          curOrd != LeafFacetCutter.NO_MORE_ORDS;
          curOrd = leafCutter.nextOrd()) {
        leafRecorder.record(doc, curOrd);
      }
    }
  }

  @Override
  public DocIdSetIterator competitiveIterator() throws IOException {
    // TODO: any ideas?
    //  1. Docs that have values for the index field we about to facet on
    //  2. TK
    return LeafCollector.super.competitiveIterator();
  }

  // TODO do need to override any other methods?
}
