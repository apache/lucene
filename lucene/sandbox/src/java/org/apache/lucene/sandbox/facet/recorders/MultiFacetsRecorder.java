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
package org.apache.lucene.sandbox.facet.recorders;

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.sandbox.facet.abstracts.FacetRecorder;
import org.apache.lucene.sandbox.facet.abstracts.FacetRollup;
import org.apache.lucene.sandbox.facet.abstracts.LeafFacetRecorder;
import org.apache.lucene.sandbox.facet.abstracts.OrdinalIterator;

/** {@link FacetRecorder} that contains multiple FacetRecorders. */
public final class MultiFacetsRecorder implements FacetRecorder {

  private final FacetRecorder[] delegates;

  /** Constructor */
  public MultiFacetsRecorder(FacetRecorder... delegates) {
    this.delegates = delegates;
  }

  @Override
  public LeafFacetRecorder getLeafRecorder(LeafReaderContext context) throws IOException {
    LeafFacetRecorder[] leafDelegates = new LeafFacetRecorder[delegates.length];
    for (int i = 0; i < delegates.length; i++) {
      leafDelegates[i] = delegates[i].getLeafRecorder(context);
    }
    return new MultiFacetsLeafRecorder(leafDelegates);
  }

  @Override
  public OrdinalIterator recordedOrds() {
    assert delegates.length > 0;
    return delegates[0].recordedOrds();
  }

  @Override
  public boolean isEmpty() {
    assert delegates.length > 0;
    return delegates[0].isEmpty();
  }

  @Override
  public void reduce(FacetRollup facetRollup) throws IOException {
    for (FacetRecorder recorder : delegates) {
      recorder.reduce(facetRollup);
    }
  }

  private static final class MultiFacetsLeafRecorder implements LeafFacetRecorder {

    private final LeafFacetRecorder[] delegates;

    private MultiFacetsLeafRecorder(LeafFacetRecorder[] delegates) {
      this.delegates = delegates;
    }

    @Override
    public void record(int docId, int facetOrd) throws IOException {
      for (LeafFacetRecorder leafRecorder : delegates) {
        leafRecorder.record(docId, facetOrd);
      }
    }
  }
}
