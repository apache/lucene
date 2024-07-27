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
package org.apache.lucene.sandbox.facet.ordinals;

import java.io.IOException;
import org.apache.lucene.sandbox.facet.recorders.FacetRecorder;

/**
 * {@link OrdinalIterator} that filters out ordinals from delegate if they are not in the candidate
 * set. Can be handy to get results only for specific facets.
 *
 * <p>Note that candidate ordinals might be relatively expensive to obtain, e.g. for taxonomy facets
 * it requires index lookup. In this case, it is recommended to check if {@link
 * FacetRecorder#isEmpty()} is false before creating candidates.
 */
public final class CandidateSetOrdinalIterator implements OrdinalIterator {

  private final OrdinalIterator candidateOrdinalIterator;
  private final FacetRecorder facetRecorder;

  /** Constructor. */
  public CandidateSetOrdinalIterator(FacetRecorder facetRecorder, int[] candidates) {
    // TODO: if candidates size >> number of ordinals in facetRecorder, it is more efficient to
    // iterate ordinals from FacetRecorder, and check if candidates contain them
    this.candidateOrdinalIterator = OrdinalIterator.fromArray(candidates);
    this.facetRecorder = facetRecorder;
  }

  @Override
  public int nextOrd() throws IOException {
    for (int ord = candidateOrdinalIterator.nextOrd();
        ord != NO_MORE_ORDS;
        ord = candidateOrdinalIterator.nextOrd()) {
      if (facetRecorder.contains(ord)) {
        return ord;
      }
    }
    return NO_MORE_ORDS;
  }
}
