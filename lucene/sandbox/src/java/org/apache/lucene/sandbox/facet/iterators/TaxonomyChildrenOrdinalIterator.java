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
package org.apache.lucene.sandbox.facet.iterators;

import java.io.IOException;
import org.apache.lucene.facet.taxonomy.ParallelTaxonomyArrays;
import org.apache.lucene.sandbox.facet.labels.LabelToOrd;

/**
 * Facets results selector to get children for selected parent. Source ordinals order is preserved.
 *
 * @lucene.experimental
 */
public final class TaxonomyChildrenOrdinalIterator implements OrdinalIterator {

  // TODO: do we want to have something like ChainOrdinalIterators to chain multiple iterators?
  //  Or are we fine with chaining them manually every time?
  private final OrdinalIterator sourceOrds;
  private final ParallelTaxonomyArrays.IntArray parents;
  private final int parentOrd;

  /** Create */
  public TaxonomyChildrenOrdinalIterator(
      OrdinalIterator sourceOrds, ParallelTaxonomyArrays.IntArray parents, int parentOrd) {
    this.sourceOrds = sourceOrds;
    this.parents = parents;
    assert parentOrd != LabelToOrd.INVALID_ORD : "Parent Ordinal is not valid";
    this.parentOrd = parentOrd;
  }

  @Override
  public int nextOrd() throws IOException {
    // TODO: in some cases it might be faster to traverse children of selected parent
    // (children/siblings IntArrays) and check if source ords contain them. We can think of some
    // heuristics to decide which approach to use on case by case basis? There is similar comment in
    // TaxonomyFacets#getTopChildrenForPath
    for (int ord = sourceOrds.nextOrd(); ord != NO_MORE_ORDS; ord = sourceOrds.nextOrd()) {
      if (parents.get(ord) == parentOrd) {
        return ord;
      }
    }
    return NO_MORE_ORDS;
  }
}
