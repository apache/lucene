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
package org.apache.lucene.sandbox.facet.cutters;

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.sandbox.facet.iterators.OrdinalIterator;

/**
 * Creates {@link LeafFacetCutter} for each leaf.
 *
 * <p>TODO: do we need FacetCutterManager similar to CollectorManager, e.g. is createLeafCutter
 * always thread safe?
 *
 * @lucene.experimental
 */
public interface FacetCutter {

  /** Get cutter for the leaf. */
  LeafFacetCutter createLeafCutter(LeafReaderContext context) throws IOException;

  /**
   * For facets that have hierarchy (levels), return all top level dimension ordinals that require
   * rollup.
   *
   * <p>Rollup is an optimization for facets types that support hierarchy, if single document
   * belongs to at most one node in the hierarchy, we can first record data for these nodes only,
   * and then roll up values to parent ordinals.
   *
   * <p>Default implementation returns null, which means that rollup is not needed.
   */
  default OrdinalIterator getOrdinalsToRollup() throws IOException {
    return null;
  }

  /** For facets that have hierarchy (levels), get all children ordinals for given ord. */
  default OrdinalIterator getChildrenOrds(int ord) throws IOException {
    return null;
  }
}
