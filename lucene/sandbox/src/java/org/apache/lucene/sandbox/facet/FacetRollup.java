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
import org.apache.lucene.sandbox.facet.iterators.OrdinalIterator;
import org.apache.lucene.sandbox.facet.recorders.FacetRecorder;

/**
 * This interface is used to rollup values in {@link FacetRecorder}.
 *
 * <p>Rollup is an optimization for facets types that support hierarchy, if single document belongs
 * to at most one leaf in the hierarchy, we can first record data for the leafs, and then roll up
 * values to parent ordinals.
 *
 * <p>TODO: should we use this interface to express facets hierarchy? E.g. add getAllDimOrds method
 * and use it to implement {@link org.apache.lucene.facet.Facets#getAllDims(int)}. It might be
 * better to have a separate interface for it, as it probably need more methods that are not
 * required during collection. At the same time, we never need rollup unless there is hierarchy, so
 * it might make sense to merge the two interfaces?
 */
public interface FacetRollup {

  /**
   * For facets that have hierarchy (levels), return all top level dimension ordinals that require
   * rollup.
   */
  OrdinalIterator getDimOrdsToRollup() throws IOException;

  /** For facets that have hierarchy (levels), get all children ordinals for given ord. */
  OrdinalIterator getChildrenOrds(int ord) throws IOException;
}
