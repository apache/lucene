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
package org.apache.lucene.sandbox.facet.utils;

import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.sandbox.facet.FacetFieldCollectorManager;

/**
 * End-to-end (request, collection and results) management of a single facet request.
 *
 * <p>Use {@link CommonFacetBuilder} unless there is a facet type specific implementation, such as
 * {@link TaxonomyFacetBuilder}. Use with {@link FacetOrchestrator} or {@link
 * DrillSidewaysFacetOrchestrator}.
 *
 * <p>Note that you need separate {@link FacetBuilder} instances even for very similar requests,
 * e.g. when sort order is the only difference. {@link FacetOrchestrator} and {@link
 * DrillSidewaysFacetOrchestrator}
 *
 * <p>It's an abstract class, not an interface, to define some methods as package private as they
 * should only be called by {@link FacetOrchestrator} or {@link DrillSidewaysFacetOrchestrator}.
 *
 * @lucene.experimental
 */
public abstract class FacetBuilder {

  /**
   * If parameter is null, init attrs required for collection phase. Otherwise, reuse the attributes
   * from {@link FacetBuilder}, so that we can share {@link
   * org.apache.lucene.search.CollectorManager} with it.
   *
   * @return similar if it is not null, otherwise this.
   */
  abstract FacetBuilder initOrReuseCollector(FacetBuilder similar);

  /** Create {@link org.apache.lucene.search.CollectorManager} for this facet request. */
  abstract FacetFieldCollectorManager<?> getCollectorManager();

  /**
   * Unique key for collection time. Multiple facet requests can use the same collector, e.g. when
   * they require counting matches for the same field.
   *
   * <p>Default: this, to use unique collector for this request.
   */
  Object collectionKey() {
    return this;
  }

  /** Get results for this request. */
  public abstract FacetResult getResult();
}
