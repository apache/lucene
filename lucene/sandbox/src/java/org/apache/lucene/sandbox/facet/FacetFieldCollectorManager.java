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
import java.util.Collection;
import org.apache.lucene.sandbox.facet.cutters.FacetCutter;
import org.apache.lucene.sandbox.facet.recorders.FacetRecorder;
import org.apache.lucene.search.CollectorManager;

/**
 * Collector manager for {@link FacetFieldCollector}. Returns the same extension of {@link
 * FacetRecorder} that was used to collect results.
 *
 * @lucene.experimental
 */
public final class FacetFieldCollectorManager<V extends FacetRecorder>
    implements CollectorManager<FacetFieldCollector, V> {

  private final FacetCutter facetCutter;
  private final V facetRecorder;

  /** Create collector for a cutter + recorder pair */
  public FacetFieldCollectorManager(FacetCutter facetCutter, V facetRecorder) {
    this.facetCutter = facetCutter;
    this.facetRecorder = facetRecorder;
  }

  @Override
  public FacetFieldCollector newCollector() throws IOException {
    return new FacetFieldCollector(facetCutter, facetRecorder);
  }

  @Override
  public V reduce(Collection<FacetFieldCollector> collectors) throws IOException {
    facetRecorder.reduce(facetCutter);
    return this.facetRecorder;
  }
}
