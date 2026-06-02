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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.sandbox.facet.FacetFieldCollectorManager;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.Query;

/**
 * Utility class to orchestrate {@link FacetBuilder}s to setup and exec collection for multiple
 * facets.
 *
 * @lucene.experimental
 */
public final class FacetOrchestrator {
  private List<FacetBuilder> facetBuilders = new ArrayList<>();

  public FacetOrchestrator() {}

  public FacetOrchestrator addBuilder(FacetBuilder request) {
    facetBuilders.add(request);
    return this;
  }

  public void collect(Query query, IndexSearcher searcher) throws IOException {
    collect(query, searcher, null);
  }

  // public FacetBuilder addCollector(CollectorManager<C, T>)
  @SuppressWarnings({"unchecked"})
  public <C extends Collector, T> T collect(
      Query query, IndexSearcher searcher, CollectorManager<C, T> mainCollector)
      throws IOException {
    MultiCollectorManager mcm = createMainCollector(facetBuilders, mainCollector);
    Object[] res = searcher.search(query, mcm);
    if (mainCollector != null) {
      return (T) res[0];
    } else {
      return null;
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  static <C extends Collector, T> MultiCollectorManager createMainCollector(
      List<FacetBuilder> facetBuilders, CollectorManager<C, T> additionalCollectorManager) {
    // drill down
    List<FacetFieldCollectorManager<?>> drillDownManagers =
        collectorManagerForBuilders(facetBuilders);
    int i = 0;
    CollectorManager[] managersArray;
    if (additionalCollectorManager != null) {
      managersArray = new CollectorManager[drillDownManagers.size() + 1];
      managersArray[i++] = additionalCollectorManager;
    } else {
      managersArray = new CollectorManager[drillDownManagers.size()];
    }
    for (FacetFieldCollectorManager<?> m : drillDownManagers) {
      managersArray[i++] = m;
    }
    return new MultiCollectorManager(managersArray);
  }

  static List<FacetFieldCollectorManager<?>> collectorManagerForBuilders(
      List<FacetBuilder> facetBuilders) {
    // Check if we can reuse collectors for some FacetBuilders
    Map<Object, FacetBuilder> buildersUniqueForCollection = new HashMap<>();
    for (FacetBuilder builder : facetBuilders) {
      buildersUniqueForCollection.compute(
          builder.collectionKey(), (_, v) -> builder.initOrReuseCollector(v));
    }
    List<FacetFieldCollectorManager<?>> managers = new ArrayList<>();
    for (FacetBuilder c : buildersUniqueForCollection.values()) {
      managers.add(c.getCollectorManager());
    }
    return managers;
  }
}
