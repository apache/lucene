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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.facet.DrillDownQuery;
import org.apache.lucene.facet.DrillSideways;
import org.apache.lucene.sandbox.facet.FacetFieldCollector;
import org.apache.lucene.sandbox.facet.FacetFieldCollectorManager;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.MultiCollectorManager;

/**
 * Utility class to orchestrate {@link FacetBuilder}s to collect facets using {@link DrillSideways}.
 *
 * @lucene.experimental
 */
public final class DrillSidewaysFacetOrchestrator {
  private List<FacetBuilder> drillDownFacetBuilders = new ArrayList<>();
  private Map<String, Integer> dimToIndex = new HashMap<>();
  private Map<Integer, List<FacetBuilder>> drillSidewaysFacetBuilders = new HashMap<>();

  public DrillSidewaysFacetOrchestrator() {}

  public DrillSidewaysFacetOrchestrator addDrillSidewaysBuilder(
      String dim, FacetBuilder facetBuilder) {
    // TODO: this looks fragile as it duplicates index assignment logic from DrillDownQuery.
    //       Instead we can change DrillSideways API to accept a dimension (String) to collector
    //       manager map instead of a list so that we can be sure that we use the right
    //       collector for the right dimension.
    //       but I think we should do it in a separate PR as it requires changing existing API.
    int dimIndex = dimToIndex.computeIfAbsent(dim, (_) -> dimToIndex.size());
    drillSidewaysFacetBuilders
        .computeIfAbsent(dimIndex, (_) -> new ArrayList<>())
        .add(facetBuilder);
    return this;
  }

  public DrillSidewaysFacetOrchestrator addDrillDownBuilder(FacetBuilder facetBuilder) {
    drillDownFacetBuilders.add(facetBuilder);
    return this;
  }

  public void collect(DrillDownQuery query, DrillSideways drillSideways) throws IOException {
    collect(query, drillSideways, null);
  }

  @SuppressWarnings({"unchecked"})
  public <T> T collect(
      DrillDownQuery query,
      DrillSideways drillSideways,
      CollectorManager<? extends Collector, T> mainCollector)
      throws IOException {
    // drill down
    MultiCollectorManager drillDownManager =
        FacetOrchestrator.createMainCollector(drillDownFacetBuilders, mainCollector);

    // drill sideways
    List<VoidFacetFieldCollectorManager> drillSidewaysManagers = new ArrayList<>();
    for (int i = 0; i < drillSidewaysFacetBuilders.size(); i++) {
      List<FacetFieldCollectorManager<?>> managers =
          FacetOrchestrator.collectorManagerForBuilders(drillSidewaysFacetBuilders.get(i));
      if (managers.size() != 1) {
        throw new IllegalArgumentException(
            "Expected exactly one collector manager per dimension but got " + managers.size());
      }
      drillSidewaysManagers.add(new VoidFacetFieldCollectorManager(managers.getFirst()));
    }

    DrillSideways.Result<Object[], ?> result =
        drillSideways.search(query, drillDownManager, drillSidewaysManagers);

    if (mainCollector != null) {
      return (T) result.drillDownResult();
    } else {
      return null;
    }
  }

  /**
   * Class to hide {@link FacetFieldCollectorManager} return type.
   *
   * <p>The reason we need it is that {@link DrillSideways} requires drill sideways dimension
   * collectors to be of the same type. As a result, we can't use wildcard (?) in the list of
   * collectors in this class, and consequently, we can't use different {@link
   * org.apache.lucene.sandbox.facet.recorders.FacetRecorder}s for different dimensions.
   *
   * <p>We can do it because each {@link FacetBuilder} keeps references to its {@link
   * org.apache.lucene.sandbox.facet.recorders.FacetRecorder}(s) so we don't need DrillSideways to
   * return them.
   */
  private static final class VoidFacetFieldCollectorManager
      implements CollectorManager<FacetFieldCollector, Void> {
    private final FacetFieldCollectorManager<?> delegate;

    public VoidFacetFieldCollectorManager(FacetFieldCollectorManager<?> delegate) {
      this.delegate = delegate;
    }

    @Override
    public FacetFieldCollector newCollector() throws IOException {
      return delegate.newCollector();
    }

    @Override
    public Void reduce(Collection<FacetFieldCollector> collection) throws IOException {
      delegate.reduce(collection);
      return null;
    }
  }
}
