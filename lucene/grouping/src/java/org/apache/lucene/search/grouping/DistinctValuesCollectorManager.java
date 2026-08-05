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
package org.apache.lucene.search.grouping;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import org.apache.lucene.search.CollectorManager;

/**
 * A {@link CollectorManager} implementation for {@link DistinctValuesCollector} that supports
 * parallel collection and merges results by taking the union of distinct values per group across
 * segments.
 *
 * @lucene.experimental
 */
public class DistinctValuesCollectorManager<T, R>
    implements CollectorManager<
        DistinctValuesCollector<T, R>, List<DistinctValuesCollector.GroupCount<T, R>>> {

  private final Supplier<GroupSelector<T>> groupSelectorFactory;
  private final Collection<SearchGroup<T>> searchGroups;
  private final Supplier<GroupSelector<R>> valueSelectorFactory;

  /**
   * Creates a new DistinctValuesCollectorManager.
   *
   * @param groupSelectorFactory factory to create group selectors for each collector
   * @param searchGroups the search groups from the first pass
   * @param valueSelectorFactory factory to create value selectors for each collector
   */
  public DistinctValuesCollectorManager(
      Supplier<GroupSelector<T>> groupSelectorFactory,
      Collection<SearchGroup<T>> searchGroups,
      Supplier<GroupSelector<R>> valueSelectorFactory) {
    this.groupSelectorFactory = groupSelectorFactory;
    this.searchGroups = searchGroups;
    this.valueSelectorFactory = valueSelectorFactory;
  }

  @Override
  public DistinctValuesCollector<T, R> newCollector() throws IOException {
    return new DistinctValuesCollector<>(
        groupSelectorFactory.get(), searchGroups, valueSelectorFactory.get());
  }

  @Override
  public List<DistinctValuesCollector.GroupCount<T, R>> reduce(
      Collection<DistinctValuesCollector<T, R>> collectors) {
    List<List<DistinctValuesCollector.GroupCount<T, R>>> allGroupsList = new ArrayList<>();
    for (DistinctValuesCollector<T, R> collector : collectors) {
      allGroupsList.add(collector.getGroups());
    }
    if (allGroupsList.isEmpty()) {
      return List.of();
    }
    // Merge by taking the union of uniqueValues for each group across all collectors.
    // All collectors share the same searchGroups so position j in each list is the same group.
    List<DistinctValuesCollector.GroupCount<T, R>> first = allGroupsList.get(0);
    List<DistinctValuesCollector.GroupCount<T, R>> merged = new ArrayList<>(first.size());
    for (int j = 0; j < first.size(); j++) {
      Set<R> union = new HashSet<>(first.get(j).uniqueValues);
      for (int i = 1; i < allGroupsList.size(); i++) {
        assert allGroupsList.get(i).size() == first.size();
        union.addAll(allGroupsList.get(i).get(j).uniqueValues);
      }
      merged.add(new DistinctValuesCollector.GroupCount<>(first.get(j).groupValue, union));
    }
    return merged;
  }
}
