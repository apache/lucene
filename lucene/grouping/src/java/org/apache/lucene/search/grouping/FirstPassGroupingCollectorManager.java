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
import java.util.List;
import java.util.function.Supplier;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Sort;

/**
 * A CollectorManager implementation for FirstPassGroupingCollector.
 *
 */
public class FirstPassGroupingCollectorManager<T>
    implements CollectorManager<FirstPassGroupingCollector<T>, Collection<SearchGroup<T>>> {

  private final Supplier<GroupSelector<T>> groupSelectorFactory;
  private final Sort groupSort;
  private final int topNGroups;
  private final boolean ignoreDocsWithoutGroupField;
  private final List<FirstPassGroupingCollector<T>> collectors;

  /**
   * Creates a new FirstPassGroupingCollectorManager.
   *
   * @param groupSelectorFactory factory to create group selectors for each collector
   * @param groupSort the sort to use for groups
   * @param topNGroups the number of top groups to collect
   */
  public FirstPassGroupingCollectorManager(
      Supplier<GroupSelector<T>> groupSelectorFactory, Sort groupSort, int topNGroups) {
    this(groupSelectorFactory, groupSort, topNGroups, false);
  }

  /**
   * Creates a new FirstPassGroupingCollectorManager.
   *
   * @param groupSelectorFactory factory to create group selectors for each collector
   * @param groupSort the sort to use for groups
   * @param topNGroups the number of top groups to collect
   * @param ignoreDocsWithoutGroupField whether to ignore documents without a group field
   */
  public FirstPassGroupingCollectorManager(
      Supplier<GroupSelector<T>> groupSelectorFactory,
      Sort groupSort,
      int topNGroups,
      boolean ignoreDocsWithoutGroupField) {
    this.groupSelectorFactory = groupSelectorFactory;
    this.groupSort = groupSort;
    this.topNGroups = topNGroups;
    this.ignoreDocsWithoutGroupField = ignoreDocsWithoutGroupField;
    this.collectors = new ArrayList<>();
  }

  @Override
  public FirstPassGroupingCollector<T> newCollector() throws IOException {
    FirstPassGroupingCollector<T> collector =
        new FirstPassGroupingCollector<>(
            groupSelectorFactory.get(), groupSort, topNGroups, ignoreDocsWithoutGroupField);
    collectors.add(collector);
    return collector;
  }

  @Override
  public Collection<SearchGroup<T>> reduce(Collection<FirstPassGroupingCollector<T>> collectors)
      throws IOException {
    if (collectors.isEmpty()) {
      return null;
    }

    List<Collection<SearchGroup<T>>> allGroups = new ArrayList<>();
    for (FirstPassGroupingCollector<T> collector : collectors) {
      Collection<SearchGroup<T>> groups = collector.getTopGroups(0);
      if (groups != null) {
        allGroups.add(groups);
      }
    }

    return SearchGroup.merge(allGroups, 0, topNGroups, groupSort);
  }

  public List<FirstPassGroupingCollector<T>> getCollectors() {
    return collectors;
  }
}
