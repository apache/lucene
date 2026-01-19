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

/** A CollectorManager implementation for TopGroupsCollector. */
public class TopGroupsCollectorManager<T>
    implements CollectorManager<TopGroupsCollector<T>, TopGroups<T>> {

  private final Supplier<GroupSelector<T>> groupSelectorFactory;
  private final Collection<SearchGroup<T>> searchGroups;
  private final Sort groupSort;
  private final Sort sortWithinGroup;
  private final int maxDocsPerGroup;
  private final boolean getMaxScores;
  private final TopGroups.ScoreMergeMode scoreMergeMode;
  private final List<TopGroupsCollector<T>> collectors;

  /**
   * Creates a new TopGroupsCollectorManager.
   *
   * @param groupSelectorFactory factory to create group selectors for each collector
   * @param searchGroups the search groups from the first pass
   * @param groupSort the sort to use for groups
   * @param sortWithinGroup the sort to use within each group
   * @param maxDocsPerGroup the maximum number of documents per group
   * @param getMaxScores whether to compute max scores
   */
  public TopGroupsCollectorManager(
      Supplier<GroupSelector<T>> groupSelectorFactory,
      Collection<SearchGroup<T>> searchGroups,
      Sort groupSort,
      Sort sortWithinGroup,
      int maxDocsPerGroup,
      boolean getMaxScores) {
    this(
        groupSelectorFactory,
        searchGroups,
        groupSort,
        sortWithinGroup,
        maxDocsPerGroup,
        getMaxScores,
        TopGroups.ScoreMergeMode.None);
  }

  /**
   * Creates a new TopGroupsCollectorManager.
   *
   * @param groupSelectorFactory factory to create group selectors for each collector
   * @param searchGroups the search groups from the first pass
   * @param groupSort the sort to use for groups
   * @param sortWithinGroup the sort to use within each group
   * @param maxDocsPerGroup the maximum number of documents per group
   * @param getMaxScores whether to compute max scores
   * @param scoreMergeMode the mode for merging scores across shards
   */
  public TopGroupsCollectorManager(
      Supplier<GroupSelector<T>> groupSelectorFactory,
      Collection<SearchGroup<T>> searchGroups,
      Sort groupSort,
      Sort sortWithinGroup,
      int maxDocsPerGroup,
      boolean getMaxScores,
      TopGroups.ScoreMergeMode scoreMergeMode) {
    this.groupSelectorFactory = groupSelectorFactory;
    this.searchGroups = searchGroups;
    this.groupSort = groupSort;
    this.sortWithinGroup = sortWithinGroup;
    this.maxDocsPerGroup = maxDocsPerGroup;
    this.getMaxScores = getMaxScores;
    this.scoreMergeMode = scoreMergeMode;
    this.collectors = new ArrayList<>();
  }

  @Override
  public TopGroupsCollector<T> newCollector() throws IOException {
    TopGroupsCollector<T> collector =
        new TopGroupsCollector<>(
            groupSelectorFactory.get(),
            searchGroups,
            groupSort,
            sortWithinGroup,
            maxDocsPerGroup,
            getMaxScores);
    collectors.add(collector);
    return collector;
  }

  @Override
  public TopGroups<T> reduce(Collection<TopGroupsCollector<T>> collectors) throws IOException {
    if (collectors.isEmpty()) {
      return null;
    }

    if (collectors.size() == 1) {
      return collectors.iterator().next().getTopGroups(0);
    }

    // Merge results from multiple collectors
    List<TopGroups<T>> shardGroupsList = new ArrayList<>();
    for (TopGroupsCollector<T> collector : collectors) {
      TopGroups<T> groups = collector.getTopGroups(0);
      if (groups != null) {
        shardGroupsList.add(groups);
      }
    }

    if (shardGroupsList.isEmpty()) {
      return null;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    TopGroups<T>[] shardGroups = (TopGroups<T>[]) shardGroupsList.toArray(new TopGroups[0]);
    return TopGroups.merge(
        shardGroups, groupSort, sortWithinGroup, 0, maxDocsPerGroup, scoreMergeMode);
  }

  public List<TopGroupsCollector<T>> getCollectors() {
    return collectors;
  }
}
