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
  private final int withinGroupOffset;
  private final int maxDocsPerGroup;
  private final boolean getMaxScores;
  private final int totalHitsThresholdPerGroup;
  private final TopGroups.ScoreMergeMode scoreMergeMode;

  /**
   * Creates a new TopGroupsCollectorManager.
   *
   * @param groupSelectorFactory factory to create group selectors for each collector
   * @param searchGroups the search groups from the first pass
   * @param groupSort the sort to use for groups
   * @param sortWithinGroup the sort to use within each group
   * @param withinGroupOffset the offset within each group to start collecting documents
   * @param maxDocsPerGroup the maximum number of documents per group
   * @param getMaxScores whether to compute max scores
   */
  public TopGroupsCollectorManager(
      Supplier<GroupSelector<T>> groupSelectorFactory,
      Collection<SearchGroup<T>> searchGroups,
      Sort groupSort,
      Sort sortWithinGroup,
      int withinGroupOffset,
      int maxDocsPerGroup,
      boolean getMaxScores) {
    this(
        groupSelectorFactory,
        searchGroups,
        groupSort,
        sortWithinGroup,
        withinGroupOffset,
        maxDocsPerGroup,
        getMaxScores,
        Integer.MAX_VALUE,
        TopGroups.ScoreMergeMode.None);
  }

  /**
   * Creates a new TopGroupsCollectorManager.
   *
   * @param groupSelectorFactory factory to create group selectors for each collector
   * @param searchGroups the search groups from the first pass
   * @param groupSort the sort to use for groups
   * @param sortWithinGroup the sort to use within each group
   * @param withinGroupOffset the offset within each group to start collecting documents
   * @param maxDocsPerGroup the maximum number of documents per group
   * @param getMaxScores whether to compute max scores
   * @param scoreMergeMode the mode for merging scores across shards
   */
  public TopGroupsCollectorManager(
      Supplier<GroupSelector<T>> groupSelectorFactory,
      Collection<SearchGroup<T>> searchGroups,
      Sort groupSort,
      Sort sortWithinGroup,
      int withinGroupOffset,
      int maxDocsPerGroup,
      boolean getMaxScores,
      TopGroups.ScoreMergeMode scoreMergeMode) {
    this(
        groupSelectorFactory,
        searchGroups,
        groupSort,
        sortWithinGroup,
        withinGroupOffset,
        maxDocsPerGroup,
        getMaxScores,
        Integer.MAX_VALUE,
        scoreMergeMode);
  }

  /**
   * Creates a new TopGroupsCollectorManager.
   *
   * @param groupSelectorFactory factory to create group selectors for each collector
   * @param searchGroups the search groups from the first pass
   * @param groupSort the sort to use for groups
   * @param sortWithinGroup the sort to use within each group
   * @param withinGroupOffset the offset within each group to start collecting documents
   * @param maxDocsPerGroup the maximum number of documents per group
   * @param getMaxScores whether to compute max scores
   * @param totalHitsThresholdPerGroup the threshold to control per-group hit count accuracy. If the
   *     number of hits for a group exceeds this threshold, the hit count may be reported as {@link
   *     org.apache.lucene.search.TotalHits.Relation#GREATER_THAN_OR_EQUAL_TO} rather than exact.
   *     Use {@link Integer#MAX_VALUE} (the default) for exact counts, or a smaller value such as
   *     {@code maxDocsPerGroup} to save the cost of exact counting once enough hits per group have
   *     been collected. Note: this parameter has no effect when {@code sortWithinGroup} sorts
   *     primarily by score descending, as that case always uses exact counting to avoid incorrectly
   *     skipping documents at the query level, which would both undercount {@code totalHitCount}
   *     and cause other groups to miss documents.
   * @param scoreMergeMode the mode for merging scores across shards
   */
  public TopGroupsCollectorManager(
      Supplier<GroupSelector<T>> groupSelectorFactory,
      Collection<SearchGroup<T>> searchGroups,
      Sort groupSort,
      Sort sortWithinGroup,
      int withinGroupOffset,
      int maxDocsPerGroup,
      boolean getMaxScores,
      int totalHitsThresholdPerGroup,
      TopGroups.ScoreMergeMode scoreMergeMode) {
    this.groupSelectorFactory = groupSelectorFactory;
    this.searchGroups = searchGroups;
    this.groupSort = groupSort;
    this.sortWithinGroup = sortWithinGroup;
    this.withinGroupOffset = withinGroupOffset;
    this.maxDocsPerGroup = maxDocsPerGroup;
    this.getMaxScores = getMaxScores;
    this.totalHitsThresholdPerGroup = totalHitsThresholdPerGroup;
    this.scoreMergeMode = scoreMergeMode;
  }

  @Override
  public TopGroupsCollector<T> newCollector() throws IOException {
    return new TopGroupsCollector<>(
        groupSelectorFactory.get(),
        searchGroups,
        groupSort,
        sortWithinGroup,
        withinGroupOffset + maxDocsPerGroup,
        getMaxScores,
        totalHitsThresholdPerGroup);
  }

  @Override
  public TopGroups<T> reduce(Collection<TopGroupsCollector<T>> collectors) throws IOException {
    // Merge results from multiple collectors
    List<TopGroups<T>> shardGroupsList = collectors.stream().map(c -> c.getTopGroups(0)).toList();

    return TopGroups.merge(
        shardGroupsList,
        groupSort,
        sortWithinGroup,
        withinGroupOffset,
        maxDocsPerGroup,
        scoreMergeMode);
  }
}
