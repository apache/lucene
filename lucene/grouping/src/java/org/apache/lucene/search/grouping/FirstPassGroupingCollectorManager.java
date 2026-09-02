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
 * A CollectorManager implementation for {@link FirstPassGroupingCollector} that supports parallel
 * collection and merges results across segments.
 *
 * <p>Example usage:
 *
 * <pre class="prettyprint">
 * IndexSearcher searcher = new IndexSearcher(reader);
 * Sort groupSort = Sort.RELEVANCE;
 * int topNGroups = 10;
 *
 * FirstPassGroupingCollectorManager&lt;BytesRef&gt; manager =
 *     new FirstPassGroupingCollectorManager&lt;&gt;(
 *         () -&gt; new TermGroupSelector("category"),
 *         groupSort,
 *         0,
 *         topNGroups);
 *
 * Collection&lt;SearchGroup&lt;BytesRef&gt;&gt; searchGroups = searcher.search(query, manager);
 *
 * // searchGroups can then be passed to a second pass collector manager like TopGroupsCollectorManager for full group results
 * </pre>
 *
 * @lucene.experimental
 */
public class FirstPassGroupingCollectorManager<T>
    implements CollectorManager<FirstPassGroupingCollector<T>, Collection<SearchGroup<T>>> {

  private final Supplier<GroupSelector<T>> groupSelectorFactory;
  private final Sort groupSort;
  private final int groupOffset;
  private final int topNGroups;
  private final boolean ignoreDocsWithoutGroupField;

  /**
   * Creates a new FirstPassGroupingCollectorManager.
   *
   * @param groupSelectorFactory factory to create group selectors for each collector
   * @param groupSort the sort to use for groups
   * @param groupOffset The offset in the collected groups
   * @param topNGroups the number of top groups to collect
   */
  public FirstPassGroupingCollectorManager(
      Supplier<GroupSelector<T>> groupSelectorFactory,
      Sort groupSort,
      int groupOffset,
      int topNGroups) {
    this(groupSelectorFactory, groupSort, groupOffset, topNGroups, false);
  }

  /**
   * Creates a new FirstPassGroupingCollectorManager.
   *
   * @param groupSelectorFactory factory to create group selectors for each collector
   * @param groupSort the sort to use for groups
   * @param groupOffset The offset in the collected groups
   * @param topNGroups the number of top groups to collect
   * @param ignoreDocsWithoutGroupField whether to ignore documents without a group field
   */
  public FirstPassGroupingCollectorManager(
      Supplier<GroupSelector<T>> groupSelectorFactory,
      Sort groupSort,
      int groupOffset,
      int topNGroups,
      boolean ignoreDocsWithoutGroupField) {
    if (groupOffset < 0) {
      throw new IllegalArgumentException("groupOffset must be >= 0 (got " + groupOffset + ")");
    }
    if (topNGroups < 1) {
      throw new IllegalArgumentException("topNGroups must be >= 1 (got " + topNGroups + ")");
    }
    this.groupSelectorFactory = groupSelectorFactory;
    this.groupSort = groupSort;
    this.groupOffset = groupOffset;
    this.topNGroups = topNGroups;
    this.ignoreDocsWithoutGroupField = ignoreDocsWithoutGroupField;
  }

  @Override
  public FirstPassGroupingCollector<T> newCollector() throws IOException {
    return new FirstPassGroupingCollector<>(
        groupSelectorFactory.get(),
        groupSort,
        groupOffset + topNGroups,
        ignoreDocsWithoutGroupField);
  }

  @Override
  public Collection<SearchGroup<T>> reduce(Collection<FirstPassGroupingCollector<T>> collectors)
      throws IOException {
    final List<Collection<SearchGroup<T>>> allGroups = new ArrayList<>();
    for (FirstPassGroupingCollector<T> collector : collectors) {
      Collection<SearchGroup<T>> groups = collector.getTopGroups(0);
      if (groups != null) {
        allGroups.add(groups);
      }
    }

    return SearchGroup.merge(allGroups, groupOffset, topNGroups, groupSort);
  }
}
