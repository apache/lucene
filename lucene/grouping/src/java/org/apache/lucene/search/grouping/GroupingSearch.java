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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.lucene.queries.function.ValueSource;
import org.apache.lucene.search.CachingCollectorManager;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiCollectorManager;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.mutable.MutableValue;

/**
 * Convenience class to perform grouping in a non distributed environment.
 *
 * @lucene.experimental
 */
public class GroupingSearch {

  private final Supplier<GroupSelector<?>> grouperFactory;
  private final Query groupEndDocs;

  private Sort groupSort = Sort.RELEVANCE;
  private Sort sortWithinGroup = Sort.RELEVANCE;

  private int groupDocsOffset;
  private int groupDocsLimit = 1;
  private boolean includeMaxScore = true;

  private Double maxCacheRAMMB;
  private Integer maxDocsToCache;
  private boolean cacheScores;
  private boolean allGroups;
  private boolean allGroupHeads;
  private boolean ignoreDocsWithoutGroupField;

  private Collection<?> matchingGroups;
  private Bits matchingGroupHeads;

  /**
   * Constructs a <code>GroupingSearch</code> instance that groups documents by index terms using
   * DocValues. The group field can only have one token per document. This means that the field must
   * not be analysed.
   *
   * @param groupField The name of the field to group by.
   */
  public GroupingSearch(String groupField) {
    this(() -> new TermGroupSelector(groupField), null);
  }

  /**
   * Constructs a <code>GroupingSearch</code> instance that groups documents using a {@link
   * GroupSelector} factory.
   *
   * @param grouperFactory a factory that creates fresh {@link GroupSelector} instances
   */
  public GroupingSearch(Supplier<GroupSelector<?>> grouperFactory) {
    this(grouperFactory, null);
  }

  /**
   * Constructs a <code>GroupingSearch</code> instance that groups documents by function using a
   * {@link ValueSource} instance.
   *
   * @param groupFunction The function to group by specified as {@link ValueSource}
   * @param valueSourceContext The context of the specified groupFunction
   */
  public GroupingSearch(ValueSource groupFunction, Map<Object, Object> valueSourceContext) {
    this(() -> new ValueSourceGroupSelector(groupFunction, valueSourceContext), null);
  }

  /**
   * Constructor for grouping documents by doc block. This constructor can only be used when
   * documents belonging in a group are indexed in one block.
   *
   * @param groupEndDocs The query that marks the last document in all doc blocks
   */
  public GroupingSearch(Query groupEndDocs) {
    this(null, groupEndDocs);
  }

  private GroupingSearch(Supplier<GroupSelector<?>> grouperFactory, Query groupEndDocs) {
    this.grouperFactory = grouperFactory;
    this.groupEndDocs = groupEndDocs;
  }

  /**
   * Executes a grouped search. Both the first pass and second pass are executed on the specified
   * searcher.
   *
   * @param searcher The {@link org.apache.lucene.search.IndexSearcher} instance to execute the
   *     grouped search on.
   * @param query The query to execute with the grouping
   * @param groupOffset The group offset
   * @param groupLimit The number of groups to return from the specified group offset
   * @return the grouped result as a {@link TopGroups} instance
   * @throws IOException If any I/O related errors occur
   */
  @SuppressWarnings("unchecked")
  public <T> TopGroups<T> search(
      IndexSearcher searcher, Query query, int groupOffset, int groupLimit) throws IOException {
    if (grouperFactory != null) {
      return groupByFieldOrFunction(searcher, query, groupOffset, groupLimit);
    } else if (groupEndDocs != null) {
      return groupByDocBlock(searcher, query, groupOffset, groupLimit);
    } else {
      throw new IllegalStateException(
          "Either groupField, groupFunction or groupEndDocs must be set."); // This can't happen...
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected <T> TopGroups<T> groupByFieldOrFunction(
      IndexSearcher searcher, Query query, int groupOffset, int groupLimit) throws IOException {
    @SuppressWarnings("unchecked")
    Supplier<GroupSelector<T>> typedGrouperFactory =
        (Supplier<GroupSelector<T>>) (Supplier<?>) grouperFactory;
    FirstPassGroupingCollectorManager<T> firstPassManager =
        new FirstPassGroupingCollectorManager<>(
            typedGrouperFactory, groupSort, groupOffset, groupLimit, ignoreDocsWithoutGroupField);
    List<CollectorManager<? extends Collector, ?>> firstRoundManagers = new ArrayList<>();
    firstRoundManagers.add(firstPassManager);
    AllGroupsCollectorManager<T> allGroupsManager;
    if (allGroups) {
      allGroupsManager = new AllGroupsCollectorManager<>(typedGrouperFactory);
      firstRoundManagers.add(allGroupsManager);
    }

    AllGroupHeadsCollectorManager<T> allGroupHeadsManager;
    if (allGroupHeads) {
      allGroupHeadsManager =
          new AllGroupHeadsCollectorManager<>(typedGrouperFactory, sortWithinGroup);
      firstRoundManagers.add(allGroupHeadsManager);
    }

    CollectorManager<?, Object[]> firstRoundManager =
        new MultiCollectorManager(firstRoundManagers.toArray(CollectorManager[]::new));

    CachingCollectorManager<?, Object[]> cachingManager = null;
    Object[] firstRoundResults;
    if (maxCacheRAMMB != null || maxDocsToCache != null) {
      cachingManager =
          new CachingCollectorManager<>(
              firstRoundManager, cacheScores, maxCacheRAMMB, maxDocsToCache);
      firstRoundResults = searcher.search(query, cachingManager);
    } else {
      firstRoundResults = searcher.search(query, firstRoundManager);
    }

    int resultIdx = 0;
    Collection<SearchGroup<T>> topSearchGroups =
        (Collection<SearchGroup<T>>) firstRoundResults[resultIdx++];

    matchingGroups =
        allGroups ? (Collection<?>) firstRoundResults[resultIdx++] : Collections.emptyList();

    if (allGroupHeads) {
      AllGroupHeadsCollectorManager.GroupHeadsResult headsResult =
          (AllGroupHeadsCollectorManager.GroupHeadsResult) firstRoundResults[resultIdx];
      matchingGroupHeads = headsResult.retrieveGroupHeads(searcher.getIndexReader().maxDoc());
    } else {
      matchingGroupHeads = new Bits.MatchNoBits(searcher.getIndexReader().maxDoc());
    }

    if (topSearchGroups.isEmpty()) {
      return new TopGroups<>(new SortField[0], new SortField[0], 0, 0, new GroupDocs[0], Float.NaN);
    }

    TopGroupsCollectorManager<T> secondPassManager =
        new TopGroupsCollectorManager<>(
            typedGrouperFactory,
            topSearchGroups,
            groupSort,
            sortWithinGroup,
            groupDocsOffset,
            groupDocsLimit,
            includeMaxScore);

    TopGroups<T> secondResult;
    if (cachingManager != null && cachingManager.isCached()) {
      secondResult = cachingManager.replay(secondPassManager);
    } else {
      secondResult = searcher.search(query, secondPassManager);
    }

    if (allGroups) {
      return new TopGroups<>(secondResult, matchingGroups.size());
    } else {
      return secondResult;
    }
  }

  protected <T> TopGroups<T> groupByDocBlock(
      IndexSearcher searcher, Query query, int groupOffset, int groupLimit) throws IOException {
    final Query endDocsQuery = searcher.rewrite(this.groupEndDocs);
    final Weight groupEndDocs =
        searcher.createWeight(endDocsQuery, ScoreMode.COMPLETE_NO_SCORES, 1);
    BlockGroupingCollectorManager<T> bcm =
        new BlockGroupingCollectorManager<>(
            groupSort,
            groupOffset,
            groupLimit,
            groupSort.needsScores() || sortWithinGroup.needsScores(),
            groupEndDocs,
            sortWithinGroup,
            groupDocsOffset,
            groupDocsOffset + groupDocsLimit);

    return searcher.search(query, bcm);
  }

  /**
   * Enables caching for the second pass search. The cache will not grow over a specified limit in
   * MB. The cache is filled during the first pass searched and then replayed during the second pass
   * searched. If the cache grows beyond the specified limit, then the cache is purged and not used
   * in the second pass search.
   *
   * @param maxCacheRAMMB The maximum amount in MB the cache is allowed to hold
   * @param cacheScores Whether to cache the scores
   * @return <code>this</code>
   */
  public GroupingSearch setCachingInMB(double maxCacheRAMMB, boolean cacheScores) {
    this.maxCacheRAMMB = maxCacheRAMMB;
    this.maxDocsToCache = null;
    this.cacheScores = cacheScores;
    return this;
  }

  /**
   * Enables caching for the second pass search. The cache will not contain more than the maximum
   * specified documents. The cache is filled during the first pass searched and then replayed
   * during the second pass searched. If the cache grows beyond the specified limit, then the cache
   * is purged and not used in the second pass search.
   *
   * @param maxDocsToCache The maximum number of documents the cache is allowed to hold
   * @param cacheScores Whether to cache the scores
   * @return <code>this</code>
   */
  public GroupingSearch setCaching(int maxDocsToCache, boolean cacheScores) {
    this.maxDocsToCache = maxDocsToCache;
    this.maxCacheRAMMB = null;
    this.cacheScores = cacheScores;
    return this;
  }

  /**
   * Disables any enabled cache.
   *
   * @return <code>this</code>
   */
  public GroupingSearch disableCaching() {
    this.maxCacheRAMMB = null;
    this.maxDocsToCache = null;
    return this;
  }

  /**
   * Specifies how groups are sorted. Defaults to {@link Sort#RELEVANCE}.
   *
   * @param groupSort The sort for the groups.
   * @return <code>this</code>
   */
  public GroupingSearch setGroupSort(Sort groupSort) {
    this.groupSort = groupSort;
    return this;
  }

  /**
   * Specified how documents inside a group are sorted. Defaults to {@link Sort#RELEVANCE}.
   *
   * @param sortWithinGroup The sort for documents inside a group
   * @return <code>this</code>
   */
  public GroupingSearch setSortWithinGroup(Sort sortWithinGroup) {
    this.sortWithinGroup = sortWithinGroup;
    return this;
  }

  /**
   * Specifies the offset for documents inside a group.
   *
   * @param groupDocsOffset The offset for documents inside a
   * @return <code>this</code>
   */
  public GroupingSearch setGroupDocsOffset(int groupDocsOffset) {
    this.groupDocsOffset = groupDocsOffset;
    return this;
  }

  /**
   * Specifies the number of documents to return inside a group from the specified groupDocsOffset.
   *
   * @param groupDocsLimit The number of documents to return inside a group
   * @return <code>this</code>
   */
  public GroupingSearch setGroupDocsLimit(int groupDocsLimit) {
    this.groupDocsLimit = groupDocsLimit;
    return this;
  }

  /**
   * Whether to include the score of the most relevant document per group.
   *
   * @param includeMaxScore Whether to include the score of the most relevant document per group
   * @return <code>this</code>
   */
  public GroupingSearch setIncludeMaxScore(boolean includeMaxScore) {
    this.includeMaxScore = includeMaxScore;
    return this;
  }

  /**
   * Whether to also compute all groups matching the query. This can be used to determine the number
   * of groups, which can be used for accurate pagination.
   *
   * <p>When grouping by doc block the number of groups are automatically included in the {@link
   * TopGroups} and this option doesn't have any influence.
   *
   * @param allGroups to also compute all groups matching the query
   * @return <code>this</code>
   */
  public GroupingSearch setAllGroups(boolean allGroups) {
    this.allGroups = allGroups;
    return this;
  }

  /**
   * If {@link #setAllGroups(boolean)} was set to <code>true</code> then all matching groups are
   * returned, otherwise an empty collection is returned.
   *
   * @param <T> The group value type. This can be a {@link BytesRef} or a {@link MutableValue}
   *     instance. If grouping by doc block this the group value is always <code>null</code>.
   * @return all matching groups are returned, or an empty collection
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public <T> Collection<T> getAllMatchingGroups() {
    return (Collection<T>) matchingGroups;
  }

  /**
   * Whether to compute all group heads (most relevant document per group) matching the query.
   *
   * <p>This feature isn't enabled when grouping by doc block.
   *
   * @param allGroupHeads Whether to compute all group heads (most relevant document per group)
   *     matching the query
   * @return <code>this</code>
   */
  public GroupingSearch setAllGroupHeads(boolean allGroupHeads) {
    this.allGroupHeads = allGroupHeads;
    return this;
  }

  /**
   * Returns the matching group heads if {@link #setAllGroupHeads(boolean)} was set to true or an
   * empty bit set.
   *
   * @return The matching group heads if {@link #setAllGroupHeads(boolean)} was set to true or an
   *     empty bit set
   */
  public Bits getAllGroupHeads() {
    return matchingGroupHeads;
  }

  /**
   * Whether to ignore documents that don't have the group field instead of putting them in a null
   * group.
   *
   * @param ignoreDocsWithoutGroupField Whether to ignore documents without group field
   * @return <code>this</code>
   */
  public GroupingSearch setIgnoreDocsWithoutGroupField(boolean ignoreDocsWithoutGroupField) {
    this.ignoreDocsWithoutGroupField = ignoreDocsWithoutGroupField;
    return this;
  }
}
