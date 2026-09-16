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
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.Weight;

/**
 * A {@link CollectorManager} for {@link BlockGroupingCollector} that merges results from multiple
 * collectors into a single {@link TopGroups}. This is intended for use with concurrent search,
 * where each slice is searched by a separate {@link BlockGroupingCollector}.
 *
 * <p>Documents must be indexed as blocks using {@link
 * org.apache.lucene.index.IndexWriter#addDocuments IndexWriter.addDocuments()} or {@link
 * org.apache.lucene.index.IndexWriter#updateDocuments IndexWriter.updateDocuments()}.
 *
 * <p><b>NOTE</b>: All documents in a group block must be processed by the same {@link
 * BlockGroupingCollector} instance. This means that the {@link
 * org.apache.lucene.search.IndexSearcher}'s slices must not split a segment in a way that places
 * documents from the same block into different slices. The default {@link
 * org.apache.lucene.search.IndexSearcher#slices} implementation (inter-segment only) satisfies this
 * constraint. If intra-segment concurrency is desired, the caller must override {@link
 * org.apache.lucene.search.IndexSearcher#slices} to ensure each doc block falls entirely within one
 * slice.
 *
 * <p>See {@link BlockGroupingCollector} for more details.
 *
 * <p>Example usage:
 *
 * <pre class="prettyprint">
 * IndexSearcher searcher = ...; // your IndexSearcher
 * Query lastDocPerGroupQuery = new TermQuery(new Term("groupEnd", "true"));
 * Weight lastDocPerGroup = searcher.createWeight(
 *     searcher.rewrite(lastDocPerGroupQuery), ScoreMode.COMPLETE_NO_SCORES, 1);
 *
 * BlockGroupingCollectorManager&lt;BytesRef&gt; manager = new BlockGroupingCollectorManager&lt;&gt;(
 *     Sort.RELEVANCE,   // groupSort
 *     0,                // groupOffset
 *     10,               // topNGroups
 *     true,             // needsScores
 *     lastDocPerGroup,
 *     Sort.RELEVANCE,   // withinGroupSort
 *     0,                // withinGroupOffset
 *     5);               // maxDocsPerGroup
 *
 * TopGroups&lt;BytesRef&gt; result = searcher.search(query, manager);
 * </pre>
 *
 * @lucene.experimental
 */
public class BlockGroupingCollectorManager<T>
    implements CollectorManager<BlockGroupingCollector, TopGroups<T>> {

  private final Sort groupSort;
  private final int groupOffset;
  private final int topNGroups;
  private final boolean needsScores;
  private final Weight lastDocPerGroup;

  private final Sort withinGroupSort;
  private final int withinGroupOffset;
  private final int maxDocsPerGroup;

  /**
   * Creates a new BlockGroupingCollectorManager.
   *
   * @param groupSort the sort used to rank groups
   * @param groupOffset the offset into the groups to start returning from
   * @param topNGroups the number of top groups to collect
   * @param needsScores whether scores are needed (must be true if groupSort or withinGroupSort uses
   *     scores)
   * @param lastDocPerGroup a {@link Weight} that matches the last document in each group block
   * @param withinGroupSort the sort used to rank documents within each group
   * @param withinGroupOffset the offset into each group's documents to start returning from
   * @param maxDocsPerGroup the maximum number of documents to return per group
   */
  public BlockGroupingCollectorManager(
      Sort groupSort,
      int groupOffset,
      int topNGroups,
      boolean needsScores,
      Weight lastDocPerGroup,
      Sort withinGroupSort,
      int withinGroupOffset,
      int maxDocsPerGroup) {
    if (groupSort == null) {
      throw new IllegalArgumentException("groupSort must not be null");
    }
    if (withinGroupSort == null) {
      throw new IllegalArgumentException("withinGroupSort must not be null");
    }

    if (groupOffset < 0) {
      throw new IllegalArgumentException("groupOffset must be >= 0 (got " + groupOffset + ")");
    }

    if (topNGroups < 1) {
      throw new IllegalArgumentException("topNGroups must be >= 1 (got " + topNGroups + ")");
    }

    if (withinGroupOffset < 0) {
      throw new IllegalArgumentException(
          "withinGroupOffset must be >= 0 (got " + withinGroupOffset + ")");
    }

    if (maxDocsPerGroup < 1) {
      throw new IllegalArgumentException(
          "maxDocsPerGroup must be >= 1 (got " + maxDocsPerGroup + ")");
    }

    this.groupSort = groupSort;
    this.groupOffset = groupOffset;
    this.topNGroups = topNGroups;
    this.needsScores = needsScores;
    this.lastDocPerGroup = lastDocPerGroup;
    this.withinGroupSort = withinGroupSort;
    this.withinGroupOffset = withinGroupOffset;
    this.maxDocsPerGroup = maxDocsPerGroup;
  }

  @Override
  public BlockGroupingCollector newCollector() throws IOException {
    return new BlockGroupingCollector(
        groupSort, groupOffset + topNGroups, needsScores, lastDocPerGroup);
  }

  @SuppressWarnings("unchecked")
  @Override
  public TopGroups<T> reduce(Collection<BlockGroupingCollector> collectors) throws IOException {
    List<TopGroups<T>> shardGroupsList = new ArrayList<>();
    for (BlockGroupingCollector collector : collectors) {
      TopGroups<?> topGroups =
          collector.getTopGroups(withinGroupSort, 0, withinGroupOffset, maxDocsPerGroup);
      if (topGroups != null && topGroups.groups.length > 0) {
        shardGroupsList.add((TopGroups<T>) topGroups);
      }
    }

    return TopGroups.mergeBlockGroups(
        shardGroupsList, groupSort, groupOffset, topNGroups, withinGroupSort);
  }
}
