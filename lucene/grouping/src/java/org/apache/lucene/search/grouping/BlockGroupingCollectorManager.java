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
 * where each segment is searched by a separate {@link BlockGroupingCollector}.
 *
 * <p>Documents must be indexed as blocks using {@link
 * org.apache.lucene.index.IndexWriter#addDocuments IndexWriter.addDocuments()} or {@link
 * org.apache.lucene.index.IndexWriter#updateDocuments IndexWriter.updateDocuments()}.
 *
 * <p>See {@link BlockGroupingCollector} for more details.
 *
 * @lucene.experimental
 */
public class BlockGroupingCollectorManager
    implements CollectorManager<BlockGroupingCollector, TopGroups<?>> {

  private final Sort groupSort;
  private final int topNGroups;
  private final boolean needsScores;
  private final Weight lastDocPerGroup;

  private final Sort withinGroupSort;
  private final int groupOffset;
  private final int withinGroupOffset;
  private final int maxDocsPerGroup;

  private final List<BlockGroupingCollector> collectors;

  public BlockGroupingCollectorManager(
      Sort groupSort,
      int topNGroups,
      boolean needsScores,
      Weight lastDocPerGroup,
      Sort withinGroupSort,
      int groupOffset,
      int withinGroupOffset,
      int maxDocsPerGroup) {
    this.groupSort = groupSort;
    this.topNGroups = topNGroups;
    this.needsScores = needsScores;
    this.lastDocPerGroup = lastDocPerGroup;
    this.collectors = new ArrayList<>();
    this.withinGroupSort = withinGroupSort;
    this.groupOffset = groupOffset;
    this.withinGroupOffset = withinGroupOffset;
    this.maxDocsPerGroup = maxDocsPerGroup;
  }

  @Override
  public BlockGroupingCollector newCollector() throws IOException {
    BlockGroupingCollector collector =
        new BlockGroupingCollector(groupSort, topNGroups, needsScores, lastDocPerGroup);
    collectors.add(collector);
    return collector;
  }

  @Override
  public TopGroups<?> reduce(Collection<BlockGroupingCollector> collectors) throws IOException {
    if (collectors.isEmpty()) {
      return null;
    }

    if (collectors.size() == 1) {
      return collectors
          .iterator()
          .next()
          .getTopGroups(withinGroupSort, groupOffset, withinGroupOffset, maxDocsPerGroup);
    }

    // Merge results from multiple collectors
    List<TopGroups<?>> shardGroupsList = new ArrayList<>();
    for (BlockGroupingCollector collector : collectors) {
      TopGroups<?> topGroups =
          collector.getTopGroups(withinGroupSort, 0, withinGroupOffset, maxDocsPerGroup);
      if (topGroups != null) {
        shardGroupsList.add(topGroups);
      }
    }

    if (shardGroupsList.isEmpty()) {
      return null;
    }

    return TopGroups.mergeBlockGroups(shardGroupsList, groupSort, groupOffset, topNGroups);
  }
}
