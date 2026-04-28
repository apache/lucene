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
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

/**
 * A CollectorManager implementation for AllGroupHeadsCollector.
 *
 * @lucene.experimental
 */
public class AllGroupHeadsCollectorManager<T>
    implements CollectorManager<
        AllGroupHeadsCollector<T>, AllGroupHeadsCollectorManager.GroupHeadsResult> {

  /** Result wrapper that allows retrieving group heads as int[] or Bits. */
  public static class GroupHeadsResult {
    private final int[] groupHeads;

    GroupHeadsResult(int[] groupHeads) {
      this.groupHeads = groupHeads;
    }

    public int[] retrieveGroupHeads() {
      return groupHeads;
    }

    public Bits retrieveGroupHeads(int maxDoc) {
      FixedBitSet result = new FixedBitSet(maxDoc);
      for (int docId : groupHeads) {
        result.set(docId);
      }
      return result;
    }
  }

  private static class GroupHeadWithValues {
    int doc;
    final Object[] sortValues;

    GroupHeadWithValues(int doc, Object[] sortValues) {
      this.doc = doc;
      this.sortValues = sortValues;
    }
  }

  private final Supplier<GroupSelector<T>> groupSelectorFactory;
  private final Sort sortWithinGroup;

  /**
   * Creates a new AllGroupHeadsCollectorManager.
   *
   * @param groupSelectorFactory factory to create group selectors for each collector
   * @param sortWithinGroup the sort to use within each group to determine the group head
   */
  public AllGroupHeadsCollectorManager(
      Supplier<GroupSelector<T>> groupSelectorFactory, Sort sortWithinGroup) {
    this.groupSelectorFactory = groupSelectorFactory;
    this.sortWithinGroup = sortWithinGroup;
  }

  @Override
  public AllGroupHeadsCollector<T> newCollector() throws IOException {
    return AllGroupHeadsCollector.newCollector(groupSelectorFactory.get(), sortWithinGroup, true);
  }

  @Override
  public GroupHeadsResult reduce(Collection<AllGroupHeadsCollector<T>> collectors) {
    Map<Object, GroupHeadWithValues> mergedHeads = new HashMap<>();
    SortField[] sortFields = sortWithinGroup.getSort();

    for (AllGroupHeadsCollector<T> collector : collectors) {
      mergeCollectorHeads(collector, mergedHeads, sortFields);
    }

    return new GroupHeadsResult(mergedHeads.values().stream().mapToInt(h -> h.doc).toArray());
  }

  private void mergeCollectorHeads(
      AllGroupHeadsCollector<T> collector,
      Map<Object, GroupHeadWithValues> mergedHeads,
      SortField[] sortFields) {
    Collection<? extends AllGroupHeadsCollector.GroupHead<T>> heads =
        collector.getCollectedGroupHeads();
    for (AllGroupHeadsCollector.GroupHead<T> head : heads) {
      Object[] sortValues = collector.getSortValues(head.groupValue);
      GroupHeadWithValues existing = mergedHeads.get(head.groupValue);
      if (existing == null) {
        mergedHeads.put(head.groupValue, new GroupHeadWithValues(head.doc, sortValues));
      } else if (sortValues != null && existing.sortValues != null) {
        int cmp = compareValues(sortValues, existing.sortValues, sortFields);
        if (cmp > 0 || (cmp == 0 && head.doc < existing.doc)) {
          mergedHeads.put(head.groupValue, new GroupHeadWithValues(head.doc, sortValues));
        }
      }
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private int compareValues(Object[] values1, Object[] values2, SortField[] sortFields) {
    for (int i = 0; i < sortFields.length; i++) {
      int cmp = 0;
      if (values1[i] == null) {
        cmp = values2[i] == null ? 0 : -1;
      } else if (values2[i] == null) {
        cmp = 1;
      } else if (values1[i] instanceof Comparable comparable) {
        cmp = comparable.compareTo(values2[i]);
      }
      if (cmp != 0) {
        // For SCORE type, natural order is descending (higher is better)
        // For other types, natural order is ascending (lower is better)
        // reverse=true flips the natural order
        boolean naturalDescending = sortFields[i].getType() == SortField.Type.SCORE;
        boolean wantDescending = naturalDescending != sortFields[i].getReverse();
        return wantDescending ? cmp : -cmp;
      }
    }
    return 0;
  }
}
