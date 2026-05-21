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
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;

/**
 * A {@link CollectorManager} implementation for {@link AllGroupHeadsCollector} that collects the
 * most relevant document (group head) for each group across multiple segments and merges the
 * per-segment results into a single {@link GroupHeadsResult}.
 *
 * <p>Example usage:
 *
 * <pre class="prettyprint">
 * IndexSearcher searcher = ...; // your IndexSearcher
 * AllGroupHeadsCollectorManager&lt;BytesRef&gt; manager =
 *     new AllGroupHeadsCollectorManager&lt;&gt;(
 *         () -&gt; new TermGroupSelector("category"), Sort.RELEVANCE);
 * GroupHeadsResult result = searcher.search(new MatchAllDocsQuery(), manager);
 * Bits groupHeadsBits = result.retrieveGroupHeads(searcher.getIndexReader().maxDoc());
 * </pre>
 *
 * @param <T> the type of the group value
 * @lucene.experimental
 */
public class AllGroupHeadsCollectorManager<T>
    implements CollectorManager<
        AllGroupHeadsCollector<T>, AllGroupHeadsCollectorManager.GroupHeadsResult> {

  /** Holds the merged group heads and provides access as an {@code int[]} or {@link Bits}. */
  public static class GroupHeadsResult {
    private final int[] groupHeads;

    private GroupHeadsResult(int[] groupHeads) {
      this.groupHeads = groupHeads;
    }

    /** Returns the group head document IDs as an array. */
    public int[] retrieveGroupHeads() {
      return groupHeads;
    }

    /**
     * Returns the group head document IDs as a {@link Bits} set of size {@code maxDoc}, suitable
     * for use as a filter.
     *
     * @param maxDoc The maxDoc of the top level {@link IndexReader}.
     */
    public Bits retrieveGroupHeads(int maxDoc) {
      FixedBitSet result = new FixedBitSet(maxDoc);
      for (int docId : groupHeads) {
        result.set(docId);
      }
      return result;
    }
  }

  private static final class GroupHeadWithValues {
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
    return AllGroupHeadsCollector.newCollector(groupSelectorFactory.get(), sortWithinGroup);
  }

  @Override
  public GroupHeadsResult reduce(Collection<AllGroupHeadsCollector<T>> collectors) {
    Map<T, GroupHeadWithValues> mergedHeads = new HashMap<>();
    SortField[] sortFields = sortWithinGroup.getSort();

    for (AllGroupHeadsCollector<T> collector : collectors) {
      mergeCollectorHeads(collector, mergedHeads, sortFields);
    }

    return new GroupHeadsResult(mergedHeads.values().stream().mapToInt(h -> h.doc).toArray());
  }

  private void mergeCollectorHeads(
      AllGroupHeadsCollector<T> collector,
      Map<T, GroupHeadWithValues> mergedHeads,
      SortField[] sortFields) {
    for (AllGroupHeadsCollector.GroupHead<T> head : collector.getCollectedGroupHeads()) {
      Object[] sortValues = head.getSortValues();
      GroupHeadWithValues existing = mergedHeads.get(head.groupValue);
      if (existing == null || isCompetitive(head, sortValues, existing, sortFields)) {
        mergedHeads.put(head.groupValue, new GroupHeadWithValues(head.doc, sortValues));
      }
    }
  }

  @SuppressWarnings({"rawtypes"})
  private boolean isCompetitive(
      AllGroupHeadsCollector.GroupHead<T> head,
      Object[] sortValues,
      GroupHeadWithValues existing,
      SortField[] sortFields) {
    FieldComparator[] comparators = head.getComparators();
    int cmp;
    if (sortWithinGroup.equals(Sort.RELEVANCE)) {
      cmp = Float.compare((float) sortValues[0], (float) existing.sortValues[0]);
      return cmp > 0 || (cmp == 0 && head.doc < existing.doc);
    } else {
      cmp = 0;
      for (int i = 0; i < sortFields.length; i++) {
        @SuppressWarnings({"unchecked"})
        int c = comparators[i].compareValues(sortValues[i], existing.sortValues[i]);
        c = sortFields[i].getReverse() ? -c : c;
        if (c != 0) {
          cmp = c;
          break;
        }
      }
      return cmp < 0 || (cmp == 0 && head.doc < existing.doc);
    }
  }
}
