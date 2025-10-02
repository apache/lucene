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
import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeSet;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.CollectionUtil;

/**
 * FirstPassGroupingCollector is the first of two passes necessary to collect grouped hits. This
 * pass gathers the top N sorted groups. Groups are defined by a {@link GroupSelector}
 *
 * <p>See {@link org.apache.lucene.search.grouping} for more details including a full code example.
 *
 * @lucene.experimental
 */
public class FirstPassGroupingCollector<T> extends SimpleCollector {

  private final GroupSelector<T> groupSelector;

  private final FieldComparator<?>[] comparators;
  private final LeafFieldComparator[] leafComparators;
  private final int[] reversed;
  private final int topNGroups;
  private final HashMap<T, CollectedSearchGroup<T>> groupMap;
  private final int compIDXEnd;
  private final ScoreMode scoreMode;
  private final boolean canSetMinScore;

  // Set once we reach topNGroups unique groups:
  /**
   * @lucene.internal
   */
  protected TreeSet<CollectedSearchGroup<T>> orderedGroups;

  private int docBase;
  private int spareSlot;
  private Scorable scorer;
  private int bottomSlot;
  private float minCompetitiveScore;

  /**
   * Create the first pass collector.
   *
   * @param groupSelector a GroupSelector used to defined groups
   * @param groupSort The {@link Sort} used to sort the groups. The top sorted document within each
   *     group according to groupSort, determines how that group sorts against other groups. This
   *     must be non-null, ie, if you want to groupSort by relevance use Sort.RELEVANCE.
   * @param topNGroups How many top groups to keep.
   */
  public FirstPassGroupingCollector(
      GroupSelector<T> groupSelector, Sort groupSort, int topNGroups) {
    this.groupSelector = groupSelector;
    if (topNGroups < 1) {
      throw new IllegalArgumentException("topNGroups must be >= 1 (got " + topNGroups + ")");
    }

    // TODO: allow null groupSort to mean "by relevance",
    // and specialize it?

    this.topNGroups = topNGroups;
    final SortField[] sortFields = groupSort.getSort();
    comparators = new FieldComparator<?>[sortFields.length];
    leafComparators = new LeafFieldComparator[sortFields.length];
    compIDXEnd = comparators.length - 1;
    reversed = new int[sortFields.length];
    for (int i = 0; i < sortFields.length; i++) {
      final SortField sortField = sortFields[i];

      final Pruning pruning;
      if (i == 0) {
        pruning = compIDXEnd >= 0 ? Pruning.GREATER_THAN : Pruning.GREATER_THAN_OR_EQUAL_TO;
      } else {
        pruning = Pruning.NONE;
      }

      // use topNGroups + 1 so we have a spare slot to use for comparing (tracked by
      // this.spareSlot):
      comparators[i] = sortField.getComparator(topNGroups + 1, pruning);
      reversed[i] = sortField.getReverse() ? -1 : 1;
    }

    if (SortField.FIELD_SCORE.equals(sortFields[0]) == true) {
      scoreMode = ScoreMode.TOP_SCORES;
      canSetMinScore = true;
    } else {
      scoreMode = groupSort.needsScores() ? ScoreMode.TOP_DOCS_WITH_SCORES : ScoreMode.TOP_DOCS;
      canSetMinScore = false;
    }

    spareSlot = topNGroups;
    groupMap = CollectionUtil.newHashMap(topNGroups);
  }

  @Override
  public ScoreMode scoreMode() {
    return scoreMode;
  }

  /**
   * Returns top groups, starting from offset. This may return null, if no groups were collected, or
   * if the number of unique groups collected is &lt;= offset.
   *
   * @param groupOffset The offset in the collected groups
   * @return top groups, starting from offset
   */
  public Collection<SearchGroup<T>> getTopGroups(int groupOffset) throws IOException {

    // System.out.println("FP.getTopGroups groupOffset=" + groupOffset + " fillFields=" + fillFields
    // + " groupMap.size()=" + groupMap.size());

    if (groupOffset < 0) {
      throw new IllegalArgumentException("groupOffset must be >= 0 (got " + groupOffset + ")");
    }

    if (groupMap.size() <= groupOffset) {
      return null;
    }

    if (orderedGroups == null) {
      buildSortedSet();
    }

    final Collection<SearchGroup<T>> result = new ArrayList<>(orderedGroups.size() - groupOffset);
    int upto = 0;
    final int sortFieldCount = comparators.length;
    for (CollectedSearchGroup<T> group : orderedGroups) {
      if (upto++ < groupOffset) {
        continue;
      }
      // System.out.println("  group=" + (group.groupValue == null ? "null" :
      // group.groupValue.toString()));
      SearchGroup<T> searchGroup = new SearchGroup<>();
      searchGroup.groupValue = group.groupValue;
      searchGroup.sortValues = new Object[sortFieldCount];
      for (int sortFieldIDX = 0; sortFieldIDX < sortFieldCount; sortFieldIDX++) {
        searchGroup.sortValues[sortFieldIDX] =
            comparators[sortFieldIDX].value(group.comparatorSlot);
      }
      result.add(searchGroup);
    }
    // System.out.println("  return " + result.size() + " groups");
    return result;
  }

  @Override
  public void setScorer(Scorable scorer) throws IOException {
    this.scorer = scorer;
    groupSelector.setScorer(scorer);
    for (LeafFieldComparator comparator : leafComparators) {
      comparator.setScorer(scorer);
    }
    setMinCompetitiveScore(scorer);
  }

  private boolean isCompetitive(int doc) throws IOException {
    // If orderedGroups != null we already have collected N groups and
    // can short circuit by comparing this document to the bottom group,
    // without having to find what group this document belongs to.

    // Even if this document belongs to a group in the top N, we'll know that
    // we don't have to update that group.

    // Downside: if the number of unique groups is very low, this is
    // wasted effort as we will most likely be updating an existing group.
    if (orderedGroups != null) {
      for (int compIDX = 0; ; compIDX++) {
        final int c = reversed[compIDX] * leafComparators[compIDX].compareBottom(doc);
        if (c < 0) {
          // Definitely not competitive. So don't even bother to continue
          return false;
        } else if (c > 0) {
          // Definitely competitive.
          break;
        } else if (compIDX == compIDXEnd) {
          // Here c=0. If we're at the last comparator, this doc is not
          // competitive, since docs are visited in doc Id order, which means
          // this doc cannot compete with any other document in the queue.
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public void collect(int doc) throws IOException {

    if (isCompetitive(doc) == false) {
      return;
    }

    // TODO: should we add option to mean "ignore docs that
    // don't have the group field" (instead of stuffing them
    // under null group)?
    groupSelector.advanceTo(doc);
    T groupValue = groupSelector.currentValue();

    final CollectedSearchGroup<T> group = groupMap.get(groupValue);

    if (group == null) {
      collectNewGroup(doc);
    } else {
      collectExistingGroup(doc, group);
    }
  }

  private void collectNewGroup(final int doc) throws IOException {
    // First time we are seeing this group, or, we've seen
    // it before but it fell out of the top N and is now
    // coming back

    if (isGroupMapFull() == false) {

      // Still in startup transient: we have not
      // seen enough unique groups to start pruning them;
      // just keep collecting them

      // Add a new CollectedSearchGroup:
      CollectedSearchGroup<T> sg = new CollectedSearchGroup<>();
      sg.groupValue = groupSelector.copyValue();
      sg.comparatorSlot = groupMap.size();
      sg.topDoc = docBase + doc;
      for (LeafFieldComparator fc : leafComparators) {
        fc.copy(sg.comparatorSlot, doc);
      }
      groupMap.put(sg.groupValue, sg);

      if (isGroupMapFull() == true) {
        // End of startup transient: we now have max
        // number of groups; from here on we will drop
        // bottom group when we insert new one:
        buildSortedSet();

        // Allow pruning for compatible leaf comparators.
        leafComparators[0].setHitsThresholdReached();
      }

    } else {
      // We already tested that the document is competitive, so replace
      // the bottom group with this new group.

      final CollectedSearchGroup<T> bottomGroup = orderedGroups.pollLast();
      assert orderedGroups.size() == topNGroups - 1;

      groupMap.remove(bottomGroup.groupValue);

      // reuse the removed CollectedSearchGroup
      bottomGroup.groupValue = groupSelector.copyValue();
      bottomGroup.topDoc = docBase + doc;

      for (LeafFieldComparator fc : leafComparators) {
        fc.copy(bottomGroup.comparatorSlot, doc);
      }

      groupMap.put(bottomGroup.groupValue, bottomGroup);
      orderedGroups.add(bottomGroup);
      assert orderedGroups.size() == topNGroups;

      final int lastComparatorSlot = orderedGroups.last().comparatorSlot;
      setBottomSlot(lastComparatorSlot);
    }
  }

  private void collectExistingGroup(final int doc, final CollectedSearchGroup<T> group)
      throws IOException {
    // Update existing group:
    for (int compIDX = 0; ; compIDX++) {
      leafComparators[compIDX].copy(spareSlot, doc);

      final int c =
          reversed[compIDX] * comparators[compIDX].compare(group.comparatorSlot, spareSlot);
      if (c < 0) {
        // Definitely not competitive.
        return;
      } else if (c > 0) {
        // Definitely competitive; set remaining comparators:
        for (int compIDX2 = compIDX + 1; compIDX2 < comparators.length; compIDX2++) {
          leafComparators[compIDX2].copy(spareSlot, doc);
        }
        break;
      } else if (compIDX == compIDXEnd) {
        // Here c=0. If we're at the last comparator, this doc is not
        // competitive, since docs are visited in doc Id order, which means
        // this doc cannot compete with any other document in the queue.
        return;
      }
    }

    // Remove before updating the group since lookup is done via comparators
    // TODO: optimize this

    final CollectedSearchGroup<T> prevLast;
    if (orderedGroups != null) {
      prevLast = orderedGroups.last();
      orderedGroups.remove(group);
      assert orderedGroups.size() == topNGroups - 1;
    } else {
      prevLast = null;
    }

    group.topDoc = docBase + doc;

    // Swap slots
    final int tmp = spareSlot;
    spareSlot = group.comparatorSlot;
    group.comparatorSlot = tmp;

    // Re-add the changed group
    if (orderedGroups != null) {
      orderedGroups.add(group);
      assert orderedGroups.size() == topNGroups;
      final CollectedSearchGroup<?> newLast = orderedGroups.last();
      // If we changed the value of the last group, or changed which group was last, then update
      // bottom:
      if (group == newLast || prevLast != newLast) {
        setBottomSlot(newLast.comparatorSlot);
      }
    }
  }

  @Override
  public DocIdSetIterator competitiveIterator() throws IOException {
    return leafComparators[0].competitiveIterator();
  }

  private void buildSortedSet() throws IOException {
    final Comparator<CollectedSearchGroup<?>> comparator =
        new Comparator<>() {
          @Override
          public int compare(CollectedSearchGroup<?> o1, CollectedSearchGroup<?> o2) {
            for (int compIDX = 0; ; compIDX++) {
              FieldComparator<?> fc = comparators[compIDX];
              final int c = reversed[compIDX] * fc.compare(o1.comparatorSlot, o2.comparatorSlot);
              if (c != 0) {
                return c;
              } else if (compIDX == compIDXEnd) {
                return o1.topDoc - o2.topDoc;
              }
            }
          }
        };

    orderedGroups = new TreeSet<>(comparator);
    orderedGroups.addAll(groupMap.values());
    assert orderedGroups.size() > 0;

    setBottomSlot(orderedGroups.last().comparatorSlot);
  }

  @Override
  protected void doSetNextReader(LeafReaderContext readerContext) throws IOException {
    minCompetitiveScore = 0f;
    docBase = readerContext.docBase;
    for (int i = 0; i < comparators.length; i++) {
      leafComparators[i] = comparators[i].getLeafComparator(readerContext);
    }
    groupSelector.setNextReader(readerContext);
  }

  /**
   * @return the GroupSelector used for this Collector
   */
  public GroupSelector<T> getGroupSelector() {
    return groupSelector;
  }

  private boolean isGroupMapFull() {
    return groupMap.size() >= topNGroups;
  }

  private void setBottomSlot(final int bottomSlot) throws IOException {
    for (LeafFieldComparator fc : leafComparators) {
      fc.setBottom(bottomSlot);
    }

    this.bottomSlot = bottomSlot;
    setMinCompetitiveScore(scorer);
  }

  private void setMinCompetitiveScore(final Scorable scorer) throws IOException {
    if (canSetMinScore == false || isGroupMapFull() == false) {
      return;
    }

    final float minScore = (float) comparators[0].value(bottomSlot);
    if (minScore > minCompetitiveScore) {
      scorer.setMinCompetitiveScore(minScore);
      minCompetitiveScore = minScore;
    }
  }
}
