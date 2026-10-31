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
package org.apache.lucene.search;

import java.io.IOException;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.PriorityQueue;

/**
 * Expert: A hit queue for sorting by hits by terms in more than one field.
 *
 * @lucene.experimental
 * @since 2.9
 * @see IndexSearcher#search(Query,int,Sort)
 */
public class FieldValueHitQueue<T extends FieldValueHitQueue.Entry> extends PriorityQueue<T> {

  /** Extension of ScoreDoc to also store the {@link FieldComparator} slot. */
  public static class Entry extends ScoreDoc {
    public int slot;

    public Entry(int slot, int doc) {
      super(doc, Float.NaN);
      this.slot = slot;
    }

    @Override
    public String toString() {
      return "slot:" + slot + " " + super.toString();
    }
  }

  private static class EntryLessThan implements LessThan<Entry> {

    // All these are required by this class's API - need to return arrays.
    // Therefore, even in the case of a single comparator, store an array
    // anyway.
    protected final FieldComparator<?>[] comparators;
    protected final int[] reverseMul;

    private EntryLessThan(FieldComparator<?>[] comparators, int[] reverseMul) {
      assert comparators.length == reverseMul.length;
      this.comparators = comparators;
      this.reverseMul = reverseMul;
    }

    @Override
    public boolean lessThan(Entry hitA, Entry hitB) {
      assert hitA != hitB;
      assert hitA.slot != hitB.slot;

      int numComparators = comparators.length;
      for (int i = 0; i < numComparators; ++i) {
        final int c = reverseMul[i] * comparators[i].compare(hitA.slot, hitB.slot);
        if (c != 0) {
          // Short circuit
          return c > 0;
        }
      }

      // avoid random sort order that could lead to duplicates (bug #31241):
      return hitA.doc > hitB.doc;
    }
  }

  // optimisation for the case when there's only one comparator
  private static class OneComparatorEntryLessThan extends EntryLessThan {
    private final int oneReverseMul;
    private final FieldComparator<?> oneComparator;

    private OneComparatorEntryLessThan(FieldComparator<?>[] comparators, int[] reverseMul) {
      super(comparators, reverseMul);

      assert comparators.length == 1;
      oneComparator = comparators[0];
      oneReverseMul = reverseMul[0];
    }

    @Override
    public boolean lessThan(Entry hitA, Entry hitB) {
      assert hitA != hitB;
      assert hitA.slot != hitB.slot;

      final int c = oneReverseMul * oneComparator.compare(hitA.slot, hitB.slot);
      if (c != 0) {
        return c > 0;
      }

      // avoid random sort order that could lead to duplicates (bug #31241):
      return hitA.doc > hitB.doc;
    }
  }

  /** Stores the sort criteria being used. */
  private final SortField[] fields;

  private final EntryLessThan lessThan;

  // prevent instantiation and extension.
  private FieldValueHitQueue(SortField[] fields, int size, EntryLessThan lessThan) {
    super(size, lessThan);
    // When we get here, fields.length is guaranteed to be > 0, therefore no
    // need to check it again.
    this.fields = fields;
    this.lessThan = lessThan;
  }

  /**
   * Creates a hit queue sorted by the given list of fields.
   *
   * <p><b>NOTE</b>: The instances returned by this method pre-allocate a full array of length
   * <code>numHits</code>.
   *
   * @param fields SortField array we are sorting by in priority order (highest priority first);
   *     cannot be <code>null</code> or empty
   * @param size The number of hits to retain. Must be greater than zero.
   */
  public static <T extends FieldValueHitQueue.Entry> FieldValueHitQueue<T> create(
      SortField[] fields, int size) {

    if (fields.length == 0) {
      throw new IllegalArgumentException("Sort must contain at least one field");
    }

    int numComparators = fields.length;
    FieldComparator<?>[] comparators = new FieldComparator<?>[numComparators];
    int[] reverseMul = new int[numComparators];
    for (int i = 0; i < numComparators; ++i) {
      SortField field = fields[i];
      reverseMul[i] = field.reverse ? -1 : 1;
      comparators[i] =
          field.getComparator(
              size,
              i == 0
                  ? (numComparators > 1 ? Pruning.GREATER_THAN : Pruning.GREATER_THAN_OR_EQUAL_TO)
                  : Pruning.NONE);
    }

    return new FieldValueHitQueue<>(
        fields,
        size,
        fields.length == 1
            ? new OneComparatorEntryLessThan(comparators, reverseMul)
            : new EntryLessThan(comparators, reverseMul));
  }

  public FieldComparator<?>[] getComparators() {
    return lessThan.comparators;
  }

  public int[] getReverseMul() {
    return lessThan.reverseMul;
  }

  /**
   * Returns a single {@link LeafFieldComparator} over all the sort fields. If the segment's index
   * sort starts with the search sort, the comparator terminates collection once the queue is full
   * of competitive hits and the hits threshold has been reached.
   *
   * @param paging whether a top value has been set on the comparators
   */
  LeafFieldComparator getLeafComparator(LeafReaderContext context, boolean paging)
      throws IOException {
    LeafFieldComparator[] leafComparators = getComparators(context);
    int[] reverseMuls = getReverseMul();
    LeafFieldComparator comparator;
    int reverseMul;
    if (leafComparators.length == 1) {
      comparator = leafComparators[0];
      reverseMul = reverseMuls[0];
    } else {
      comparator = new MultiLeafFieldComparator(leafComparators, reverseMuls);
      reverseMul = 1;
    }
    if (canEarlyTerminate(fields, context.reader())) {
      comparator = new IndexSortedLeafFieldComparator(comparator, reverseMul, paging);
    }
    return comparator;
  }

  /**
   * Whether docs in the segment are visited in the order of the search sort: the first search sort
   * is {@code _doc}, or the search sort is a prefix of the segment's index sort. Index sort fields
   * that have no effect in this segment are ignored. Segments with document blocks only terminate
   * early for {@code _doc}.
   */
  static boolean canEarlyTerminate(SortField[] searchSort, LeafReader reader) {
    if (reader.getMetaData().hasBlocks()) {
      // index sorting only orders parent documents, so child documents break the index sort order
      return SortField.FIELD_DOC.equals(unwrap(searchSort[0]));
    }
    if (canEarlyTerminate(new Sort(searchSort), reader.getMetaData().sort())) {
      return true;
    }
    // leading index sort fields may be no-ops in this segment, in which case docs are ordered by
    // the remaining ones
    int primary = Sort.primarySortFieldIndex(reader);
    return primary > 0 && isPrefix(searchSort, reader.getMetaData().sort().getSort(), primary);
  }

  /**
   * Whether the first search sort is {@code _doc}, or the search sort is a prefix of the index
   * sort, which may be null.
   */
  static boolean canEarlyTerminate(Sort searchSort, Sort indexSort) {
    SortField[] searchFields = searchSort.getSort();
    if (SortField.FIELD_DOC.equals(unwrap(searchFields[0]))) {
      return true;
    }
    return indexSort != null && isPrefix(searchFields, indexSort.getSort(), 0);
  }

  private static boolean isPrefix(SortField[] searchSort, SortField[] indexSort, int offset) {
    if (searchSort.length > indexSort.length - offset) {
      return false;
    }
    for (int i = 0; i < searchSort.length; i++) {
      if (unwrap(searchSort[i]).equals(indexSort[offset + i]) == false) {
        return false;
      }
    }
    return true;
  }

  // Disabling pruning on indexed data does not change the order docs are visited in
  private static SortField unwrap(SortField sortField) {
    return sortField instanceof NonPruningSortField np ? np.getDelegate() : sortField;
  }

  public LeafFieldComparator[] getComparators(LeafReaderContext context) throws IOException {
    LeafFieldComparator[] comparators = new LeafFieldComparator[lessThan.comparators.length];
    for (int i = 0; i < comparators.length; ++i) {
      comparators[i] = lessThan.comparators[i].getLeafComparator(context);
    }
    return comparators;
  }

  /**
   * Given a queue Entry, creates a corresponding FieldDoc that contains the values used to sort the
   * given document. These values are not the raw values out of the index, but the internal
   * representation of them. This is so the given search hit can be collated by a MultiSearcher with
   * other search hits.
   *
   * @param entry The Entry used to create a FieldDoc
   * @return The newly created FieldDoc
   * @see IndexSearcher#search(Query,int,Sort)
   */
  FieldDoc fillFields(final Entry entry) {
    final int n = lessThan.comparators.length;
    final Object[] fields = new Object[n];
    for (int i = 0; i < n; ++i) {
      fields[i] = lessThan.comparators[i].value(entry.slot);
    }
    // if (maxscore > 1.0f) doc.score /= maxscore;   // normalize scores
    return new FieldDoc(entry.doc, entry.score, fields);
  }

  /** Returns the SortFields being used by this hit queue. */
  SortField[] getFields() {
    return fields;
  }
}
