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
package org.apache.lucene.search.join;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.join.BlockJoinSelector.Type;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;

class ToParentDocValues extends DocIdSetIterator {

  interface Accumulator {
    void reset() throws IOException;

    void increment() throws IOException;

    int getChildrenWithValuesCount();
  }

  private static final class SortedDVs extends SortedDocValues implements Accumulator {
    private final SortedDocValues values;
    private final BlockJoinSelector.Type selection;
    private int ord = -1;
    private int childrenWithValuesCount = 0;
    private final ToParentDocValues iter;

    private SortedDVs(
        SortedDocValues values,
        BlockJoinSelector.Type selection,
        BitSet parents,
        DocIdSetIterator children,
        boolean reverse,
        boolean sortMissingLast) {
      this.values = values;
      this.selection = selection;
      this.iter =
          new ToParentDocValues(values, parents, children, reverse == sortMissingLast, this);
    }

    @Override
    public int docID() {
      return iter.docID();
    }

    @Override
    public void reset() throws IOException {
      ord = values.ordValue();
      childrenWithValuesCount = 1;
    }

    @Override
    public void increment() throws IOException {
      if (selection == BlockJoinSelector.Type.MIN) {
        ord = Math.min(ord, values.ordValue());
      } else if (selection == BlockJoinSelector.Type.MAX) {
        ord = Math.max(ord, values.ordValue());
      } else {
        throw new AssertionError();
      }
      childrenWithValuesCount++;
    }

    @Override
    public int nextDoc() throws IOException {
      return iter.nextDoc();
    }

    @Override
    public int advance(int target) throws IOException {
      return iter.advance(target);
    }

    @Override
    public boolean advanceExact(int targetParentDocID) throws IOException {
      return iter.advanceExact(targetParentDocID);
    }

    @Override
    public int ordValue() {
      return ord;
    }

    @Override
    public BytesRef lookupOrd(int ord) throws IOException {
      return values.lookupOrd(ord);
    }

    @Override
    public int getValueCount() {
      return values.getValueCount();
    }

    @Override
    public long cost() {
      return values.cost();
    }

    @Override
    public int getChildrenWithValuesCount() {
      return childrenWithValuesCount;
    }
  }

  private static final class NumDV extends NumericDocValues implements Accumulator {
    private final NumericDocValues values;
    private long value;
    private int childrenWithValuesCount = 0;
    private final BlockJoinSelector.Type selection;
    private final Long missingValue;

    private final ToParentDocValues iter;

    private NumDV(
        NumericDocValues values,
        BlockJoinSelector.Type selection,
        BitSet parents,
        DocIdSetIterator children) {
      this(values, selection, parents, children, null);
    }

    private NumDV(
        NumericDocValues values,
        BlockJoinSelector.Type selection,
        BitSet parents,
        DocIdSetIterator children,
        Long missingValue) {
      this.values = values;
      this.selection = selection;
      this.missingValue = missingValue;
      iter = new ToParentDocValues(values, parents, children, this);
    }

    @Override
    public void reset() throws IOException {
      value = values.longValue();
      childrenWithValuesCount = 1;
    }

    @Override
    public void increment() throws IOException {
      switch (selection) {
        case MIN -> value = Math.min(value, values.longValue());
        case MAX -> value = Math.max(value, values.longValue());
      }
      childrenWithValuesCount++;
    }

    @Override
    public int nextDoc() throws IOException {
      return iter.nextDoc();
    }

    @Override
    public int advance(int targetParentDocID) throws IOException {
      return iter.advance(targetParentDocID);
    }

    @Override
    public boolean advanceExact(int targetParentDocID) throws IOException {
      return iter.advanceExact(targetParentDocID);
    }

    @Override
    public long longValue() {
      if (missingValue != null && iter.hasChildWithMissingValue()) {
        return switch (selection) {
          case MIN -> Math.min(value, missingValue);
          case MAX -> Math.max(value, missingValue);
        };
      }
      return value;
    }

    @Override
    public int docID() {
      return iter.docID();
    }

    @Override
    public long cost() {
      return values.cost();
    }

    @Override
    public int getChildrenWithValuesCount() {
      return childrenWithValuesCount;
    }
  }

  private ToParentDocValues(
      DocIdSetIterator values, BitSet parents, DocIdSetIterator children, Accumulator collect) {
    this(values, parents, children, false, collect);
  }

  private ToParentDocValues(
      DocIdSetIterator values,
      BitSet parents,
      DocIdSetIterator children,
      boolean skipParentsWithMissingChildValues,
      Accumulator collect) {
    this.parents = parents;
    childWithValues = ConjunctionUtils.intersectIterators(Arrays.asList(children, values));
    this.collector = collect;
    this.skipParentsWithMissingChildValues = skipParentsWithMissingChildValues;
  }

  private final BitSet parents;
  private int docID = -1;
  private boolean hasChildWithMissingValue = false;
  private final Accumulator collector;
  boolean seen = false;
  private final DocIdSetIterator childWithValues;
  boolean skipParentsWithMissingChildValues;

  @Override
  public int docID() {
    return docID;
  }

  boolean hasChildWithMissingValue() {
    return hasChildWithMissingValue;
  }

  @Override
  public int nextDoc() throws IOException {
    assert docID != NO_MORE_DOCS;

    assert childWithValues.docID() != docID || docID == -1;
    if (childWithValues.docID() < docID || docID == -1) {
      childWithValues.nextDoc();
    }

    seen = false;
    while (seen == false) {
      if (childWithValues.docID() == NO_MORE_DOCS) {
        docID = NO_MORE_DOCS;
        return docID;
      }
      assert parents.get(childWithValues.docID()) == false;

      int nextParentDocID = parents.nextSetBit(childWithValues.docID());
      collector.reset();

      while (true) {
        int childDocID = childWithValues.nextDoc();
        assert childDocID != nextParentDocID;
        if (childDocID > nextParentDocID) {
          break;
        }
        collector.increment();
      }

      docID = nextParentDocID;
      int prevParentDocID = parents.prevSetBit(docID - 1);
      int totalChildren = docID - prevParentDocID - 1;
      hasChildWithMissingValue = collector.getChildrenWithValuesCount() < totalChildren;
      seen = hasChildWithMissingValue == false || skipParentsWithMissingChildValues == false;
    }

    return docID;
  }

  @Override
  public int advance(int target) throws IOException {
    if (target >= parents.length()) {
      docID = NO_MORE_DOCS;
      return docID;
    }
    if (target == 0) {
      assert docID() == -1;
      return nextDoc();
    }
    int prevParentDocID = parents.prevSetBit(target - 1);
    if (childWithValues.docID() <= prevParentDocID) {
      childWithValues.advance(prevParentDocID + 1);
    }
    return nextDoc();
  }

  /**
   * Advance the iterator to exactly {@code targetParentDocID} and return whether it has a value
   * derived from its child documents. {@code targetParentDocID} must be greater than or equal to
   * the current {@link #docID() doc ID}. After this method returns, {@link #docID()} returns {@code
   * targetParentDocID}.
   *
   * <p>A parent document is considered to have a value when at least one of its child documents has
   * a value. When {@code skipParentsWithMissingChildValues} is {@code true}, the parent is
   * additionally required to have <em>all</em> its children carry a value; if any child is missing
   * a value the parent is treated as missing and this method returns {@code false}.
   *
   * @param targetParentDocID the parent document ID to advance to
   * @return {@code true} if the target parent document has a (selected) value, {@code false}
   *     otherwise
   * @throws IOException if an I/O error occurs
   * @throws IllegalArgumentException if {@code targetParentDocID} is less than the current doc ID
   */
  // @Override
  public boolean advanceExact(int targetParentDocID) throws IOException {
    if (targetParentDocID < docID) {
      throw new IllegalArgumentException(
          "target must be after the current document: current="
              + docID
              + " target="
              + targetParentDocID);
    }
    int previousDocId = docID;
    docID = targetParentDocID;
    if (targetParentDocID == previousDocId) {
      return seen; // ord != -1; rlly???
    }
    seen = false;
    hasChildWithMissingValue = false;
    // ord = -1;
    if (parents.get(targetParentDocID) == false) {
      return false;
    }
    int prevParentDocId = docID == 0 ? -1 : parents.prevSetBit(docID - 1);
    int totalChildren = docID - prevParentDocId - 1;
    int childDoc = childWithValues.docID();
    if (childDoc <= prevParentDocId) {
      childDoc = childWithValues.advance(prevParentDocId + 1);
    }
    if (childDoc >= docID) {
      return false;
    }

    boolean hasAtLeastOneChildWithValue = false;
    if (childWithValues.docID() < docID) {
      collector.reset();
      hasAtLeastOneChildWithValue = true;
      childWithValues.nextDoc();
    }

    if (hasAtLeastOneChildWithValue == false) {
      return false;
    }

    for (int doc = childWithValues.docID(); doc < docID; doc = childWithValues.nextDoc()) {
      collector.increment();
    }
    hasChildWithMissingValue = collector.getChildrenWithValuesCount() < totalChildren;
    if (hasChildWithMissingValue && skipParentsWithMissingChildValues) {
      return false;
    }
    // We can only set the flag seen if the parent is not skipped
    seen = true;
    return true;
  }

  @Override
  public long cost() {
    return 0;
  }

  static NumericDocValues wrap(
      NumericDocValues values, Type selection, BitSet parents2, DocIdSetIterator children) {
    return new ToParentDocValues.NumDV(values, selection, parents2, children);
  }

  static NumericDocValues wrap(
      NumericDocValues values,
      Type selection,
      BitSet parents,
      DocIdSetIterator children,
      Long missingValue) {
    return new ToParentDocValues.NumDV(values, selection, parents, children, missingValue);
  }

  static SortedDocValues wrap(
      SortedDocValues values,
      Type selection,
      BitSet parents,
      DocIdSetIterator children,
      boolean reverse,
      boolean sortMissingLast) {
    return new ToParentDocValues.SortedDVs(
        values, selection, parents, children, reverse, sortMissingLast);
  }
}
