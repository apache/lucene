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
    private final int missingOrd;
    private int ord = -1;
    private int childrenWithValuesCount = 0;
    private final ToParentDocValues iter;

    private SortedDVs(
        SortedDocValues values,
        BlockJoinSelector.Type selection,
        BitSet parents,
        DocIdSetIterator children,
        boolean sortMissingLast) {
      this.values = values;
      this.selection = selection;
      this.missingOrd = sortMissingLast ? Integer.MAX_VALUE : -1;
      this.iter = new ToParentDocValues(values, parents, children, this);
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
      assert selection != null;
      switch (selection) {
        case MIN -> ord = Math.min(ord, values.ordValue());
        case MAX -> ord = Math.max(ord, values.ordValue());
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
      if (iter.hasChildWithMissingValue()) {
        return switch (selection) {
          case MIN -> Math.min(ord, missingOrd);
          case MAX -> Math.max(ord, missingOrd);
        };
      }
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
    this.parents = parents;
    childWithValues = ConjunctionUtils.intersectIterators(Arrays.asList(children, values));
    this.collector = collect;
  }

  private final BitSet parents;
  private int docID = -1;
  private boolean hasChildWithMissingValue = false;
  private final Accumulator collector;
  boolean seen = false;
  private final DocIdSetIterator childWithValues;

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
    if (childWithValues.docID() == NO_MORE_DOCS) {
      docID = NO_MORE_DOCS;
      return docID;
    }

    assert parents.get(childWithValues.docID()) == false;

    int nextParentDocID = parents.nextSetBit(childWithValues.docID());
    collector.reset();
    seen = true;

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
    docID = targetParentDocID;
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

    if (childWithValues.docID() < docID) {
      collector.reset();
      seen = true;
      childWithValues.nextDoc();
    }

    if (seen == false) {
      return false;
    }

    for (int doc = childWithValues.docID(); doc < docID; doc = childWithValues.nextDoc()) {
      collector.increment();
    }
    hasChildWithMissingValue = collector.getChildrenWithValuesCount() < totalChildren;
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
      boolean sortMissingLast) {
    return new ToParentDocValues.SortedDVs(values, selection, parents, children, sortMissingLast);
  }
}
