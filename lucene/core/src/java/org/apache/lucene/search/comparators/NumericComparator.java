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

package org.apache.lucene.search.comparators;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.function.Consumer;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntArrayDocIdSet;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.LSBRadixSorter;
import org.apache.lucene.util.PriorityQueue;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Abstract numeric comparator for comparing numeric values. This comparator provides a skipping
 * functionality – an iterator that can skip over non-competitive documents.
 *
 * <p>Parameter {@code field} provided in the constructor is used as a field name in the default
 * implementations of the methods {@code getNumericDocValues} and {@code getPointValues} to retrieve
 * doc values and points. You can pass a dummy value for a field name (e.g. when sorting by script),
 * but in this case you must override both of these methods.
 */
public abstract class NumericComparator<T extends Number> extends FieldComparator<T> {

  protected final T missingValue;
  private final long missingValueAsLong;
  protected final String field;
  protected final boolean reverse;
  private final int bytesCount; // how many bytes are used to encode this number

  protected boolean topValueSet;
  protected boolean singleSort; // singleSort is true, if sort is based on a single sort field.
  protected boolean hitsThresholdReached;
  protected boolean queueFull;
  protected Pruning pruning;

  protected NumericComparator(
      String field, T missingValue, boolean reverse, Pruning pruning, int bytesCount) {
    this.field = field;
    this.missingValue = missingValue;
    this.missingValueAsLong = missingValueAsComparableLong();
    this.reverse = reverse;
    this.pruning = pruning;
    this.bytesCount = bytesCount;
  }

  @Override
  public void setTopValue(T value) {
    topValueSet = true;
  }

  @Override
  public void setSingleSort() {
    singleSort = true;
  }

  @Override
  public void disableSkipping() {
    pruning = Pruning.NONE;
  }

  protected abstract long missingValueAsComparableLong();

  /**
   * Decode sortable bytes to long. It should be consistent with the codec that {@link PointValues}
   * of this field is using.
   */
  protected abstract long sortableBytesToLong(byte[] bytes);

  /** Leaf comparator for {@link NumericComparator} that provides skipping functionality */
  public abstract class NumericLeafComparator implements LeafFieldComparator {
    private static final long MAX_DISJUNCTION_CLAUSE = 128;
    private final LeafReaderContext context;
    protected final NumericDocValues docValues;
    private final PointValues pointValues;
    // if skipping functionality should be enabled on this segment
    private final boolean enableSkipping;
    private final int maxDoc;

    /** According to {@link FieldComparator#setTopValue}, topValueSet is final in leafComparator */
    private final boolean leafTopSet = topValueSet;

    private long minValueAsLong = Long.MIN_VALUE;
    private long maxValueAsLong = Long.MAX_VALUE;
    private Long thresholdAsLong;

    private DocIdSetIterator competitiveIterator;
    private long leadCost = -1;
    private int maxDocVisited = -1;

    public NumericLeafComparator(LeafReaderContext context) throws IOException {
      this.context = context;
      this.docValues = getNumericDocValues(context, field);
      this.pointValues = pruning != Pruning.NONE ? context.reader().getPointValues(field) : null;
      if (pointValues != null) {
        FieldInfo info = context.reader().getFieldInfos().fieldInfo(field);
        if (info == null || info.getPointDimensionCount() == 0) {
          throw new IllegalStateException(
              "Field "
                  + field
                  + " doesn't index points according to FieldInfos yet returns non-null PointValues");
        } else if (info.getPointDimensionCount() > 1) {
          throw new IllegalArgumentException(
              "Field " + field + " is indexed with multiple dimensions, sorting is not supported");
        } else if (info.getPointNumBytes() != bytesCount) {
          throw new IllegalArgumentException(
              "Field "
                  + field
                  + " is indexed with "
                  + info.getPointNumBytes()
                  + " bytes per dimension, but "
                  + NumericComparator.this
                  + " expected "
                  + bytesCount);
        }
        this.enableSkipping = true; // skipping is enabled when points are available
        this.maxDoc = context.reader().maxDoc();
        this.competitiveIterator = DocIdSetIterator.all(maxDoc);
        if (leafTopSet) {
          encodeTop();
        }
      } else {
        this.enableSkipping = false;
        this.maxDoc = 0;
      }
    }

    /**
     * Retrieves the NumericDocValues for the field in this segment
     *
     * <p>If you override this method, you should probably always disable skipping as the comparator
     * uses values from the points index to build its competitive iterators, and assumes that the
     * values in doc values and points are the same.
     *
     * @param context – reader context
     * @param field - field name
     * @return numeric doc values for the field in this segment.
     * @throws IOException If there is a low-level I/O error
     */
    protected NumericDocValues getNumericDocValues(LeafReaderContext context, String field)
        throws IOException {
      return DocValues.getNumeric(context.reader(), field);
    }

    @Override
    public void setBottom(int slot) throws IOException {
      queueFull = true; // if we are setting bottom, it means that we have collected enough hits
      updateCompetitiveIterator(); // update an iterator if we set a new bottom
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      maxDocVisited = doc;
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
      if (leadCost == -1) {
        if (scorer instanceof Scorer) {
          leadCost =
              ((Scorer) scorer).iterator().cost(); // starting iterator cost is the scorer's cost
        } else {
          leadCost = maxDoc;
        }
        updateCompetitiveIterator(); // update an iterator when we have a new segment
      }
    }

    @Override
    public void setHitsThresholdReached() throws IOException {
      hitsThresholdReached = true;
      updateCompetitiveIterator();
    }

    // update its iterator to include possibly only docs that are "stronger" than the current bottom
    // entry
    private void updateCompetitiveIterator() throws IOException {
      if (enableSkipping == false
          || hitsThresholdReached == false
          || (leafTopSet == false && queueFull == false)) return;
      // if some documents have missing points, check that missing values prohibits optimization
      boolean dense = pointValues.getDocCount() == maxDoc;
      if (dense == false && isMissingValueCompetitive()) {
        return; // we can't filter out documents, as documents with missing values are competitive
      }

      if (competitiveIterator instanceof CompetitiveIterator iter) {
        if (queueFull) {
          encodeBottom();
        }
        // CompetitiveIterator already built, try to reduce clause.
        tryReduceDisjunctionClause(iter);
        return;
      }

      if (thresholdAsLong == null) {
        if (dense == false) {
          competitiveIterator = getNumericDocValues(context, field);
          leadCost = Math.min(leadCost, competitiveIterator.cost());
        }
        long threshold = Math.min(leadCost >> 3, maxDoc >> 5);
        thresholdAsLong = intersectThresholdValue(threshold);
      }

      if ((reverse == false && bottomAsComparableLong() <= thresholdAsLong)
          || (reverse && bottomAsComparableLong() >= thresholdAsLong)) {
        if (queueFull) {
          encodeBottom();
        }
        DisjunctionBuildVisitor visitor = new DisjunctionBuildVisitor();
        competitiveIterator = visitor.generateCompetitiveIterator();
      }
    }

    private void tryReduceDisjunctionClause(CompetitiveIterator iter) {
      int originalSize = iter.disis.size();

      while (iter.disis.isEmpty() == false
          && (iter.disis.getFirst().mostCompetitiveValue > maxValueAsLong
              || iter.disis.getFirst().mostCompetitiveValue < minValueAsLong)) {
        iter.disis.removeFirst();
      }

      if (originalSize != iter.disis.size()) {
        iter.disjunction.clear();
        iter.disjunction.addAll(iter.disis);
      }
    }

    /** Find out the value that threshold docs away from topValue/infinite. */
    private long intersectThresholdValue(long threshold) throws IOException {
      long thresholdValuePos;
      if (leafTopSet) {
        long topValue = topAsComparableLong();
        PointValues.IntersectVisitor visitor = new RangeVisitor(Long.MIN_VALUE, topValue, -1);
        long topValuePos = pointValues.estimatePointCount(visitor);
        thresholdValuePos = reverse == false ? topValuePos + threshold : topValuePos - threshold;
      } else {
        thresholdValuePos = reverse == false ? threshold : pointValues.size() - threshold;
      }
      if (thresholdValuePos <= 0) {
        return sortableBytesToLong(pointValues.getMinPackedValue());
      } else if (thresholdValuePos >= pointValues.size()) {
        return sortableBytesToLong(pointValues.getMaxPackedValue());
      } else {
        return intersectValueByPos(pointValues.getPointTree(), thresholdValuePos);
      }
    }

    /** Get the point value by a left-to-right position. */
    private long intersectValueByPos(PointValues.PointTree pointTree, long pos) throws IOException {
      assert pos > 0 : pos;
      while (pointTree.size() < pos) {
        pos -= pointTree.size();
        pointTree.moveToSibling();
      }
      if (pointTree.size() == pos) {
        return sortableBytesToLong(pointTree.getMaxPackedValue());
      } else if (pos == 0) {
        return sortableBytesToLong(pointTree.getMinPackedValue());
      } else if (pointTree.moveToChild()) {
        return intersectValueByPos(pointTree, pos);
      } else {
        return reverse == false
            ? sortableBytesToLong(pointTree.getMaxPackedValue())
            : sortableBytesToLong(pointTree.getMinPackedValue());
      }
    }

    /**
     * If {@link NumericComparator#pruning} equals {@link Pruning#GREATER_THAN_OR_EQUAL_TO}, we
     * could better tune the {@link NumericLeafComparator#maxValueAsLong}/{@link
     * NumericLeafComparator#minValueAsLong}. For instance, if the sort is ascending and bottom
     * value is 5, we will use a range on [MIN_VALUE, 4].
     */
    private void encodeBottom() {
      if (reverse == false) {
        maxValueAsLong = bottomAsComparableLong();
        if (pruning == Pruning.GREATER_THAN_OR_EQUAL_TO && maxValueAsLong != Long.MIN_VALUE) {
          maxValueAsLong--;
        }
      } else {
        minValueAsLong = bottomAsComparableLong();
        if (pruning == Pruning.GREATER_THAN_OR_EQUAL_TO && minValueAsLong != Long.MAX_VALUE) {
          minValueAsLong++;
        }
      }
    }

    /**
     * If {@link NumericComparator#pruning} equals {@link Pruning#GREATER_THAN_OR_EQUAL_TO}, we
     * could better tune the {@link NumericLeafComparator#minValueAsLong}/{@link
     * NumericLeafComparator#minValueAsLong}. For instance, if the sort is ascending and top value
     * is 3, we will use a range on [4, MAX_VALUE].
     */
    private void encodeTop() {
      if (reverse == false) {
        minValueAsLong = topAsComparableLong();
        if (singleSort
            && pruning == Pruning.GREATER_THAN_OR_EQUAL_TO
            && queueFull
            && minValueAsLong != Long.MAX_VALUE) {
          minValueAsLong++;
        }
      } else {
        maxValueAsLong = topAsComparableLong();
        if (singleSort
            && pruning == Pruning.GREATER_THAN_OR_EQUAL_TO
            && queueFull
            && maxValueAsLong != Long.MIN_VALUE) {
          maxValueAsLong--;
        }
      }
    }

    private boolean isMissingValueCompetitive() {
      // if queue is full, compare with bottom first,
      // if competitive, then check if we can compare with topValue
      if (queueFull) {
        int result = Long.compare(missingValueAsLong, bottomAsComparableLong());
        // in reverse (desc) sort missingValue is competitive when it's greater or equal to bottom,
        // in asc sort missingValue is competitive when it's smaller or equal to bottom
        final boolean competitive =
            reverse
                ? (pruning == Pruning.GREATER_THAN_OR_EQUAL_TO ? result > 0 : result >= 0)
                : (pruning == Pruning.GREATER_THAN_OR_EQUAL_TO ? result < 0 : result <= 0);
        if (competitive == false) {
          return false;
        }
      }

      if (leafTopSet) {
        int result = Long.compare(missingValueAsLong, topAsComparableLong());
        // in reverse (desc) sort missingValue is competitive when it's smaller or equal to
        // topValue,
        // in asc sort missingValue is competitive when it's greater or equal to topValue
        return reverse ? (result <= 0) : (result >= 0);
      }

      // by default competitive
      return true;
    }

    @Override
    public DocIdSetIterator competitiveIterator() {
      if (enableSkipping == false) return null;
      return new DocIdSetIterator() {
        private int docID = competitiveIterator.docID();

        @Override
        public int nextDoc() throws IOException {
          return advance(docID + 1);
        }

        @Override
        public int docID() {
          return docID;
        }

        @Override
        public long cost() {
          return competitiveIterator.cost();
        }

        @Override
        public int advance(int target) throws IOException {
          return docID = competitiveIterator.advance(target);
        }
      };
    }

    protected abstract long bottomAsComparableLong();

    protected abstract long topAsComparableLong();

    class DisjunctionBuildVisitor extends RangeVisitor {

      final Deque<DisiAndMostCompetitiveValue> disis = new ArrayDeque<>();
      // most competitive entry stored last.
      final Consumer<DisiAndMostCompetitiveValue> adder =
          reverse == false ? disis::addFirst : disis::addLast;

      final int minBlockLength = minBlockLength();

      final LSBRadixSorter sorter = new LSBRadixSorter();
      int[] docs = IntsRef.EMPTY_INTS;
      int index = 0;
      int blockMaxDoc = -1;
      boolean docsInOrder = true;
      long blockMinValue = Long.MAX_VALUE;
      long blockMaxValue = Long.MIN_VALUE;

      private DisjunctionBuildVisitor() {
        super(minValueAsLong, maxValueAsLong, maxDocVisited);
      }

      @Override
      public void grow(int count) {
        docs = ArrayUtil.grow(docs, index + count + 1);
      }

      @Override
      protected void consumeDoc(int doc) {
        docs[index++] = doc;
        if (doc >= blockMaxDoc) {
          blockMaxDoc = doc;
        } else {
          docsInOrder = false;
        }
      }

      void intersectLeaves(PointValues.PointTree pointTree) throws IOException {
        PointValues.Relation r =
            compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());
        switch (r) {
          case CELL_INSIDE_QUERY, CELL_CROSSES_QUERY -> {
            if (pointTree.moveToChild()) {
              do {
                intersectLeaves(pointTree);
              } while (pointTree.moveToSibling());
              pointTree.moveToParent();
            } else {
              if (r == PointValues.Relation.CELL_CROSSES_QUERY) {
                pointTree.visitDocValues(this);
              } else {
                pointTree.visitDocIDs(this);
              }
              updateMinMax(
                  sortableBytesToLong(pointTree.getMinPackedValue()),
                  sortableBytesToLong(pointTree.getMaxPackedValue()));
            }
          }
          case CELL_OUTSIDE_QUERY -> {}
          default -> throw new IllegalStateException("unreachable code");
        }
      }

      void updateMinMax(long leafMinValue, long leafMaxValue) throws IOException {
        this.blockMinValue = Math.min(blockMinValue, leafMinValue);
        this.blockMaxValue = Math.max(blockMaxValue, leafMaxValue);
        if (index >= minBlockLength) {
          update();
          this.blockMinValue = Long.MAX_VALUE;
          this.blockMaxValue = Long.MIN_VALUE;
        }
      }

      void update() throws IOException {
        if (blockMinValue > blockMaxValue) {
          return;
        }
        long mostCompetitiveValue =
            reverse == false
                ? Math.max(blockMinValue, minValueAsLong)
                : Math.min(blockMaxValue, maxValueAsLong);

        if (docsInOrder == false) {
          sorter.sort(PackedInts.bitsRequired(blockMaxDoc), docs, index);
        }
        docs[index] = DocIdSetIterator.NO_MORE_DOCS;
        DocIdSetIterator iter = new IntArrayDocIdSet(docs, index).iterator();
        adder.accept(new DisiAndMostCompetitiveValue(iter, mostCompetitiveValue));
        docs = IntsRef.EMPTY_INTS;
        index = 0;
        blockMaxDoc = -1;
        docsInOrder = true;
      }

      DocIdSetIterator generateCompetitiveIterator() throws IOException {
        intersectLeaves(pointValues.getPointTree());
        update();

        if (disis.isEmpty()) {
          return DocIdSetIterator.empty();
        }
        assert assertMostCompetitiveValuesSorted(disis);

        PriorityQueue<DisiAndMostCompetitiveValue> disjunction =
            new PriorityQueue<>(disis.size()) {
              @Override
              protected boolean lessThan(
                  DisiAndMostCompetitiveValue a, DisiAndMostCompetitiveValue b) {
                return a.disi.docID() < b.disi.docID();
              }
            };
        disjunction.addAll(disis);

        return new CompetitiveIterator(maxDoc, disis, disjunction);
      }

      /**
       * Used for assert. When reverse is false, smaller values are more competitive, so
       * mostCompetitiveValues should be in desc order.
       */
      private boolean assertMostCompetitiveValuesSorted(Deque<DisiAndMostCompetitiveValue> deque) {
        long lastValue = reverse == false ? Long.MAX_VALUE : Long.MIN_VALUE;
        for (DisiAndMostCompetitiveValue value : deque) {
          if (reverse == false) {
            assert value.mostCompetitiveValue <= lastValue
                : deque.stream().map(d -> d.mostCompetitiveValue).toList().toString();
          } else {
            assert value.mostCompetitiveValue >= lastValue
                : deque.stream().map(d -> d.mostCompetitiveValue).toList().toString();
          }
          lastValue = value.mostCompetitiveValue;
        }
        return true;
      }

      private int minBlockLength() {
        // bottom value can be much more competitive than thresholdAsLong, recompute the cost.
        long cost =
            pointValues.estimatePointCount(new RangeVisitor(minValueAsLong, maxValueAsLong, -1));
        long disjunctionClause = Math.min(MAX_DISJUNCTION_CLAUSE, cost / 512 + 1);
        return Math.toIntExact(cost / disjunctionClause);
      }
    }
  }

  private class RangeVisitor implements PointValues.IntersectVisitor {

    private final long minInclusive;
    private final long maxInclusive;
    private final int docLowerBound;

    private RangeVisitor(long minInclusive, long maxInclusive, int docLowerBound) {
      this.minInclusive = minInclusive;
      this.maxInclusive = maxInclusive;
      this.docLowerBound = docLowerBound;
    }

    @Override
    public void visit(int docID) throws IOException {
      if (docID <= docLowerBound) {
        return; // Already visited or skipped
      }
      consumeDoc(docID);
    }

    @Override
    public void visit(int docID, byte[] packedValue) throws IOException {
      if (docID <= docLowerBound) {
        return; // already visited or skipped
      }
      long l = sortableBytesToLong(packedValue);
      if (l >= minInclusive && l <= maxInclusive) {
        consumeDoc(docID);
      }
    }

    @Override
    public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
      long l = sortableBytesToLong(packedValue);
      if (l >= minInclusive && l <= maxInclusive) {
        int doc = docLowerBound >= 0 ? iterator.advance(docLowerBound) : iterator.nextDoc();
        while (doc != DocIdSetIterator.NO_MORE_DOCS) {
          consumeDoc(doc);
          doc = iterator.nextDoc();
        }
      }
    }

    @Override
    public void visit(DocIdSetIterator iterator) throws IOException {
      int doc = docLowerBound >= 0 ? iterator.advance(docLowerBound) : iterator.nextDoc();
      while (doc != DocIdSetIterator.NO_MORE_DOCS) {
        consumeDoc(doc);
        doc = iterator.nextDoc();
      }
    }

    @Override
    public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
      long min = sortableBytesToLong(minPackedValue);
      long max = sortableBytesToLong(maxPackedValue);

      if (min > maxInclusive || max < minInclusive) {
        // 1. cmp ==0 and pruning==Pruning.GREATER_THAN_OR_EQUAL_TO : if the sort is
        // ascending then maxValueAsLong is bottom's next less value, so it is competitive
        // 2. cmp ==0 and pruning==Pruning.GREATER_THAN: maxValueAsLong equals to
        // bottom, but there are multiple comparators, so it could be competitive
        return PointValues.Relation.CELL_OUTSIDE_QUERY;
      }

      if (min < minInclusive || max > maxInclusive) {
        return PointValues.Relation.CELL_CROSSES_QUERY;
      }
      return PointValues.Relation.CELL_INSIDE_QUERY;
    }

    void consumeDoc(int doc) {
      throw new UnsupportedOperationException();
    }
  }

  private record DisiAndMostCompetitiveValue(DocIdSetIterator disi, long mostCompetitiveValue) {}

  private static class CompetitiveIterator extends DocIdSetIterator {

    private final int maxDoc;
    private int doc = -1;
    private final Deque<DisiAndMostCompetitiveValue> disis;
    private final PriorityQueue<DisiAndMostCompetitiveValue> disjunction;

    CompetitiveIterator(
        int maxDoc,
        Deque<DisiAndMostCompetitiveValue> disis,
        PriorityQueue<DisiAndMostCompetitiveValue> disjunction) {
      this.maxDoc = maxDoc;
      this.disis = disis;
      this.disjunction = disjunction;
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(docID() + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      if (target >= maxDoc) {
        return doc = NO_MORE_DOCS;
      } else {
        DisiAndMostCompetitiveValue top = disjunction.top();
        if (top == null) {
          // priority queue is empty, none of the remaining documents are competitive
          return doc = NO_MORE_DOCS;
        }
        while (top.disi.docID() < target) {
          top.disi.advance(target);
          top = disjunction.updateTop();
        }
        return doc = top.disi.docID();
      }
    }

    @Override
    public long cost() {
      return maxDoc;
    }
  }
}
