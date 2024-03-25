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
import java.util.Arrays;
import java.util.Deque;
import java.util.function.Consumer;
import java.util.function.Predicate;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntArrayDocIdSet;
import org.apache.lucene.util.LSBRadixSorter;
import org.apache.lucene.util.NumericUtils;
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

  private static long bytesAsLong(byte[] bytes) {
    return switch (bytes.length) {
      case 4 -> NumericUtils.sortableBytesToInt(bytes, 0);
      case 8 -> NumericUtils.sortableBytesToLong(bytes, 0);
      default -> throw new IllegalStateException("bytes count should be 4 or 8");
    };
  }

  protected abstract long missingValueAsComparableLong();

  /** Leaf comparator for {@link NumericComparator} that provides skipping functionality */
  public abstract class NumericLeafComparator implements LeafFieldComparator {
    private static final int MAX_DISJUNCTION_CLAUSE = 64;
    private static final int MIN_DISJUNCTION_LENGTH = 512;
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
    private long thresholdAsLong = -1;

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
        encodeTop();
      } else {
        this.enableSkipping = false;
        this.maxDoc = 0;
        this.competitiveIterator = null;
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
      if (thresholdAsLong == -1) {
        if (dense == false) {
          competitiveIterator = getNumericDocValues(context, field);
          leadCost = Math.min(leadCost, competitiveIterator.cost());
        }
        // limit the memory to maxDoc bits.
        long threshold = Math.min(leadCost >> 1, maxDoc >> 5);
        thresholdAsLong = intersectThresholdValue(threshold);
      }

      if ((reverse == false && bottomAsComparableLong() <= thresholdAsLong)
          || (reverse && bottomAsComparableLong() >= thresholdAsLong)) {
        encodeBottom();
        if (update() == false) {
          competitiveIterator = initIterator();
        }
      }
    }

    /**
     * Find out the value that {@param threshold} docs away from topValue/infinite. TODO: we are
     * asserting binary tree
     */
    private long intersectThresholdValue(long threshold) throws IOException {
      PointValues.PointTree pointTree = pointValues.getPointTree();
      if (reverse == false) {
        if (leafTopSet) {
          return intersectL2R(pointTree, threshold, topAsComparableLong());
        } else {
          return intersectL2R(pointTree, threshold);
        }
      } else {
        if (leafTopSet) {
          return intersectR2L(pointTree, threshold, topAsComparableLong());
        } else {
          return intersectR2L(pointTree, threshold);
        }
      }
    }

    private long intersectL2R(PointValues.PointTree pointTree, long threshold) throws IOException {
      if (pointTree.moveToChild()) {
        long leftSize = pointTree.size();
        if (leftSize > threshold) {
          return intersectL2R(pointTree, threshold);
        } else if (leftSize < threshold) {
          pointTree.moveToSibling();
          return intersectL2R(pointTree, threshold - leftSize);
        }
      }
      return bytesAsLong(pointTree.getMaxPackedValue());
    }

    private long intersectL2R(PointValues.PointTree pointTree, long threshold, long topValue)
        throws IOException {
      if (pointTree.moveToChild()) {
        long leftSize =
            PointValues.estimatePointCount(
                new RangeVisitor(topValue, Long.MAX_VALUE, -1), pointTree, threshold);
        if (leftSize > threshold) {
          return intersectL2R(pointTree, threshold);
        } else if (leftSize < threshold) {
          pointTree.moveToSibling();
          return intersectL2R(pointTree, threshold - leftSize);
        }
      }
      return bytesAsLong(pointTree.getMaxPackedValue());
    }

    private long intersectR2L(PointValues.PointTree pointTree, long threshold) throws IOException {
      if (pointTree.moveToChild()) {
        pointTree.moveToSibling();
        long rightSize = pointTree.size();
        if (rightSize > threshold) {
          return intersectR2L(pointTree, threshold);
        } else if (rightSize < threshold) {
          pointTree.moveToParent();
          pointTree.moveToChild();
          return intersectR2L(pointTree, threshold - rightSize);
        }
      }
      return bytesAsLong(pointTree.getMinPackedValue());
    }

    private long intersectR2L(PointValues.PointTree pointTree, long threshold, long topValue)
        throws IOException {
      if (pointTree.moveToChild()) {
        pointTree.moveToSibling();
        long rightSize =
            PointValues.estimatePointCount(
                new RangeVisitor(Long.MIN_VALUE, topValue, -1), pointTree, threshold);
        if (rightSize > threshold) {
          return intersectR2L(pointTree, threshold);
        } else if (rightSize < threshold) {
          pointTree.moveToParent();
          pointTree.moveToChild();
          return intersectR2L(pointTree, threshold - rightSize);
        }
      }
      return bytesAsLong(pointTree.getMinPackedValue());
    }

    /**
     * If {@link NumericComparator#pruning} equals {@link Pruning#GREATER_THAN_OR_EQUAL_TO}, we
     * could better tune the {@link NumericLeafComparator#maxValueAsLong}/{@link
     * NumericLeafComparator#minValueAsLong}. For instance, if the sort is ascending and bottom
     * value is 5, we will use a range on [MIN_VALUE, 4].
     */
    private void encodeBottom() {
      if (queueFull == false) {
        return;
      }
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
      if (leafTopSet == false) {
        return;
      }
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
      // if queue is full, always compare with bottom,
      // if not, check if we can compare with topValue
      if (queueFull) {
        int result = Long.compare(missingValueAsLong, bottomAsComparableLong());
        // in reverse (desc) sort missingValue is competitive when it's greater or equal to bottom,
        // in asc sort missingValue is competitive when it's smaller or equal to bottom
        return reverse
            ? (pruning == Pruning.GREATER_THAN_OR_EQUAL_TO ? result > 0 : result >= 0)
            : (pruning == Pruning.GREATER_THAN_OR_EQUAL_TO ? result < 0 : result <= 0);
      } else if (leafTopSet) {
        int result = Long.compare(missingValueAsLong, topAsComparableLong());
        // in reverse (desc) sort missingValue is competitive when it's smaller or equal to
        // topValue,
        // in asc sort missingValue is competitive when it's greater or equal to topValue
        return reverse ? (result <= 0) : (result >= 0);
      } else {
        // by default competitive
        return true;
      }
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

    private static void intersectLeaves(PointValues.PointTree pointTree, RangeVisitor visitor)
        throws IOException {
      PointValues.Relation r =
          visitor.compare(pointTree.getMinPackedValue(), pointTree.getMaxPackedValue());
      switch (r) {
        case CELL_OUTSIDE_QUERY:
          break;
        case CELL_INSIDE_QUERY:
        case CELL_CROSSES_QUERY:
          if (pointTree.moveToChild()) {
            do {
              intersectLeaves(pointTree, visitor);
            } while (pointTree.moveToSibling());
            pointTree.moveToParent();
          } else {
            if (r == PointValues.Relation.CELL_CROSSES_QUERY) {
              pointTree.visitDocValues(visitor);
            } else {
              pointTree.visitDocIDs(visitor);
            }
            visitor.updateMinMax(
                bytesAsLong(pointTree.getMinPackedValue()),
                bytesAsLong(pointTree.getMaxPackedValue()));
          }
          break;
        default:
          throw new IllegalArgumentException("Unreachable code");
      }
    }

    private DocIdSetIterator initIterator() throws IOException {
      final long cost =
          pointValues.estimatePointCount(
              new RangeVisitor(minValueAsLong, maxValueAsLong, maxDocVisited));

      final int maxDisjunctions =
          Math.min(MAX_DISJUNCTION_CLAUSE, IndexSearcher.getMaxClauseCount());
      final int disiLength =
          Math.max(Math.toIntExact(cost / maxDisjunctions) + 1, MIN_DISJUNCTION_LENGTH);
      final int size = Math.toIntExact(cost / disiLength) + 1;

      Deque<DisiAndMostCompetitiveValue> disis = new ArrayDeque<>(size);
      // most competitive entry stored last.
      Consumer<DisiAndMostCompetitiveValue> adder =
          reverse == false ? disis::addFirst : disis::addLast;
      LSBRadixSorter sorter = new LSBRadixSorter();

      intersectLeaves(
          pointValues.getPointTree(),
          new RangeVisitor(minValueAsLong, maxValueAsLong, maxDocVisited) {

            int[] docs = new int[disiLength + 1];
            int index = 0;
            int maxDocLeaf = -1;
            boolean sorted = true;

            @Override
            public void grow(int count) {
              docs = ArrayUtil.grow(docs, count + 1);
            }

            @Override
            protected void consumeDoc(int doc) {
              docs[index++] = doc;
              if (doc > maxDocLeaf) {
                maxDocLeaf = doc;
              } else {
                sorted = false;
              }
            }

            @Override
            void updateMinMax(long leafMinValue, long leafMaxValue) throws IOException {
              if (index > 0) {
                // todo: not bind with leaf size
                long mostCompetitiveValue =
                    reverse == false
                        ? Math.max(leafMinValue, minValueAsLong)
                        : Math.min(leafMaxValue, maxValueAsLong);
                if (!sorted) {
                  sorter.sort(PackedInts.bitsRequired(maxDocLeaf), docs, index);
                }
                docs[index] = DocIdSetIterator.NO_MORE_DOCS;
                adder.accept(
                    new DisiAndMostCompetitiveValue(
                        new IntArrayDocIdSet(docs, index).iterator(), mostCompetitiveValue));
                docs = new int[disiLength + 1];
                index = 0;
                maxDocLeaf = -1;
                sorted = true;
              }
            }
          });

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

    private boolean update() {
      if (competitiveIterator instanceof CompetitiveIterator iter) {
        int originalSize = iter.disis.size();
        Predicate<DisiAndMostCompetitiveValue> isCompetitive =
            d ->
                d.mostCompetitiveValue <= maxValueAsLong
                    && d.mostCompetitiveValue >= minValueAsLong;

        while (iter.disis.isEmpty() == false
            && isCompetitive.test(iter.disis.getFirst()) == false) {
          iter.disis.removeFirst();
        }

        if (originalSize != iter.disis.size()) {
          iter.disjunction.clear();
          iter.disjunction.addAll(iter.disis);
        }
        return true;
      }
      return false;
    }
  }

  private static class RangeVisitor implements PointValues.IntersectVisitor {

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
      long l = bytesAsLong(packedValue);
      if (l >= minInclusive && l <= maxInclusive) {
        consumeDoc(docID);
      }
    }

    @Override
    public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
      long min = bytesAsLong(minPackedValue);
      long max = bytesAsLong(maxPackedValue);

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

    /** Set the min/max point value of the docs visited since last set. */
    void updateMinMax(long leafMinValue, long leafMaxValue) throws IOException {
      throw new UnsupportedOperationException();
    }

    void consumeDoc(int doc) throws IOException {
      throw new UnsupportedOperationException();
    }
  }

  private static class DisiAndMostCompetitiveValue {
    private final DocIdSetIterator disi;
    private final long mostCompetitiveValue;

    private DisiAndMostCompetitiveValue(DocIdSetIterator disi, long mostCompetitiveValue) {
      this.disi = disi;
      this.mostCompetitiveValue = mostCompetitiveValue;
    }

    @Override
    public String toString() {
      return "DisiAndMostCompetitiveValue{"
          + "disi.cost="
          + disi.cost()
          + ", mostCompetitiveValue="
          + mostCompetitiveValue
          + '}';
    }
  }

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

    @Override
    public String toString() {
      return "CompetitiveIterator{"
          + "maxDoc="
          + maxDoc
          + ", doc="
          + doc
          + ", disis="
          + disis
          + '}';
    }
  }
}
