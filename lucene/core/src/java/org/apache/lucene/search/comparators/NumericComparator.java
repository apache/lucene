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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.AbstractDocIdSetIterator;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IntsRef;

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

  // MIN_SKIP_INTERVAL and MAX_SKIP_INTERVAL both should be powers of 2
  private static final int MIN_SKIP_INTERVAL = 32;
  private static final int MAX_SKIP_INTERVAL = 8192;
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
    private final LeafReaderContext context;
    protected final NumericDocValues docValues;
    private final PointValues pointValues;
    // lazily constructed to avoid performance overhead when this is not used
    private PointValues.PointTree pointTree;
    // if skipping functionality should be enabled on this segment
    private final boolean enableSkipping;
    private final int maxDoc;

    /** According to {@link FieldComparator#setTopValue}, topValueSet is final in leafComparator */
    private final boolean leafTopSet = topValueSet;

    private long minValueAsLong = Long.MIN_VALUE;
    private long maxValueAsLong = Long.MAX_VALUE;

    private DocIdSetIterator competitiveIterator;
    private long iteratorCost = -1;
    private int maxDocVisited = -1;
    private int updateCounter = 0;
    private int currentSkipInterval = MIN_SKIP_INTERVAL;
    // helps to be conservative about increasing the sampling interval
    private int tryUpdateFailCount = 0;

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
      if (iteratorCost == -1) {
        if (scorer instanceof Scorer) {
          iteratorCost =
              ((Scorer) scorer).iterator().cost(); // starting iterator cost is the scorer's cost
        } else {
          iteratorCost = maxDoc;
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
      if ((pointValues.getDocCount() < maxDoc) && isMissingValueCompetitive()) {
        return; // we can't filter out documents, as documents with missing values are competitive
      }

      updateCounter++;
      // Start sampling if we get called too much
      if (updateCounter > 256
          && (updateCounter & (currentSkipInterval - 1)) != currentSkipInterval - 1) {
        return;
      }

      if (queueFull) {
        encodeBottom();
      }

      DocIdSetBuilder result = new DocIdSetBuilder(maxDoc);
      PointValues.IntersectVisitor visitor =
          new PointValues.IntersectVisitor() {
            DocIdSetBuilder.BulkAdder adder;

            @Override
            public void grow(int count) {
              adder = result.grow(count);
            }

            @Override
            public void visit(int docID) {
              if (docID <= maxDocVisited) {
                return; // Already visited or skipped
              }
              adder.add(docID);
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
              if (docID <= maxDocVisited) {
                return; // already visited or skipped
              }
              long l = sortableBytesToLong(packedValue);
              if (l >= minValueAsLong && l <= maxValueAsLong) {
                adder.add(docID); // doc is competitive
              }
            }

            @Override
            public void visit(DocIdSetIterator iterator) throws IOException {
              if (iterator.advance(maxDocVisited + 1) != DocIdSetIterator.NO_MORE_DOCS) {
                adder.add(iterator.docID());
                adder.add(iterator);
              }
            }

            @Override
            public void visit(IntsRef ref) {
              adder.add(ref, maxDocVisited + 1);
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
              long min = sortableBytesToLong(minPackedValue);
              long max = sortableBytesToLong(maxPackedValue);

              if (min > maxValueAsLong || max < minValueAsLong) {
                // 1. cmp ==0 and pruning==Pruning.GREATER_THAN_OR_EQUAL_TO : if the sort is
                // ascending then maxValueAsLong is bottom's next less value, so it is competitive
                // 2. cmp ==0 and pruning==Pruning.GREATER_THAN: maxValueAsLong equals to
                // bottom, but there are multiple comparators, so it could be competitive
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
              }

              if (min < minValueAsLong || max > maxValueAsLong) {
                return PointValues.Relation.CELL_CROSSES_QUERY;
              }
              return PointValues.Relation.CELL_INSIDE_QUERY;
            }
          };

      final long threshold = iteratorCost >>> 3;

      if (PointValues.isEstimatedPointCountGreaterThanOrEqualTo(
          visitor, getPointTree(), threshold)) {
        // the new range is not selective enough to be worth materializing, it doesn't reduce number
        // of docs at least 8x
        updateSkipInterval(false);
        if (pointValues.getDocCount() < iteratorCost) {
          // Use the set of doc with values to help drive iteration
          competitiveIterator = getNumericDocValues(context, field);
          iteratorCost = pointValues.getDocCount();
        }
        return;
      }
      pointValues.intersect(visitor);
      competitiveIterator = result.build().iterator();
      iteratorCost = competitiveIterator.cost();
      updateSkipInterval(true);
    }

    private PointValues.PointTree getPointTree() throws IOException {
      if (pointTree == null) {
        pointTree = pointValues.getPointTree();
      }
      return pointTree;
    }

    private void updateSkipInterval(boolean success) {
      if (updateCounter > 256) {
        if (success) {
          currentSkipInterval = Math.max(currentSkipInterval / 2, MIN_SKIP_INTERVAL);
          tryUpdateFailCount = 0;
        } else {
          if (tryUpdateFailCount >= 3) {
            currentSkipInterval = Math.min(currentSkipInterval * 2, MAX_SKIP_INTERVAL);
            tryUpdateFailCount = 0;
          } else {
            tryUpdateFailCount++;
          }
        }
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
      return new AbstractDocIdSetIterator() {

        {
          doc = competitiveIterator.docID();
        }

        @Override
        public int nextDoc() throws IOException {
          return advance(doc + 1);
        }

        @Override
        public long cost() {
          return competitiveIterator.cost();
        }

        @Override
        public int advance(int target) throws IOException {
          return doc = competitiveIterator.advance(target);
        }

        @Override
        public void intoBitSet(int upTo, FixedBitSet bitSet, int offset) throws IOException {
          // The competitive iterator is usually a BitSetIterator, which has an optimized
          // implementation of #intoBitSet.
          if (competitiveIterator.docID() < doc) {
            competitiveIterator.advance(doc);
          }
          competitiveIterator.intoBitSet(upTo, bitSet, offset);
          doc = competitiveIterator.docID();
        }
      };
    }

    protected abstract long bottomAsComparableLong();

    protected abstract long topAsComparableLong();
  }
}
