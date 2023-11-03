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
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ArrayUtil.ByteArrayComparator;
import org.apache.lucene.util.DocIdSetBuilder;

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
  protected final String field;
  protected final boolean reverse;
  private final int bytesCount; // how many bytes are used to encode this number
  private final ByteArrayComparator bytesComparator;

  protected boolean topValueSet;
  protected boolean singleSort; // singleSort is true, if sort is based on a single sort field.
  protected boolean hitsThresholdReached;
  protected boolean queueFull;
  private boolean canSkipDocuments;

  protected NumericComparator(
      String field, T missingValue, boolean reverse, boolean enableSkipping, int bytesCount) {
    this.field = field;
    this.missingValue = missingValue;
    this.reverse = reverse;
    // skipping functionality is only relevant for primary sort
    this.canSkipDocuments = enableSkipping;
    this.bytesCount = bytesCount;
    this.bytesComparator = ArrayUtil.getUnsignedComparator(bytesCount);
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
    canSkipDocuments = false;
  }

  /** Leaf comparator for {@link NumericComparator} that provides skipping functionality */
  public abstract class NumericLeafComparator implements LeafFieldComparator {
    protected final NumericDocValues docValues;
    private final PointValues pointValues;
    // if skipping functionality should be enabled on this segment
    private final boolean enableSkipping;
    private final int maxDoc;
    private byte[] minValueAsBytes;
    private byte[] maxValueAsBytes;

    private DocIdSetIterator competitiveIterator;
    private long iteratorCost = -1;
    private int maxDocVisited = -1;
    private int updateCounter = 0;
    private int currentSkipInterval = MIN_SKIP_INTERVAL;
    // helps to be conservative about increasing the sampling interval
    private int tryUpdateFailCount = 0;

    public NumericLeafComparator(LeafReaderContext context) throws IOException {
      this.docValues = getNumericDocValues(context, field);
      this.pointValues = canSkipDocuments ? context.reader().getPointValues(field) : null;
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
          || (queueFull == false && topValueSet == false)) return;
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
      if (reverse == false) {
        if (queueFull) { // bottom is avilable only when queue is full
          maxValueAsBytes = maxValueAsBytes == null ? new byte[bytesCount] : maxValueAsBytes;
          encodeBottom(maxValueAsBytes);
        }
        if (topValueSet) {
          minValueAsBytes = minValueAsBytes == null ? new byte[bytesCount] : minValueAsBytes;
          encodeTop(minValueAsBytes);
        }
      } else {
        if (queueFull) { // bottom is avilable only when queue is full
          minValueAsBytes = minValueAsBytes == null ? new byte[bytesCount] : minValueAsBytes;
          encodeBottom(minValueAsBytes);
        }
        if (topValueSet) {
          maxValueAsBytes = maxValueAsBytes == null ? new byte[bytesCount] : maxValueAsBytes;
          encodeTop(maxValueAsBytes);
        }
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
              if (maxValueAsBytes != null) {
                int cmp = bytesComparator.compare(packedValue, 0, maxValueAsBytes, 0);
                // if doc's value is too high or for single sort even equal, it is not competitive
                // and the doc can be skipped
                if (cmp > 0 || (singleSort && cmp == 0)) return;
              }
              if (minValueAsBytes != null) {
                int cmp = bytesComparator.compare(packedValue, 0, minValueAsBytes, 0);
                // if doc's value is too low or for single sort even equal, it is not competitive
                // and the doc can be skipped
                if (cmp < 0 || (singleSort && cmp == 0)) return;
              }
              adder.add(docID); // doc is competitive
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
              if (maxValueAsBytes != null) {
                int cmp = bytesComparator.compare(minPackedValue, 0, maxValueAsBytes, 0);
                if (cmp > 0 || (singleSort && cmp == 0))
                  return PointValues.Relation.CELL_OUTSIDE_QUERY;
              }
              if (minValueAsBytes != null) {
                int cmp = bytesComparator.compare(maxPackedValue, 0, minValueAsBytes, 0);
                if (cmp < 0 || (singleSort && cmp == 0))
                  return PointValues.Relation.CELL_OUTSIDE_QUERY;
              }
              if ((maxValueAsBytes != null
                      && bytesComparator.compare(maxPackedValue, 0, maxValueAsBytes, 0) > 0)
                  || (minValueAsBytes != null
                      && bytesComparator.compare(minPackedValue, 0, minValueAsBytes, 0) < 0)) {
                return PointValues.Relation.CELL_CROSSES_QUERY;
              }
              return PointValues.Relation.CELL_INSIDE_QUERY;
            }
          };
      final long threshold = iteratorCost >>> 3;
      long estimatedNumberOfMatches =
          pointValues.estimatePointCount(visitor); // runs in O(log(numPoints))
      if (estimatedNumberOfMatches >= threshold) {
        // the new range is not selective enough to be worth materializing, it doesn't reduce number
        // of docs at least 8x
        updateSkipInterval(false);
        return;
      }
      pointValues.intersect(visitor);
      competitiveIterator = result.build().iterator();
      iteratorCost = competitiveIterator.cost();
      updateSkipInterval(true);
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

    private boolean isMissingValueCompetitive() {
      // if queue is full, always compare with bottom,
      // if not, check if we can compare with topValue
      if (queueFull) {
        int result = compareMissingValueWithBottomValue();
        // in reverse (desc) sort missingValue is competitive when it's greater or equal to bottom,
        // in asc sort missingValue is competitive when it's smaller or equal to bottom
        return reverse ? (result >= 0) : (result <= 0);
      } else if (topValueSet) {
        int result = compareMissingValueWithTopValue();
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

    protected abstract int compareMissingValueWithTopValue();

    protected abstract int compareMissingValueWithBottomValue();

    protected abstract void encodeBottom(byte[] packedValue);

    protected abstract void encodeTop(byte[] packedValue);
  }
}
