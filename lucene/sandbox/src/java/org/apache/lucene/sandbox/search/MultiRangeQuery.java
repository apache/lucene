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

package org.apache.lucene.sandbox.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.DocIdSetBuilder;

/**
 * Abstract class for range queries involving multiple ranges against physical points such as {@code
 * IntPoints} All ranges are logically ORed together
 *
 * @lucene.experimental
 */
public abstract class MultiRangeQuery extends Query implements Cloneable {
  /** Representation of a single clause in a MultiRangeQuery */
  public static final class RangeClause {
    byte[] lowerValue;
    byte[] upperValue;

    public RangeClause(byte[] lowerValue, byte[] upperValue) {
      this.lowerValue = lowerValue;
      this.upperValue = upperValue;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RangeClause that = (RangeClause) o;
      return Arrays.equals(lowerValue, that.lowerValue)
          && Arrays.equals(upperValue, that.upperValue);
    }

    @Override
    public int hashCode() {
      int result = Arrays.hashCode(lowerValue);
      result = 31 * result + Arrays.hashCode(upperValue);
      return result;
    }
  }

  /** A builder for multirange queries. */
  public abstract static class Builder {

    protected final String field;
    protected final int bytesPerDim;
    protected final int numDims;
    protected final List<RangeClause> clauses = new ArrayList<>();

    /** Sole constructor. */
    public Builder(String field, int bytesPerDim, int numDims) {
      if (field == null) {
        throw new IllegalArgumentException("field should not be null");
      }
      if (bytesPerDim <= 0) {
        throw new IllegalArgumentException("bytesPerDim should be a valid value");
      }
      if (numDims <= 0) {
        throw new IllegalArgumentException("numDims should be a valid value");
      }

      this.field = field;
      this.bytesPerDim = bytesPerDim;
      this.numDims = numDims;
    }

    /** Add a new clause to this {@link Builder}. */
    public Builder add(RangeClause clause) {
      clauses.add(clause);
      return this;
    }

    /** Add a new clause to this {@link Builder}. */
    public Builder add(byte[] lowerValue, byte[] upperValue) {
      checkArgs(lowerValue, upperValue);
      return add(new RangeClause(lowerValue, upperValue));
    }

    /**
     * Create a new {@link MultiRangeQuery} based on the parameters that have been set on this
     * builder.
     */
    public abstract MultiRangeQuery build();

    /**
     * Check preconditions for all factory methods
     *
     * @throws IllegalArgumentException if {@code field}, {@code lowerPoint} or {@code upperPoint}
     *     are null.
     */
    private void checkArgs(Object lowerPoint, Object upperPoint) {
      if (lowerPoint == null) {
        throw new IllegalArgumentException("lowerPoint must not be null");
      }
      if (upperPoint == null) {
        throw new IllegalArgumentException("upperPoint must not be null");
      }
    }
  }

  final String field;
  final int numDims;
  final int bytesPerDim;
  List<RangeClause> rangeClauses;
  /**
   * Expert: create a multidimensional range query with multiple connected ranges
   *
   * @param field field name. must not be {@code null}.
   * @param rangeClauses Range Clauses for this query
   * @param numDims number of dimensions.
   */
  protected MultiRangeQuery(
      String field, int numDims, int bytesPerDim, List<RangeClause> rangeClauses) {
    this.field = field;
    this.numDims = numDims;
    this.bytesPerDim = bytesPerDim;
    this.rangeClauses = rangeClauses;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  /**
   * Merges the overlapping ranges and returns unconnected ranges by calling {@link
   * #mergeOverlappingRanges}
   */
  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    if (numDims != 1) {
      return this;
    }
    List<RangeClause> mergedRanges = mergeOverlappingRanges(rangeClauses, bytesPerDim);
    if (mergedRanges != rangeClauses) {
      try {
        MultiRangeQuery clone = (MultiRangeQuery) super.clone();
        clone.rangeClauses = mergedRanges;
        return clone;
      } catch (CloneNotSupportedException e) {
        throw new AssertionError(e);
      }
    } else {
      return this;
    }
  }

  /**
   * Merges overlapping ranges and returns unconnected ranges
   *
   * @param rangeClauses some overlapping ranges
   * @param bytesPerDim bytes per Dimension of the point value
   * @return unconnected ranges
   */
  static List<RangeClause> mergeOverlappingRanges(List<RangeClause> rangeClauses, int bytesPerDim) {
    if (rangeClauses.size() <= 1) {
      return rangeClauses;
    }
    List<RangeClause> originRangeClause = new ArrayList<>(rangeClauses);
    final ArrayUtil.ByteArrayComparator comparator = ArrayUtil.getUnsignedComparator(bytesPerDim);
    originRangeClause.sort(
        new Comparator<RangeClause>() {
          @Override
          public int compare(RangeClause o1, RangeClause o2) {
            int result = comparator.compare(o1.lowerValue, 0, o2.lowerValue, 0);
            if (result == 0) {
              return comparator.compare(o1.upperValue, 0, o2.upperValue, 0);
            } else {
              return result;
            }
          }
        });
    List<RangeClause> finalRangeClause = new ArrayList<>();
    RangeClause current = originRangeClause.get(0);
    for (int i = 1; i < originRangeClause.size(); i++) {
      RangeClause nextClause = originRangeClause.get(i);
      if (comparator.compare(nextClause.lowerValue, 0, current.upperValue, 0) > 0) {
        finalRangeClause.add(current);
        current = nextClause;
      } else {
        if (comparator.compare(nextClause.upperValue, 0, current.upperValue, 0) > 0) {
          current = new RangeClause(current.lowerValue, nextClause.upperValue);
        }
      }
    }
    finalRangeClause.add(current);
    /**
     * in {@link #rewrite} it compares the returned rangeClauses with origin rangeClauses to decide
     * if rewrite should return a new query or the origin query
     */
    if (finalRangeClause.size() != rangeClauses.size()) {
      return finalRangeClause;
    } else {
      return rangeClauses;
    }
  }

  /*
   * TODO: Organize ranges similar to how EdgeTree does, to avoid linear scan of ranges
   */
  @Override
  public final Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {

    final ArrayUtil.ByteArrayComparator comparator = ArrayUtil.getUnsignedComparator(bytesPerDim);

    // We don't use RandomAccessWeight here: it's no good to approximate with "match all docs".
    // This is an inverted structure and should be used in the first pass:

    return new ConstantScoreWeight(this, boost) {

      private PointValues.IntersectVisitor getIntersectVisitor(
          DocIdSetBuilder result, Relatable range) {
        return new PointValues.IntersectVisitor() {

          DocIdSetBuilder.BulkAdder adder;

          @Override
          public void grow(int count) {
            adder = result.grow(count);
          }

          @Override
          public void visit(int docID) {
            adder.add(docID);
          }

          @Override
          public void visit(DocIdSetIterator iterator) throws IOException {
            adder.add(iterator);
          }

          @Override
          public void visit(int docID, byte[] packedValue) {
            if (range.matches(packedValue)) {
              adder.add(docID);
            }
          }

          @Override
          public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
            return range.relate(minPackedValue, maxPackedValue);
          }
        };
      }

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();

        PointValues values = reader.getPointValues(field);
        if (values == null) {
          // No docs in this segment/field indexed any points
          return null;
        }

        if (values.getNumIndexDimensions() != numDims) {
          throw new IllegalArgumentException(
              "field=\""
                  + field
                  + "\" was indexed with numIndexDimensions="
                  + values.getNumIndexDimensions()
                  + " but this query has numDims="
                  + numDims);
        }
        if (bytesPerDim != values.getBytesPerDimension()) {
          throw new IllegalArgumentException(
              "field=\""
                  + field
                  + "\" was indexed with bytesPerDim="
                  + values.getBytesPerDimension()
                  + " but this query has bytesPerDim="
                  + bytesPerDim);
        }

        Relatable range;
        if (rangeClauses.size() == 1) {
          range = getRange(rangeClauses.get(0), numDims, bytesPerDim, comparator);
        } else {
          range = createTree(rangeClauses, numDims, bytesPerDim, comparator);
        }

        boolean allDocsMatch;
        if (values.getDocCount() == reader.maxDoc()) {
          final byte[] fieldPackedLower = values.getMinPackedValue();
          final byte[] fieldPackedUpper = values.getMaxPackedValue();
          allDocsMatch =
              range.relate(fieldPackedLower, fieldPackedUpper)
                  == PointValues.Relation.CELL_INSIDE_QUERY;
        } else {
          allDocsMatch = false;
        }

        final Weight weight = this;
        if (allDocsMatch) {
          // all docs have a value and all points are within bounds, so everything matches
          return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) {
              return new ConstantScoreScorer(
                  weight, score(), scoreMode, DocIdSetIterator.all(reader.maxDoc()));
            }

            @Override
            public long cost() {
              return reader.maxDoc();
            }
          };
        } else {
          return new ScorerSupplier() {

            final DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values, field);
            final PointValues.IntersectVisitor visitor = getIntersectVisitor(result, range);
            long cost = -1;

            @Override
            public Scorer get(long leadCost) throws IOException {
              values.intersect(visitor);
              DocIdSetIterator iterator = result.build().iterator();
              return new ConstantScoreScorer(weight, score(), scoreMode, iterator);
            }

            @Override
            public long cost() {
              if (cost == -1) {
                // Computing the cost may be expensive, so only do it if necessary
                cost = values.estimateDocCount(visitor) * rangeClauses.size();
                assert cost >= 0;
              }
              return cost;
            }
          };
        }
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        ScorerSupplier scorerSupplier = scorerSupplier(context);
        if (scorerSupplier == null) {
          return null;
        }
        return scorerSupplier.get(Long.MAX_VALUE);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return true;
      }

      @Override
      public int count(LeafReaderContext context) throws IOException {
        if (numDims != 1 || context.reader().hasDeletions() == true) {
          return super.count(context);
        }
        PointValues pointValues = context.reader().getPointValues(field);
        if (pointValues == null || pointValues.size() != pointValues.getDocCount()) {
          return super.count(context);
        }
        int total = 0;
        for (RangeClause rangeClause : rangeClauses) {
          PointRangeQuery pointRangeQuery =
              new PointRangeQuery(field, rangeClause.lowerValue, rangeClause.upperValue, numDims) {
                @Override
                protected String toString(int dimension, byte[] value) {
                  return MultiRangeQuery.this.toString(dimension, value);
                }
              };
          int count =
              pointRangeQuery
                  .createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1f)
                  .count(context);

          if (count != -1) {
            total += count;
          } else {
            return super.count(context);
          }
        }
        return total;
      }
    };
  }

  public String getField() {
    return field;
  }

  public int getNumDims() {
    return numDims;
  }

  public int getBytesPerDim() {
    return bytesPerDim;
  }

  @Override
  public final int hashCode() {
    int hash = classHash();
    hash = 31 * hash + field.hashCode();
    hash = 31 * hash + rangeClauses.hashCode();
    hash = 31 * hash + numDims;
    hash = 31 * hash + Objects.hashCode(bytesPerDim);
    return hash;
  }

  @Override
  public final boolean equals(Object o) {
    return sameClassAs(o) && equalsTo(getClass().cast(o));
  }

  private boolean equalsTo(MultiRangeQuery other) {
    return Objects.equals(field, other.field)
        && numDims == other.numDims
        && bytesPerDim == other.bytesPerDim
        && rangeClauses.equals(other.rangeClauses);
  }

  @Override
  public final String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    if (this.field.equals(field) == false) {
      sb.append(this.field);
      sb.append(':');
    }

    int count = 0;
    // print ourselves as "range per dimension per value"
    for (RangeClause rangeClause : rangeClauses) {
      if (count > 0) {
        sb.append(',');
      }
      sb.append('{');
      for (int i = 0; i < numDims; i++) {
        if (i > 0) {
          sb.append(',');
        }

        int startOffset = bytesPerDim * i;

        sb.append('[');
        sb.append(
            toString(
                i,
                ArrayUtil.copyOfSubArray(
                    rangeClause.lowerValue, startOffset, startOffset + bytesPerDim)));
        sb.append(" TO ");
        sb.append(
            toString(
                i,
                ArrayUtil.copyOfSubArray(
                    rangeClause.upperValue, startOffset, startOffset + bytesPerDim)));
        sb.append(']');
      }
      sb.append('}');
      ++count;
    }

    return sb.toString();
  }

  /**
   * Returns a string of a single value in a human-readable format for debugging. This is used by
   * {@link #toString()}.
   *
   * @param dimension dimension of the particular value
   * @param value single value, never null
   * @return human readable value for debugging
   */
  protected abstract String toString(int dimension, byte[] value);

  /**
   * Represents a range that can compute its relation with another range and can compute if a point
   * is inside it
   */
  private interface Relatable {
    /** return true if the provided point is inside the range */
    boolean matches(byte[] packedValue);
    /** return the relation between this range and the provided range */
    PointValues.Relation relate(byte[] minPackedValue, byte[] maxPackedValue);
  }

  /**
   * A range represents anything with a min/max value that can compute its relation with another
   * range and can compute if a point is inside it
   */
  private interface Range extends Relatable {
    /** min value of this range */
    byte[] getMinPackedValue();
    /** max value of this range */
    byte[] getMaxPackedValue();
  }

  /** An interval tree of Ranges for speeding up computations */
  private static class RangeTree implements Relatable {
    /** maximum value contained in this range sub-tree */
    private final byte[] maxPackedValue;

    /** Left child, it can be null */
    private RangeTree left;
    /** Right child, it can be null */
    private RangeTree right;
    /** which dimension was this node split on */
    private final int split;
    /** Range of this tree node */
    private final Range component;
    // Utility variables for computing relationships
    private final ArrayUtil.ByteArrayComparator comparator;
    private final int numIndexDim;
    private final int bytesPerDim;

    private RangeTree(
        Range component,
        int split,
        ArrayUtil.ByteArrayComparator comparator,
        int numIndexDim,
        int bytesPerDim) {
      this.maxPackedValue = component.getMaxPackedValue().clone();
      this.component = component;
      this.split = split;
      this.comparator = comparator;
      this.numIndexDim = numIndexDim;
      this.bytesPerDim = bytesPerDim;
    }

    @Override
    public boolean matches(byte[] packedValue) {
      boolean valid = true;
      for (int i = 0; i < numIndexDim; i++) {
        int offset = bytesPerDim * i;
        if (comparator.compare(packedValue, offset, maxPackedValue, offset) > 0) {
          valid = false;
          break;
        }
      }
      if (valid) {
        if (component.matches(packedValue)) {
          return true;
        }
        if (left != null) {
          if (left.matches(packedValue)) {
            return true;
          }
        }
        if (right != null
            && comparator.compare(
                    packedValue,
                    split * bytesPerDim,
                    component.getMinPackedValue(),
                    split * bytesPerDim)
                >= 0) {
          if (right.matches(packedValue)) {
            return true;
          }
        }
      }
      return false;
    }

    @Override
    public PointValues.Relation relate(byte[] minPackedValue, byte[] maxPackedValue) {
      boolean valid = true;
      for (int i = 0; i < numIndexDim; i++) {
        int offset = bytesPerDim * i;
        if (comparator.compare(minPackedValue, offset, this.maxPackedValue, offset) > 0) {
          valid = false;
          break;
        }
      }
      if (valid) {
        PointValues.Relation relation = component.relate(minPackedValue, maxPackedValue);
        if (relation != PointValues.Relation.CELL_OUTSIDE_QUERY) {
          return relation;
        }
        if (left != null) {
          relation = left.relate(minPackedValue, maxPackedValue);
          if (relation != PointValues.Relation.CELL_OUTSIDE_QUERY) {
            return relation;
          }
        }
        if (right != null
            && comparator.compare(
                    maxPackedValue,
                    split * bytesPerDim,
                    component.getMinPackedValue(),
                    split * bytesPerDim)
                >= 0) {
          relation = right.relate(minPackedValue, maxPackedValue);
          if (relation != PointValues.Relation.CELL_OUTSIDE_QUERY) {
            return relation;
          }
        }
      }
      return PointValues.Relation.CELL_OUTSIDE_QUERY;
    }
  }

  /** Creates a tree from provided clauses */
  static RangeTree createTree(
      List<RangeClause> clauses,
      int numIndexDim,
      int bytesPerDim,
      ArrayUtil.ByteArrayComparator comparator) {
    Range[] ranges = new Range[clauses.size()];
    for (int i = 0; i < clauses.size(); i++) {
      ranges[i] = getRange(clauses.get(i), numIndexDim, bytesPerDim, comparator);
    }

    return createTree(ranges, 0, ranges.length - 1, 0, numIndexDim, bytesPerDim, comparator);
  }

  /** Creates tree from sorted ranges (with range low and high inclusive) */
  private static RangeTree createTree(
      Range[] components,
      int low,
      int high,
      int split,
      int numIndexDim,
      int bytesPerDim,
      ArrayUtil.ByteArrayComparator comparator) {
    if (low > high) {
      return null;
    }
    final int mid = (low + high) >>> 1;
    if (low < high) {
      int offset = split * bytesPerDim;
      Comparator<Range> comp =
          (left, right) -> {
            int ret =
                comparator.compare(
                    left.getMinPackedValue(), offset, right.getMinPackedValue(), offset);
            if (ret == 0) {
              ret =
                  comparator.compare(
                      left.getMaxPackedValue(), offset, right.getMaxPackedValue(), offset);
            }
            return ret;
          };
      ArrayUtil.select(components, low, high + 1, mid, comp);
    }
    RangeTree newNode = new RangeTree(components[mid], split, comparator, numIndexDim, bytesPerDim);
    // find children
    split++;
    if (split == numIndexDim) {
      split = 0;
    }
    newNode.left =
        createTree(components, low, mid - 1, split, numIndexDim, bytesPerDim, comparator);
    newNode.right =
        createTree(components, mid + 1, high, split, numIndexDim, bytesPerDim, comparator);

    // pull up max values to this node
    if (newNode.left != null) {
      for (int i = 0; i < numIndexDim; i++) {
        int offset = i * bytesPerDim;
        if (comparator.compare(newNode.maxPackedValue, offset, newNode.left.maxPackedValue, offset)
            < 0) {
          System.arraycopy(
              newNode.left.maxPackedValue, offset, newNode.maxPackedValue, offset, bytesPerDim);
        }
      }
    }
    if (newNode.right != null) {
      for (int i = 0; i < numIndexDim; i++) {
        int offset = i * bytesPerDim;
        if (comparator.compare(newNode.maxPackedValue, offset, newNode.right.maxPackedValue, offset)
            < 0) {
          System.arraycopy(
              newNode.right.maxPackedValue, offset, newNode.maxPackedValue, offset, bytesPerDim);
        }
      }
    }
    return newNode;
  }

  /** Builds a Range object from a range clause */
  private static Range getRange(
      RangeClause clause,
      int numIndexDim,
      int bytesPerDim,
      ArrayUtil.ByteArrayComparator comparator) {
    return new Range() {
      @Override
      public byte[] getMinPackedValue() {
        return clause.lowerValue;
      }

      @Override
      public byte[] getMaxPackedValue() {
        return clause.upperValue;
      }

      @Override
      public boolean matches(byte[] packedValue) {
        for (int dim = 0; dim < numIndexDim; dim++) {
          int offset = dim * bytesPerDim;
          if (comparator.compare(packedValue, offset, clause.lowerValue, offset) < 0) {
            // Doc's value is too low, in this dimension
            return false;
          }
          if (comparator.compare(packedValue, offset, clause.upperValue, offset) > 0) {
            // Doc's value is too high, in this dimension
            return false;
          }
        }
        return true;
      }

      @Override
      public PointValues.Relation relate(byte[] minPackedValue, byte[] maxPackedValue) {
        boolean crosses = false;

        for (int dim = 0; dim < numIndexDim; dim++) {
          int offset = dim * bytesPerDim;

          if (comparator.compare(minPackedValue, offset, clause.upperValue, offset) > 0
              || comparator.compare(maxPackedValue, offset, clause.lowerValue, offset) < 0) {
            return PointValues.Relation.CELL_OUTSIDE_QUERY;
          }

          crosses |=
              comparator.compare(minPackedValue, offset, clause.lowerValue, offset) < 0
                  || comparator.compare(maxPackedValue, offset, clause.upperValue, offset) > 0;
        }

        if (crosses) {
          return PointValues.Relation.CELL_CROSSES_QUERY;
        } else {
          return PointValues.Relation.CELL_INSIDE_QUERY;
        }
      }
    };
  }
}
