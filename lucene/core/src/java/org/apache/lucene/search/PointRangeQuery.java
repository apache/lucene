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
import java.util.Arrays;
import java.util.Objects;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ArrayUtil.ByteArrayComparator;
import org.apache.lucene.util.search.PointQueryUtils;

/**
 * Abstract class for range queries against single or multidimensional points such as {@link
 * IntPoint}.
 *
 * <p>This is for subclasses and works on the underlying binary encoding: to create range queries
 * for lucene's standard {@code Point} types, refer to factory methods on those classes, e.g. {@link
 * IntPoint#newRangeQuery IntPoint.newRangeQuery()} for fields indexed with {@link IntPoint}.
 *
 * <p>For a single-dimensional field this query is a simple range query; in a multi-dimensional
 * field it's a box shape.
 *
 * @see PointValues
 * @lucene.experimental
 */
public abstract class PointRangeQuery extends Query {
  final String field;
  final int numDims;
  final int bytesPerDim;
  final byte[] lowerPoint;
  final byte[] upperPoint;
  final ByteArrayComparator comparator;

  /**
   * Expert: create a multidimensional range query for point values.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerPoint lower portion of the range (inclusive).
   * @param upperPoint upper portion of the range (inclusive).
   * @param numDims number of dimensions.
   * @throws IllegalArgumentException if {@code field} is null, or if {@code lowerValue.length !=
   *     upperValue.length}
   */
  protected PointRangeQuery(String field, byte[] lowerPoint, byte[] upperPoint, int numDims) {
    checkArgs(field, lowerPoint, upperPoint);
    this.field = field;
    if (numDims <= 0) {
      throw new IllegalArgumentException("numDims must be positive, got " + numDims);
    }
    if (lowerPoint.length == 0) {
      throw new IllegalArgumentException("lowerPoint has length of zero");
    }
    if (lowerPoint.length % numDims != 0) {
      throw new IllegalArgumentException("lowerPoint is not a fixed multiple of numDims");
    }
    if (lowerPoint.length != upperPoint.length) {
      throw new IllegalArgumentException(
          "lowerPoint has length="
              + lowerPoint.length
              + " but upperPoint has different length="
              + upperPoint.length);
    }
    this.numDims = numDims;
    this.bytesPerDim = lowerPoint.length / numDims;

    this.lowerPoint = lowerPoint;
    this.upperPoint = upperPoint;

    this.comparator = ArrayUtil.getUnsignedComparator(bytesPerDim);
  }

  /**
   * Check preconditions for all factory methods
   *
   * @throws IllegalArgumentException if {@code field}, {@code lowerPoint} or {@code upperPoint} are
   *     null.
   */
  public static void checkArgs(String field, Object lowerPoint, Object upperPoint) {
    if (field == null) {
      throw new IllegalArgumentException("field must not be null");
    }
    if (lowerPoint == null) {
      throw new IllegalArgumentException("lowerPoint must not be null");
    }
    if (upperPoint == null) {
      throw new IllegalArgumentException("upperPoint must not be null");
    }
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public final Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {

    // We don't use RandomAccessWeight here: it's no good to approximate with "match all docs".
    // This is an inverted structure and should be used in the first pass:

    return new PointRangeWeight(this, boost, scoreMode);
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

  public byte[] getLowerPoint() {
    return lowerPoint.clone();
  }

  public byte[] getUpperPoint() {
    return upperPoint.clone();
  }

  @Override
  public final int hashCode() {
    int hash = classHash();
    hash = 31 * hash + field.hashCode();
    hash = 31 * hash + Arrays.hashCode(lowerPoint);
    hash = 31 * hash + Arrays.hashCode(upperPoint);
    hash = 31 * hash + numDims;
    hash = 31 * hash + Objects.hashCode(bytesPerDim);
    return hash;
  }

  @Override
  public final boolean equals(Object o) {
    return sameClassAs(o) && equalsTo(getClass().cast(o));
  }

  private boolean equalsTo(PointRangeQuery other) {
    return Objects.equals(field, other.field)
        && numDims == other.numDims
        && bytesPerDim == other.bytesPerDim
        && Arrays.equals(lowerPoint, other.lowerPoint)
        && Arrays.equals(upperPoint, other.upperPoint);
  }

  @Override
  public final String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    if (this.field.equals(field) == false) {
      sb.append(this.field);
      sb.append(':');
    }

    // print ourselves as "range per dimension"
    for (int i = 0; i < numDims; i++) {
      if (i > 0) {
        sb.append(',');
      }

      int startOffset = bytesPerDim * i;

      sb.append('[');
      sb.append(
          toString(
              i, ArrayUtil.copyOfSubArray(lowerPoint, startOffset, startOffset + bytesPerDim)));
      sb.append(" TO ");
      sb.append(
          toString(
              i, ArrayUtil.copyOfSubArray(upperPoint, startOffset, startOffset + bytesPerDim)));
      sb.append(']');
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

  @Override
  public Query rewrite(IndexSearcher searcher) throws IOException {
    IndexReader reader = searcher.getIndexReader();

    for (LeafReaderContext leaf : reader.leaves()) {
      PointQueryUtils.checkValidPointValues(
          leaf.reader().getPointValues(field), field, numDims, bytesPerDim);
    }

    // fetch the global min/max packed values across all segments
    byte[] globalMinPacked = PointValues.getMinPackedValue(reader, getField());
    byte[] globalMaxPacked = PointValues.getMaxPackedValue(reader, getField());

    if (globalMinPacked == null || globalMaxPacked == null) {
      return new MatchNoDocsQuery();
    }

    return switch (PointQueryUtils.relate(
        globalMinPacked,
        globalMaxPacked,
        lowerPoint,
        upperPoint,
        numDims,
        bytesPerDim,
        comparator)) {
      case CELL_INSIDE_QUERY -> {
        if (canRewriteToMatchAllQuery(reader)) {
          yield new MatchAllDocsQuery();
        } else if (canRewriteToFieldExistsQuery(reader)) {
          yield new FieldExistsQuery(field);
        } else {
          yield super.rewrite(searcher);
        }
      }
      case CELL_OUTSIDE_QUERY -> new MatchNoDocsQuery();
      case CELL_CROSSES_QUERY -> super.rewrite(searcher);
    };
  }

  private boolean canRewriteToMatchAllQuery(IndexReader reader) throws IOException {
    for (LeafReaderContext context : reader.leaves()) {
      LeafReader leaf = context.reader();
      PointValues values = leaf.getPointValues(field);

      if (values == null || values.getDocCount() != leaf.maxDoc()) {
        return false;
      }
    }

    return true;
  }

  private boolean canRewriteToFieldExistsQuery(IndexReader reader) {
    for (LeafReaderContext leaf : reader.leaves()) {
      FieldInfo info = leaf.reader().getFieldInfos().fieldInfo(field);

      if (info != null
          && info.getDocValuesType() == DocValuesType.NONE
          && !info.hasNorms()
          && info.getVectorDimension() == 0) {
        // Can't use a FieldExistsQuery on this segment, so return false
        return false;
      }
    }

    return true;
  }
}
