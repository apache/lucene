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
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.PointTree;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ArrayUtil.ByteArrayComparator;
import org.apache.lucene.util.NumericUtils;

/**
 * A range query that can take advantage of the fact that the index is sorted to speed up execution.
 * If the index is sorted on the same field as the query, it performs binary search on the field's
 * numeric doc values to find the documents at the lower and upper ends of the range.
 *
 * <p>This optimized execution strategy is only used if the following conditions hold:
 *
 * <ul>
 *   <li>The index is sorted, and its primary sort is on the same field as the query.
 *   <li>The query field has either {@link SortedNumericDocValues} or {@link NumericDocValues}.
 *   <li>The sort field is of type {@code SortField.Type.LONG}, {@code SortField.Type.INT}, {@code
 *       SortField.Type.FLOAT} or {@code SortField.Type.DOUBLE}.
 *   <li>The segments must have at most one field value per document (otherwise we cannot easily
 *       determine the matching document IDs through a binary search).
 * </ul>
 *
 * If any of these conditions isn't met, the search is delegated to {@code fallbackQuery}.
 *
 * <p>The {@code lowerValue}/{@code upperValue} bounds are expressed in the same {@code long} space
 * as the field's stored doc values:
 *
 * <ul>
 *   <li>For int/long fields, the raw value.
 *   <li>For float/double fields backed by {@link SortedNumericDocValues} (e.g. {@code FloatField}),
 *       the sortable encoding, exactly as passed to {@link
 *       org.apache.lucene.document.SortedNumericDocValuesField#newSlowRangeQuery}: {@link
 *       org.apache.lucene.util.NumericUtils#floatToSortableInt} / {@link
 *       org.apache.lucene.util.NumericUtils#doubleToSortableLong}.
 *   <li>For float/double fields backed by single-valued {@link NumericDocValues} (e.g. {@code
 *       FloatDocValuesField}), the raw IEEE-754 bits, as passed to {@link
 *       org.apache.lucene.document.NumericDocValuesField}: {@link Float#floatToIntBits} / {@link
 *       Double#doubleToLongBits}.
 * </ul>
 *
 * The concrete numeric type and doc-values type are discovered per-segment so the bounds, missing
 * values, and points are all interpreted with the right encoding.
 *
 * <p>This fallback must be an equivalent range query -- it should produce the same documents and
 * give constant scores. As an example, an {@link IndexSortSortedNumericDocValuesRangeQuery} might
 * be constructed as follows:
 *
 * <pre><code class="language-java">
 *   String field = "field";
 *   long lowerValue = 0, long upperValue = 10;
 *   Query fallbackQuery = LongPoint.newRangeQuery(field, lowerValue, upperValue);
 *   Query rangeQuery = new IndexSortSortedNumericDocValuesRangeQuery(
 *       field, lowerValue, upperValue, fallbackQuery);
 * </code></pre>
 *
 * @lucene.experimental
 */
public class IndexSortSortedNumericDocValuesRangeQuery extends NumericDocValuesRangeQuery {

  private final Query fallbackQuery;

  /**
   * Creates a new {@link IndexSortSortedNumericDocValuesRangeQuery}.
   *
   * @param field The field name.
   * @param lowerValue The lower end of the range (inclusive), in the field's doc-values long space.
   * @param upperValue The upper end of the range (inclusive), in the field's doc-values long space.
   * @param fallbackQuery A query to fall back to if the optimization cannot be applied.
   */
  public IndexSortSortedNumericDocValuesRangeQuery(
      String field, long lowerValue, long upperValue, Query fallbackQuery) {
    super(field, lowerValue, upperValue);
    this.fallbackQuery = fallbackQuery;
  }

  public Query getFallbackQuery() {
    return fallbackQuery;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    IndexSortSortedNumericDocValuesRangeQuery that = (IndexSortSortedNumericDocValuesRangeQuery) o;
    return lowerValue == that.lowerValue
        && upperValue == that.upperValue
        && Objects.equals(field, that.field)
        && Objects.equals(fallbackQuery, that.fallbackQuery);
  }

  @Override
  public int hashCode() {
    return Objects.hash(field, lowerValue, upperValue, fallbackQuery);
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
      fallbackQuery.visit(visitor);
    }
  }

  @Override
  public String toString(String field) {
    StringBuilder b = new StringBuilder();
    if (this.field.equals(field) == false) {
      b.append(this.field).append(":");
    }
    return b.append("[")
        .append(lowerValue)
        .append(" TO ")
        .append(upperValue)
        .append("]")
        .toString();
  }

  @Override
  public Query rewrite(IndexSearcher indexSearcher) throws IOException {
    if (lowerValue == Long.MIN_VALUE && upperValue == Long.MAX_VALUE) {
      return new FieldExistsQuery(field);
    }

    Query rewrittenFallback = fallbackQuery.rewrite(indexSearcher);
    // This query matches the same documents as its fallback, so if the fallback simplifies to a
    // terminal query - match-all, match-none, or field-exists (e.g. a range covering every value of
    // a sparse field) - the index-sort optimization adds nothing and we return it directly.
    if (rewrittenFallback.getClass() == MatchAllDocsQuery.class
        || rewrittenFallback.getClass() == MatchNoDocsQuery.class
        || rewrittenFallback.getClass() == FieldExistsQuery.class) {
      return rewrittenFallback;
    }
    if (rewrittenFallback == fallbackQuery) {
      return this;
    } else {
      return new IndexSortSortedNumericDocValuesRangeQuery(
          field, lowerValue, upperValue, rewrittenFallback);
    }
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
      throws IOException {
    Weight fallbackWeight = fallbackQuery.createWeight(searcher, scoreMode, boost);

    return new ConstantScoreWeight(this, boost) {

      @Override
      public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        IteratorAndCount itAndCount = getDocIdSetIteratorOrNull(context);
        if (itAndCount != null) {
          DocIdSetIterator disi = itAndCount.it;
          return ConstantScoreScorerSupplier.fromIterator(
              disi, score(), scoreMode, context.reader().maxDoc());
        }
        return fallbackWeight.scorerSupplier(context);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        // Both queries should always return the same values, so we can just check
        // if the fallback query is cacheable.
        return fallbackWeight.isCacheable(ctx);
      }

      @Override
      public int count(LeafReaderContext context) throws IOException {
        LeafReader reader = context.reader();
        if (reader.hasDeletions() == false) {
          // The optimization requires the index to be sorted on this field with a supported type.
          SortField primarySortField = Sort.getPrimarySortField(reader);
          if (primarySortField != null && primarySortField.getField().equals(field)) {
            final SortField.Type sortFieldType = getSortFieldType(primarySortField);
            if (isSupportedSortType(sortFieldType)) {
              // Normalize the bounds into sortable-comparable-long space based on how this field
              // stores its doc values (see #comparableValue).
              final long comparableLower = comparableValue(lowerValue, reader, sortFieldType);
              final long comparableUpper = comparableValue(upperValue, reader, sortFieldType);
              if (comparableLower > comparableUpper) {
                return 0;
              }

              SortedNumericDocValues sortedNumericValues =
                  DocValues.getSortedNumeric(reader, field);
              NumericDocValues numericValues = DocValues.unwrapSingleton(sortedNumericValues);
              PointValues pointValues = reader.getPointValues(field);
              IteratorAndCount itAndCount = null;

              // first use bkd optimization if possible
              if (pointValues != null && pointValues.getDocCount() == reader.maxDoc()) {
                itAndCount =
                    getDocIdSetIteratorOrNullFromBkd(
                        context,
                        numericValues,
                        primarySortField,
                        sortFieldType,
                        comparableLower,
                        comparableUpper);
              }
              if (itAndCount != null && itAndCount.count != -1) {
                return itAndCount.count;
              }

              final long missingComparableValue =
                  missingComparableValue(primarySortField.getMissingValue(), sortFieldType);
              // all documents have docValues or missing value falls outside the range
              if ((pointValues != null && pointValues.getDocCount() == reader.maxDoc())
                  || (missingComparableValue < comparableLower
                      || missingComparableValue > comparableUpper)) {
                itAndCount =
                    getDocIdSetIterator(
                        primarySortField,
                        sortFieldType,
                        comparableLower,
                        comparableUpper,
                        context,
                        numericValues);
              }
              if (itAndCount != null && itAndCount.count != -1) {
                return itAndCount.count;
              }
            }
          }
        }
        return fallbackWeight.count(context);
      }
    };
  }

  private static class ValueAndDoc {
    byte[] value;
    int docID;
    boolean done;
  }

  /**
   * Move to the minimum leaf node that has at least one value that is greater than (or equal to if
   * {@code allowEqual}) {@code value}, and return the next greater value on this block. Upon
   * returning, the {@code pointTree} must be on the leaf node where the value was found.
   */
  private static ValueAndDoc findNextValue(
      PointTree pointTree,
      byte[] value,
      boolean allowEqual,
      ByteArrayComparator comparator,
      boolean lastDoc)
      throws IOException {
    int cmp = comparator.compare(pointTree.getMaxPackedValue(), 0, value, 0);
    if (cmp < 0 || (cmp == 0 && allowEqual == false)) {
      return null;
    }
    if (pointTree.moveToChild() == false) {
      ValueAndDoc vd = new ValueAndDoc();
      pointTree.visitDocValues(
          new IntersectVisitor() {

            @Override
            public void visit(int docID, byte[] packedValue) throws IOException {
              if (vd.value == null) {
                int cmp = comparator.compare(packedValue, 0, value, 0);
                if (cmp > 0 || (cmp == 0 && allowEqual)) {
                  vd.value = packedValue.clone();
                  vd.docID = docID;
                }
              } else if (lastDoc && vd.done == false) {
                int cmp = comparator.compare(packedValue, 0, vd.value, 0);
                assert cmp >= 0;
                if (cmp > 0) {
                  vd.done = true;
                } else {
                  vd.docID = docID;
                }
              }
            }

            @Override
            public void visit(int docID) throws IOException {
              throw new UnsupportedOperationException();
            }

            @Override
            public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
              return Relation.CELL_CROSSES_QUERY;
            }
          });
      if (vd.value != null) {
        return vd;
      } else {
        return null;
      }
    }

    // Recurse
    do {
      ValueAndDoc vd = findNextValue(pointTree, value, allowEqual, comparator, lastDoc);
      if (vd != null) {
        return vd;
      }
    } while (pointTree.moveToSibling());

    boolean moved = pointTree.moveToParent();
    assert moved;
    return null;
  }

  /**
   * Find the next value that is greater than (or equal to if {@code allowEqual}) and return either
   * its first doc ID or last doc ID depending on {@code lastDoc}. This method returns -1 if there
   * is no greater value in the dataset.
   */
  private static int nextDoc(
      PointTree pointTree,
      byte[] value,
      boolean allowEqual,
      ByteArrayComparator comparator,
      boolean lastDoc)
      throws IOException {
    ValueAndDoc vd = findNextValue(pointTree, value, allowEqual, comparator, lastDoc);
    if (vd == null) {
      return -1;
    }
    if (lastDoc == false || vd.done) {
      return vd.docID;
    }

    // We found the next value, now we need the last doc ID.
    int doc = lastDoc(pointTree, vd.value, comparator);
    if (doc == -1) {
      // vd.docID was actually the last doc ID
      return vd.docID;
    } else {
      return doc;
    }
  }

  /**
   * Compute the last doc ID that matches the given value and is stored on a leaf node that compares
   * greater than the current leaf node that the provided {@link PointTree} is positioned on. This
   * returns -1 if no other leaf node contains the provided {@code value}.
   */
  private static int lastDoc(PointTree pointTree, byte[] value, ByteArrayComparator comparator)
      throws IOException {
    // Create a stack of nodes that may contain value that we'll use to search for the last leaf
    // node that contains `value`.
    // While the logic looks a bit complicated due to the fact that the PointTree API doesn't allow
    // moving back to previous siblings, this effectively performs a binary search.
    Deque<PointTree> stack = new ArrayDeque<>();

    outer:
    while (true) {

      // Move to the next node
      while (pointTree.moveToSibling() == false) {
        if (pointTree.moveToParent() == false) {
          // No next node
          break outer;
        }
      }

      int cmp = comparator.compare(pointTree.getMinPackedValue(), 0, value, 0);
      if (cmp > 0) {
        // This node doesn't have `value`, so next nodes can't either
        break;
      }

      stack.push(pointTree.clone());
    }

    while (stack.isEmpty() == false) {
      PointTree next = stack.pop();
      if (next.moveToChild() == false) {
        int[] lastDoc = {-1};
        next.visitDocValues(
            new IntersectVisitor() {

              @Override
              public void visit(int docID) throws IOException {
                throw new UnsupportedOperationException();
              }

              @Override
              public void visit(int docID, byte[] packedValue) throws IOException {
                int cmp = comparator.compare(value, 0, packedValue, 0);
                if (cmp == 0) {
                  lastDoc[0] = docID;
                }
              }

              @Override
              public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                return Relation.CELL_CROSSES_QUERY;
              }
            });
        if (lastDoc[0] != -1) {
          return lastDoc[0];
        }
      } else {
        do {
          int cmp = comparator.compare(next.getMinPackedValue(), 0, value, 0);
          if (cmp > 0) {
            // This node doesn't have `value`, so next nodes can't either
            break;
          }
          stack.push(next.clone());
        } while (next.moveToSibling());
      }
    }

    return -1;
  }

  private boolean matchNone(PointValues points, byte[] queryLowerPoint, byte[] queryUpperPoint)
      throws IOException {
    assert points.getNumDimensions() == 1;
    final ByteArrayComparator comparator =
        ArrayUtil.getUnsignedComparator(points.getBytesPerDimension());
    return comparator.compare(points.getMinPackedValue(), 0, queryUpperPoint, 0) > 0
        || comparator.compare(points.getMaxPackedValue(), 0, queryLowerPoint, 0) < 0;
  }

  private boolean matchAll(PointValues points, byte[] queryLowerPoint, byte[] queryUpperPoint)
      throws IOException {
    assert points.getNumDimensions() == 1;
    final ByteArrayComparator comparator =
        ArrayUtil.getUnsignedComparator(points.getBytesPerDimension());
    return comparator.compare(points.getMinPackedValue(), 0, queryLowerPoint, 0) >= 0
        && comparator.compare(points.getMaxPackedValue(), 0, queryUpperPoint, 0) <= 0;
  }

  private IteratorAndCount getDocIdSetIteratorOrNullFromBkd(
      LeafReaderContext context,
      DocIdSetIterator delegate,
      SortField primarySortField,
      SortField.Type sortFieldType,
      long comparableLower,
      long comparableUpper)
      throws IOException {
    final boolean reverse = primarySortField.getReverse();

    PointValues points = context.reader().getPointValues(field);
    if (points == null) {
      return null;
    }

    if (points.getNumDimensions() != 1) {
      return null;
    }

    // The point width must be consistent with the sort type so that the query bounds are packed the
    // same way the values were indexed (float/int as 4 bytes, double/long as 8 bytes).
    final int bytesPerDim = points.getBytesPerDimension();
    if (bytesPerDim != bytesForSortType(sortFieldType)) {
      return null;
    }

    if (points.size() != points.getDocCount()) {
      return null;
    }

    assert comparableLower <= comparableUpper;
    // Points always store values in sortable-bytes order, so the packed query bounds below (and the
    // unsigned byte comparisons on them) work uniformly across all supported types.
    byte[] queryLowerPoint = packComparableValue(comparableLower, sortFieldType);
    byte[] queryUpperPoint = packComparableValue(comparableUpper, sortFieldType);
    if (matchNone(points, queryLowerPoint, queryUpperPoint)) {
      return IteratorAndCount.empty();
    }
    if (matchAll(points, queryLowerPoint, queryUpperPoint)) {
      int maxDoc = context.reader().maxDoc();
      if (points.getDocCount() == maxDoc) {
        return IteratorAndCount.all(maxDoc);
      } else {
        return IteratorAndCount.sparseRange(0, maxDoc, delegate);
      }
    }

    int minDocId, maxDocId;
    final ByteArrayComparator comparator =
        ArrayUtil.getUnsignedComparator(points.getBytesPerDimension());

    if (reverse) {
      minDocId = nextDoc(points.getPointTree(), queryUpperPoint, false, comparator, true) + 1;
    } else {
      minDocId = nextDoc(points.getPointTree(), queryLowerPoint, true, comparator, false);
      if (minDocId == -1) {
        // No matches
        return IteratorAndCount.empty();
      }
    }

    if (reverse) {
      maxDocId = nextDoc(points.getPointTree(), queryLowerPoint, true, comparator, true) + 1;
      if (maxDocId == 0) {
        // No matches
        return IteratorAndCount.empty();
      }
    } else {
      maxDocId = nextDoc(points.getPointTree(), queryUpperPoint, false, comparator, false);
      if (maxDocId == -1) {
        maxDocId = context.reader().maxDoc();
      }
    }

    if (minDocId == maxDocId) {
      return IteratorAndCount.empty();
    }

    if ((points.getDocCount() == context.reader().maxDoc())) {
      return IteratorAndCount.denseRange(minDocId, maxDocId);
    } else {
      return IteratorAndCount.sparseRange(minDocId, maxDocId, delegate);
    }
  }

  private IteratorAndCount getDocIdSetIteratorOrNull(LeafReaderContext context) throws IOException {
    LeafReader reader = context.reader();
    // The optimization requires the index to be sorted on this field with a supported type.
    SortField primarySortField = Sort.getPrimarySortField(reader);
    if (primarySortField == null || primarySortField.getField().equals(field) == false) {
      return null;
    }
    final SortField.Type sortFieldType = getSortFieldType(primarySortField);
    if (isSupportedSortType(sortFieldType) == false) {
      return null;
    }

    // Normalize the bounds into sortable-comparable-long space based on how this field stores its
    // doc values (see #comparableValue).
    final long comparableLower = comparableValue(lowerValue, reader, sortFieldType);
    final long comparableUpper = comparableValue(upperValue, reader, sortFieldType);
    if (comparableLower > comparableUpper) {
      return IteratorAndCount.empty();
    }

    SortedNumericDocValues sortedNumericValues = DocValues.getSortedNumeric(reader, field);
    NumericDocValues numericValues = DocValues.unwrapSingleton(sortedNumericValues);
    if (numericValues != null) {
      IteratorAndCount itAndCount =
          getDocIdSetIteratorOrNullFromBkd(
              context,
              numericValues,
              primarySortField,
              sortFieldType,
              comparableLower,
              comparableUpper);
      if (itAndCount != null) {
        return itAndCount;
      }
      return getDocIdSetIterator(
          primarySortField,
          sortFieldType,
          comparableLower,
          comparableUpper,
          context,
          numericValues);
    }
    return null;
  }

  /**
   * Computes the document IDs that lie within the range [lowerValue, upperValue] by performing
   * binary search on the field's doc values.
   *
   * <p>Because doc values only allow forward iteration, we need to reload the field comparator
   * every time the binary search accesses an earlier element.
   *
   * <p>We must also account for missing values when performing the binary search. For this reason,
   * we load the {@link FieldComparator} instead of checking the docvalues directly. The returned
   * {@link DocIdSetIterator} makes sure to wrap the original docvalues to skip over documents with
   * no value.
   */
  private IteratorAndCount getDocIdSetIterator(
      SortField sortField,
      SortField.Type sortFieldType,
      long comparableLower,
      long comparableUpper,
      LeafReaderContext context,
      DocIdSetIterator delegate)
      throws IOException {
    long lower = sortField.getReverse() ? comparableUpper : comparableLower;
    long upper = sortField.getReverse() ? comparableLower : comparableUpper;
    int maxDoc = context.reader().maxDoc();

    // Perform a binary search to find the first document with value >= lower.
    ValueComparator comparator = loadComparator(sortField, sortFieldType, lower, context);
    int low = 0;
    int high = maxDoc - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      if (comparator.compare(mid) <= 0) {
        high = mid - 1;
        comparator = loadComparator(sortField, sortFieldType, lower, context);
      } else {
        low = mid + 1;
      }
    }
    int firstDocIdInclusive = high + 1;

    // Perform a binary search to find the first document with value > upper.
    // Since we know that upper >= lower, we can initialize the lower bound
    // of the binary search to the result of the previous search.
    comparator = loadComparator(sortField, sortFieldType, upper, context);
    low = firstDocIdInclusive;
    high = maxDoc - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      if (comparator.compare(mid) < 0) {
        high = mid - 1;
        comparator = loadComparator(sortField, sortFieldType, upper, context);
      } else {
        low = mid + 1;
      }
    }

    int lastDocIdExclusive = high + 1;

    if (firstDocIdInclusive == lastDocIdExclusive) {
      return IteratorAndCount.empty();
    }

    Object missingValue = sortField.getMissingValue();
    LeafReader reader = context.reader();
    PointValues pointValues = reader.getPointValues(field);
    final long missingComparableValue = missingComparableValue(missingValue, sortFieldType);
    // all documents have docValues or missing value falls outside the range
    if ((pointValues != null && pointValues.getDocCount() == reader.maxDoc())
        || (missingComparableValue < comparableLower || missingComparableValue > comparableUpper)) {
      return IteratorAndCount.denseRange(firstDocIdInclusive, lastDocIdExclusive);
    } else {
      return IteratorAndCount.sparseRange(firstDocIdInclusive, lastDocIdExclusive, delegate);
    }
  }

  /** Compares the given document's value with a stored reference value. */
  private interface ValueComparator {
    int compare(int docID) throws IOException;
  }

  private static ValueComparator loadComparator(
      SortField sortField, SortField.Type type, long topValue, LeafReaderContext context)
      throws IOException {
    @SuppressWarnings("unchecked")
    FieldComparator<Number> fieldComparator =
        (FieldComparator<Number>) sortField.getComparator(1, Pruning.NONE);
    // topValue is stored as a sortable comparable long; decode it to the sort field's real type so
    // the comparator (which reads and decodes the doc values itself) compares against a like value.
    switch (type) {
      case INT -> fieldComparator.setTopValue((int) topValue);
      case FLOAT -> fieldComparator.setTopValue(NumericUtils.sortableIntToFloat((int) topValue));
      case DOUBLE -> fieldComparator.setTopValue(NumericUtils.sortableLongToDouble(topValue));
      // $CASES-OMITTED$
      default -> fieldComparator.setTopValue(topValue); // LONG
    }

    LeafFieldComparator leafFieldComparator = fieldComparator.getLeafComparator(context);
    int direction = sortField.getReverse() ? -1 : 1;

    return doc -> {
      int value = leafFieldComparator.compareTop(doc);
      return direction * value;
    };
  }

  private static SortField.Type getSortFieldType(SortField sortField) {
    // We expect the sortField to be SortedNumericSortField
    if (sortField instanceof SortedNumericSortField snsf) {
      return snsf.getNumericType();
    } else {
      return sortField.getType();
    }
  }

  /** Whether the index-sort field type is a numeric type that this optimization supports. */
  private static boolean isSupportedSortType(SortField.Type sortFieldType) {
    return sortFieldType == Type.INT
        || sortFieldType == Type.LONG
        || sortFieldType == Type.FLOAT
        || sortFieldType == Type.DOUBLE;
  }

  /** The point width (bytes per dimension) expected for a given numeric sort type. */
  private static int bytesForSortType(SortField.Type sortFieldType) {
    return (sortFieldType == Type.INT || sortFieldType == Type.FLOAT) ? Integer.BYTES : Long.BYTES;
  }

  /**
   * Normalizes a bound (expressed in the field's doc-values long space) into the sortable
   * comparable-long space used internally for ordering, point packing, and {@code setTopValue}
   * decoding.
   *
   * <p>The encoding of a stored float/double depends on the doc-values type:
   *
   * <ul>
   *   <li>{@link DocValuesType#SORTED_NUMERIC} fields (e.g. {@code FloatField}) already store the
   *       sortable encoding ({@link NumericUtils#floatToSortableInt}/{@link
   *       NumericUtils#doubleToSortableLong}), so the bound is used as-is.
   *   <li>Single-valued {@link DocValuesType#NUMERIC} fields (e.g. {@code FloatDocValuesField})
   *       store the raw IEEE-754 bits, which do not order correctly as signed longs, so the bound
   *       is converted to sortable order here.
   * </ul>
   *
   * <p>Integral (int/long) sorts store their raw value under both doc-values types, so the bound is
   * returned unchanged.
   */
  private long comparableValue(long bound, LeafReader reader, SortField.Type sortFieldType) {
    if (sortFieldType != Type.FLOAT && sortFieldType != Type.DOUBLE) {
      return bound;
    }
    FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
    if (fieldInfo != null && fieldInfo.getDocValuesType() == DocValuesType.NUMERIC) {
      return sortFieldType == Type.FLOAT
          ? NumericUtils.sortableFloatBits((int) bound)
          : NumericUtils.sortableDoubleBits(bound);
    }
    return bound;
  }

  /**
   * Converts a sort field's missing value into the same doc-values comparable-long space used for
   * the {@code lowerValue}/{@code upperValue} bounds, so it can be compared against them with
   * signed long ordering.
   */
  private static long missingComparableValue(Object missingValue, SortField.Type sortFieldType) {
    return switch (sortFieldType) {
      case FLOAT ->
          NumericUtils.floatToSortableInt(
              missingValue == null ? 0.0f : ((Number) missingValue).floatValue());
      case DOUBLE ->
          NumericUtils.doubleToSortableLong(
              missingValue == null ? 0.0 : ((Number) missingValue).doubleValue());
      // $CASES-OMITTED$
      default -> missingValue == null ? 0L : ((Number) missingValue).longValue();
    };
  }

  /**
   * Packs a comparable-long bound into the point encoding used by the BKD tree. Float/double bounds
   * are written as sortable bytes (matching {@code FloatPoint}/{@code DoublePoint}); integral
   * bounds use {@code IntPoint}/{@code LongPoint} packing.
   */
  private static byte[] packComparableValue(long comparableValue, SortField.Type sortFieldType) {
    switch (sortFieldType) {
      case INT, FLOAT -> {
        return IntPoint.pack((int) comparableValue).bytes;
      }
      // $CASES-OMITTED$
      default -> {
        return LongPoint.pack(comparableValue).bytes;
      }
    }
  }

  /**
   * Provides a {@code DocIdSetIterator} along with an accurate count of documents provided by the
   * iterator (or {@code -1} if an accurate count is unknown).
   */
  private record IteratorAndCount(DocIdSetIterator it, int count) {

    static IteratorAndCount empty() {
      return new IteratorAndCount(DocIdSetIterator.empty(), 0);
    }

    static IteratorAndCount all(int maxDoc) {
      return new IteratorAndCount(DocIdSetIterator.all(maxDoc), maxDoc);
    }

    static IteratorAndCount denseRange(int minDoc, int maxDoc) {
      return new IteratorAndCount(DocIdSetIterator.range(minDoc, maxDoc), maxDoc - minDoc);
    }

    static IteratorAndCount sparseRange(int minDoc, int maxDoc, DocIdSetIterator delegate) {
      return new IteratorAndCount(new BoundedDocIdSetIterator(minDoc, maxDoc, delegate), -1);
    }
  }

  /**
   * A doc ID set iterator that wraps a delegate iterator and only returns doc IDs in the range
   * [firstDocInclusive, lastDoc).
   */
  private static class BoundedDocIdSetIterator extends AbstractDocIdSetIterator {
    private final int firstDoc;
    private final int lastDoc;
    private final DocIdSetIterator delegate;

    BoundedDocIdSetIterator(int firstDoc, int lastDoc, DocIdSetIterator delegate) {
      assert delegate != null;
      this.firstDoc = firstDoc;
      this.lastDoc = lastDoc;
      this.delegate = delegate;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(doc + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      if (target < firstDoc) {
        target = firstDoc;
      }

      int result = delegate.advance(target);
      if (result < lastDoc) {
        doc = result;
      } else {
        doc = NO_MORE_DOCS;
      }
      return doc;
    }

    @Override
    public long cost() {
      return Math.min(delegate.cost(), lastDoc - firstDoc);
    }
  }
}
