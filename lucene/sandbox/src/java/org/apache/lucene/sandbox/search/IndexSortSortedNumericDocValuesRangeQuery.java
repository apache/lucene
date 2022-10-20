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
import java.util.Objects;
import java.util.function.Predicate;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ArrayUtil.ByteArrayComparator;

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
 *   <li>The sort field is of type {@code SortField.Type.LONG} or {@code SortField.Type.INT}.
 *   <li>The segments must have at most one field value per document (otherwise we cannot easily
 *       determine the matching document IDs through a binary search).
 * </ul>
 *
 * If any of these conditions isn't met, the search is delegated to {@code fallbackQuery}.
 *
 * <p>This fallback must be an equivalent range query -- it should produce the same documents and
 * give constant scores. As an example, an {@link IndexSortSortedNumericDocValuesRangeQuery} might
 * be constructed as follows:
 *
 * <pre class="prettyprint">
 *   String field = "field";
 *   long lowerValue = 0, long upperValue = 10;
 *   Query fallbackQuery = LongPoint.newRangeQuery(field, lowerValue, upperValue);
 *   Query rangeQuery = new IndexSortSortedNumericDocValuesRangeQuery(
 *       field, lowerValue, upperValue, fallbackQuery);
 * </pre>
 *
 * @lucene.experimental
 */
public class IndexSortSortedNumericDocValuesRangeQuery extends Query {

  private final String field;
  private final long lowerValue;
  private final long upperValue;
  private final Query fallbackQuery;

  /**
   * Creates a new {@link IndexSortSortedNumericDocValuesRangeQuery}.
   *
   * @param field The field name.
   * @param lowerValue The lower end of the range (inclusive).
   * @param upperValue The upper end of the range (exclusive).
   * @param fallbackQuery A query to fall back to if the optimization cannot be applied.
   */
  public IndexSortSortedNumericDocValuesRangeQuery(
      String field, long lowerValue, long upperValue, Query fallbackQuery) {
    this.field = Objects.requireNonNull(field);
    this.lowerValue = lowerValue;
    this.upperValue = upperValue;
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
    if (rewrittenFallback.getClass() == MatchAllDocsQuery.class) {
      return new MatchAllDocsQuery();
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
        final Weight weight = this;
        DocIdSetIterator disi = getDocIdSetIteratorOrNull(context);
        if (disi != null) {
          return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) throws IOException {
              return new ConstantScoreScorer(weight, score(), scoreMode, disi);
            }

            @Override
            public long cost() {
              return disi.cost();
            }
          };
        }
        return fallbackWeight.scorerSupplier(context);
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
        // Both queries should always return the same values, so we can just check
        // if the fallback query is cacheable.
        return fallbackWeight.isCacheable(ctx);
      }

      @Override
      public int count(LeafReaderContext context) throws IOException {
        if (context.reader().hasDeletions() == false) {
          BoundedDocIdSetIterator disi = getDocIdSetIteratorOrNull(context);
          if (disi != null && disi.delegate == null) {
            return disi.lastDoc - disi.firstDoc;
          }
        }
        return fallbackWeight.count(context);
      }
    };
  }

  /**
   * Returns the first document whose packed value is greater than or equal (if allowEqual is true)
   * to the provided packed value or -1 if all packed values are smaller than the provided one,
   */
  public final int nextDoc(PointValues values, byte[] packedValue, boolean allowEqual)
      throws IOException {
    assert values.getNumDimensions() == 1;
    final int bytesPerDim = values.getBytesPerDimension();
    final ByteArrayComparator comparator = ArrayUtil.getUnsignedComparator(bytesPerDim);
    final Predicate<byte[]> biggerThan =
        testPackedValue -> {
          int cmp = comparator.compare(testPackedValue, 0, packedValue, 0);
          return cmp > 0 || (cmp == 0 && allowEqual);
        };
    return nextDoc(values.getPointTree(), biggerThan);
  }

  private int nextDoc(PointValues.PointTree pointTree, Predicate<byte[]> biggerThan)
      throws IOException {
    if (biggerThan.test(pointTree.getMaxPackedValue()) == false) {
      // doc is before us
      return -1;
    } else if (pointTree.moveToChild()) {
      // navigate down
      do {
        final int doc = nextDoc(pointTree, biggerThan);
        if (doc != -1) {
          return doc;
        }
      } while (pointTree.moveToSibling());
      pointTree.moveToParent();
      return -1;
    } else {
      // doc is in this leaf
      final int[] doc = {-1};
      pointTree.visitDocValues(
          new IntersectVisitor() {
            @Override
            public void visit(int docID) {
              throw new AssertionError("Invalid call to visit(docID)");
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
              if (doc[0] == -1 && biggerThan.test(packedValue)) {
                doc[0] = docID;
              }
            }

            @Override
            public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
              return Relation.CELL_CROSSES_QUERY;
            }
          });
      return doc[0];
    }
  }

  private boolean matchNone(PointValues points, byte[] queryLowerPoint, byte[] queryUpperPoint)
      throws IOException {
    final ByteArrayComparator comparator =
        ArrayUtil.getUnsignedComparator(points.getBytesPerDimension());
    for (int dim = 0; dim < points.getNumDimensions(); dim++) {
      int offset = dim * points.getBytesPerDimension();
      if (comparator.compare(points.getMinPackedValue(), offset, queryUpperPoint, offset) > 0
          || comparator.compare(points.getMaxPackedValue(), offset, queryLowerPoint, offset) < 0) {
        return true;
      }
    }
    return false;
  }

  private boolean matchAll(PointValues points, byte[] queryLowerPoint, byte[] queryUpperPoint)
      throws IOException {
    final ByteArrayComparator comparator =
        ArrayUtil.getUnsignedComparator(points.getBytesPerDimension());
    for (int dim = 0; dim < points.getNumDimensions(); dim++) {
      int offset = dim * points.getBytesPerDimension();
      if (comparator.compare(points.getMinPackedValue(), offset, queryUpperPoint, offset) > 0) {
        return false;
      }
      if (comparator.compare(points.getMaxPackedValue(), offset, queryLowerPoint, offset) < 0) {
        return false;
      }
      if (comparator.compare(points.getMinPackedValue(), offset, queryLowerPoint, offset) < 0
          || comparator.compare(points.getMaxPackedValue(), offset, queryUpperPoint, offset) > 0) {
        return false;
      }
    }
    return true;
  }

  private BoundedDocIdSetIterator getDocIdSetIteratorOrNullFromBkd(
      LeafReaderContext context, DocIdSetIterator delegate) throws IOException {
    Sort indexSort = context.reader().getMetaData().getSort();
    if (indexSort != null
        && indexSort.getSort().length > 0
        && indexSort.getSort()[0].getField().equals(field)
        && indexSort.getSort()[0].getReverse() == false) {
      PointValues points = context.reader().getPointValues(field);
      if (points == null) {
        return null;
      }

      if (points.getNumDimensions() != 1) {
        return null;
      }

      if (points.getBytesPerDimension() != Long.BYTES
          && points.getBytesPerDimension() != Integer.BYTES) {
        return null;
      }

      // Each doc that has points has exactly one point.
      if (points.size() == points.getDocCount()) {

        byte[] queryLowerPoint;
        byte[] queryUpperPoint;
        if (points.getBytesPerDimension() == Integer.BYTES) {
          queryLowerPoint = IntPoint.pack((int) lowerValue).bytes;
          queryUpperPoint = IntPoint.pack((int) upperValue).bytes;
        } else {
          queryLowerPoint = LongPoint.pack(lowerValue).bytes;
          queryUpperPoint = LongPoint.pack(upperValue).bytes;
        }
        if (lowerValue > upperValue || matchNone(points, queryLowerPoint, queryUpperPoint)) {
          return new BoundedDocIdSetIterator(0, 0, null);
        }
        int minDocId, maxDocId;
        if (matchAll(points, queryLowerPoint, queryUpperPoint)) {
          minDocId = 0;
          maxDocId = context.reader().maxDoc();
        } else {
          // >=queryLowerPoint
          minDocId = nextDoc(points, queryLowerPoint, true);

          if (minDocId == -1) {
            return new BoundedDocIdSetIterator(0, 0, null);
          }
          // >queryUpperPoint,
          maxDocId = nextDoc(points, queryUpperPoint, false);
          if (maxDocId == -1) {
            maxDocId = context.reader().maxDoc();
          }
        }

        if ((points.getDocCount() == context.reader().maxDoc())) {
          return new BoundedDocIdSetIterator(minDocId, maxDocId, null);
        } else {
          return new BoundedDocIdSetIterator(minDocId, maxDocId, delegate);
        }
      }
    }
    return null;
  }

  private BoundedDocIdSetIterator getDocIdSetIteratorOrNull(LeafReaderContext context)
      throws IOException {
    SortedNumericDocValues sortedNumericValues =
        DocValues.getSortedNumeric(context.reader(), field);
    NumericDocValues numericValues = DocValues.unwrapSingleton(sortedNumericValues);
    if (numericValues != null) {
      BoundedDocIdSetIterator iterator = getDocIdSetIteratorOrNullFromBkd(context, numericValues);
      if (iterator != null) {
        return iterator;
      }
      Sort indexSort = context.reader().getMetaData().getSort();
      if (indexSort != null
          && indexSort.getSort().length > 0
          && indexSort.getSort()[0].getField().equals(field)) {

        final SortField sortField = indexSort.getSort()[0];
        final SortField.Type sortFieldType = getSortFieldType(sortField);
        // The index sort optimization is only supported for Type.INT and Type.LONG
        if (sortFieldType == Type.INT || sortFieldType == Type.LONG) {
          return getDocIdSetIterator(sortField, sortFieldType, context, numericValues);
        }
      }
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
  private BoundedDocIdSetIterator getDocIdSetIterator(
      SortField sortField,
      SortField.Type sortFieldType,
      LeafReaderContext context,
      DocIdSetIterator delegate)
      throws IOException {
    long lower = sortField.getReverse() ? upperValue : lowerValue;
    long upper = sortField.getReverse() ? lowerValue : upperValue;
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
    Object missingValue = sortField.getMissingValue();
    BoundedDocIdSetIterator disi;
    LeafReader reader = context.reader();
    PointValues pointValues = reader.getPointValues(field);
    final long missingLongValue = missingValue == null ? 0L : (long) missingValue;
    // all documents have docValues or missing value falls outside the range
    if ((pointValues != null && pointValues.getDocCount() == reader.maxDoc())
        || (missingLongValue < lowerValue || missingLongValue > upperValue)) {
      disi = new BoundedDocIdSetIterator(firstDocIdInclusive, lastDocIdExclusive, null);
    } else {
      disi = new BoundedDocIdSetIterator(firstDocIdInclusive, lastDocIdExclusive, delegate);
    }
    return disi;
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
        (FieldComparator<Number>) sortField.getComparator(1, false);
    if (type == Type.INT) {
      fieldComparator.setTopValue((int) topValue);
    } else {
      // Since we support only Type.INT and Type.LONG, assuming LONG for all other cases
      fieldComparator.setTopValue(topValue);
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
    if (sortField instanceof SortedNumericSortField) {
      return ((SortedNumericSortField) sortField).getNumericType();
    } else {
      return sortField.getType();
    }
  }

  /**
   * A doc ID set iterator that wraps a delegate iterator and only returns doc IDs in the range
   * [firstDocInclusive, lastDoc).
   */
  private static class BoundedDocIdSetIterator extends DocIdSetIterator {
    private final int firstDoc;
    private final int lastDoc;
    private final DocIdSetIterator delegate;

    private int docID = -1;

    BoundedDocIdSetIterator(int firstDoc, int lastDoc, DocIdSetIterator delegate) {
      this.firstDoc = firstDoc;
      this.lastDoc = lastDoc;
      this.delegate = delegate;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int nextDoc() throws IOException {
      return advance(docID + 1);
    }

    @Override
    public int advance(int target) throws IOException {
      if (target < firstDoc) {
        target = firstDoc;
      }

      int result;
      if (delegate != null) {
        result = delegate.advance(target);
      } else {
        result = target;
      }
      if (result < lastDoc) {
        docID = result;
      } else {
        docID = NO_MORE_DOCS;
      }
      return docID;
    }

    @Override
    public long cost() {
      return lastDoc - firstDoc;
    }
  }
}
