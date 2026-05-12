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
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;

/**
 * Shared utilities for {@link PrimarySortAlignable} queries. Callers normally rely on {@code
 * instanceof} {@link PrimarySortAlignable} rather than using this class directly.
 *
 * @lucene.experimental
 */
public final class PrimarySortAlignables {

  private PrimarySortAlignables() {}

  /**
   * True if any segment's primary {@link SortField} targets {@code field}.
   *
   * @lucene.internal
   */
  public static boolean canOptimizePrimarySortOnField(IndexSearcher searcher, String field)
      throws IOException {
    for (LeafReaderContext context : searcher.getIndexReader().leaves()) {
      if (primaryIndexSortField(context, field) != null) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns the primary index sort field if it targets {@code field}, or {@code null}.
   *
   * @lucene.internal
   */
  public static SortField primaryIndexSortField(LeafReaderContext context, String field) {
    Sort indexSort = context.reader().getMetaData().sort();
    if (indexSort != null
        && indexSort.getSort().length > 0
        && indexSort.getSort()[0].getField().equals(field)) {
      return indexSort.getSort()[0];
    }
    return null;
  }

  /**
   * Dense doc-id span for the sorted-numeric primary-sort fast path, for bounds {@code field},
   * {@code lowerValue}, and {@code upperValue} (inclusive, same semantics as {@link
   * IndexSortSortedNumericDocValuesRangeQuery}). Used when the filter query exposes sorted-numeric
   * bounds (e.g. {@code SortedNumericDocValuesRangeQuery} or a 1D {@link PointRangeQuery}) but is
   * not itself an {@link IndexSortSortedNumericDocValuesRangeQuery}.
   *
   * <p>This allocates a short-lived query instance internally. It is called once per leaf at weight
   * creation time, not on a hot scoring path.
   *
   * @param context the leaf reader context
   * @param field the sorted-numeric field name (must be the primary index sort field)
   * @param lowerValue inclusive lower bound
   * @param upperValue inclusive upper bound
   * @return the dense doc-id range, or {@code null} if a safe range cannot be determined
   * @lucene.internal
   */
  public static DocIdRange denseDocIdRangeOrNullForSortedNumericBounds(
      LeafReaderContext context, String field, long lowerValue, long upperValue)
      throws IOException {
    return new IndexSortSortedNumericDocValuesRangeQuery(
            field, lowerValue, upperValue, MatchNoDocsQuery.INSTANCE)
        .denseDocIdRangeOrNull(context);
  }

  /**
   * Contiguous matching doc-id half-open interval for a term filter on a primary {@link
   * SortedSetSortField}, or {@code null} when a safe dense block cannot be proved.
   *
   * <p>Callers rely on agreement between postings and singleton sorted-set doc values: same doc
   * count as the binary-search range, first/last postings land on range ends, and no posting exists
   * past the range. Returning {@code null} is always safe (fallback to full boolean).
   */
  static DocIdRange termFilterDenseDocIdRange(LeafReaderContext context, Term term)
      throws IOException {
    if (primaryIndexSortField(context, term.field()) instanceof SortedSetSortField == false) {
      return null;
    }
    SortedSetDocValues values = DocValues.getSortedSet(context.reader(), term.field());
    SortedDocValues singleton = DocValues.unwrapSingleton(values);
    if (singleton == null) {
      return null;
    }
    int docFreq = context.reader().docFreq(term);
    long ord = values.lookupTerm(term.bytes());
    if (ord < 0) {
      return docFreq == 0 ? new DocIdRange(0, 0) : null;
    }
    DocIdRange range = getDocIdRangeForPrimarySort(context, term.bytes());
    if (range.isEmpty()) {
      return range;
    }
    if (docFreq != range.maxDoc() - range.minDoc()) {
      return null;
    }
    PostingsEnum postings = context.reader().postings(term, PostingsEnum.NONE);
    if (postings == null || postings.nextDoc() != range.minDoc()) {
      return null;
    }
    if (postings.advance(range.maxDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
      return null;
    }
    return range;
  }

  private static DocIdRange getDocIdRangeForPrimarySort(LeafReaderContext context, BytesRef value)
      throws IOException {
    SortField sortField = context.reader().getMetaData().sort().getSort()[0];
    int maxDoc = context.reader().maxDoc();

    ValueComparator comparator = loadComparator(sortField, value, context);
    int low = 0;
    int high = maxDoc - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      if (comparator.compare(mid) <= 0) {
        high = mid - 1;
        comparator = loadComparator(sortField, value, context);
      } else {
        low = mid + 1;
      }
    }
    int firstDocIdInclusive = high + 1;

    comparator = loadComparator(sortField, value, context);
    low = firstDocIdInclusive;
    high = maxDoc - 1;
    while (low <= high) {
      int mid = (low + high) >>> 1;
      if (comparator.compare(mid) < 0) {
        high = mid - 1;
        comparator = loadComparator(sortField, value, context);
      } else {
        low = mid + 1;
      }
    }
    int lastDocIdExclusive = high + 1;

    return new DocIdRange(firstDocIdInclusive, lastDocIdExclusive);
  }

  private interface ValueComparator {
    int compare(int docID) throws IOException;
  }

  private static ValueComparator loadComparator(
      SortField sortField, BytesRef topValue, LeafReaderContext context) throws IOException {
    @SuppressWarnings("unchecked")
    FieldComparator<BytesRef> fieldComparator =
        (FieldComparator<BytesRef>) sortField.getComparator(1, Pruning.NONE);
    fieldComparator.setTopValue(topValue);
    LeafFieldComparator leafFieldComparator = fieldComparator.getLeafComparator(context);
    int direction = sortField.getReverse() ? -1 : 1;

    return doc -> direction * leafFieldComparator.compareTop(doc);
  }
}
