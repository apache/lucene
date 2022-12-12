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
package org.apache.lucene.facet.rangeonrange;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.apache.lucene.document.BinaryRangeDocValues;
import org.apache.lucene.document.RangeFieldQuery;
import org.apache.lucene.facet.FacetCountsWithFilterQuery;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.PriorityQueue;

abstract class RangeOnRangeFacetCounts extends FacetCountsWithFilterQuery {

  private final String[] labels;

  /** Counts, initialized in by subclass. */
  private final int[] counts;

  /** Our field name. */
  private final String field;

  /** Total number of hits. */
  private int totCount;

  protected RangeOnRangeFacetCounts(
      String field,
      FacetsCollector hits,
      RangeFieldQuery.QueryType queryType,
      Query fastMatchQuery,
      int numEncodedValueBytes,
      byte[][] encodedRanges,
      String[] labels)
      throws IOException {
    super(fastMatchQuery);

    assert encodedRanges.length == labels.length;
    assert encodedRanges[0].length % (2 * numEncodedValueBytes) == 0;

    this.field = field;
    this.labels = labels;
    this.counts = new int[encodedRanges.length];

    count(field, hits.getMatchingDocs(), encodedRanges, numEncodedValueBytes, queryType);
  }

  /** Counts from the provided field. */
  protected void count(
      String field,
      List<FacetsCollector.MatchingDocs> matchingDocs,
      byte[][] encodedRanges,
      int numEncodedValueBytes,
      RangeFieldQuery.QueryType queryType)
      throws IOException {
    // TODO: We currently just exhaustively check the ranges in each document with every range in
    // the ranges array.
    // We might be able to do something more efficient here by grouping the ranges array into a
    // space partitioning
    // data structure of some sort.

    int dims = encodedRanges[0].length / (2 * numEncodedValueBytes);
    ArrayUtil.ByteArrayComparator comparator =
        ArrayUtil.getUnsignedComparator(numEncodedValueBytes);

    int missingCount = 0;

    for (FacetsCollector.MatchingDocs hits : matchingDocs) {

      BinaryRangeDocValues binaryRangeDocValues =
          new BinaryRangeDocValues(
              DocValues.getBinary(hits.context.reader(), field), dims, numEncodedValueBytes);

      final DocIdSetIterator it = createIterator(hits);
      if (it == null) {
        continue;
      }

      totCount += hits.totalHits;
      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; ) {
        if (binaryRangeDocValues.advanceExact(doc)) {
          boolean hasValidRange = false;
          for (int range = 0; range < encodedRanges.length; range++) {
            byte[] encodedRange = encodedRanges[range];
            byte[] packedRange = binaryRangeDocValues.getPackedValue();
            assert encodedRange.length == packedRange.length;
            if (queryType.matches(
                encodedRange, packedRange, dims, numEncodedValueBytes, comparator)) {
              counts[range]++;
              hasValidRange = true;
            }
          }
          if (hasValidRange == false) {
            missingCount++;
          }
        } else {
          missingCount++;
        }
        doc = it.nextDoc();
      }
    }
    totCount -= missingCount;
  }

  /**
   * {@inheritDoc}
   *
   * <p>NOTE: This implementation guarantees that ranges will be returned in the order specified by
   * the user when calling the constructor.
   */
  @Override
  public FacetResult getAllChildren(String dim, String... path) throws IOException {
    validateDimAndPathForGetChildren(dim, path);
    LabelAndValue[] labelValues = new LabelAndValue[counts.length];
    for (int i = 0; i < counts.length; i++) {
      labelValues[i] = new LabelAndValue(labels[i], counts[i]);
    }
    return new FacetResult(dim, path, totCount, labelValues, labelValues.length);
  }

  @Override
  public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
    validateTopN(topN);
    validateDimAndPathForGetChildren(dim, path);

    PriorityQueue<Entry> pq =
        new PriorityQueue<>(Math.min(topN, counts.length)) {
          @Override
          protected boolean lessThan(Entry a, Entry b) {
            int cmp = Integer.compare(a.count, b.count);
            if (cmp == 0) {
              cmp = b.label.compareTo(a.label);
            }
            return cmp < 0;
          }
        };

    int childCount = 0;
    Entry e = null;
    for (int i = 0; i < counts.length; i++) {
      if (counts[i] != 0) {
        childCount++;
        if (e == null) {
          e = new Entry();
        }
        e.label = labels[i];
        e.count = counts[i];
        e = pq.insertWithOverflow(e);
      }
    }

    LabelAndValue[] results = new LabelAndValue[pq.size()];
    while (pq.size() != 0) {
      Entry entry = pq.pop();
      assert entry != null;
      results[pq.size()] = new LabelAndValue(entry.label, entry.count);
    }
    return new FacetResult(dim, path, totCount, results, childCount);
  }

  @Override
  public Number getSpecificValue(String dim, String... path) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<FacetResult> getAllDims(int topN) throws IOException {
    validateTopN(topN);
    return Collections.singletonList(getTopChildren(topN, field));
  }

  private void validateDimAndPathForGetChildren(String dim, String... path) {
    if (dim.equals(field) == false) {
      throw new IllegalArgumentException(
          "invalid dim \"" + dim + "\"; should be \"" + field + "\"");
    }
    if (path.length != 0) {
      throw new IllegalArgumentException("path.length should be 0");
    }
  }

  private static final class Entry {
    int count;
    String label;
  }
}
