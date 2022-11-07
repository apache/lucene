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

  private final Range[] ranges;
  private final String[] labels;
  private final int numBytesPerRange;
  private final int numRanges;

  /** Counts, initialized in by subclass. */
  protected final int[] counts;

  /** Our field name. */
  protected final String field;

  /** Total number of hits. */
  protected int totCount;

  private final ArrayUtil.ByteArrayComparator comparator;

  /** Type of "range overlap" we want to count. */
  RangeFieldQuery.QueryType queryType;

  protected RangeOnRangeFacetCounts(
      String field,
      FacetsCollector hits,
      RangeFieldQuery.QueryType queryType,
      Query fastMatchQuery,
      Range... ranges)
      throws IOException {
    super(fastMatchQuery);
    this.field = field;
    this.ranges = ranges;
    this.numBytesPerRange = ranges[0].getNumBytesPerRange();
    this.labels = getLabels(ranges);
    this.numRanges = ranges.length;
    this.queryType = queryType;
    this.comparator = ArrayUtil.getUnsignedComparator(this.numBytesPerRange);
    counts = new int[numRanges];
    count(field, hits.getMatchingDocs());
  }

  /** Counts from the provided field. */
  protected void count(String field, List<FacetsCollector.MatchingDocs> matchingDocs)
      throws IOException {

    int missingCount = 0;

    for (int i = 0; i < matchingDocs.size(); i++) {

      FacetsCollector.MatchingDocs hits = matchingDocs.get(i);
      BinaryRangeDocValues binaryRangeDocValues =
          new BinaryRangeDocValues(
              DocValues.getBinary(hits.context.reader(), field), 1, numBytesPerRange);

      final DocIdSetIterator it = createIterator(hits);
      if (it == null) {
        continue;
      }

      totCount += hits.totalHits;
      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; ) {
        if (binaryRangeDocValues.advanceExact(doc)) {
          boolean validRange = false;
          for (int range = 0; range < numRanges; range++) {
            byte[] encodedRange = getEncodedRange(ranges[range]);
            byte[] packedRange = binaryRangeDocValues.getPackedValue();
            assert encodedRange.length == packedRange.length;
            if (queryType.matches(encodedRange, packedRange, 1, numBytesPerRange, comparator)) {
              counts[range]++;
              validRange = true;
            }
          }
          if (validRange == false) {
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

  private static String[] getLabels(Range... ranges) {
    String[] labels = new String[ranges.length];
    for (int i = 0; i < ranges.length; i++) {
      labels[i] = ranges[i].label;
    }
    return labels;
  }

  public abstract byte[] getEncodedRange(Range range);

  private static final class Entry {
    int count;
    String label;
  }
}
