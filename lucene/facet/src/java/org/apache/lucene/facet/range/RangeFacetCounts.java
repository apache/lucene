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
package org.apache.lucene.facet.range;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.facet.FacetCountsWithFilterQuery;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.PriorityQueue;

/**
 * Base class for range faceting.
 *
 * @lucene.experimental
 */
abstract class RangeFacetCounts extends FacetCountsWithFilterQuery {
  /** Ranges passed to constructor. */
  protected final Range[] ranges;

  /** Counts. */
  protected int[] counts;

  /** Our field name. */
  protected final String field;

  /** Total number of hits. */
  protected int totCount;

  /** Create {@code RangeFacetCounts} */
  protected RangeFacetCounts(String field, Range[] ranges, Query fastMatchQuery) {
    super(fastMatchQuery);
    this.field = field;
    this.ranges = ranges;
  }

  protected abstract LongRange[] getLongRanges();

  protected long mapDocValue(long l) {
    return l;
  }

  protected LongRangeCounter setupCounter() {
    assert counts == null;
    counts = new int[ranges.length];
    return LongRangeCounter.create(getLongRanges(), counts);
  }

  /** Counts from the provided field. */
  protected void count(String field, List<FacetsCollector.MatchingDocs> matchingDocs)
      throws IOException {

    // load doc values for all segments up front and keep track of whether-or-not we found any that
    // were actually multi-valued. this allows us to optimize the case where all segments contain
    // single-values.
    SortedNumericDocValues[] multiValuedDocVals = null;
    NumericDocValues[] singleValuedDocVals = null;
    boolean foundMultiValued = false;

    for (int i = 0; i < matchingDocs.size(); i++) {
      FacetsCollector.MatchingDocs hits = matchingDocs.get(i);
      if (hits.totalHits() == 0) {
        continue;
      }

      SortedNumericDocValues multiValues =
          DocValues.getSortedNumeric(hits.context().reader(), field);
      if (multiValuedDocVals == null) {
        multiValuedDocVals = new SortedNumericDocValues[matchingDocs.size()];
      }
      multiValuedDocVals[i] = multiValues;

      // only bother trying to unwrap a singleton if we haven't yet seen any true multi-valued cases
      if (foundMultiValued == false) {
        NumericDocValues singleValues = DocValues.unwrapSingleton(multiValues);
        if (singleValues != null) {
          if (singleValuedDocVals == null) {
            singleValuedDocVals = new NumericDocValues[matchingDocs.size()];
          }
          singleValuedDocVals[i] = singleValues;
        } else {
          foundMultiValued = true;
        }
      }
    }

    if (multiValuedDocVals == null) {
      // no hits or no doc values in all segments. nothing to count:
      return;
    }

    // we only need to keep around one or the other at this point
    if (foundMultiValued) {
      singleValuedDocVals = null;
    } else {
      multiValuedDocVals = null;
    }

    LongRangeCounter counter = setupCounter();

    int missingCount = 0;

    // if we didn't find any multi-valued cases, we can run a more optimal counting algorithm
    if (foundMultiValued == false) {

      for (int i = 0; i < matchingDocs.size(); i++) {

        FacetsCollector.MatchingDocs hits = matchingDocs.get(i);

        final DocIdSetIterator it = createIterator(hits);
        if (it == null) {
          continue;
        }

        assert singleValuedDocVals != null;
        NumericDocValues singleValues = singleValuedDocVals[i];

        totCount += hits.totalHits();
        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; ) {
          if (singleValues.advanceExact(doc)) {
            counter.addSingleValued(mapDocValue(singleValues.longValue()));
          } else {
            missingCount++;
          }

          doc = it.nextDoc();
        }
      }
    } else {

      for (int i = 0; i < matchingDocs.size(); i++) {

        final DocIdSetIterator it = createIterator(matchingDocs.get(i));
        if (it == null) {
          continue;
        }

        SortedNumericDocValues multiValues = multiValuedDocVals[i];

        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; ) {
          if (multiValues.advanceExact(doc)) {
            int limit = multiValues.docValueCount();
            // optimize single-value case
            if (limit == 1) {
              counter.addSingleValued(mapDocValue(multiValues.nextValue()));
              totCount++;
            } else {
              counter.startMultiValuedDoc();
              long previous = 0;
              for (int j = 0; j < limit; j++) {
                long val = mapDocValue(multiValues.nextValue());
                if (j == 0 || val != previous) {
                  counter.addMultiValued(val);
                  previous = val;
                }
              }
              if (counter.endMultiValuedDoc()) {
                totCount++;
              }
            }
          }

          doc = it.nextDoc();
        }
      }
    }

    missingCount += counter.finish();
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
    LabelAndValue[] labelValues = new LabelAndValue[ranges.length];
    if (counts == null) {
      for (int i = 0; i < ranges.length; i++) {
        labelValues[i] = new LabelAndValue(ranges[i].label, 0);
      }
    } else {
      for (int i = 0; i < ranges.length; i++) {
        labelValues[i] = new LabelAndValue(ranges[i].label, counts[i]);
      }
    }
    return new FacetResult(dim, path, totCount, labelValues, labelValues.length);
  }

  @Override
  public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
    validateTopN(topN);
    validateDimAndPathForGetChildren(dim, path);

    if (counts == null) {
      assert totCount == 0;
      return new FacetResult(dim, path, totCount, new LabelAndValue[0], 0);
    }

    PriorityQueue<Entry> pq =
        PriorityQueue.usingComparator(
            Math.min(topN, counts.length),
            Comparator.<Entry>comparingInt(e -> e.count)
                .thenComparing(e -> e.label, Comparator.reverseOrder()));

    int childCount = 0;
    Entry e = null;
    for (int i = 0; i < counts.length; i++) {
      if (counts[i] != 0) {
        childCount++;
        if (e == null) {
          e = new Entry();
        }
        e.label = ranges[i].label;
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
    // TODO: should we impl this?
    throw new UnsupportedOperationException();
  }

  @Override
  public List<FacetResult> getAllDims(int topN) throws IOException {
    validateTopN(topN);
    return Collections.singletonList(getTopChildren(topN, field));
  }

  @Override
  public String toString() {
    StringBuilder b = new StringBuilder();
    b.append("RangeFacetCounts totCount=");
    b.append(totCount);
    b.append(":\n");
    for (int i = 0; i < ranges.length; i++) {
      b.append("  ");
      b.append(ranges[i].label);
      b.append(" -> count=");
      b.append(counts != null ? counts[i] : 0);
      b.append('\n');
    }
    return b.toString();
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

  /** Reusable entry to hold range label and int count. */
  private static final class Entry {
    int count;
    String label;
  }
}
