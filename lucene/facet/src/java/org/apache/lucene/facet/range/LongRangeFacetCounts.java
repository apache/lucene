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
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConjunctionDISI;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.search.LongValuesSource;
import org.apache.lucene.search.Query;

/**
 * {@link Facets} implementation that computes counts for dynamic long ranges. Use this for
 * dimensions that change in real-time (e.g. a relative time based dimension like "Past day", "Past
 * 2 days", etc.) or that change for each request (e.g. distance from the user's location, "&lt; 1
 * km", "&lt; 2 km", etc.).
 *
 * @lucene.experimental
 */
public class LongRangeFacetCounts extends RangeFacetCounts {

  /**
   * Create {@code LongRangeFacetCounts} using long values from the specified field. The field may
   * be single-valued ({@link NumericDocValues}) or multi-valued ({@link SortedNumericDocValues}),
   * and will be interpreted as containing long values.
   */
  public LongRangeFacetCounts(String field, FacetsCollector hits, LongRange... ranges)
      throws IOException {
    this(field, null, hits, ranges);
  }

  /**
   * Create {@code LongRangeFacetCounts}, using the provided {@link LongValuesSource} if non-null.
   * If {@code valueSource} is null, doc values from the provided {@code field} will be used.
   */
  public LongRangeFacetCounts(
      String field, LongValuesSource valueSource, FacetsCollector hits, LongRange... ranges)
      throws IOException {
    this(field, valueSource, hits, null, ranges);
  }

  /**
   * Create {@code LongRangeFacetCounts}, using the provided {@link LongValuesSource} if non-null.
   * If {@code valueSource} is null, doc values from the provided {@code field} will be used. Use
   * the provided {@code Query} as a fastmatch: only documents passing the filter are checked for
   * the matching ranges, which is helpful when the provided {@link LongValuesSource} is costly
   * per-document, such as a geo distance.
   */
  public LongRangeFacetCounts(
      String field,
      LongValuesSource valueSource,
      FacetsCollector hits,
      Query fastMatchQuery,
      LongRange... ranges)
      throws IOException {
    super(field, ranges, fastMatchQuery);
    // use the provided valueSource if non-null, otherwise use the doc values associated with the
    // field
    if (valueSource != null) {
      count(valueSource, hits.getMatchingDocs());
    } else {
      count(field, hits.getMatchingDocs());
    }
  }

  /** Counts from the provided valueSource. */
  private void count(LongValuesSource valueSource, List<MatchingDocs> matchingDocs)
      throws IOException {

    LongRange[] ranges = (LongRange[]) this.ranges;

    LongRangeCounter counter = new LongRangeCounter(ranges, counts, false);

    int missingCount = 0;

    for (MatchingDocs hits : matchingDocs) {
      LongValues fv = valueSource.getValues(hits.context, null);
      totCount += hits.totalHits;

      final DocIdSetIterator it;
      if (fastMatchQuery != null) {
        DocIdSetIterator fastMatchDocs = createFastMatchDisi(hits.context);
        if (fastMatchDocs == null) {
          continue;
        } else {
          it =
              ConjunctionDISI.intersectIterators(
                  Arrays.asList(hits.bits.iterator(), fastMatchDocs));
        }
      } else {
        it = hits.bits.iterator();
      }

      for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; ) {
        // Skip missing docs:
        if (fv.advanceExact(doc)) {
          counter.add(fv.longValue());
        } else {
          missingCount++;
        }

        doc = it.nextDoc();
      }
    }

    missingCount += counter.finish();
    totCount -= missingCount;
  }

  /** Counts from the provided field. */
  private void count(String field, List<MatchingDocs> matchingDocs) throws IOException {

    LongRange[] ranges = (LongRange[]) this.ranges;

    LongRangeCounter counter = null;

    int missingCount = 0;

    for (MatchingDocs hits : matchingDocs) {

      final DocIdSetIterator it;
      if (fastMatchQuery != null) {
        DocIdSetIterator fastMatchDocs = createFastMatchDisi(hits.context);
        if (fastMatchDocs == null) {
          continue;
        } else {
          it =
              ConjunctionDISI.intersectIterators(
                  Arrays.asList(hits.bits.iterator(), fastMatchDocs));
        }
      } else {
        it = hits.bits.iterator();
      }

      SortedNumericDocValues multiValues = DocValues.getSortedNumeric(hits.context.reader(), field);
      NumericDocValues singleValues = DocValues.unwrapSingleton(multiValues);

      if (singleValues != null) {

        if (counter == null) {
          counter = new LongRangeCounter(ranges, counts, false);
        }
        assert counter.isMultiValued == false;

        totCount += hits.totalHits;
        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; ) {
          if (singleValues.advanceExact(doc)) {
            counter.add(singleValues.longValue());
          } else {
            missingCount++;
          }

          doc = it.nextDoc();
        }
      } else {

        if (counter == null) {
          counter = new LongRangeCounter(ranges, counts, true);
        }
        assert counter.isMultiValued == true;

        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; ) {
          if (multiValues.advanceExact(doc)) {
            int limit = multiValues.docValueCount();
            counter.startDoc();
            for (int i = 0; i < limit; i++) {
              counter.add(multiValues.nextValue());
            }
            if (counter.endDoc()) {
              totCount++;
            }
          }

          doc = it.nextDoc();
        }
      }
    }

    if (counter != null && counter.isMultiValued == false) {
      missingCount += counter.finish();
      totCount -= missingCount;
    }
  }
}
