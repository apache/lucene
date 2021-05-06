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
import java.util.Collections;
import java.util.List;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.ConjunctionDISI;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

/**
 * Base class for range faceting.
 *
 * @lucene.experimental
 */
abstract class RangeFacetCounts extends Facets {
  /** Ranges passed to constructor. */
  protected final Range[] ranges;

  /** Counts, initialized in by subclass. */
  protected final int[] counts;

  /**
   * Optional: if specified, we first test this Query to see whether the document should be checked
   * for matching ranges. If this is null, all documents are checked.
   */
  protected final Query fastMatchQuery;

  /** Our field name. */
  protected final String field;

  /** Total number of hits. */
  protected int totCount;

  /** Create {@code RangeFacetCounts} */
  protected RangeFacetCounts(String field, Range[] ranges, Query fastMatchQuery)
      throws IOException {
    this.field = field;
    this.ranges = ranges;
    this.fastMatchQuery = fastMatchQuery;
    counts = new int[ranges.length];
  }

  /**
   * Create a {@link org.apache.lucene.search.DocIdSetIterator} of {@code fastMatchQuery} for the
   * provided {@code context}. A null response indicates no documents will match. Note that invoking
   * this when fastMatchQuery is null will result in a null response as well.
   */
  protected DocIdSetIterator createFastMatchDisi(LeafReaderContext context) throws IOException {
    assert context != null : "context must not be null";
    if (fastMatchQuery == null) {
      return null;
    }
    final IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(context);
    final IndexSearcher searcher = new IndexSearcher(topLevelContext);
    searcher.setQueryCache(null);
    final Weight fastMatchWeight =
        searcher.createWeight(searcher.rewrite(fastMatchQuery), ScoreMode.COMPLETE_NO_SCORES, 1);
    final Scorer s = fastMatchWeight.scorer(context);
    if (s == null) {
      return null;
    } else {
      return s.iterator();
    }
  }

  protected DocIdSetIterator createIterator(FacetsCollector.MatchingDocs hits) throws IOException {
    final DocIdSetIterator it;
    if (fastMatchQuery != null) {
      DocIdSetIterator fastMatchDocs = createFastMatchDisi(hits.context);
      if (fastMatchDocs == null) {
        return null;
      } else {
        it = ConjunctionDISI.intersectIterators(Arrays.asList(hits.bits.iterator(), fastMatchDocs));
      }
    } else {
      it = hits.bits.iterator();
    }

    return it;
  }

  @Override
  public FacetResult getTopChildren(int topN, String dim, String... path) {
    if (dim.equals(field) == false) {
      throw new IllegalArgumentException(
          "invalid dim \"" + dim + "\"; should be \"" + field + "\"");
    }
    if (path.length != 0) {
      throw new IllegalArgumentException("path.length should be 0");
    }
    LabelAndValue[] labelValues = new LabelAndValue[counts.length];
    for (int i = 0; i < counts.length; i++) {
      labelValues[i] = new LabelAndValue(ranges[i].label, counts[i]);
    }
    return new FacetResult(dim, path, totCount, labelValues, labelValues.length);
  }

  @Override
  public Number getSpecificValue(String dim, String... path) throws IOException {
    // TODO: should we impl this?
    throw new UnsupportedOperationException();
  }

  @Override
  public List<FacetResult> getAllDims(int topN) throws IOException {
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
      b.append(counts[i]);
      b.append('\n');
    }
    return b.toString();
  }
}
