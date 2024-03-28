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
package org.apache.lucene.facet.sortedset;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.facet.FacetUtils;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.LongValues;

/**
 * Compute facets counts from previously indexed {@link SortedSetDocValuesFacetField}, without
 * require a separate taxonomy index. Faceting is a bit slower (~25%), and there is added cost on
 * every {@link IndexReader} open to create a new {@link SortedSetDocValuesReaderState}.
 *
 * <p><b>NOTE</b>: this class should be instantiated and then used from a single thread, because it
 * holds a thread-private instance of {@link SortedSetDocValues}.
 *
 * <p><b>NOTE</b>: tie-break is by unicode sort order
 *
 * <p><b>NOTE</b>: if you have multi-valued dims that require dim counts (see {@link FacetsConfig},
 * make sure to provide your {@code FacetsConfig} instance when instantiating {@link
 * SortedSetDocValuesReaderState}, or else dim counts can be inaccurate
 *
 * @lucene.experimental
 */
public class SortedSetDocValuesFacetCounts extends AbstractSortedSetDocValueFacetCounts {
  private final SortedSetDocValuesReaderState state;
  int[] counts;

  /** Returns all facet counts, same result as searching on {@link MatchAllDocsQuery} but faster. */
  public SortedSetDocValuesFacetCounts(SortedSetDocValuesReaderState state) throws IOException {
    this(state, null);
  }

  /** Counts all facet dimensions across the provided hits. */
  public SortedSetDocValuesFacetCounts(SortedSetDocValuesReaderState state, FacetsCollector hits)
      throws IOException {
    super(state);
    this.state = state;
    if (hits == null) {
      // browse only
      countAll();
    } else {
      count(hits.getMatchingDocs());
    }
  }

  private void initializeCounts() {
    if (counts == null) {
      counts = new int[state.getSize()];
    }
  }

  @Override
  boolean hasCounts() {
    return counts != null;
  }

  @Override
  int getCount(int ord) {
    return counts[ord];
  }

  // Variant of countOneSegment, that has No Hits or Live Docs
  private void countOneSegmentNHLD(OrdinalMap ordinalMap, LeafReader reader, int segOrd)
      throws IOException {
    SortedSetDocValues multiValues = DocValues.getSortedSet(reader, field);
    if (multiValues == null) {
      // nothing to count
      return;
    }

    // Initialize counts:
    initializeCounts();

    // It's slightly more efficient to work against SortedDocValues if the field is actually
    // single-valued (see: LUCENE-5309)
    SortedDocValues singleValues = DocValues.unwrapSingleton(multiValues);

    // TODO: yet another option is to count all segs
    // first, only in seg-ord space, and then do a
    // merge-sort-PQ in the end to only "resolve to
    // global" those seg ords that can compete, if we know
    // we just want top K?  ie, this is the same algo
    // that'd be used for merging facets across shards
    // (distributed faceting).  but this has much higher
    // temp ram req'ts (sum of number of ords across all
    // segs)
    if (ordinalMap != null) {
      final LongValues ordMap = ordinalMap.getGlobalOrds(segOrd);
      int numSegOrds = (int) multiValues.getValueCount();

      // First count in seg-ord space:
      final int[] segCounts = new int[numSegOrds];
      if (singleValues != null) {
        for (int doc = singleValues.nextDoc();
            doc != DocIdSetIterator.NO_MORE_DOCS;
            doc = singleValues.nextDoc()) {
          segCounts[singleValues.ordValue()]++;
        }
      } else {
        for (int doc = multiValues.nextDoc();
            doc != DocIdSetIterator.NO_MORE_DOCS;
            doc = multiValues.nextDoc()) {
          for (int i = 0; i < multiValues.docValueCount(); i++) {
            int term = (int) multiValues.nextOrd();
            segCounts[term]++;
          }
        }
      }

      // Then, migrate to global ords:
      for (int ord = 0; ord < numSegOrds; ord++) {
        int count = segCounts[ord];
        if (count != 0) {
          // ordinalMap.getGlobalOrd(segOrd, ord));
          counts[(int) ordMap.get(ord)] += count;
        }
      }
    } else {
      // No ord mapping (e.g., single segment index):
      // just aggregate directly into counts:
      if (singleValues != null) {
        for (int doc = singleValues.nextDoc();
            doc != DocIdSetIterator.NO_MORE_DOCS;
            doc = singleValues.nextDoc()) {
          counts[singleValues.ordValue()]++;
        }
      } else {
        for (int doc = multiValues.nextDoc();
            doc != DocIdSetIterator.NO_MORE_DOCS;
            doc = multiValues.nextDoc()) {
          for (int i = 0; i < multiValues.docValueCount(); i++) {
            int term = (int) multiValues.nextOrd();
            counts[term]++;
          }
        }
      }
    }
  }

  private void countOneSegment(
      OrdinalMap ordinalMap, LeafReader reader, int segOrd, MatchingDocs hits, Bits liveDocs)
      throws IOException {
    if (hits != null && hits.totalHits == 0) {
      return;
    }

    SortedSetDocValues multiValues = DocValues.getSortedSet(reader, field);
    if (multiValues == null) {
      // nothing to count
      return;
    }

    // Initialize counts:
    initializeCounts();

    // It's slightly more efficient to work against SortedDocValues if the field is actually
    // single-valued (see: LUCENE-5309)
    SortedDocValues singleValues = DocValues.unwrapSingleton(multiValues);
    DocIdSetIterator valuesIt = singleValues != null ? singleValues : multiValues;

    DocIdSetIterator it;
    if (hits == null) {
      assert liveDocs != null;
      it = FacetUtils.liveDocsDISI(valuesIt, liveDocs);
    } else {
      it = ConjunctionUtils.intersectIterators(Arrays.asList(hits.bits.iterator(), valuesIt));
    }

    // TODO: yet another option is to count all segs
    // first, only in seg-ord space, and then do a
    // merge-sort-PQ in the end to only "resolve to
    // global" those seg ords that can compete, if we know
    // we just want top K?  ie, this is the same algo
    // that'd be used for merging facets across shards
    // (distributed faceting).  but this has much higher
    // temp ram req'ts (sum of number of ords across all
    // segs)
    if (ordinalMap != null) {
      final LongValues ordMap = ordinalMap.getGlobalOrds(segOrd);

      int numSegOrds = (int) multiValues.getValueCount();

      if (hits != null && hits.totalHits < numSegOrds / 10) {
        // Remap every ord to global ord as we iterate:
        if (singleValues != null) {
          for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
            counts[(int) ordMap.get(singleValues.ordValue())]++;
          }
        } else {
          for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
            for (int i = 0; i < multiValues.docValueCount(); i++) {
              int term = (int) multiValues.nextOrd();
              counts[(int) ordMap.get(term)]++;
            }
          }
        }
      } else {
        // First count in seg-ord space:
        final int[] segCounts = new int[numSegOrds];
        if (singleValues != null) {
          for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
            segCounts[singleValues.ordValue()]++;
          }
        } else {
          for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
            for (int i = 0; i < multiValues.docValueCount(); i++) {
              int term = (int) multiValues.nextOrd();
              segCounts[term]++;
            }
          }
        }

        // Then, migrate to global ords:
        for (int ord = 0; ord < numSegOrds; ord++) {
          int count = segCounts[ord];
          if (count != 0) {
            // ordinalMap.getGlobalOrd(segOrd, ord));
            counts[(int) ordMap.get(ord)] += count;
          }
        }
      }
    } else {
      // No ord mapping (e.g., single segment index):
      // just aggregate directly into counts:
      if (singleValues != null) {
        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
          counts[singleValues.ordValue()]++;
        }
      } else {
        for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
          for (int i = 0; i < multiValues.docValueCount(); i++) {
            int term = (int) multiValues.nextOrd();
            counts[term]++;
          }
        }
      }
    }
  }

  /** Does all the "real work" of tallying up the counts. */
  private void count(List<MatchingDocs> matchingDocs) throws IOException {

    OrdinalMap ordinalMap;

    // TODO: is this right?  really, we need a way to
    // verify that this ordinalMap "matches" the leaves in
    // matchingDocs...
    if (dv instanceof MultiDocValues.MultiSortedSetDocValues && matchingDocs.size() > 1) {
      ordinalMap = ((MultiSortedSetDocValues) dv).mapping;
    } else {
      ordinalMap = null;
    }

    IndexReader reader = state.getReader();

    for (MatchingDocs hits : matchingDocs) {

      // LUCENE-5090: make sure the provided reader context "matches"
      // the top-level reader passed to the
      // SortedSetDocValuesReaderState, else cryptic
      // AIOOBE can happen:
      if (ReaderUtil.getTopLevelContext(hits.context).reader() != reader) {
        throw new IllegalStateException(
            "the SortedSetDocValuesReaderState provided to this class does not match the reader being searched; you must create a new SortedSetDocValuesReaderState every time you open a new IndexReader");
      }

      countOneSegment(ordinalMap, hits.context.reader(), hits.context.ord, hits, null);
    }
  }

  /** Does all the "real work" of tallying up the counts. */
  private void countAll() throws IOException {

    OrdinalMap ordinalMap;

    // TODO: is this right?  really, we need a way to
    // verify that this ordinalMap "matches" the leaves in
    // matchingDocs...
    if (dv instanceof MultiDocValues.MultiSortedSetDocValues) {
      ordinalMap = ((MultiSortedSetDocValues) dv).mapping;
    } else {
      ordinalMap = null;
    }

    for (LeafReaderContext context : state.getReader().leaves()) {

      Bits liveDocs = context.reader().getLiveDocs();
      if (liveDocs == null) {
        countOneSegmentNHLD(ordinalMap, context.reader(), context.ord);
      } else {
        countOneSegment(ordinalMap, context.reader(), context.ord, null, liveDocs);
      }
    }
  }
}
