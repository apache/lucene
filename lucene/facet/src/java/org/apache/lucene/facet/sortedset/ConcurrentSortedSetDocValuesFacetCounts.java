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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.PrimitiveIterator;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicIntegerArray;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetUtils;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollector.MatchingDocs;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.TopOrdAndIntQueue;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState.OrdRange;
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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LongValues;

/**
 * Like {@link SortedSetDocValuesFacetCounts}, but aggregates counts concurrently across segments.
 *
 * @lucene.experimental
 */
public class ConcurrentSortedSetDocValuesFacetCounts extends Facets {

  final ExecutorService exec;
  final SortedSetDocValuesReaderState state;
  final FacetsConfig stateConfig;
  final SortedSetDocValues dv;
  final String field;
  final AtomicIntegerArray counts;

  private static final String[] emptyPath = new String[0];

  /** Returns all facet counts, same result as searching on {@link MatchAllDocsQuery} but faster. */
  public ConcurrentSortedSetDocValuesFacetCounts(
      SortedSetDocValuesReaderState state, ExecutorService exec)
      throws IOException, InterruptedException {
    this(state, null, exec);
  }

  /** Counts all facet dimensions across the provided hits. */
  public ConcurrentSortedSetDocValuesFacetCounts(
      SortedSetDocValuesReaderState state, FacetsCollector hits, ExecutorService exec)
      throws IOException, InterruptedException {
    this.state = state;
    this.field = state.getField();
    this.stateConfig = state.getFacetsConfig();
    this.exec = exec;
    dv = state.getDocValues();
    counts = new AtomicIntegerArray(state.getSize());
    if (hits == null) {
      // browse only
      countAll();
    } else {
      count(hits.getMatchingDocs());
    }
  }

  @Override
  public FacetResult getTopChildren(int topN, String dim, String... path) throws IOException {
    validateTopN(topN);
    FacetsConfig.DimConfig dimConfig = stateConfig.getDimConfig(dim);

    if (dimConfig.hierarchical) {
      int pathOrd = (int) dv.lookupTerm(new BytesRef(FacetsConfig.pathToString(dim, path)));
      if (pathOrd < 0) {
        // path was never indexed
        return null;
      }
      SortedSetDocValuesReaderState.DimTree dimTree = state.getDimTree(dim);
      return getPathResult(dimConfig, dim, path, pathOrd, dimTree.iterator(pathOrd), topN);
    } else {
      if (path.length > 0) {
        throw new IllegalArgumentException(
            "Field is not configured as hierarchical, path should be 0 length");
      }
      OrdRange ordRange = state.getOrdRange(dim);
      if (ordRange == null) {
        // means dimension was never indexed
        return null;
      }
      int dimOrd = ordRange.start;
      PrimitiveIterator.OfInt childIt = ordRange.iterator();
      if (dimConfig.multiValued && dimConfig.requireDimCount) {
        // If the dim is multi-valued and requires dim counts, we know we've explicitly indexed
        // the dimension and we need to skip past it so the iterator is positioned on the first
        // child:
        childIt.next();
      }
      return getPathResult(dimConfig, dim, null, dimOrd, childIt, topN);
    }
  }

  private FacetResult getPathResult(
      FacetsConfig.DimConfig dimConfig,
      String dim,
      String[] path,
      int pathOrd,
      PrimitiveIterator.OfInt childOrds,
      int topN)
      throws IOException {

    TopOrdAndIntQueue q = null;

    int bottomCount = 0;

    int dimCount = 0;
    int childCount = 0;

    TopOrdAndIntQueue.OrdAndValue reuse = null;

    while (childOrds.hasNext()) {
      int ord = childOrds.next();
      if (counts.get(ord) > 0) {
        dimCount += counts.get(ord);
        childCount++;
        if (counts.get(ord) > bottomCount) {
          if (reuse == null) {
            reuse = new TopOrdAndIntQueue.OrdAndValue();
          }
          reuse.ord = ord;
          reuse.value = counts.get(ord);
          if (q == null) {
            // Lazy init, so we don't create this for the
            // sparse case unnecessarily
            q = new TopOrdAndIntQueue(topN);
          }
          reuse = q.insertWithOverflow(reuse);
          if (q.size() == topN) {
            bottomCount = q.top().value;
          }
        }
      }
    }

    if (q == null) {
      return null;
    }

    LabelAndValue[] labelValues = new LabelAndValue[q.size()];
    for (int i = labelValues.length - 1; i >= 0; i--) {
      TopOrdAndIntQueue.OrdAndValue ordAndValue = q.pop();
      assert ordAndValue != null;
      final BytesRef term = dv.lookupOrd(ordAndValue.ord);
      String[] parts = FacetsConfig.stringToPath(term.utf8ToString());
      labelValues[i] = new LabelAndValue(parts[parts.length - 1], ordAndValue.value);
    }

    if (dimConfig.hierarchical == false) {
      // see if dimCount is actually reliable or needs to be reset
      if (dimConfig.multiValued) {
        if (dimConfig.requireDimCount) {
          dimCount = counts.get(pathOrd);
        } else {
          dimCount = -1; // dimCount is in accurate at this point, so set it to -1
        }
      }
      return new FacetResult(dim, emptyPath, dimCount, labelValues, childCount);
    } else {
      return new FacetResult(dim, path, counts.get(pathOrd), labelValues, childCount);
    }
  }

  private class CountOneSegment implements Callable<Void> {
    final LeafReader leafReader;
    final MatchingDocs hits;
    final OrdinalMap ordinalMap;
    final int segOrd;

    public CountOneSegment(
        LeafReader leafReader, MatchingDocs hits, OrdinalMap ordinalMap, int segOrd) {
      assert leafReader != null;
      this.leafReader = leafReader;
      this.hits = hits;
      this.ordinalMap = ordinalMap;
      this.segOrd = segOrd;
    }

    @Override
    public Void call() throws IOException {
      SortedSetDocValues multiValues = DocValues.getSortedSet(leafReader, field);
      if (multiValues == null) {
        // nothing to count here
        return null;
      }

      // It's slightly more efficient to work against SortedDocValues if the field is actually
      // single-valued (see: LUCENE-5309)
      SortedDocValues singleValues = DocValues.unwrapSingleton(multiValues);
      DocIdSetIterator valuesIt = singleValues != null ? singleValues : multiValues;

      // TODO: yet another option is to count all segs
      // first, only in seg-ord space, and then do a
      // merge-sort-PQ in the end to only "resolve to
      // global" those seg ords that can compete, if we know
      // we just want top K?  ie, this is the same algo
      // that'd be used for merging facets across shards
      // (distributed faceting).  but this has much higher
      // temp ram req'ts (sum of number of ords across all
      // segs)
      DocIdSetIterator it;
      if (hits == null) {
        // count all
        // Initializing liveDocs bits in the constructor leads to a situation where liveDocs bits
        // get initialized in the calling thread but get used in a different thread leading to an
        // AssertionError. See LUCENE-10134
        final Bits liveDocs = leafReader.getLiveDocs();
        it = (liveDocs != null) ? FacetUtils.liveDocsDISI(valuesIt, liveDocs) : valuesIt;
      } else {
        it = ConjunctionUtils.intersectIterators(Arrays.asList(hits.bits.iterator(), valuesIt));
      }

      if (ordinalMap != null) {
        final LongValues ordMap = ordinalMap.getGlobalOrds(segOrd);

        int numSegOrds = (int) multiValues.getValueCount();

        if (hits != null && hits.totalHits < numSegOrds / 10) {
          // Remap every ord to global ord as we iterate:
          if (singleValues != null) {
            for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
              counts.incrementAndGet((int) ordMap.get(singleValues.ordValue()));
            }
          } else {
            for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
              int term = (int) multiValues.nextOrd();
              while (term != SortedSetDocValues.NO_MORE_ORDS) {
                counts.incrementAndGet((int) ordMap.get(term));
                term = (int) multiValues.nextOrd();
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
              int term = (int) multiValues.nextOrd();
              while (term != SortedSetDocValues.NO_MORE_ORDS) {
                segCounts[term]++;
                term = (int) multiValues.nextOrd();
              }
            }
          }

          // Then, migrate to global ords:
          for (int ord = 0; ord < numSegOrds; ord++) {
            int count = segCounts[ord];
            if (count != 0) {
              counts.addAndGet((int) ordMap.get(ord), count);
            }
          }
        }
      } else {
        // No ord mapping (e.g., single segment index):
        // just aggregate directly into counts:
        if (singleValues != null) {
          for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
            counts.incrementAndGet(singleValues.ordValue());
          }
        } else {
          for (int doc = it.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = it.nextDoc()) {
            int term = (int) multiValues.nextOrd();
            while (term != SortedSetDocValues.NO_MORE_ORDS) {
              counts.incrementAndGet(term);
              term = (int) multiValues.nextOrd();
            }
          }
        }
      }

      return null;
    }
  }

  /** Does all the "real work" of tallying up the counts. */
  private void count(List<MatchingDocs> matchingDocs) throws IOException, InterruptedException {

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
    List<Future<Void>> results = new ArrayList<>();

    for (MatchingDocs hits : matchingDocs) {
      // LUCENE-5090: make sure the provided reader context "matches"
      // the top-level reader passed to the
      // SortedSetDocValuesReaderState, else cryptic
      // AIOOBE can happen:
      if (ReaderUtil.getTopLevelContext(hits.context).reader() != reader) {
        throw new IllegalStateException(
            "the SortedSetDocValuesReaderState provided to this class does not match the reader being searched; you must create a new SortedSetDocValuesReaderState every time you open a new IndexReader");
      }

      results.add(
          exec.submit(
              new CountOneSegment(hits.context.reader(), hits, ordinalMap, hits.context.ord)));
    }

    for (Future<Void> result : results) {
      try {
        result.get();
      } catch (ExecutionException ee) {
        // Theoretically cause can be null; guard against that.
        Throwable cause = ee.getCause();
        throw IOUtils.rethrowAlways(cause != null ? cause : ee);
      }
    }
  }

  /** Does all the "real work" of tallying up the counts. */
  private void countAll() throws IOException, InterruptedException {

    OrdinalMap ordinalMap;

    // TODO: is this right?  really, we need a way to
    // verify that this ordinalMap "matches" the leaves in
    // matchingDocs...
    if (dv instanceof MultiDocValues.MultiSortedSetDocValues) {
      ordinalMap = ((MultiSortedSetDocValues) dv).mapping;
    } else {
      ordinalMap = null;
    }

    List<Future<Void>> results = new ArrayList<>();

    for (LeafReaderContext context : state.getReader().leaves()) {
      results.add(
          exec.submit(new CountOneSegment(context.reader(), null, ordinalMap, context.ord)));
    }

    for (Future<Void> result : results) {
      try {
        result.get();
      } catch (ExecutionException ee) {
        // Theoretically cause can be null; guard against that.
        Throwable cause = ee.getCause();
        throw IOUtils.rethrowAlways(cause != null ? cause : ee);
      }
    }
  }

  @Override
  public Number getSpecificValue(String dim, String... path) throws IOException {
    if (path.length != 1) {
      throw new IllegalArgumentException("path must be length=1");
    }
    int ord = (int) dv.lookupTerm(new BytesRef(FacetsConfig.pathToString(dim, path)));
    if (ord < 0) {
      return -1;
    }

    return counts.get(ord);
  }

  @Override
  public List<FacetResult> getAllDims(int topN) throws IOException {
    validateTopN(topN);
    List<FacetResult> results = new ArrayList<>();
    for (String dim : state.getDims()) {
      FacetsConfig.DimConfig dimConfig = stateConfig.getDimConfig(dim);
      if (dimConfig.hierarchical) {
        SortedSetDocValuesReaderState.DimTree dimTree = state.getDimTree(dim);
        int dimOrd = dimTree.dimStartOrd;
        FacetResult fr = getPathResult(dimConfig, dim, emptyPath, dimOrd, dimTree.iterator(), topN);
        if (fr != null) {
          results.add(fr);
        }
      } else {
        OrdRange ordRange = state.getOrdRange(dim);
        int dimOrd = ordRange.start;
        PrimitiveIterator.OfInt childIt = ordRange.iterator();
        if (dimConfig.multiValued && dimConfig.requireDimCount) {
          // If the dim is multi-valued and requires dim counts, we know we've explicitly indexed
          // the dimension and we need to skip past it so the iterator is positioned on the first
          // child:
          childIt.next();
        }
        FacetResult fr = getPathResult(dimConfig, dim, emptyPath, dimOrd, childIt, topN);
        if (fr != null) {
          results.add(fr);
        }
      }
    }

    // Sort by highest count:
    Collections.sort(
        results,
        new Comparator<FacetResult>() {
          @Override
          public int compare(FacetResult a, FacetResult b) {
            if (a.value.intValue() > b.value.intValue()) {
              return -1;
            } else if (b.value.intValue() > a.value.intValue()) {
              return 1;
            } else {
              return a.dim.compareTo(b.dim);
            }
          }
        });

    return results;
  }
}
