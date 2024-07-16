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
package org.apache.lucene.sandbox.facet.recorders;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.internal.hppc.IntCursor;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.sandbox.facet.abstracts.FacetRecorder;
import org.apache.lucene.sandbox.facet.abstracts.FacetRollup;
import org.apache.lucene.sandbox.facet.abstracts.LeafFacetRecorder;
import org.apache.lucene.sandbox.facet.abstracts.OrdinalIterator;
import org.apache.lucene.sandbox.facet.abstracts.Reducer;
import org.apache.lucene.search.LongValues;
import org.apache.lucene.search.LongValuesSource;

/**
 * {@link FacetRecorder} that computes multiple long aggregations per facet. TODO: [premature
 * optimization idea] if instead of one array we keep aggregations in two LongVector (one for MAX
 * aggregation and one for SUM) we can benefit from SIMD?
 */
public class LongAggregationsFacetRecorder implements FacetRecorder {

  private IntObjectHashMap<long[]> perOrdinalValues;
  private List<IntObjectHashMap<long[]>> leafValues;

  private final LongValuesSource[] longValuesSources;
  private final Reducer[] reducers;

  /** Constructor. */
  public LongAggregationsFacetRecorder(LongValuesSource[] longValuesSources, Reducer[] reducers) {
    assert longValuesSources.length == reducers.length;
    this.longValuesSources = longValuesSources;
    this.reducers = reducers;
    perOrdinalValues = new IntObjectHashMap<>();
    leafValues = Collections.synchronizedList(new ArrayList<>());
  }

  @Override
  public LeafFacetRecorder getLeafRecorder(LeafReaderContext context) throws IOException {
    LongValues[] longValues = new LongValues[longValuesSources.length];
    for (int i = 0; i < longValuesSources.length; i++) {
      longValues[i] = longValuesSources[i].getValues(context, null);
    }
    IntObjectHashMap<long[]> valuesRecorder = new IntObjectHashMap<>();
    leafValues.add(valuesRecorder);
    return new LongAggregationsLeafFacetRecorder(longValues, reducers, valuesRecorder);
  }

  @Override
  public OrdinalIterator recordedOrds() {
    if (perOrdinalValues.isEmpty()) {
      return null;
    }
    Iterator<IntCursor> ordIterator = perOrdinalValues.keys().iterator();
    return new OrdinalIterator() {
      @Override
      public int nextOrd() throws IOException {
        if (ordIterator.hasNext()) {
          return ordIterator.next().value;
        } else {
          return NO_MORE_ORDS;
        }
      }
    };
  }

  @Override
  public boolean isEmpty() {
    return perOrdinalValues.isEmpty();
  }

  @Override
  public void reduce(FacetRollup facetRollup) throws IOException {
    boolean firstElement = true;
    for (IntObjectHashMap<long[]> leafValue : leafValues) {
      if (firstElement) {
        perOrdinalValues = leafValue;
        firstElement = false;
      } else {
        for (IntObjectHashMap.IntObjectCursor<long[]> elem : leafValue) {
          long[] vals = perOrdinalValues.get(elem.key);
          if (vals == null) {
            perOrdinalValues.put(elem.key, elem.value);
          } else {
            for (int i = 0; i < longValuesSources.length; i++) {
              vals[i] = reducers[i].reduce(vals[i], elem.value[i]);
            }
          }
        }
      }
    }
    if (firstElement) {
      // TODO: do we need empty map by default?
      perOrdinalValues = new IntObjectHashMap<>();
    }
    // TODO: implement rollup
    if (facetRollup != null
        && facetRollup.getDimOrdsToRollup().nextOrd() != OrdinalIterator.NO_MORE_ORDS) {
      throw new UnsupportedOperationException("Rollup is required, but not implemented");
    }
  }

  public long getRecordedValue(int ord, int valuesId) {
    if (valuesId < 0 || valuesId >= longValuesSources.length) {
      throw new IllegalArgumentException("Invalid request for ordinal values");
    }
    long[] valuesForOrd = perOrdinalValues.get(ord);
    if (valuesForOrd != null) {
      return valuesForOrd[valuesId];
    }
    return -1; // TODO: missing value, what do we want to return? Zero might be a better option.
  }

  private static class LongAggregationsLeafFacetRecorder implements LeafFacetRecorder {

    private final LongValues[] longValues;

    private final Reducer[] reducers;
    private final IntObjectHashMap<long[]> perOrdinalValues;

    LongAggregationsLeafFacetRecorder(
        LongValues[] longValues, Reducer[] reducers, IntObjectHashMap<long[]> perOrdinalValues) {
      this.longValues = longValues;
      this.reducers = reducers;
      this.perOrdinalValues = perOrdinalValues;
    }

    @Override
    public void record(int docId, int facetId) throws IOException {
      long[] valuesForOrd = perOrdinalValues.get(facetId);
      if (valuesForOrd == null) {
        valuesForOrd = new long[longValues.length];
        perOrdinalValues.put(facetId, valuesForOrd);
      }

      LongValues values;
      for (int i = 0; i < longValues.length; i++) {
        // TODO: cache advance/longValue results for current doc? Skipped for now as LongValues
        // themselves can keep the cache.
        values = longValues[i];
        if (values.advanceExact(docId)) {
          valuesForOrd[i] = reducers[i].reduce(valuesForOrd[i], values.longValue());
        }
      }
    }
  }
}
