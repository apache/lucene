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

import static org.apache.lucene.sandbox.facet.abstracts.OrdinalIterator.NO_MORE_ORDS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.internal.hppc.IntCursor;
import org.apache.lucene.internal.hppc.IntIntHashMap;
import org.apache.lucene.sandbox.facet.abstracts.FacetRecorder;
import org.apache.lucene.sandbox.facet.abstracts.FacetRollup;
import org.apache.lucene.sandbox.facet.abstracts.LeafFacetCutter;
import org.apache.lucene.sandbox.facet.abstracts.LeafFacetRecorder;
import org.apache.lucene.sandbox.facet.abstracts.OrdinalIterator;

/**
 * {@link FacetRecorder} to count facets. TODO: add an option to keep counts in an array, to improve
 * performance for facets with small number of ordinals e.g. range facets. Options: - {@link
 * LeafFacetCutter} can inform {@link LeafFacetRecorder} about expected number of facet ordinals
 * ({@link org.apache.lucene.sandbox.facet.FacetFieldCollector} can orchestrate that). If expeted
 * facet ord number is below some threshold - use array instead of a map? - first 100/1k counts in
 * array, the rest - in a map; the limit can also be provided in a constructor? It is similar to
 * what LongValuesFacetCounts does today.
 */
public class CountFacetRecorder implements FacetRecorder {

  // TODO: deprecate - it is cheaper to merge during reduce than to lock threads during collection.
  // TODO: alternatively, we can consider collecting 2 (3, 4, ..., can be parametrizes) slices to a
  // single sync map
  //       which can reduce thread contention compared to single sync map for all slices; at the
  // same time there will
  //       be less work for reduce method. So far reduce wasn't a bottleneck for us, but it is
  // definitely not free.
  private final boolean useSyncMap;

  /** Create */
  public CountFacetRecorder() {
    this(false);
  }

  IntIntHashMap values;
  List<IntIntHashMap> perLeafValues;

  /**
   * Create.
   *
   * @param useSyncMap if true, use single sync map for all leafs.
   */
  public CountFacetRecorder(boolean useSyncMap) {
    super();
    if (useSyncMap) {
      values = new SafeIntIntHashMap();
    } else {
      // Has to be synchronizedList as we have one recorder per all slices.
      perLeafValues = Collections.synchronizedList(new ArrayList<>());
    }
    this.useSyncMap = useSyncMap;
  }

  /** Get count for provided ordinal. */
  public int getCount(int ord) {
    // TODO: allow or don't allow missing values?
    return values.get(ord);
  }

  private static final class SafeIntIntHashMap extends IntIntHashMap {
    @Override
    public synchronized int addTo(int key, int incrementValue) {
      return super.addTo(key, incrementValue);
    }
  }

  @Override
  public LeafFacetRecorder getLeafRecorder(LeafReaderContext context) {
    if (useSyncMap) {
      return new CountLeafRecorder(values);
    } else {
      IntIntHashMap leafValues = new IntIntHashMap();
      perLeafValues.add(leafValues);
      return new CountLeafRecorder(leafValues);
    }
  }

  @Override
  public OrdinalIterator recordedOrds() {
    // TODO: is that performant enough?
    // TODO: even if this is called before collection started, we want it to use results from the
    // time when nextOrd
    //  is first called. Does ordIterator work like that? I've run some tests that confirmed
    // expected behavior,
    //  but I'm not sure IntIntMap guarantees that. We should at least add a unit test to make sure
    // it always work
    //  that way.
    Iterator<IntCursor> ordIterator = values.keys().iterator();
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
    return values.isEmpty();
  }

  @Override
  public void reduce(FacetRollup facetRollup) throws IOException {
    if (useSyncMap == false) {
      boolean firstElement = true;
      for (IntIntHashMap leafRecords : perLeafValues) {
        if (firstElement) {
          values = leafRecords;
          firstElement = false;
        } else {
          for (IntIntHashMap.IntIntCursor elem : leafRecords) {
            values.addTo(elem.key, elem.value);
          }
        }
      }
      if (firstElement) {
        // TODO: do we need empty map by default?
        values = new IntIntHashMap();
      }
    }

    if (facetRollup == null) {
      return;
    }
    // Don't need to do anything now because we collect all to a sync IntIntMap
    OrdinalIterator dimOrds = facetRollup.getDimOrdsToRollup();
    for (int dimOrd = dimOrds.nextOrd(); dimOrd != NO_MORE_ORDS; ) {
      // TODO: we call addTo because this is what IntTaxonomyFacets does (add to current value).
      //  We might want to just replace the value instead? We should not have dimOrd in the map.
      values.addTo(dimOrd, rollup(dimOrd, facetRollup));
      dimOrd = dimOrds.nextOrd();
    }
  }

  private int rollup(int ord, FacetRollup facetRollup) throws IOException {
    OrdinalIterator childOrds = facetRollup.getChildrenOrds(ord);
    int accum = 0;
    for (int nextChild = childOrds.nextOrd(); nextChild != NO_MORE_ORDS; ) {
      // TODO: we call addTo because this is what IntTaxonomyFacets does (add to current value).
      //  We might want to just replace the value instead? We should not have nextChild in the map.
      accum += values.addTo(nextChild, rollup(nextChild, facetRollup));
      nextChild = childOrds.nextOrd();
    }
    return accum;
  }

  private static class CountLeafRecorder implements LeafFacetRecorder {

    private final IntIntHashMap values;

    public CountLeafRecorder(IntIntHashMap values) {
      this.values = values;
    }

    @Override
    public void record(int docId, int facetId) {
      this.values.addTo(facetId, 1);
    }
  }
}
