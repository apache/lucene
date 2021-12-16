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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.MultiDocValues.MultiSortedSetDocValues;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.Accountables;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;

/**
 * Default implementation of {@link SortedSetDocValuesFacetCounts}. You must ensure the original
 * {@link IndexReader} passed to the constructor is not closed whenever you use this class!
 */
public class DefaultSortedSetDocValuesReaderState extends SortedSetDocValuesReaderState {

  private final String field;
  private final int valueCount;

  // denotes whether next ord is child or not
  private FixedBitSet children;
  // maps an ord to its first sibling
  private int[] siblings;

  private List<DimAndOrd> dims;

  /** {@link IndexReader} passed to the constructor. */
  public final IndexReader reader;

  private final Map<String, OrdinalMap> cachedOrdMaps = new HashMap<>();

  /**
   * Creates this, pulling doc values from the default {@link
   * FacetsConfig#DEFAULT_INDEX_FIELD_NAME}.
   */
  public DefaultSortedSetDocValuesReaderState(IndexReader reader) throws IOException {
    this(reader, FacetsConfig.DEFAULT_INDEX_FIELD_NAME);
  }

  /** Creates this, pulling doc values from the specified field. */
  public DefaultSortedSetDocValuesReaderState(IndexReader reader, String field) throws IOException {
    this.field = field;
    this.reader = reader;

    // We need this to create thread-safe MultiSortedSetDV
    // per collector:
    SortedSetDocValues dv = getDocValues();
    if (dv == null) {
      throw new IllegalArgumentException(
          "field \"" + field + "\" was not indexed with SortedSetDocValues");
    }
    if (dv.getValueCount() > Integer.MAX_VALUE) {
      throw new IllegalArgumentException(
          "can only handle valueCount < Integer.MAX_VALUE; got " + dv.getValueCount());
    }
    valueCount = (int) dv.getValueCount();

    // keeps track of last path with length i where the path has an unfilled sibling
    Map<Integer, OrdAndComponent> pathsWithUnfilledSiblings = new HashMap<>();

    BytesRef nextTerm = null;
    String[] nextComponents = null;

    for (int ord = 0; ord < valueCount; ord++) {
      if (children == null) {
        children = new FixedBitSet(valueCount);
        siblings = new int[valueCount];
      }

      String[] components;

      if (nextTerm == null) {
        BytesRef term = dv.lookupOrd(ord);
        components = FacetsConfig.stringToPath(term.utf8ToString());
      } else {
        components = nextComponents;
      }

      if (components.length == 1) {
        // current ord is a dim
        if (dims == null) {
          dims = new ArrayList<>();
        }
        dims.add(new DimAndOrd(components[0], ord));
      }

      if (pathsWithUnfilledSiblings.containsKey(components.length - 1)) {
        OrdAndComponent possibleSibling = pathsWithUnfilledSiblings.get(components.length - 1);
        boolean isSibling = true;
        for (int i = 0; i < possibleSibling.component.length - 1; i++) {
          if (!possibleSibling.component[i].equals(components[i])) {
            isSibling = false;
            break;
          }
        }
        if (isSibling) {
          siblings[possibleSibling.ord] = ord;
        } else {
          siblings[possibleSibling.ord] = INVALID_ORDINAL;
        }
        pathsWithUnfilledSiblings.remove(components.length - 1);
      }

      if (ord + 1 == valueCount) {
        // last value, cannot have children or links to more siblings
        siblings[ord] = INVALID_ORDINAL;
        break;
      }

      nextTerm = dv.lookupOrd(ord + 1);
      nextComponents = FacetsConfig.stringToPath(nextTerm.utf8ToString());

      if (components.length < nextComponents.length) {
        // next ord must be a direct child of current ord
        children.set(ord);
        // we don't know if this ord has a sibling or where it's sibling could be yet
        pathsWithUnfilledSiblings.put(components.length - 1, new OrdAndComponent(ord, components));
      } else if (components.length == nextComponents.length) {
        // next ord must be a sibling of current and there are no direct children or current
        siblings[ord] = ord + 1;
      } else {
        // components.length > nextComponents.length
        // next ord is neither sibling nor child
        siblings[ord] = INVALID_ORDINAL;
      }
    }

    for (OrdAndComponent path : pathsWithUnfilledSiblings.values()) {
      siblings[path.ord] = INVALID_ORDINAL;
    }
  }

  /** Return the memory usage of this object in bytes. Negative values are illegal. */
  @Override
  public long ramBytesUsed() {
    synchronized (cachedOrdMaps) {
      long bytes = 0;
      for (OrdinalMap map : cachedOrdMaps.values()) {
        bytes += map.ramBytesUsed();
      }

      return bytes;
    }
  }

  /**
   * Returns nested resources of this class. The result should be a point-in-time snapshot (to avoid
   * race conditions).
   *
   * @see Accountables
   */
  @Override
  public Collection<Accountable> getChildResources() {
    synchronized (cachedOrdMaps) {
      return Accountables.namedAccountables("DefaultSortedSetDocValuesReaderState", cachedOrdMaps);
    }
  }

  @Override
  public String toString() {
    return "DefaultSortedSetDocValuesReaderState(field=" + field + " reader=" + reader + ")";
  }

  /** Return top-level doc values. */
  @Override
  public SortedSetDocValues getDocValues() throws IOException {
    // TODO: this is dup'd from slow composite reader wrapper ... can we factor it out to share?
    OrdinalMap map = null;
    // TODO: why are we lazy about this?  It's better if ctor pays the cost, not first query?  Oh,
    // but we
    // call this method from ctor, ok.  Also, we only ever store one entry in the map (for
    // key=field) so
    // why are we using a map?
    synchronized (cachedOrdMaps) {
      map = cachedOrdMaps.get(field);
      if (map == null) {
        // uncached, or not a multi dv
        SortedSetDocValues dv = MultiDocValues.getSortedSetValues(reader, field);
        if (dv instanceof MultiDocValues.MultiSortedSetDocValues) {
          map = ((MultiDocValues.MultiSortedSetDocValues) dv).mapping;
          IndexReader.CacheHelper cacheHelper = reader.getReaderCacheHelper();
          if (cacheHelper != null && map.owner == cacheHelper.getKey()) {
            cachedOrdMaps.put(field, map);
          }
        }
        return dv;
      }
    }

    assert map != null;
    int size = reader.leaves().size();
    final SortedSetDocValues[] values = new SortedSetDocValues[size];
    final int[] starts = new int[size + 1];
    long cost = 0;
    for (int i = 0; i < size; i++) {
      LeafReaderContext context = reader.leaves().get(i);
      final LeafReader reader = context.reader();
      final FieldInfo fieldInfo = reader.getFieldInfos().fieldInfo(field);
      if (fieldInfo != null && fieldInfo.getDocValuesType() != DocValuesType.SORTED_SET) {
        return null;
      }
      SortedSetDocValues v = reader.getSortedSetDocValues(field);
      if (v == null) {
        v = DocValues.emptySortedSet();
      }
      values[i] = v;
      starts[i] = context.docBase;
      cost += v.cost();
    }
    starts[size] = reader.maxDoc();
    return new MultiSortedSetDocValues(values, starts, map, cost);
  }

  /** Indexed field we are reading. */
  @Override
  public String getField() {
    return field;
  }

  @Override
  public IndexReader getReader() {
    return reader;
  }

  /** Number of unique labels. */
  @Override
  public int getSize() {
    return valueCount;
  }

  @Override
  public Iterable<Integer> childOrds(int pathOrd) {
    return () ->
        new Iterator<>() {

          boolean atStart = true;
          int currentOrd = pathOrd;

          @Override
          public boolean hasNext() {
            if (atStart) {
              if (currentOrd < 0 || currentOrd >= children.length()) {
                return false;
              }
              return children.get(currentOrd);
            } else {
              return siblings[currentOrd] != INVALID_ORDINAL;
            }
          }

          @Override
          public Integer next() {
            if (atStart) {
              if (currentOrd < 0 || currentOrd >= children.length()) {
                return INVALID_ORDINAL;
              }
              atStart = false;
              if (children.get(currentOrd)) {
                return ++currentOrd;
              } else {
                return INVALID_ORDINAL;
              }
            } else {
              currentOrd = siblings[currentOrd];
              return currentOrd;
            }
          }
        };
  }

  @Override
  public Iterable<DimAndOrd> getDims() {
    return dims;
  }

  private static class OrdAndComponent {
    int ord;
    String[] component;

    public OrdAndComponent(int ord, String[] component) {
      this.ord = ord;
      this.component = component;
    }
  }
}
