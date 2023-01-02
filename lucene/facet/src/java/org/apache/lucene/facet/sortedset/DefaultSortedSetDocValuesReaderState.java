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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
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

/**
 * Default implementation of {@link SortedSetDocValuesFacetCounts}. You must ensure the original
 * {@link IndexReader} passed to the constructor is not closed whenever you use this class!
 */
public class DefaultSortedSetDocValuesReaderState extends SortedSetDocValuesReaderState {

  private final String field;
  private final int valueCount;

  /** {@link IndexReader} passed to the constructor. */
  public final IndexReader reader;

  private final Map<String, OrdinalMap> cachedOrdMaps = new HashMap<>();

  private final FacetsConfig config;

  /** Used for hierarchical dims. */
  private final Map<String, DimTree> prefixToDimTree = new HashMap<>();

  /** Used for flat dims. */
  private final Map<String, OrdRange> prefixToOrdRange = new HashMap<>();

  /**
   * Creates this with a config, pulling doc values from the default {@link
   * FacetsConfig#DEFAULT_INDEX_FIELD_NAME}.
   */
  public DefaultSortedSetDocValuesReaderState(IndexReader reader, FacetsConfig config)
      throws IOException {
    this(reader, FacetsConfig.DEFAULT_INDEX_FIELD_NAME, config);
  }

  /**
   * Creates this without a config, pulling doc values from the default {@link
   * FacetsConfig#DEFAULT_INDEX_FIELD_NAME}.
   *
   * @deprecated Users should explicitly provide facet configuration during instantiation. See
   *     {@link #DefaultSortedSetDocValuesReaderState(IndexReader, FacetsConfig)}. To maintain all
   *     existing behavior, a "default" facet configuration can be provided with {@link
   *     FacetsConfig#FacetsConfig()}.
   */
  @Deprecated
  public DefaultSortedSetDocValuesReaderState(IndexReader reader) throws IOException {
    this(reader, FacetsConfig.DEFAULT_INDEX_FIELD_NAME, null);
  }

  /**
   * Creates this without a config, pulling doc values from the specified field.
   *
   * @deprecated Users should explicitly provide facet configuration during instantiation. See
   *     {@link #DefaultSortedSetDocValuesReaderState(IndexReader, String, FacetsConfig)}. To
   *     maintain all existing behavior, a "default" facet configuration can be provided with {@link
   *     FacetsConfig#FacetsConfig()}.
   */
  @Deprecated
  public DefaultSortedSetDocValuesReaderState(IndexReader reader, String field) throws IOException {
    this(reader, field, null);
  }

  /** Creates this, pulling doc values from the specified field. */
  public DefaultSortedSetDocValuesReaderState(IndexReader reader, String field, FacetsConfig config)
      throws IOException {
    this.field = field;
    this.reader = reader;
    this.config = config;

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

    int ord = 0;
    while (ord != valueCount) {
      BytesRef term = dv.lookupOrd(ord);
      String[] components = FacetsConfig.stringToPath(term.utf8ToString());
      String dim = components[0];
      if (config != null && config.getDimConfig(dim).hierarchical) {
        ord = createOneHierarchicalFacetDimState(dv, ord) + 1;
      } else {
        ord = createOneFlatFacetDimState(dv, ord) + 1;
      }
    }
  }

  // returns last ord of dimension
  private int createOneHierarchicalFacetDimState(SortedSetDocValues dv, int dimStartOrd)
      throws IOException {
    List<Boolean> hasChildren = new ArrayList<>();
    List<Integer> siblings = new ArrayList<>();

    // stack of paths with unfulfilled siblings
    Stack<OrdAndComponent> siblingStack = new Stack<>();

    int dimEndOrd = dimStartOrd;

    BytesRef nextTerm = dv.lookupOrd(dimEndOrd);
    String[] nextComponents = FacetsConfig.stringToPath(nextTerm.utf8ToString());
    String dim = nextComponents[0];

    while (true) {
      String[] components = nextComponents;

      int ord = dimEndOrd - dimStartOrd;

      while (siblingStack.empty() == false
          && siblingStack.peek().component.length >= components.length) {
        OrdAndComponent possibleSibling = siblingStack.pop();
        if (possibleSibling.component.length == components.length) {
          // lengths are equal, all non-siblings of equal length will have already been popped off
          // so this must be sibling
          siblings.set(possibleSibling.ord, ord);
        }
      }

      if (dimEndOrd + 1 == valueCount) {
        // current ord needs to be added, can't have children or siblings
        siblings.add(-1);
        hasChildren.add(false);
        break;
      }

      nextTerm = dv.lookupOrd(dimEndOrd + 1);
      nextComponents = FacetsConfig.stringToPath(nextTerm.utf8ToString());

      if (nextComponents[0].equals(components[0]) == false) {
        // current ord needs to be added, can't have children or siblings
        siblings.add(-1);
        hasChildren.add(false);
        break;
      }

      if (components.length < nextComponents.length) {
        // next ord must be a direct child of current ord, this is because we are indexing all
        // ancestral paths
        hasChildren.add(ord, true);
        // we don't know if this ord has a sibling or where it's sibling could be yet
        siblingStack.push(new OrdAndComponent(ord, components));
        // we still add INVALID_ORDINAL, which will be replaced if a valid sibling is found
        siblings.add(ord, INVALID_ORDINAL);
      } else if (components.length == nextComponents.length) {
        // next ord must be a sibling of current and there are no direct children of current, this
        // is because we
        // are indexing all ancestral paths
        siblings.add(ord, ord + 1);
        hasChildren.add(ord, false);
      } else {
        // components.length > nextComponents.length
        // next ord is neither sibling nor child
        siblings.add(ord, INVALID_ORDINAL);
        hasChildren.add(ord, false);
      }

      dimEndOrd++;
    }

    prefixToDimTree.put(dim, new DimTree(dimStartOrd, siblings, hasChildren));

    return dimEndOrd;
  }

  // returns last ord of dimension
  private int createOneFlatFacetDimState(SortedSetDocValues dv, int dimStartOrd)
      throws IOException {

    int dimEndOrd = dimStartOrd;

    BytesRef nextTerm = dv.lookupOrd(dimEndOrd);
    String[] nextComponents = FacetsConfig.stringToPath(nextTerm.utf8ToString());
    // The first entry should always be length 1 or 2 (either just the dim itself if we explicitly
    // indexed it, or the first child):
    if (nextComponents.length > 2) {
      throw new IllegalArgumentException(
          "dimension not configured to handle hierarchical field; got: "
              + Arrays.toString(nextComponents)
              + " "
              + nextTerm.utf8ToString());
    }
    String dim = nextComponents[0];

    while (true) {
      String[] components = nextComponents;

      if (dimEndOrd + 1 == valueCount) {
        break;
      }

      nextTerm = dv.lookupOrd(dimEndOrd + 1);
      nextComponents = FacetsConfig.stringToPath(nextTerm.utf8ToString());

      if (nextComponents[0].equals(components[0]) == false) {
        break;
      }

      // Each entry should have a length of exactly 2 since the dim is non-hierarchical:
      if (nextComponents.length != 2) {
        throw new IllegalArgumentException(
            "dimension not configured to handle hierarchical field; got: "
                + Arrays.toString(nextComponents)
                + " "
                + nextTerm.utf8ToString());
      }

      dimEndOrd++;
    }
    prefixToOrdRange.put(dim, new OrdRange(dimStartOrd, dimEndOrd));

    return dimEndOrd;
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
  public FacetsConfig getFacetsConfig() {
    return config;
  }

  @Override
  public Iterable<String> getDims() {
    return () ->
        new Iterator<>() {

          final Iterator<String> dimTreeIterator = prefixToDimTree.keySet().iterator();
          final Iterator<String> ordRangeIterator = prefixToOrdRange.keySet().iterator();

          @Override
          public boolean hasNext() {
            return ordRangeIterator.hasNext() || dimTreeIterator.hasNext();
          }

          @Override
          public String next() {
            if (dimTreeIterator.hasNext()) {
              return dimTreeIterator.next();
            } else if (ordRangeIterator.hasNext()) {
              return ordRangeIterator.next();
            } else {
              return null;
            }
          }
        };
  }

  /* Flat facet operations */

  @Override
  public Map<String, OrdRange> getPrefixToOrdRange() {
    return prefixToOrdRange;
  }

  @Override
  public OrdRange getOrdRange(String dim) {
    if (config != null && config.getDimConfig(dim).hierarchical) {
      throw new UnsupportedOperationException(
          "This operation is only supported for flat dimensions");
    }
    return prefixToOrdRange.get(dim);
  }

  /* Hierarchical facet operations */

  @Override
  public DimTree getDimTree(String dim) {
    if (config == null || config.getDimConfig(dim).hierarchical == false) {
      throw new UnsupportedOperationException(
          "This operation is only supported for hierarchical facets");
    }
    return prefixToDimTree.get(dim);
  }

  private static final class OrdAndComponent {
    int ord;
    String[] component;

    public OrdAndComponent(int ord, String[] component) {
      this.ord = ord;
      this.component = component;
    }
  }
}
