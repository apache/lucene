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
package org.apache.lucene.sandbox.facet.cutters;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.IntSupplier;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.internal.hppc.IntLongHashMap;
import org.apache.lucene.internal.hppc.LongIntHashMap;
import org.apache.lucene.sandbox.facet.labels.OrdToLabel;

/**
 * {@link FacetCutter} and {@link OrdToLabel} for distinct long values.
 *
 * <p>TODO: This class is quite inefficient. Will optimise later.
 *
 * <p>TODO: add support for other value sources e.g: LongValues
 *
 * @lucene.experimental
 */
public final class LongValueFacetCutter implements FacetCutter, OrdToLabel {
  private final String field;
  // TODO: consider alternatives if this is a bottleneck
  private final LongIntHashMapSyncCompute valueToOrdMap;
  private IntLongHashMap ordToValueMap;
  private final AtomicInteger maxOrdinal;

  /**
   * Constructor.
   *
   * @param field field name to read long values from.
   */
  public LongValueFacetCutter(String field) {
    this.field = field;
    valueToOrdMap = new LongIntHashMapSyncCompute();
    ordToValueMap = null;
    maxOrdinal = new AtomicInteger(-1);
  }

  @Override
  public LeafFacetCutter createLeafCutter(LeafReaderContext context) throws IOException {
    SortedNumericDocValues docValues = DocValues.getSortedNumeric(context.reader(), field);
    return new LeafFacetCutter() {
      int docValueCount;
      long lastDocValue;
      int docValueCursor;

      @Override
      public boolean advanceExact(int doc) throws IOException {
        if (docValues.advanceExact(doc)) {
          docValueCount = docValues.docValueCount();
          docValueCursor = 0;
          return true;
        }
        return false;
      }

      @Override
      public int nextOrd() throws IOException {
        while (docValueCursor++ < docValueCount) {
          long value = docValues.nextValue();
          // SortedNumericDocValues can have duplicates, but values are sorted, so we only need to
          // check previous value to remove duplicates
          if (docValueCursor == 1 || value != lastDocValue) {
            lastDocValue = value;
            return valueToOrdMap.computeIfAbsent(value, maxOrdinal::incrementAndGet);
          }
        }
        return NO_MORE_ORDS;
      }
    };
  }

  @Override
  public FacetLabel getLabel(int ordinal) {
    if (ordToValueMap == null) {
      buildOrdToValueMap();
    }
    if (ordToValueMap.containsKey(ordinal)) {
      return new FacetLabel(String.valueOf(ordToValueMap.get(ordinal)));
    }
    assert false
        : "ordinal="
            + ordinal
            + ", ordToValueMap.size="
            + ordToValueMap.size()
            + ", valueToOrdMap.size="
            + valueToOrdMap.size();
    return null;
  }

  /**
   * Get value by ordinal. Should only be called after collection phase.
   *
   * <p>TODO: we need it to tie break sort by value. Alternatively we can sort by label (then we
   * don't need this method), but we would have to convert FacetLabel to "long" to have the same
   * order... Overall, it is probably not important to tie break by value, and we can tie break by
   * ord same as for other facets; but for now we don't want to change results order just in case.
   *
   * @param ordinal facet ordinal.
   * @return long value
   */
  public long getValue(int ordinal) {
    // TODO: do we want to create #finish method that called by #reduce to build the map?
    if (ordToValueMap == null) {
      buildOrdToValueMap();
    }
    return ordToValueMap.get(ordinal);
  }

  private void buildOrdToValueMap() {
    ordToValueMap = new IntLongHashMap(valueToOrdMap.size());
    for (LongIntHashMap.LongIntCursor cursor : valueToOrdMap) {
      ordToValueMap.put(cursor.value, cursor.key);
    }
  }

  @Override
  public FacetLabel[] getLabels(int[] ordinals) throws IOException {
    FacetLabel[] facetLabels = new FacetLabel[ordinals.length];
    for (int i = 0; i < ordinals.length; i++) {
      facetLabels[i] = getLabel(ordinals[i]);
    }
    return facetLabels;
  }

  /** {@link LongIntHashMap} with threadsafe computeIfAbsent method */
  private static class LongIntHashMapSyncCompute extends LongIntHashMap {
    private final ReentrantReadWriteLock rwl = new ReentrantReadWriteLock();
    private final Lock r = rwl.readLock();
    private final Lock w = rwl.writeLock();

    /**
     * If key exists in the map return its value, otherwise insert value from the value supplier and
     * return it.
     *
     * <p>The method is threadsafe, and it allows concurrent reading from the map, but it locks the
     * map to insert a new value as it might require rehashing.
     */
    public int computeIfAbsent(long key, IntSupplier valueSupplier) {
      r.lock();
      int value;
      try {
        value = super.getOrDefault(key, -1);
      } finally {
        r.unlock();
      }
      if (value == -1) {
        w.lock();
        try {
          int index = super.indexOf(key);
          if (super.indexExists(index)) {
            return super.indexGet(index);
          } else {
            value = valueSupplier.getAsInt();
            super.indexInsert(index, key, value);
            return value;
          }
        } finally {
          w.unlock();
        }
      } else {
        return value;
      }
    }
  }
}
