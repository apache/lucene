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
package org.apache.lucene.sandbox.facet.misc;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.internal.hppc.IntLongHashMap;
import org.apache.lucene.internal.hppc.LongCursor;
import org.apache.lucene.internal.hppc.LongHashSet;
import org.apache.lucene.internal.hppc.LongIntHashMap;
import org.apache.lucene.sandbox.facet.cutters.FacetCutter;
import org.apache.lucene.sandbox.facet.cutters.LeafFacetCutter;
import org.apache.lucene.sandbox.facet.labels.OrdLabelBiMap;

/**
 * {@link FacetCutter} and {@link OrdLabelBiMap} for distinct long values.
 *
 * <p>TODO: This class is quite inefficient. Will optimise later. TODO: add support for other value
 * sources e.g: LongValues
 */
public class LongValueFacetCutter implements FacetCutter, OrdLabelBiMap {
  private final String field;
  // TODO: consider alternatives if this is a bottleneck
  private final LongIntHashMap valueToOrdMap;
  private final IntLongHashMap ordToValueMap;
  private final AtomicInteger maxOrdinal;

  /**
   * Constructor.
   *
   * @param field field name to read long values from.
   */
  public LongValueFacetCutter(String field) {
    this.field = field;
    valueToOrdMap =
        new LongIntHashMap() {
          @Override
          public synchronized boolean putIfAbsent(long key, int value) {
            return super.putIfAbsent(key, value);
          }
        };
    ordToValueMap = new IntLongHashMap();
    maxOrdinal = new AtomicInteger(-1);
  }

  @Override
  public LeafFacetCutter createLeafCutter(LeafReaderContext context) throws IOException {
    SortedNumericDocValues docValues = DocValues.getSortedNumeric(context.reader(), field);
    return new LeafFacetCutter() {
      int currDoc = -1;
      final LongHashSet valuesForDoc = new LongHashSet();
      private Iterator<LongCursor> valuesCursor;

      @Override
      public boolean advanceExact(int doc) throws IOException {
        if (doc < currDoc) {
          return false;
        }
        if (doc == currDoc) {
          return true;
        }
        valuesForDoc.clear();
        if (docValues.advanceExact(doc)) {
          int numValues = docValues.docValueCount();
          for (int i = 0; i < numValues; i++) {
            long value = docValues.nextValue();
            valueToOrdMap.putIfAbsent(value, maxOrdinal.incrementAndGet());
            ordToValueMap.put(valueToOrdMap.get(value), value);
            valuesForDoc.add(value);
          }
          currDoc = doc;
          valuesCursor = valuesForDoc.iterator();
          return true;
        }
        return false;
      }

      @Override
      public int nextOrd() throws IOException {
        if (valuesCursor.hasNext()) {
          return valueToOrdMap.get(valuesCursor.next().value);
        }
        return NO_MORE_ORDS;
      }
    };
  }

  @Override
  public FacetLabel getLabel(int ordinal) throws IOException {
    if (ordToValueMap.containsKey(ordinal)) {
      return new FacetLabel(String.valueOf(ordToValueMap.get(ordinal)));
    }
    return null;
  }

  /**
   * Get value by ordinal. Should only be called after collection phase. TODO: we need it to tie
   * break sort by value. Alternatively we can sort by label (then we don't need this method), but
   * we would have to convert FacetLabel to "long" to have the same order... Overall, it is probably
   * not important to tie break by value, and we can tie break by ord same as for other facets; but
   * for now we don't want to change results order just in case.
   *
   * @param ordinal facet ordinal.
   * @return long value
   */
  public long getValue(int ordinal) {
    return ordToValueMap.get(ordinal);
  }

  @Override
  public FacetLabel[] getLabels(int[] ordinals) throws IOException {
    FacetLabel[] facetLabels = new FacetLabel[ordinals.length];
    for (int i = 0; i < ordinals.length; i++) {
      facetLabels[i] = getLabel(ordinals[i]);
    }
    return facetLabels;
  }

  @Override
  public int getOrd(FacetLabel label) throws IOException {
    long value = Long.parseLong(label.lastComponent());
    if (valueToOrdMap.containsKey(value)) {
      return valueToOrdMap.get(value);
    }
    return -1;
  }

  @Override
  public int[] getOrds(FacetLabel[] labels) throws IOException {
    int[] ords = new int[labels.length];
    for (int i = 0; i < labels.length; i++) {
      ords[i] = getOrd(labels[i]);
    }
    return ords;
  }
}
