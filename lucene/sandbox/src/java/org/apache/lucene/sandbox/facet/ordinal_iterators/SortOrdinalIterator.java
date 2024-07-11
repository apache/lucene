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
package org.apache.lucene.sandbox.facet.ordinal_iterators;

import java.io.IOException;
import org.apache.lucene.sandbox.facet.abstracts.GetOrd;
import org.apache.lucene.sandbox.facet.abstracts.OrdToComparable;
import org.apache.lucene.sandbox.facet.abstracts.OrdinalIterator;
import org.apache.lucene.util.InPlaceMergeSorter;

/**
 * {@link OrdinalIterator} that consumes incoming ordinals, sorts them by Comparable, and returns in
 * sorted order.
 */
public class SortOrdinalIterator<T extends Comparable<T> & GetOrd> implements OrdinalIterator {

  private final OrdToComparable<T> ordToComparable;
  private final OrdinalIterator sourceOrds;
  private int[] result;
  private int currentIndex;

  /**
   * @param sourceOrds source ordinals
   * @param ordToComparable object that creates Comparable for provided facet ordinal. If null,
   *     ordinals are sorted in natural order (ascending).
   */
  public SortOrdinalIterator(OrdinalIterator sourceOrds, OrdToComparable<T> ordToComparable) {
    this.sourceOrds = sourceOrds;
    this.ordToComparable = ordToComparable;
  }

  private void sort() throws IOException {
    assert result == null;
    result = sourceOrds.toArray();
    // TODO: it doesn't really work - we need List<T>.
    @SuppressWarnings({"unchecked"})
    T[] comparables = (T[]) new Object[result.length];
    for (int i = 0; i < result.length; i++) {
      comparables[i] = ordToComparable.getComparable(result[i], null);
    }
    new InPlaceMergeSorter() {
      @Override
      protected void swap(int i, int j) {
        int tmp = result[i];
        result[i] = result[j];
        result[j] = tmp;
        T tmp2 = comparables[i];
        comparables[i] = comparables[j];
        comparables[j] = tmp2;
      }

      @Override
      protected int compare(int i, int j) {
        return comparables[i].compareTo(comparables[j]);
      }
    }.sort(0, result.length);
    currentIndex = 0;
  }

  @Override
  public int nextOrd() throws IOException {
    if (result == null) {
      sort();
    }
    assert result != null;
    if (currentIndex >= result.length) {
      return NO_MORE_ORDS;
    }
    return result[currentIndex++];
  }
}
