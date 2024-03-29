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

package org.apache.lucene.search.comparators;

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.Pruning;
import org.apache.lucene.util.NumericUtils;

/**
 * Comparator based on {@link Integer#compare} for {@code numHits}. This comparator provides a
 * skipping functionality – an iterator that can skip over non-competitive documents.
 */
public class IntComparator extends NumericComparator<Integer> {
  private final int[] values;
  protected int topValue;
  protected int bottom;

  public IntComparator(
      int numHits, String field, Integer missingValue, boolean reverse, Pruning pruning) {
    super(field, missingValue != null ? missingValue : 0, reverse, pruning, Integer.BYTES);
    values = new int[numHits];
  }

  @Override
  public int compare(int slot1, int slot2) {
    return Integer.compare(values[slot1], values[slot2]);
  }

  @Override
  public void setTopValue(Integer value) {
    super.setTopValue(value);
    topValue = value;
  }

  @Override
  public Integer value(int slot) {
    return Integer.valueOf(values[slot]);
  }

  @Override
  protected long missingValueAsComparableLong() {
    return missingValue;
  }

  @Override
  protected long sortableBytesToLong(byte[] bytes) {
    return NumericUtils.sortableBytesToInt(bytes, 0);
  }

  @Override
  public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
    return new IntLeafComparator(context);
  }

  /** Leaf comparator for {@link IntComparator} that provides skipping functionality */
  public class IntLeafComparator extends NumericLeafComparator {

    public IntLeafComparator(LeafReaderContext context) throws IOException {
      super(context);
    }

    private int getValueForDoc(int doc) throws IOException {
      if (docValues.advanceExact(doc)) {
        return (int) docValues.longValue();
      } else {
        return missingValue;
      }
    }

    @Override
    public void setBottom(int slot) throws IOException {
      bottom = values[slot];
      super.setBottom(slot);
    }

    @Override
    public int compareBottom(int doc) throws IOException {
      return Integer.compare(bottom, getValueForDoc(doc));
    }

    @Override
    public int compareTop(int doc) throws IOException {
      return Integer.compare(topValue, getValueForDoc(doc));
    }

    @Override
    public void copy(int slot, int doc) throws IOException {
      values[slot] = getValueForDoc(doc);
      super.copy(slot, doc);
    }

    @Override
    protected long bottomAsComparableLong() {
      return bottom;
    }

    @Override
    protected long topAsComparableLong() {
      return topValue;
    }
  }
}
