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
package org.apache.lucene.index;

import java.io.IOException;
import org.apache.lucene.util.FixedBitSet;

/** A list of per-document numeric values, sorted according to {@link Long#compare(long, long)}. */
public abstract class SortedNumericDocValues extends DocValuesIterator {

  /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
  protected SortedNumericDocValues() {}

  /**
   * Iterates to the next value in the current document. Do not call this more than {@link
   * #docValueCount} times for the document.
   */
  public abstract long nextValue() throws IOException;

  /**
   * Retrieves the number of values for the current document. This must always be greater than zero.
   * It is illegal to call this method after {@link #advanceExact(int)} returned {@code false}.
   */
  public abstract int docValueCount();

  /**
   * Fills a {@link FixedBitSet} with the doc IDs in {@code [fromDoc, toDoc)} whose sorted numeric
   * values contain at least one value in {@code [minValue, maxValue]}.
   *
   * <p>The default implementation falls back to per-doc evaluation via {@link #advanceExact(int)},
   * {@link #docValueCount()}, and {@link #nextValue()}. Subclasses with random-access storage can
   * override this to avoid per-doc virtual dispatch.
   *
   * <p>Callers should not rely on the final iterator position after this method returns.
   * Implementations may advance through some or all of the requested range.
   *
   * @param fromDoc first doc ID to evaluate (inclusive)
   * @param toDoc last doc ID to evaluate (exclusive)
   * @param minValue lower bound of the range (inclusive)
   * @param maxValue upper bound of the range (inclusive)
   * @param bitSet the bitset to fill
   * @param offset subtracted from each doc ID before setting the bit
   */
  public void rangeIntoBitSet(
      int fromDoc, int toDoc, long minValue, long maxValue, FixedBitSet bitSet, int offset)
      throws IOException {
    for (int doc = fromDoc; doc < toDoc; doc++) {
      if (advanceExact(doc)) {
        for (int i = 0, count = docValueCount(); i < count; i++) {
          long value = nextValue();
          if (value >= minValue) {
            if (value <= maxValue) {
              bitSet.set(doc - offset);
            }
            break;
          }
        }
      }
    }
  }
}
