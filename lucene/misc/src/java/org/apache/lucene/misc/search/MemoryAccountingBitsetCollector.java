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

package org.apache.lucene.misc.search;

import java.io.IOException;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.misc.CollectorMemoryTracker;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.util.FixedBitSet;

/** Bitset collector which supports memory tracking. */
public class MemoryAccountingBitsetCollector extends SimpleCollector {

  final CollectorMemoryTracker tracker;

  /**
   * Backing bitset for matched docs. Lazily allocated on the first {@link #collect(int)} and stays
   * {@code null} for collectors that see no matches. Package-private callers (in particular {@link
   * MemoryAccountingBitsetCollectorManager#reduce}) MUST null-check before dereferencing. The
   * invariant {@code (bitSet == null) == (highestSetBit == -1)} holds after construction.
   */
  FixedBitSet bitSet;

  int docBase = 0;
  int minDocBase = Integer.MAX_VALUE;
  // Highest bit set so far, or -1 if none. Docs arrive in ascending order (see collect()).
  int highestSetBit = -1;

  public MemoryAccountingBitsetCollector(CollectorMemoryTracker tracker) {
    this.tracker = tracker;
  }

  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    docBase = context.docBase;
    minDocBase = Math.min(minDocBase, docBase);
    // The bitSet is grown lazily in collect() rather than pre-sized to the full leaf span, so
    // leaves (and partitions under intra-segment concurrency) that see no matches contribute
    // no allocation.
  }

  @Override
  public void collect(int doc) {
    int local = docBase - minDocBase + doc;
    assert local > highestSetBit
        : "collect() must receive docs in strictly ascending order; got local="
            + local
            + " after highestSetBit="
            + highestSetBit;
    // Grow the bitset lazily rather than pre-sizing to the full leaf span.
    if (bitSet == null) {
      // FixedBitSet(N) takes a bit count; +1 sizes it to cover indices 0..local.
      bitSet = new FixedBitSet(local + 1);
      tracker.updateBytes(bitSet.ramBytesUsed());
    } else {
      // ensureCapacity's second arg is a max bit index (not a count).
      FixedBitSet newBitSet = FixedBitSet.ensureCapacity(bitSet, local);
      if (newBitSet != bitSet) {
        tracker.updateBytes(newBitSet.ramBytesUsed() - bitSet.ramBytesUsed());
        bitSet = newBitSet;
      }
    }
    bitSet.set(local);
    highestSetBit = local;
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }

  int getMinDocBase() {
    return minDocBase;
  }

  int getHighestSetBit() {
    return highestSetBit;
  }
}
