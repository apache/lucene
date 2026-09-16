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
  FixedBitSet bitSet = new FixedBitSet(0);
  int length = 0;
  int docBase = 0;

  int minDocBase = Integer.MAX_VALUE;
  int maxDocEnd = 0;
  // Highest bit index set in bitSet, or -1 if no doc has been collected. Docs are collected in
  // strictly ascending order: within a leaf by the Collector contract, and across leaves for a
  // given collector because IndexSearcher sorts partitions within a slice by docBase and rejects
  // multiple partitions of the same leaf sharing a slice. So this is simply the position written
  // by the most recent collect() call.
  int highestSetBit = -1;

  public MemoryAccountingBitsetCollector(CollectorMemoryTracker tracker) {
    this.tracker = tracker;
    tracker.updateBytes(bitSet.ramBytesUsed());
  }

  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    docBase = context.docBase;
    int docEnd = docBase + context.reader().maxDoc();
    minDocBase = Math.min(minDocBase, docBase);
    maxDocEnd = Math.max(maxDocEnd, docEnd);
    length = maxDocEnd - minDocBase;

    FixedBitSet newBitSet = FixedBitSet.ensureCapacity(bitSet, length);
    if (newBitSet != bitSet) {
      tracker.updateBytes(newBitSet.ramBytesUsed() - bitSet.ramBytesUsed());
      bitSet = newBitSet;
    }
  }

  @Override
  public void collect(int doc) {
    int local = docBase - minDocBase + doc;
    assert local > highestSetBit
        : "collect() must receive docs in strictly ascending order; got local="
            + local
            + " after highestSetBit="
            + highestSetBit;
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
