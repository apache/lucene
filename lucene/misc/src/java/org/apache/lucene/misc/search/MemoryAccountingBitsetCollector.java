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

/**
 * Bitset collector which supports memory tracking. Can operate in two modes: Full index mode:
 * allocates bitset for entire index (for single-threaded search), Segment-local mode: allocates
 * bitset only for segments processed (memory-efficient for concurrent search)
 */
public class MemoryAccountingBitsetCollector extends SimpleCollector {

  final CollectorMemoryTracker tracker;
  final boolean segmentLocal;
  FixedBitSet bitSet = new FixedBitSet(0);
  int length = 0;
  int docBase = 0;

  // For segment-local mode
  int minDocBase = Integer.MAX_VALUE;
  int maxDocEnd = 0;

  public MemoryAccountingBitsetCollector(CollectorMemoryTracker tracker) {
    this(tracker, false);
  }

  public MemoryAccountingBitsetCollector(CollectorMemoryTracker tracker, boolean segmentLocal) {
    this.tracker = tracker;
    this.segmentLocal = segmentLocal;
    tracker.updateBytes(bitSet.ramBytesUsed());
  }

  @Override
  protected void doSetNextReader(LeafReaderContext context) throws IOException {
    docBase = context.docBase;

    if (segmentLocal) {
      int docEnd = docBase + context.reader().maxDoc();
      minDocBase = Math.min(minDocBase, docBase);
      maxDocEnd = Math.max(maxDocEnd, docEnd);
      length = maxDocEnd - minDocBase;
    } else {
      length += context.reader().maxDoc();
    }

    FixedBitSet newBitSet = FixedBitSet.ensureCapacity(bitSet, length);
    if (newBitSet != bitSet) {
      tracker.updateBytes(newBitSet.ramBytesUsed() - bitSet.ramBytesUsed());
      bitSet = newBitSet;
    }
  }

  @Override
  public void collect(int doc) {
    if (segmentLocal) {
      bitSet.set(docBase - minDocBase + doc);
    } else {
      bitSet.set(docBase + doc);
    }
  }

  @Override
  public ScoreMode scoreMode() {
    return ScoreMode.COMPLETE_NO_SCORES;
  }

  int getMinDocBase() {
    return segmentLocal ? minDocBase : 0;
  }

  int getMaxDocEnd() {
    return segmentLocal ? maxDocEnd : length;
  }
}
