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

import java.util.Collection;
import org.apache.lucene.misc.CollectorMemoryTracker;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.util.FixedBitSet;

/**
 * CollectorManager for MemoryAccountingBitsetCollector that supports concurrent search.
 *
 * <p>Creates multiple collectors for concurrent execution, each collector only allocates bitset for
 * slices it processes, then merges with proper offset in reduce().
 */
public class MemoryAccountingBitsetCollectorManager
    implements CollectorManager<
        MemoryAccountingBitsetCollector, MemoryAccountingBitsetCollectorManager.Result> {

  /** The result of a search, containing the matched document IDs and total memory used. */
  public record Result(FixedBitSet bitSet, long totalBytesUsed) {}

  private final CollectorMemoryTracker tracker;

  public MemoryAccountingBitsetCollectorManager(CollectorMemoryTracker tracker) {
    this.tracker = tracker;
  }

  @Override
  public MemoryAccountingBitsetCollector newCollector() {
    return new MemoryAccountingBitsetCollector(tracker);
  }

  @Override
  public Result reduce(Collection<MemoryAccountingBitsetCollector> collectors) {
    int globalMaxDocEnd = 0;
    for (MemoryAccountingBitsetCollector collector : collectors) {
      globalMaxDocEnd = Math.max(globalMaxDocEnd, collector.getMaxDocEnd());
    }

    // TODO: with intra-segment concurrency enabled, globalMaxDocEnd equals the full index maxDoc
    // even when only a portion of the index was searched, causing over-allocation of the result
    // bitset.
    FixedBitSet result = new FixedBitSet(globalMaxDocEnd);
    tracker.updateBytes(result.ramBytesUsed());

    for (MemoryAccountingBitsetCollector collector : collectors) {
      if (collector.bitSet != null && collector.bitSet.length() > 0) {
        int length = collector.getMaxDocEnd() - collector.getMinDocBase();
        FixedBitSet.orRange(collector.bitSet, 0, result, collector.getMinDocBase(), length);
      }
    }

    return new Result(result, tracker.getBytes());
  }
}
