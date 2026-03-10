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
 * segments it processes, then merges with proper offset in reduce().
 */
public class MemoryAccountingBitsetCollectorManager
    implements CollectorManager<MemoryAccountingBitsetCollector, FixedBitSet> {

  private final CollectorMemoryTracker tracker;
  private long totalBytesUsed;

  public MemoryAccountingBitsetCollectorManager(CollectorMemoryTracker tracker) {
    this.tracker = tracker;
  }

  @Override
  public MemoryAccountingBitsetCollector newCollector() {
    return new MemoryAccountingBitsetCollector(
        new CollectorMemoryTracker(tracker.getName() + "-collector", tracker.getMemoryLimit()),
        true);
  }

  @Override
  public FixedBitSet reduce(Collection<MemoryAccountingBitsetCollector> collectors) {
    if (collectors.isEmpty()) {
      return new FixedBitSet(0);
    }

    if (collectors.size() == 1) {
      MemoryAccountingBitsetCollector collector = collectors.iterator().next();
      this.totalBytesUsed = collector.tracker.getBytes();

      if (collector.getMinDocBase() == 0) {
        return collector.bitSet;
      }

      FixedBitSet result = new FixedBitSet(collector.getMaxDocEnd());
      int length = collector.getMaxDocEnd() - collector.getMinDocBase();
      FixedBitSet.orRange(collector.bitSet, 0, result, collector.getMinDocBase(), length);
      return result;
    }

    // Find global doc range across all collectors
    int globalMinDocBase = Integer.MAX_VALUE;
    int globalMaxDocEnd = 0;
    for (MemoryAccountingBitsetCollector collector : collectors) {
      globalMinDocBase = Math.min(globalMinDocBase, collector.getMinDocBase());
      globalMaxDocEnd = Math.max(globalMaxDocEnd, collector.getMaxDocEnd());
      long collectorBytes = collector.tracker.getBytes();
      this.totalBytesUsed += collectorBytes;
    }

    FixedBitSet result = new FixedBitSet(globalMaxDocEnd);

    for (MemoryAccountingBitsetCollector collector : collectors) {
      if (collector.bitSet != null && collector.bitSet.length() > 0) {
        int length = collector.getMaxDocEnd() - collector.getMinDocBase();
        FixedBitSet.orRange(collector.bitSet, 0, result, collector.getMinDocBase(), length);
      }
    }

    return result;
  }

  public long getBytes() {
    return this.totalBytesUsed;
  }
}
