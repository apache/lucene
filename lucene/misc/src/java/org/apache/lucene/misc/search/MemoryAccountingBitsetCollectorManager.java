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
 * <p>Creates multiple collectors for concurrent execution; each collector only allocates a bitset
 * for the slices it processes, and {@link #reduce} merges them into a single {@link Result} sized
 * to the highest matched document across all collectors.
 */
public class MemoryAccountingBitsetCollectorManager
    implements CollectorManager<
        MemoryAccountingBitsetCollector, MemoryAccountingBitsetCollectorManager.Result> {

  /**
   * The result of a search, containing the matched document IDs and total memory used.
   *
   * <p>The returned {@link FixedBitSet} has length {@code highestMatchedDoc + 1} when any document
   * matched, or {@code 1} (an empty single-word bitset) otherwise, so that iteration via {@link
   * FixedBitSet#nextSetBit(int)} from index {@code 0} is always safe. It is not padded to the
   * searched index range; callers probing document ids beyond the last match should first check
   * {@link FixedBitSet#length()} before calling {@link FixedBitSet#get(int)}.
   */
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
    // Size the result to just cover the highest matched doc across all collectors. Each
    // collector's maxDocEnd is inflated by doSetNextReader to the full leaf regardless of what
    // actually matches, so keying off it can significantly over-allocate on selective queries or
    // narrow intra-segment slices; use the actual high-water mark tracked at collect time.
    int resultSize = 0;
    for (MemoryAccountingBitsetCollector collector : collectors) {
      int last = collector.getHighestSetBit();
      if (last >= 0) {
        resultSize = Math.max(resultSize, collector.getMinDocBase() + last + 1);
      }
    }

    // Allocate at least one bit even when no doc matched, so that the natural iteration idiom
    // (FixedBitSet#nextSetBit(0)) on the result remains safe rather than reading past an empty
    // long[] backing array.
    FixedBitSet result = new FixedBitSet(Math.max(1, resultSize));
    tracker.updateBytes(result.ramBytesUsed());

    for (MemoryAccountingBitsetCollector collector : collectors) {
      int last = collector.getHighestSetBit();
      if (last >= 0) {
        FixedBitSet.orRange(collector.bitSet, 0, result, collector.getMinDocBase(), last + 1);
      }
    }

    return new Result(result, tracker.getBytes());
  }
}
