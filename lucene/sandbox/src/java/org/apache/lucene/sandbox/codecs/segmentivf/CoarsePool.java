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

package org.apache.lucene.sandbox.codecs.segmentivf;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.util.ArrayUtil;

/**
 * The slots a coarse scan admits, kept small without sorting: a histogram of admitted distances
 * tells the scan which distance it can stop admitting at.
 *
 * <p>Coarse distances are small integers in {@code [0, bins)}. Each admitted slot is recorded as
 * {@code distance << 32 | slot} and counted in its distance's histogram bucket. Once the nearer
 * buckets alone hold {@code capacity} slots, the farthest bucket can never be selected, so the
 * admission threshold drops below it ({@link #tighten}) and later slots that far are rejected by
 * one comparison. {@link #selectNearest} then picks the nearest slots with a counting sort over the
 * histogram.
 *
 * <p>An entry packs both of its values into one {@code long}, the distance in the high 32 bits and
 * the slot in the low 32:
 *
 * <pre>
 *   bits 63..32   bits 31..0
 *   [ distance ]  [   slot   ]
 *
 *   entry    = (long) distance &lt;&lt; 32 | slot
 *   distance = (int) (entry &gt;&gt;&gt; 32)
 *   slot     = (int) entry
 * </pre>
 *
 * <p>So the pool is one primitive array: an admit is one store, and an entry moves into {@link
 * SearchScratch#selected} as one value. With the distance on top, entries compare as numbers by
 * distance, then slot, the layout {@link SegmentSearch} also sorts its packed cell ranks in; the
 * pool itself never sorts by value, since {@link #selectNearest} sorts by distance alone. A slot
 * must be non-negative: a negative one would sign-extend over the distance when widened for the
 * {@code |}.
 */
final class CoarsePool {
  private final int bins;
  private final int capacity;
  private final SearchScratch scratch;
  private final int[] histogram;

  /** Admitted entries, packed as the class describes, in admission order. */
  private long[] entries;

  private int size;

  /**
   * Admitted slots still counted against the capacity: those at or below the threshold. Entries
   * recorded by {@link #add} alone are not counted.
   */
  private int admitted;

  /** The farthest distance still admitted. */
  private int threshold;

  CoarsePool(SearchScratch scratch, int bins, int capacity) {
    this.bins = bins;
    this.capacity = capacity;
    this.scratch = scratch;
    histogram = scratch.histogram = ArrayUtil.growNoCopy(scratch.histogram, bins);
    Arrays.fill(histogram, 0, bins, 0);
    entries = scratch.pool;
    threshold = bins - 1;
  }

  /** How many slots were admitted, and counted against the capacity. */
  int admitted() {
    return admitted;
  }

  /** The farthest distance still admitted. */
  int threshold() {
    return threshold;
  }

  /** Forgets every entry and restores the threshold, as a new pool of the same capacity. */
  void clear() {
    Arrays.fill(histogram, 0, bins, 0);
    size = 0;
    admitted = 0;
    threshold = bins - 1;
  }

  /** Makes room for {@code more} entries. */
  void reserve(int more) {
    entries = scratch.pool = ArrayUtil.grow(entries, size + more);
  }

  /**
   * Records a slot without counting it against the capacity or tightening. Only for a pool that
   * takes every slot and is never admitted into: mixed with {@link #admit}, the threshold would
   * ignore these entries.
   */
  void add(int distance, int slot) {
    histogram[distance]++;
    entries[size++] = ((long) distance << 32) | slot;
  }

  /** Which slots may be admitted. */
  interface SlotFilter {
    boolean accepts(int slot) throws IOException;
  }

  /**
   * Admits the rows among {@code count} distances from {@code from} that are within the threshold
   * and that {@code accept} accepts (all when it is null), row {@code r} being slot {@code
   * firstSlot + r}, then tightens the threshold. Only rows within the threshold are offered to
   * {@code accept}, so once the threshold tightens it sees few of the rows scanned.
   */
  void admitBlock(int[] distances, int from, int count, int firstSlot, SlotFilter accept)
      throws IOException {
    int[] kept = scratch.admittedRows;
    int n = Kernels.INSTANCE.filterAtMost(distances, from, count, threshold, kept);
    for (int i = 0; i < n; i++) {
      int row = from + kept[i];
      int slot = firstSlot + row;
      if (accept == null || accept.accepts(slot)) {
        add(distances[row], slot);
        // Counted as each is added, so the count is right even if accept throws mid-block.
        admitted++;
      }
    }
    tighten();
  }

  /** Admits one slot if within the threshold, tightening once the pool is over capacity. */
  void admit(int distance, int slot) {
    if (distance > threshold) {
      return;
    }
    add(distance, slot);
    admitted++;
    if (admitted > capacity) {
      tighten();
    }
  }

  /** Lowers the threshold past every far bucket that the nearer buckets make unselectable. */
  private void tighten() {
    while (threshold > 0 && admitted - histogram[threshold] >= capacity) {
      admitted -= histogram[threshold];
      threshold--;
    }
  }

  /**
   * Selects the {@code need} nearest entries, or all of them if fewer, into {@link
   * SearchScratch#selected} ordered by distance, ties in admission order, and returns how many.
   *
   * <p>A counting sort: a prefix sum over the histogram gives each distance's first output
   * position, up to the cut distance at which {@code need} is reached; at the cut only as many
   * entries as fill {@code need} are taken.
   *
   * <p>{@code need} must not exceed the capacity. Tightening keeps every slot among the {@code
   * capacity} nearest scanned, but may already have rejected slots nearer than some entries beyond
   * that, so asking for more would silently return entries that are not the nearest.
   */
  int selectNearest(int need) {
    assert need <= capacity : "need " + need + " exceeds the capacity " + capacity;
    int[] next = scratch.bucketStart = ArrayUtil.growNoCopy(scratch.bucketStart, bins + 1);
    int cut = 0;
    next[0] = 0;
    while (cut < bins && next[cut] + histogram[cut] < need) {
      next[cut + 1] = next[cut] + histogram[cut];
      cut++;
    }
    int ties = need - next[cut];
    long[] selected = scratch.selected = ArrayUtil.growNoCopy(scratch.selected, need);
    for (int i = 0; i < size; i++) {
      int distance = (int) (entries[i] >>> 32);
      boolean keep = distance < cut || (distance == cut && ties > 0);
      if (keep) {
        if (distance == cut) {
          ties--;
        }
        selected[next[distance]] = entries[i];
        next[distance]++;
      }
    }
    return next[cut];
  }
}
