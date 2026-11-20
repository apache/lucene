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

import java.lang.foreign.MemorySegment;
import java.util.Arrays;
import org.apache.lucene.util.ArrayUtil;

/**
 * Per-thread buffers reused by every search on the thread, so a query allocates little. Arrays only
 * grow; each search sizes what it uses.
 *
 * <p>A search holds its scratch from {@link #acquire} to {@link #release}. Searches on one thread
 * can nest: a filter's scorer, built or advanced mid-search, may itself run a SegmentIVF search (a
 * vector similarity query on any SegmentIVF field). The nested search must not share the outer
 * one's histogram, pool and dedup set, so while the thread's scratch is held it gets a fresh one.
 */
final class SearchScratch {
  private static final ThreadLocal<SearchScratch> LOCAL =
      ThreadLocal.withInitial(SearchScratch::new);

  /** Whether a search holds this scratch. */
  private boolean inUse;

  /** The thread's scratch, or a fresh one when a search on this thread already holds it. */
  static SearchScratch acquire() {
    SearchScratch scratch = LOCAL.get();
    if (scratch.inUse) {
      scratch = new SearchScratch();
    }
    scratch.inUse = true;
    return scratch;
  }

  /** Hands the scratch back for the thread's next search. */
  void release() {
    inUse = false;
    trim();
  }

  /**
   * The largest buffer, in bytes, kept for the thread's next search. A larger one, which only a
   * large scan grows (a wide nprobe, a widened filter, many deletions), is dropped on release:
   * thread-local buffers otherwise pin the largest search's memory on every thread that ever ran
   * one. 4 MB holds a rerank block of page-sized codes.
   */
  static final long MAX_RETAINED_BYTES = 4 << 20;

  private static final int MAX_INTS = (int) (MAX_RETAINED_BYTES / Integer.BYTES);
  private static final int MAX_LONGS = (int) (MAX_RETAINED_BYTES / Long.BYTES);

  /** Drops every buffer larger than {@link #MAX_RETAINED_BYTES}. */
  private void trim() {
    rowDistances = rowDistances.length > MAX_INTS ? new int[0] : rowDistances;
    pool = pool.length > MAX_LONGS ? new long[0] : pool;
    selected = selected.length > MAX_LONGS ? new long[0] : selected;
    codeOffsets = codeOffsets.length > MAX_LONGS ? new long[0] : codeOffsets;
    scores = scores.length > MAX_INTS ? new float[0] : scores;
    bytes = bytes.length > MAX_RETAINED_BYTES ? new byte[0] : bytes;
    readPositions = readPositions.length > MAX_LONGS ? new long[0] : readPositions;
    if (uringBuffer.byteSize() > MAX_RETAINED_BYTES) {
      uringBuffer = MemorySegment.NULL;
    }
  }

  /** Coarse distances are admitted this many at a time. */
  static final int ADMIT_BLOCK = 256;

  // ---- Hashing for the open-addressing tables below.
  //
  // Fibonacci hashing (Knuth, The Art of Computer Programming, vol. 3, section 6.4): multiply the
  // key by 2^w / phi, the golden ratio's fraction of the word, and keep the TOP bits as the slot.
  // The top bits of the product depend on every bit of the key, so keys that share low bits, such
  // as clustered or strided doc ids, still spread over the table. The low bits would not do: the
  // low k bits of a product with an odd constant depend only on the key's low k bits.

  /** 2^32 / phi, truncated (odd, as a multiplier must be): Fibonacci hashing for 32-bit keys. */
  static final int GOLDEN_RATIO_32 = 0x9E3779B9;

  /** 2^64 / phi, truncated (odd, as a multiplier must be): Fibonacci hashing for 64-bit keys. */
  static final long GOLDEN_RATIO_64 = 0x9E3779B97F4A7C15L;

  /**
   * The slot of {@code key} in a power-of-two table whose slots are {@code mask + 1}: the top
   * {@code log2(mask + 1)} bits of {@code key * GOLDEN_RATIO_32}.
   */
  static int hashSlot(int key, int mask) {
    int bits = Integer.bitCount(mask);
    // A shift by the full width is no shift in Java, so a one-slot table is special.
    return bits == 0 ? 0 : (key * GOLDEN_RATIO_32) >>> (Integer.SIZE - bits);
  }

  /** {@link #hashSlot(int, int)} for a 64-bit key, with {@link #GOLDEN_RATIO_64}. */
  static int hashSlot(long key, int mask) {
    int bits = Integer.bitCount(mask);
    return bits == 0 ? 0 : (int) ((key * GOLDEN_RATIO_64) >>> (Long.SIZE - bits));
  }

  // Cell selection.
  int[] graphCells = new int[0];
  int[] graphDistances = new int[0];
  float[] cellDistances = new float[0];

  // ---- The coarse scan and the admitted pool (CoarsePool).
  //
  // A scan scores every slot of the probed cells, often tens of thousands, but only the nearest few
  // hundred are reranked. Sorting every scored slot to find them would cost more than the scan, so
  // the pool uses the fact that coarse distances are small integers, in [0, CoarseCodec.bins):
  //
  //  1. Admit. Each slot within the current threshold is appended to the pool as
  //     (distance << 32 | slot), and histogram[distance] counts it. Rows are filtered against the
  //     threshold ADMIT_BLOCK at a time (Kernels.filterAtMost), the survivors' rows in
  //     admittedRows.
  //  2. Tighten. Once the buckets nearer than the farthest admitted distance already hold the
  //     pool's capacity, nothing at that distance can ever be selected, so the threshold drops
  //     below it. Later slots that far are rejected by one comparison, without being stored, so
  //     the pool usually stays a small multiple of the capacity however many slots are scanned.
  //     Not always: slots tied at the threshold keep being admitted until the nearer buckets alone
  //     hold the capacity, so with heavily tied distances nearly every scanned slot is stored.
  //     CoarsePool.reserve therefore sizes the pool for every slot a round scans.
  //  3. Select. To take the `need` nearest, a prefix sum over the histogram gives each distance's
  //     first position in the output (bucketStart), up to the distance at which `need` is reached.
  //     One pass over the pool then drops each entry into its distance's next position: a counting
  //     sort, linear in the pool, into `selected`. At the cut distance only as many entries as fill
  //     `need` are taken, in admission order.
  //
  // For example, with capacity 3, after admitting distances 5, 2, 9, 2, 4 the histogram holds
  // 2 -> 2, 4 -> 1, 5 -> 1, 9 -> 1. Buckets 2 and 4 already hold 3, so 5 and 9 can never be among
  // the 3 nearest: the threshold drops to 4, and a later slot at distance 6 is rejected unstored.
  // Selecting 3 then puts bucket 2 at positions 0-1 and bucket 4 at position 2.

  /** Slots admitted per distance; {@code bins} buckets, cleared by each search. */
  int[] histogram = new int[0];

  /** Each distance's next output position while {@link CoarsePool#selectNearest} sorts. */
  int[] bucketStart = new int[0];

  /** The coarse distances of one cell's slots, in slot order, as the scan scores it. */
  int[] rowDistances = new int[0];

  /** The rows of one admission block that are within the threshold. */
  int[] admittedRows = new int[ADMIT_BLOCK];

  /** The admitted entries, {@code distance << 32 | slot}, in admission order. */
  long[] pool = new long[0];

  /** The nearest entries, ordered by distance, that {@link CoarsePool#selectNearest} picks. */
  long[] selected = new long[0];

  // The deduplicated shortlist and its rerank.
  int[] shortlist = new int[0];
  int[] shortlistDistances = new int[0];
  long[] codeOffsets = new long[0];
  float[] scores = new float[0];
  byte[] bytes = new byte[0];

  // ---- Fine-code reads for the rerank (FineCodeIO).
  //
  // Why io_uring: a rerank needs a few hundred fine codes at scattered ordinals, and fine codes are
  // deliberately not kept in memory, so most of them come off the SSD. Read one at a time, through
  // the mapping (a page fault each) or with positional reads, every read waits for the previous
  // one, and the rerank costs the sum of a few hundred device round trips. io_uring submits the
  // whole batch in one system call, so the device works on all the reads at once and the batch
  // costs about one round trip. The reads are O_DIRECT where the layout allows, so fine codes do
  // not evict the postings and doc values that every query rereads from the page cache.

  /**
   * Where io_uring reads of fine codes land ({@link IoUring#buffer}). Off-heap and page-aligned,
   * since O_DIRECT reads need an aligned buffer. Allocating aligned native memory per query would
   * be costly, so each thread keeps one and grows it as a batch needs. A batch that fails drops it
   * (sets it back to {@code NULL}): reads still in flight may land in it later, so it must not be
   * handed to another batch.
   */
  MemorySegment uringBuffer = MemorySegment.NULL;

  /** The file positions of a batch of fine-code reads. */
  long[] readPositions = new long[0];

  // Open-addressing map from a page to its read in the batch, so codes sharing a page read it once;
  // stamped like the dedup set below.
  //
  // Used when FineCodeIO builds a batch of fine-code reads for the rerank. O_DIRECT reads whole
  // pages, and a fine code is smaller than a page, so several shortlisted codes can sit on one.
  // Each code's position is rounded down to its page: a page new to the batch gets the next read
  // index and is submitted; a page already in the batch hands back its read index, and the code is
  // found at its offset within that read's page. The batch thus holds one read per distinct page,
  // not one per code.
  private long[] pageKeys = new long[0];
  private int[] pageReads = new int[0];
  private int[] pageStamps = new int[0];
  private int pageStamp;
  private int pageMask;

  /**
   * A power-of-two table capacity holding {@code n} keys less than half full. Past 2^28 keys the
   * capacity would overflow an int, and a table that never grew would fill and never terminate.
   */
  private static int tableCapacity(int n) {
    if (n > 1 << 28) {
      throw new IllegalArgumentException("too many keys for one search: " + n);
    }
    return Integer.highestOneBit(Math.max(n, 16)) << 2;
  }

  /** Empties the page map, sizing it less than half full for {@code n} pages. */
  void clearPages(int n) {
    int capacity = tableCapacity(n);
    if (pageKeys.length < capacity) {
      pageKeys = new long[capacity];
      pageReads = new int[capacity];
      pageStamps = new int[capacity];
      pageMask = capacity - 1;
      pageStamp = 0;
    }
    pageStamp++;
    if (pageStamp == 0) {
      Arrays.fill(pageStamps, 0);
      pageStamp = 1;
    }
  }

  /** The read of {@code page}, which becomes read {@code next} if the page is new. */
  int pageRead(long page, int next) {
    // Pages are page-aligned positions; hash the page number, not its always-zero low bits.
    int slot = hashSlot(page / IoUring.PAGE_BYTES, pageMask);
    while (pageStamps[slot] == pageStamp) {
      if (pageKeys[slot] == page) {
        return pageReads[slot];
      }
      slot = (slot + 1) & pageMask;
    }
    pageStamps[slot] = pageStamp;
    pageKeys[slot] = page;
    pageReads[slot] = next;
    return next;
  }

  // Open-addressing set of documents: a key is present when its stamp is the current one, so
  // clearing the set is one increment.
  //
  // Spilling stores a vector in more than one cell, so a search that probes both cells sees the
  // same document twice. The set is used twice per search, cleared before each:
  //
  //  1. Counting during a filtered scan (SegmentSearch.admitMatch). Rounds of probing stop once
  //     enough distinct documents have matched; counting slots instead would let spilled copies
  //     stop the search early.
  //  2. Building the shortlist (SegmentSearch.shortlistPool). The selected slots come nearest
  //     first, so keeping only a document's first slot keeps its nearest copy, and the shortlist
  //     neither reranks a document twice nor loses a place to a duplicate.
  private int[] dedupKeys = new int[0];
  private int[] dedupStamps = new int[0];
  private int dedupStamp;
  private int dedupMask;

  /** Sizes the shortlist and a dedup table less than half full for {@code n} documents. */
  void reserve(int n) {
    shortlist = ArrayUtil.growNoCopy(shortlist, n);
    shortlistDistances = ArrayUtil.growNoCopy(shortlistDistances, n);
    int capacity = tableCapacity(n);
    if (dedupKeys.length < capacity) {
      dedupKeys = new int[capacity];
      dedupStamps = new int[capacity];
      dedupMask = capacity - 1;
      dedupStamp = 0;
    }
  }

  /** Empties the dedup set. */
  void clearDedup() {
    dedupStamp++;
    if (dedupStamp == 0) {
      Arrays.fill(dedupStamps, 0);
      dedupStamp = 1;
    }
  }

  /** Adds {@code doc} to the dedup set, returning false if it was already there. */
  boolean addDistinct(int doc) {
    int slot = hashSlot(doc, dedupMask);
    while (dedupStamps[slot] == dedupStamp) {
      if (dedupKeys[slot] == doc) {
        return false;
      }
      slot = (slot + 1) & dedupMask;
    }
    dedupStamps[slot] = dedupStamp;
    dedupKeys[slot] = doc;
    return true;
  }
}
