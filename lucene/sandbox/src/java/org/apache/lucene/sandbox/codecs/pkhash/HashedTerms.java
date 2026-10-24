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
package org.apache.lucene.sandbox.codecs.pkhash;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.apache.lucene.util.IntroSorter;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * An immutable set of terms to look up, called keys, sorted in unsigned byte order and
 * de-duplicated, together with their hashes under the hash function of {@link
 * PrimaryKeyHashPostingsFormat}.
 *
 * <p>The hash function and its seed are the same for every segment, so a set of keys built once can
 * be looked up in any number of segments with {@link HashTerms#lookupBatch} without hashing its
 * keys again. What the filters of the segments need from the hash of a key is computed once too. A
 * set takes 28 bytes per key on top of the keys' bytes. Instances are safe to share between
 * threads.
 *
 * @lucene.experimental
 */
public final class HashedTerms implements Accountable {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(HashedTerms.class);

  private final byte[] bytes;
  // offsets[i] is where key i starts in bytes, and offsets[size] == bytes.length
  private final int[] offsets;
  private final long[] hashes;

  /**
   * What the filter of any segment needs of every key: the {@link TermHash#mix} of its hash, which
   * gives its word together with the field's number of partitions, and the {@link
   * BlockedBloomFilter#mask} of the mix, its bits in that word.
   */
  private final long[] mixes, filterMasks;

  /**
   * Computed on first use, as the query cache needs it, and 0 until then. Threads that race to
   * compute it compute the same value, as for {@link String#hashCode()}.
   */
  private int hashCode;

  /** Creates a set of the given keys. The keys are copied; duplicates are removed. */
  public HashedTerms(Collection<BytesRef> keys) {
    this(keys.toArray(new BytesRef[0]));
  }

  /**
   * Creates a set of the given keys. The keys are copied; duplicates are removed. The array is not
   * modified.
   */
  public HashedTerms(BytesRef... keys) {
    final long[] prefixes = new long[keys.length];
    for (int i = 0; i < keys.length; i++) {
      BytesRef key = Objects.requireNonNull(keys[i], "keys must not contain null");
      prefixes[i] = HashTerms.prefix(key.bytes, key.offset, key.length);
    }
    final int[] order = new KeySorter(keys, prefixes).sort();

    // order[0..size) becomes the distinct keys
    int size = 0;
    long totalBytes = 0;
    for (int i = 0; i < order.length; i++) {
      BytesRef key = keys[order[i]];
      if (size == 0 || key.bytesEquals(keys[order[size - 1]]) == false) {
        order[size++] = order[i];
        totalBytes += key.length;
      }
    }
    if (totalBytes > ArrayUtil.MAX_ARRAY_LENGTH) {
      throw new IllegalArgumentException(
          "keys hold " + totalBytes + " bytes, more than an array can: split them");
    }

    bytes = new byte[(int) totalBytes];
    offsets = new int[size + 1];
    hashes = new long[size];
    mixes = new long[size];
    filterMasks = new long[size];
    int upto = 0;
    for (int i = 0; i < size; i++) {
      BytesRef key = keys[order[i]];
      final long hash = TermHash.hash(key.bytes, key.offset, key.length);
      final long mixed = TermHash.mix(hash);
      hashes[i] = hash;
      mixes[i] = mixed;
      filterMasks[i] = BlockedBloomFilter.mask(mixed);
      offsets[i] = upto;
      System.arraycopy(key.bytes, key.offset, bytes, upto, key.length);
      upto += key.length;
    }
    offsets[size] = upto;
  }

  /**
   * Sorts keys in unsigned byte order, the order of every terms dictionary, by their {@link
   * HashTerms#prefix prefixes}: their first 8 bytes as unsigned longs. Keys are only compared in
   * full when their prefixes are equal. The keys do not move: the sort permutes their indices along
   * with their prefixes. It is used instead of {@link org.apache.lucene.util.StringSorter} because
   * most keys are told apart by these cached prefixes alone.
   *
   * <p>The sort is an MSB radix sort on the bytes of the prefixes that skips the bytes that all
   * keys of a range share, down to ranges of a few keys, which are sorted by insertion. Keys whose
   * prefixes are all equal, as URLs of one host often are, are sorted again by the 8 bytes after
   * all the bytes they share, at most {@link #MAX_LEVELS} times so as not to recurse deeply, and
   * otherwise by comparison. Keys that are already in order, or in reverse order, as from a sorted
   * set, are only checked, or reversed, in one pass.
   */
  private static final class KeySorter {

    /** Ranges of at most this many keys are sorted by insertion. */
    private static final int INSERTION_SORT_THRESHOLD = 32;

    /** How many times keys with equal prefixes are sorted by their next prefixes, at most. */
    private static final int MAX_LEVELS = 8;

    private final BytesRef[] keys;

    /** The index in keys of the key at every position. */
    private final int[] order;

    /**
     * The prefix of the key at every position, from the offset up to which all keys of its range
     * are the same.
     */
    private final long[] prefixes;

    // for the radix sort, allocated on first use
    private int[] counts;
    private int[] scratchOrder;
    private long[] scratchPrefixes;

    KeySorter(BytesRef[] keys, long[] prefixes) {
      this.keys = keys;
      this.prefixes = prefixes;
      this.order = new int[keys.length];
      for (int i = 0; i < order.length; i++) {
        order[i] = i;
      }
    }

    /** Returns the indices of the keys in sorted order, equal keys next to each other. */
    int[] sort() {
      final int n = order.length;
      if (inOrder(false) == false) {
        if (inOrder(true)) {
          for (int i = 0; i < n; i++) {
            order[i] = n - 1 - i;
          }
        } else {
          sort(0, n, 0, 0);
        }
      }
      return order;
    }

    /** Whether keys are in order, or in reverse order, equal keys included. */
    private boolean inOrder(boolean reverse) {
      for (int i = 1; i < order.length; i++) {
        int cmp = compare(prefixes[i - 1], i - 1, prefixes[i], i, 0);
        if (reverse ? cmp < 0 : cmp > 0) {
          return false;
        }
      }
      return true;
    }

    /**
     * Sorts the keys at positions [from, to), whose first {@code offset} bytes are the same and
     * whose prefixes are from there.
     */
    private void sort(int from, int to, int offset, int level) {
      if (to - from <= INSERTION_SORT_THRESHOLD) {
        insertionSort(from, to, offset);
        return;
      }
      final long first = prefixes[from];
      long diff = 0;
      for (int i = from + 1; i < to; i++) {
        diff |= prefixes[i] ^ first;
      }
      if (diff == 0) {
        sortEqualPrefixes(from, to, offset, level);
      } else {
        // the first byte of the prefixes where they differ
        final int shift = (Long.SIZE - 1 - Long.numberOfLeadingZeros(diff)) & ~(Byte.SIZE - 1);
        radixSort(from, to, offset, level, shift);
      }
    }

    /** Sorts the keys of a range by the byte of their prefixes at {@code shift}, then on. */
    private void radixSort(int from, int to, int offset, int level, int shift) {
      if (counts == null) {
        counts = new int[1 << Byte.SIZE];
        scratchOrder = new int[order.length];
        scratchPrefixes = new long[prefixes.length];
      }
      final int[] counts = this.counts;
      Arrays.fill(counts, 0);
      for (int i = from; i < to; i++) {
        counts[digit(prefixes[i], shift)]++;
      }
      int start = from;
      for (int b = 0; b < counts.length; b++) {
        int count = counts[b];
        counts[b] = start;
        start += count;
      }
      for (int i = from; i < to; i++) {
        long prefix = prefixes[i];
        int position = counts[digit(prefix, shift)]++;
        scratchPrefixes[position] = prefix;
        scratchOrder[position] = order[i];
      }
      System.arraycopy(scratchPrefixes, from, prefixes, from, to - from);
      System.arraycopy(scratchOrder, from, order, from, to - from);

      // sorting a bucket reuses the counts, so buckets are told apart by their bytes again
      for (int bucketStart = from; bucketStart < to; ) {
        final int digit = digit(prefixes[bucketStart], shift);
        int bucketEnd = bucketStart + 1;
        while (bucketEnd < to && digit(prefixes[bucketEnd], shift) == digit) {
          bucketEnd++;
        }
        if (bucketEnd - bucketStart > 1) {
          sort(bucketStart, bucketEnd, offset, level);
        }
        bucketStart = bucketEnd;
      }
    }

    private static int digit(long prefix, int shift) {
      return ((int) (prefix >>> shift)) & 0xFF;
    }

    /**
     * Sorts the keys of a range whose prefixes are all equal. Unless they are all the same key,
     * they are sorted by their prefixes after the bytes they all share, which are at least 8 more
     * than before if none of them ends within its prefix. Otherwise, or after {@link #MAX_LEVELS}
     * levels, they are sorted by comparison.
     */
    private void sortEqualPrefixes(int from, int to, int offset, int level) {
      final BytesRef first = keys[order[from]];
      int common = first.length;
      int maxLength = first.length;
      for (int i = from + 1; i < to; i++) {
        BytesRef key = keys[order[i]];
        int mismatch =
            Arrays.mismatch(
                first.bytes,
                first.offset + offset,
                first.offset + common,
                key.bytes,
                key.offset + offset,
                key.offset + Math.min(key.length, common));
        if (mismatch >= 0) {
          common = offset + mismatch;
        }
        maxLength = Math.max(maxLength, key.length);
      }
      if (common == maxLength) {
        return; // copies of one key
      }
      if (common >= offset + Long.BYTES && level < MAX_LEVELS) {
        for (int i = from; i < to; i++) {
          BytesRef key = keys[order[i]];
          prefixes[i] = HashTerms.prefix(key.bytes, key.offset + common, key.length - common);
        }
        sort(from, to, common, level + 1);
      } else {
        comparisonSort(from, to, common);
      }
    }

    private void insertionSort(int from, int to, int offset) {
      for (int i = from + 1; i < to; i++) {
        final long prefix = prefixes[i];
        final int index = order[i];
        int j = i;
        for (; j > from && compare(prefixes[j - 1], order[j - 1], prefix, index, offset) > 0; j--) {
          prefixes[j] = prefixes[j - 1];
          order[j] = order[j - 1];
        }
        prefixes[j] = prefix;
        order[j] = index;
      }
    }

    /** Sorts a range by comparing whole keys; their prefixes are no longer needed. */
    private void comparisonSort(int from, int to, int offset) {
      new IntroSorter() {
        private BytesRef pivot;

        @Override
        protected void setPivot(int i) {
          pivot = keys[order[i]];
        }

        @Override
        protected int comparePivot(int j) {
          return compareFrom(pivot, keys[order[j]], offset);
        }

        @Override
        protected int compare(int i, int j) {
          return compareFrom(keys[order[i]], keys[order[j]], offset);
        }

        @Override
        protected void swap(int i, int j) {
          int tmp = order[i];
          order[i] = order[j];
          order[j] = tmp;
        }
      }.sort(from, to);
    }

    /** Compares two keys equal up to {@code offset}, given their prefixes from there. */
    private int compare(long prefix1, int index1, long prefix2, int index2, int offset) {
      if (prefix1 != prefix2) {
        return Long.compareUnsigned(prefix1, prefix2);
      }
      return compareFrom(keys[index1], keys[index2], offset);
    }

    private static int compareFrom(BytesRef a, BytesRef b, int offset) {
      return Arrays.compareUnsigned(
          a.bytes,
          a.offset + offset,
          a.offset + a.length,
          b.bytes,
          b.offset + offset,
          b.offset + b.length);
    }
  }

  /** The number of distinct keys. */
  public int size() {
    return hashes.length;
  }

  /**
   * Returns the key at {@code index}, in sorted order. The returned {@link BytesRef} shares this
   * set's bytes and must not be modified.
   */
  public BytesRef get(int index) {
    Objects.checkIndex(index, size());
    return new BytesRef(bytes, offsets[index], offsets[index + 1] - offsets[index]);
  }

  /**
   * Returns an iterator over the keys in sorted order that allocates nothing per key: the {@link
   * BytesRef} it returns is the same one every time, shares this set's bytes, and must not be
   * modified.
   */
  public BytesRefIterator iterator() {
    return new BytesRefIterator() {
      private final BytesRef scratch = new BytesRef(bytes, 0, 0);
      private int upto;

      @Override
      public BytesRef next() {
        if (upto == size()) {
          return null;
        }
        scratch.offset = offsets[upto];
        scratch.length = offsets[upto + 1] - offsets[upto];
        upto++;
        return scratch;
      }
    };
  }

  byte[] bytes() {
    return bytes;
  }

  int offset(int index) {
    return offsets[index];
  }

  int length(int index) {
    return offsets[index + 1] - offsets[index];
  }

  long hash(int index) {
    return hashes[index];
  }

  /** The hash of every key, by index. Not to be modified. */
  long[] hashes() {
    return hashes;
  }

  /** The {@link TermHash#mix} of the hash of every key, by index. Not to be modified. */
  long[] mixes() {
    return mixes;
  }

  /**
   * The {@link BlockedBloomFilter#mask} of every key in the filter of any segment, by index. Not to
   * be modified.
   */
  long[] filterMasks() {
    return filterMasks;
  }

  int tag(int index) {
    return TermHash.tag(hashes[index]);
  }

  int control(int index) {
    return TermHash.control(hashes[index]);
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED
        + RamUsageEstimator.sizeOf(bytes)
        + RamUsageEstimator.sizeOf(offsets)
        + RamUsageEstimator.sizeOf(hashes)
        + RamUsageEstimator.sizeOf(mixes)
        + RamUsageEstimator.sizeOf(filterMasks);
  }

  @Override
  public boolean equals(Object obj) {
    // sorted and de-duplicated, so the packed form is canonical
    return obj instanceof HashedTerms other
        && Arrays.equals(offsets, other.offsets)
        && Arrays.equals(bytes, other.bytes);
  }

  @Override
  public int hashCode() {
    int h = hashCode;
    if (h == 0) {
      // equal sets have the same keys in the same order, and so the same hashes
      h = Arrays.hashCode(hashes);
      hashCode = h;
    }
    return h;
  }

  @Override
  public String toString() {
    return "HashedTerms(size=" + size() + ")";
  }
}
