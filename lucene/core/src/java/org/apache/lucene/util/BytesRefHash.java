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
package org.apache.lucene.util;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.util.ByteBlockPool.DirectAllocator;

/**
 * {@link BytesRefHash} is a special purpose hash-map like data-structure optimized for {@link
 * BytesRef} instances. BytesRefHash maintains mappings of byte arrays to ids
 * (Map&lt;BytesRef,int&gt;) storing the hashed bytes efficiently in continuous storage. The mapping
 * to the id is encapsulated inside {@link BytesRefHash} and is guaranteed to be increased for each
 * added {@link BytesRef}.
 *
 * <p>Note: The maximum capacity {@link BytesRef} instance passed to {@link #add(BytesRef)} must not
 * be longer than {@link ByteBlockPool#BYTE_BLOCK_SIZE}-2. The internal storage is limited to 2GB
 * total byte storage.
 *
 * @lucene.internal
 */
public final class BytesRefHash implements Accountable {
  private static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(BytesRefHash.class)
          +
          // size of Counter
          RamUsageEstimator.primitiveSizes.get(long.class);

  public static final int DEFAULT_CAPACITY = 16;

  // the following fields are needed by comparator,
  // so package private to prevent access$-methods:
  final BytesRefBlockPool pool;
  int[] bytesStart;

  private int hashSize;
  private int hashHalfSize;
  private int hashMask;
  // This mask is used to extract the high bits from a hashcode
  private int highMask;
  private int count;
  private int lastCount = -1;

  /**
   * The <code>ids</code> array serves a dual purpose:
   *
   * <ol>
   *   <li>When the value is <code>-1</code>, it indicates an empty slot in the hash table.
   *   <li>When the value is not <code>-1</code>, it stores:
   *       <ul>
   *         <li>The actual index into the <code>bytesStart</code> array (low bits, masked by <code>
   *             hashMask</code>).
   *         <li>The high bits of the original hashcode (high bits, masked by <code>highMask</code>
   *             ).
   *       </ul>
   * </ol>
   *
   * <p>This "trick" allows us to store both the index and part of the hashcode in a single int,
   * which speeds up hash collisions by quickly rejecting non-matching entries without having to
   * compare the actual byte values. During lookups, we can immediately check if the high bits match
   * before doing the more expensive byte comparison.
   *
   * <p><b>Example:</b>
   *
   * <ul>
   *   <li>hashSize = 16, therefore <code>hashMask = 15</code> (<code>0x0000000F</code>)
   *   <li><code>highMask = ~hashMask = 0xFFFFFFF0</code>
   * </ul>
   *
   * <p>When storing the value 7 with hashcode <code>0x12345678</code>:
   *
   * <ul>
   *   <li>The low bits (index) are 7 (<code>0x00000007</code>)
   *   <li>The high bits of hashcode are <code>0x12345670</code>
   *   <li>The stored value becomes: <code>0x12345677</code>
   * </ul>
   *
   * <p><b>During lookup:</b>
   *
   * <ol>
   *   <li>We compute the hashcode and find the slot.
   *   <li>We extract the stored value's high bits (<code>& highMask</code>).
   *   <li>If they match the lookup hashcode's high bits, we proceed to comparing actual bytes.
   *   <li>Otherwise, we immediately know it's not a match and continue probing.
   * </ol>
   *
   * <p>This significantly improves performance for hash lookups, especially with many collisions.
   */
  private int[] ids;

  private final BytesStartArray bytesStartArray;
  private final Counter bytesUsed;

  /**
   * Creates a new {@link BytesRefHash} with a {@link ByteBlockPool} using a {@link
   * DirectAllocator}.
   */
  public BytesRefHash() {
    this(new ByteBlockPool(new DirectAllocator()));
  }

  /** Creates a new {@link BytesRefHash} */
  public BytesRefHash(ByteBlockPool pool) {
    this(pool, DEFAULT_CAPACITY, new DirectBytesStartArray(DEFAULT_CAPACITY));
  }

  /** Creates a new {@link BytesRefHash} */
  public BytesRefHash(ByteBlockPool pool, int capacity, BytesStartArray bytesStartArray) {
    if (capacity <= 0) {
      throw new IllegalArgumentException("capacity must be greater than 0");
    }

    if (BitUtil.isZeroOrPowerOfTwo(capacity) == false) {
      throw new IllegalArgumentException("capacity must be a power of two, got " + capacity);
    }
    hashSize = capacity;
    hashHalfSize = hashSize >> 1;
    hashMask = hashSize - 1;
    highMask = ~hashMask;
    this.pool = new BytesRefBlockPool(pool);
    ids = new int[hashSize];
    Arrays.fill(ids, -1);
    this.bytesStartArray = bytesStartArray;
    bytesStart = bytesStartArray.init();
    final Counter bytesUsed = bytesStartArray.bytesUsed();
    this.bytesUsed = bytesUsed == null ? Counter.newCounter() : bytesUsed;
    bytesUsed.addAndGet(hashSize * (long) Integer.BYTES);
  }

  /**
   * Returns the number of {@link BytesRef} values in this {@link BytesRefHash}.
   *
   * @return the number of {@link BytesRef} values in this {@link BytesRefHash}.
   */
  public int size() {
    return count;
  }

  /**
   * Populates and returns a {@link BytesRef} with the bytes for the given bytesID.
   *
   * <p>Note: the given bytesID must be a positive integer less than the current size ({@link
   * #size()})
   *
   * @param bytesID the id
   * @param ref the {@link BytesRef} to populate
   * @return the given BytesRef instance populated with the bytes for the given bytesID
   */
  public BytesRef get(int bytesID, BytesRef ref) {
    assert bytesStart != null : "bytesStart is null - not initialized";
    assert bytesID < bytesStart.length : "bytesID exceeds byteStart len: " + bytesStart.length;
    pool.fillBytesRef(ref, bytesStart[bytesID]);
    return ref;
  }

  /**
   * Returns the ids array in arbitrary order. Valid ids start at offset of 0 and end at a limit of
   * {@link #size()} - 1
   *
   * <p>Note: This is a destructive operation. {@link #clear()} must be called in order to reuse
   * this {@link BytesRefHash} instance.
   *
   * @lucene.internal
   */
  public int[] compact() {
    assert bytesStart != null : "bytesStart is null - not initialized";
    int upto = 0;
    for (int i = 0; i < hashSize; i++) {
      if (ids[i] != -1) {
        ids[upto] = ids[i] & hashMask;
        if (upto < i) {
          ids[i] = -1;
        }
        upto++;
      }
    }

    assert upto == count;
    lastCount = count;
    return ids;
  }

  /**
   * Returns the values array sorted by the referenced byte values.
   *
   * <p>Note: This is a destructive operation. {@link #clear()} must be called in order to reuse
   * this {@link BytesRefHash} instance.
   */
  public int[] sort() {
    final int[] compact = compact();
    assert count * 2 <= compact.length : "We need load factor <= 0.5f to speed up this sort";
    final int tmpOffset = count;
    new StringSorter(BytesRefComparator.NATURAL) {

      @Override
      protected Sorter radixSorter(BytesRefComparator cmp) {
        return new MSBStringRadixSorter(cmp) {

          private int k;

          @Override
          protected void buildHistogram(
              int prefixCommonBucket,
              int prefixCommonLen,
              int from,
              int to,
              int k,
              int[] histogram) {
            this.k = k;
            histogram[prefixCommonBucket] = prefixCommonLen;
            Arrays.fill(
                compact, tmpOffset + from - prefixCommonLen, tmpOffset + from, prefixCommonBucket);
            for (int i = from; i < to; ++i) {
              int b = getBucket(i, k);
              compact[tmpOffset + i] = b;
              histogram[b]++;
            }
          }

          @Override
          protected boolean shouldFallback(int from, int to, int l) {
            // We lower the fallback threshold because the bucket cache speeds up the reorder
            return to - from <= LENGTH_THRESHOLD / 2 || l >= LEVEL_THRESHOLD;
          }

          private void swapBucketCache(int i, int j) {
            swap(i, j);
            int tmp = compact[tmpOffset + i];
            compact[tmpOffset + i] = compact[tmpOffset + j];
            compact[tmpOffset + j] = tmp;
          }

          @Override
          protected void reorder(int from, int to, int[] startOffsets, int[] endOffsets, int k) {
            assert this.k == k;
            for (int i = 0; i < HISTOGRAM_SIZE; ++i) {
              final int limit = endOffsets[i];
              for (int h1 = startOffsets[i]; h1 < limit; h1 = startOffsets[i]) {
                final int b = compact[tmpOffset + from + h1];
                final int h2 = startOffsets[b]++;
                swapBucketCache(from + h1, from + h2);
              }
            }
          }
        };
      }

      @Override
      protected void swap(int i, int j) {
        int tmp = compact[i];
        compact[i] = compact[j];
        compact[j] = tmp;
      }

      @Override
      protected void get(BytesRefBuilder builder, BytesRef result, int i) {
        pool.fillBytesRef(result, bytesStart[compact[i]]);
      }
    }.sort(0, count);
    Arrays.fill(compact, tmpOffset, compact.length, -1);
    return compact;
  }

  private boolean shrink(int targetSize) {
    // Cannot use ArrayUtil.shrink because we require power
    // of 2:
    int newSize = hashSize;
    while (newSize >= 8 && newSize / 4 > targetSize) {
      newSize /= 2;
    }
    if (newSize != hashSize) {
      bytesUsed.addAndGet(Integer.BYTES * (long) -(hashSize - newSize));
      hashSize = newSize;
      ids = new int[hashSize];
      Arrays.fill(ids, -1);
      hashHalfSize = newSize / 2;
      hashMask = newSize - 1;
      highMask = ~hashMask;
      return true;
    } else {
      return false;
    }
  }

  /** Clears the {@link BytesRef} which maps to the given {@link BytesRef} */
  public void clear(boolean resetPool) {
    lastCount = count;
    count = 0;
    if (resetPool) {
      pool.reset();
    }
    bytesStart = bytesStartArray.clear();
    if (lastCount != -1 && shrink(lastCount)) {
      // shrink clears the hash entries
      return;
    }
    Arrays.fill(ids, -1);
  }

  public void clear() {
    clear(true);
  }

  /** Closes the BytesRefHash and releases all internally used memory */
  public void close() {
    clear(true);
    ids = null;
    bytesUsed.addAndGet(Integer.BYTES * (long) -hashSize);
  }

  /**
   * Adds a new {@link BytesRef}
   *
   * @param bytes the bytes to hash
   * @return the id the given bytes are hashed if there was no mapping for the given bytes,
   *     otherwise <code>(-(id)-1)</code>. This guarantees that the return value will always be
   *     &gt;= 0 if the given bytes haven't been hashed before.
   * @throws MaxBytesLengthExceededException if the given bytes are {@code > 2 +} {@link
   *     ByteBlockPool#BYTE_BLOCK_SIZE}
   */
  public int add(BytesRef bytes) {
    assert bytesStart != null : "Bytesstart is null - not initialized";
    final int hashcode = doHash(bytes.bytes, bytes.offset, bytes.length);
    // final position
    final int hashPos = findHash(bytes, hashcode);
    int e = ids[hashPos];

    if (e == -1) {
      // new entry
      if (count >= bytesStart.length) {
        bytesStart = bytesStartArray.grow();
        assert count < bytesStart.length + 1 : "count: " + count + " len: " + bytesStart.length;
      }
      bytesStart[count] = pool.addBytesRef(bytes);
      e = count++;
      assert ids[hashPos] == -1;
      ids[hashPos] = e | (hashcode & highMask);

      if (count == hashHalfSize) {
        rehash(2 * hashSize, true);
      }
      return e;
    }
    e = e & hashMask;
    return -(e + 1);
  }

  /**
   * Returns the id of the given {@link BytesRef}.
   *
   * @param bytes the bytes to look for
   * @return the id of the given bytes, or {@code -1} if there is no mapping for the given bytes.
   */
  public int find(BytesRef bytes) {
    final int hashcode = doHash(bytes.bytes, bytes.offset, bytes.length);
    final int id = ids[findHash(bytes, hashcode)];
    return id == -1 ? -1 : id & hashMask;
  }

  private int findHash(BytesRef bytes, int hashcode) {
    assert bytesStart != null : "bytesStart is null - not initialized";
    assert hashcode == doHash(bytes.bytes, bytes.offset, bytes.length);

    int code = hashcode;
    // final position
    int hashPos = code & hashMask;
    int e = ids[hashPos];
    final int highBits = hashcode & highMask;

    // Conflict; use linear probe to find an open slot
    // (see LUCENE-5604):
    while (e != -1
        && ((e & highMask) != highBits || pool.equals(bytesStart[e & hashMask], bytes) == false)) {
      code++;
      hashPos = code & hashMask;
      e = ids[hashPos];
    }

    return hashPos;
  }

  /**
   * Adds a "arbitrary" int offset instead of a BytesRef term. This is used in the indexer to hold
   * the hash for term vectors, because they do not redundantly store the byte[] term directly and
   * instead reference the byte[] term already stored by the postings BytesRefHash. See add(int
   * textStart) in TermsHashPerField.
   */
  public int addByPoolOffset(int offset) {
    assert bytesStart != null : "Bytesstart is null - not initialized";
    // final position
    int code = offset;
    int hashPos = offset & hashMask;
    int e = ids[hashPos];

    // Conflict; use linear probe to find an open slot
    // (see LUCENE-5604):
    while (e != -1 && bytesStart[e] != offset) {
      code++;
      hashPos = code & hashMask;
      e = ids[hashPos];
    }
    if (e == -1) {
      // new entry
      if (count >= bytesStart.length) {
        bytesStart = bytesStartArray.grow();
        assert count < bytesStart.length + 1 : "count: " + count + " len: " + bytesStart.length;
      }
      e = count++;
      bytesStart[e] = offset;
      assert ids[hashPos] == -1;
      ids[hashPos] = e;

      if (count == hashHalfSize) {
        rehash(2 * hashSize, false);
      }
      return e;
    }
    return -(e + 1);
  }

  /**
   * Called when hash is too small ({@code > 50%} occupied) or too large ({@code < 20%} occupied).
   */
  private void rehash(final int newSize, boolean hashOnData) {
    final int newMask = newSize - 1;
    final int newHighMask = ~newMask;
    bytesUsed.addAndGet(Integer.BYTES * (long) newSize);
    final int[] newHash = new int[newSize];
    Arrays.fill(newHash, -1);
    for (int i = 0; i < hashSize; i++) {
      int e0 = ids[i];
      if (e0 != -1) {
        e0 &= hashMask;
        final int hashcode;
        int code;
        if (hashOnData) {
          hashcode = code = pool.hash(bytesStart[e0]);
        } else {
          code = bytesStart[e0];
          hashcode = 0;
        }

        int hashPos = code & newMask;
        assert hashPos >= 0;

        // Conflict; use linear probe to find an open slot
        // (see LUCENE-5604):
        while (newHash[hashPos] != -1) {
          code++;
          hashPos = code & newMask;
        }

        newHash[hashPos] = e0 | (hashcode & newHighMask);
      }
    }

    hashMask = newMask;
    highMask = newHighMask;
    bytesUsed.addAndGet(Integer.BYTES * (long) -ids.length);
    ids = newHash;
    hashSize = newSize;
    hashHalfSize = newSize / 2;
  }

  // TODO: maybe use long?  But our keys are typically short...
  static int doHash(byte[] bytes, int offset, int length) {
    return StringHelper.murmurhash3_x86_32(bytes, offset, length, StringHelper.GOOD_FAST_HASH_SEED);
  }

  /**
   * reinitializes the {@link BytesRefHash} after a previous {@link #clear()} call. If {@link
   * #clear()} has not been called previously this method has no effect.
   */
  public void reinit() {
    if (bytesStart == null) {
      bytesStart = bytesStartArray.init();
    }

    if (ids == null) {
      ids = new int[hashSize];
      bytesUsed.addAndGet(Integer.BYTES * (long) hashSize);
    }
  }

  /**
   * Returns the bytesStart offset into the internally used {@link ByteBlockPool} for the given
   * bytesID
   *
   * @param bytesID the id to look up
   * @return the bytesStart offset into the internally used {@link ByteBlockPool} for the given id
   */
  public int byteStart(int bytesID) {
    assert bytesStart != null : "bytesStart is null - not initialized";
    assert bytesID >= 0 && bytesID < count : bytesID;
    return bytesStart[bytesID];
  }

  @Override
  public long ramBytesUsed() {
    long size =
        BASE_RAM_BYTES
            + RamUsageEstimator.sizeOfObject(bytesStart)
            + RamUsageEstimator.sizeOfObject(ids)
            + RamUsageEstimator.sizeOfObject(pool);
    return size;
  }

  /**
   * Thrown if a {@link BytesRef} exceeds the {@link BytesRefHash} limit of {@link
   * ByteBlockPool#BYTE_BLOCK_SIZE}-2.
   */
  @SuppressWarnings("serial")
  public static class MaxBytesLengthExceededException extends RuntimeException {
    MaxBytesLengthExceededException(String message) {
      super(message);
    }
  }

  /** Manages allocation of the per-term addresses. */
  public abstract static class BytesStartArray {
    /**
     * Initializes the BytesStartArray. This call will allocate memory
     *
     * @return the initialized bytes start array
     */
    public abstract int[] init();

    /**
     * Grows the {@link BytesStartArray}
     *
     * @return the grown array
     */
    public abstract int[] grow();

    /**
     * clears the {@link BytesStartArray} and returns the cleared instance.
     *
     * @return the cleared instance, this might be <code>null</code>
     */
    public abstract int[] clear();

    /**
     * A {@link Counter} reference holding the number of bytes used by this {@link BytesStartArray}.
     * The {@link BytesRefHash} uses this reference to track it memory usage
     *
     * @return a {@link AtomicLong} reference holding the number of bytes used by this {@link
     *     BytesStartArray}.
     */
    public abstract Counter bytesUsed();
  }

  /**
   * A simple {@link BytesStartArray} that tracks memory allocation using a private {@link Counter}
   * instance.
   */
  public static class DirectBytesStartArray extends BytesStartArray {
    // TODO: can't we just merge this w/
    // TrackingDirectBytesStartArray...?  Just add a ctor
    // that makes a private bytesUsed?

    protected final int initSize;
    private int[] bytesStart;
    private final Counter bytesUsed;

    public DirectBytesStartArray(int initSize, Counter counter) {
      this.bytesUsed = counter;
      this.initSize = initSize;
    }

    public DirectBytesStartArray(int initSize) {
      this(initSize, Counter.newCounter());
    }

    @Override
    public int[] clear() {
      return bytesStart = null;
    }

    @Override
    public int[] grow() {
      assert bytesStart != null;
      return bytesStart = ArrayUtil.grow(bytesStart, bytesStart.length + 1);
    }

    @Override
    public int[] init() {
      return bytesStart = new int[ArrayUtil.oversize(initSize, Integer.BYTES)];
    }

    @Override
    public Counter bytesUsed() {
      return bytesUsed;
    }
  }
}
