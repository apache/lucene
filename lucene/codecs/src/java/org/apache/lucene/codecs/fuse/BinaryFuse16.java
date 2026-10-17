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
package org.apache.lucene.codecs.fuse;

import java.io.IOException;
import java.util.Arrays;
import java.util.SplittableRandom;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.StringHelper;

/**
 * A 16-bit Binary Fuse Filter implementation based on Graf &amp; Lemire (ACM JEA 2022).
 *
 * <p>Achieves an empirical false-positive rate of ~0.0015% (1 in 65,536) using ~18 bits per key.
 * Lookups require only 3 localized memory accesses.
 *
 * @lucene.experimental
 */
public final class BinaryFuse16 implements MembershipFilter {

  private static final long BASE_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(BinaryFuse16.class);
  private static final int ARITY = 3;

  private final int segmentLength;
  private final int segmentLengthMask;
  private final int segmentCountLength;
  private final long seed;
  private final short[] fingerprints;

  public BinaryFuse16(int segmentLength, int segmentCountLength, long seed, short[] fingerprints) {
    this.segmentLength = segmentLength;
    this.segmentLengthMask = segmentLength - 1;
    this.segmentCountLength = segmentCountLength;
    this.seed = seed;
    this.fingerprints = fingerprints;
  }

  @Override
  public boolean contains(BytesRef term) {
    long[] h = StringHelper.murmurhash3_x64_128(term);
    return contains(h[0] ^ Long.rotateLeft(h[1], 32));
  }

  public boolean contains(long hash) {
    long mixed = mix64(hash + seed);
    if (mixed == 0L) {
      mixed = 1L;
    }
    short f = (short) mixed;

    int h0 = reduce((int) (mixed >>> 32), segmentCountLength);
    int h1 = h0 + segmentLength;
    int h2 = h1 + segmentLength;

    h1 ^= (int) ((mixed >>> 18) & segmentLengthMask);
    h2 ^= (int) (mixed & segmentLengthMask);

    f ^= (short) (fingerprints[h0] ^ fingerprints[h1] ^ fingerprints[h2]);
    return f == 0;
  }

  @Override
  public long ramBytesUsed() {
    return BASE_RAM_BYTES_USED + RamUsageEstimator.sizeOf(fingerprints);
  }

  public void serialize(DataOutput out) throws IOException {
    out.writeInt(segmentLength);
    out.writeInt(segmentCountLength);
    out.writeLong(seed);
    out.writeInt(fingerprints.length);
    for (short fp : fingerprints) {
      out.writeShort(fp);
    }
  }

  public static BinaryFuse16 deserialize(DataInput in) throws IOException {
    int segmentLength = in.readInt();
    int segmentCountLength = in.readInt();
    long seed = in.readLong();
    int len = in.readInt();
    short[] fingerprints = new short[len];
    for (int i = 0; i < len; i++) {
      fingerprints[i] = in.readShort();
    }
    return new BinaryFuse16(segmentLength, segmentCountLength, seed, fingerprints);
  }

  public static BinaryFuse16 construct(long[] keys) {
    int size = keys.length;
    if (size == 0) {
      return new BinaryFuse16(1, 1, 0L, new short[ARITY]);
    }

    int segmentLength = calculateSegmentLength(size);
    double sizeFactor = calculateSizeFactor(size);
    int capacity = (int) (size * sizeFactor);
    int segmentCount = (capacity + segmentLength - 1) / segmentLength - (ARITY - 1);
    segmentCount = Math.max(1, segmentCount);

    int arrayLength = (segmentCount + ARITY - 1) * segmentLength;
    int segmentCountLength = segmentCount * segmentLength;
    int segmentLengthMask = segmentLength - 1;

    short[] fingerprints = new short[arrayLength];
    long[] reverseOrder = new long[size + 1];
    byte[] reverseH = new byte[size];
    byte[] t2count = new byte[arrayLength];
    long[] t2hash = new long[arrayLength];
    int[] alone = new int[arrayLength];
    int[] h012 = new int[5];

    int blockBits = 1;
    while ((1 << blockBits) < segmentCount) {
      blockBits++;
    }
    int block = 1 << blockBits;
    int[] startPos = new int[block];

    SplittableRandom random = new SplittableRandom(size ^ 0x9e3779b97f4a7c15L);
    long currentSeed = random.nextLong();

    for (int attempts = 0; attempts < 100; attempts++) {
      Arrays.fill(t2count, (byte) 0);
      Arrays.fill(t2hash, 0L);
      Arrays.fill(reverseOrder, 0L);
      reverseOrder[size] = 1L; // Sentinel

      for (int i = 0; i < block; i++) {
        startPos[i] = (int) ((long) i * size / block);
      }

      for (int i = 0; i < size; i++) {
        long mixed = mix64(keys[i] + currentSeed);
        if (mixed == 0L) {
          mixed = 1L;
        }
        int segmentIndex = (int) (mixed >>> (64 - blockBits));
        while (reverseOrder[startPos[segmentIndex]] != 0L) {
          segmentIndex = (segmentIndex + 1) & (block - 1);
        }
        reverseOrder[startPos[segmentIndex]] = mixed;
        startPos[segmentIndex]++;
      }

      byte countMask = 0;
      for (int i = 0; i < size; i++) {
        long hash = reverseOrder[i];
        for (int hi = 0; hi < 3; hi++) {
          int index = getSlot(hash, hi, segmentCountLength, segmentLength, segmentLengthMask);
          t2count[index] += 4;
          t2count[index] ^= hi;
          t2hash[index] ^= hash;
          countMask |= t2count[index];
        }
      }

      int reverseOrderPos = 0;
      if (countMask >= 0) {
        int alonePos = 0;
        for (int i = 0; i < arrayLength; i++) {
          alone[alonePos] = i;
          alonePos += ((t2count[i] >> 2) == 1) ? 1 : 0;
        }

        while (alonePos > 0) {
          alonePos--;
          int index = alone[alonePos];
          if ((t2count[index] >> 2) == 1) {
            long hash = t2hash[index];
            byte found = (byte) (t2count[index] & 3);

            reverseH[reverseOrderPos] = found;
            reverseOrder[reverseOrderPos] = hash;

            h012[0] = getSlot(hash, 0, segmentCountLength, segmentLength, segmentLengthMask);
            h012[1] = getSlot(hash, 1, segmentCountLength, segmentLength, segmentLengthMask);
            h012[2] = getSlot(hash, 2, segmentCountLength, segmentLength, segmentLengthMask);

            int index3 = h012[mod3(found + 1)];
            alone[alonePos] = index3;
            alonePos += ((t2count[index3] >> 2) == 2 ? 1 : 0);
            t2count[index3] -= 4;
            t2count[index3] ^= mod3(found + 1);
            t2hash[index3] ^= hash;

            index3 = h012[mod3(found + 2)];
            alone[alonePos] = index3;
            alonePos += ((t2count[index3] >> 2) == 2 ? 1 : 0);
            t2count[index3] -= 4;
            t2count[index3] ^= mod3(found + 2);
            t2hash[index3] ^= hash;

            reverseOrderPos++;
          }
        }
      }

      if (reverseOrderPos == size) {
        for (int i = reverseOrderPos - 1; i >= 0; i--) {
          long hash = reverseOrder[i];
          int found = reverseH[i];
          short xor = (short) hash;
          h012[0] = getSlot(hash, 0, segmentCountLength, segmentLength, segmentLengthMask);
          h012[1] = getSlot(hash, 1, segmentCountLength, segmentLength, segmentLengthMask);
          h012[2] = getSlot(hash, 2, segmentCountLength, segmentLength, segmentLengthMask);
          h012[3] = h012[0];
          h012[4] = h012[1];
          fingerprints[h012[found]] =
              (short) (xor ^ fingerprints[h012[found + 1]] ^ fingerprints[h012[found + 2]]);
        }
        return new BinaryFuse16(segmentLength, segmentCountLength, currentSeed, fingerprints);
      }

      currentSeed = random.nextLong();
    }

    throw new IllegalStateException("Failed to construct BinaryFuse16 filter after 100 attempts");
  }

  private static int getSlot(
      long hash, int index, int segmentCountLength, int segmentLength, int segmentLengthMask) {
    long h = reduce((int) (hash >>> 32), segmentCountLength) + (long) index * segmentLength;
    long hh = hash & ((1L << 36) - 1);
    h ^= (int) ((hh >>> (36 - 18 * index)) & segmentLengthMask);
    return (int) h;
  }

  private static int mod3(int x) {
    return x > 2 ? x - 3 : x;
  }

  private static int reduce(int hash, int n) {
    return (int) (((hash & 0xffffffffL) * (n & 0xffffffffL)) >>> 32);
  }

  private static long mix64(long z) {
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L;
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL;
    return z ^ (z >>> 31);
  }

  private static int calculateSegmentLength(int size) {
    int segmentLength = 1 << (int) Math.floor(Math.log(size) / Math.log(3.33) + 2.11);
    return Math.min(segmentLength, 1 << 18);
  }

  private static double calculateSizeFactor(int size) {
    return Math.max(1.125, 0.875 + 0.25 * Math.log(1_000_000) / Math.log(size));
  }
}
