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

import java.io.DataInputStream;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Properties;

/**
 * Methods for manipulating strings.
 *
 * @lucene.internal
 */
public abstract class StringHelper {

  /**
   * Compares two {@link BytesRef}, element by element, and returns the number of elements common to
   * both arrays (from the start of each). This method assumes currentTerm comes after priorTerm.
   *
   * @param priorTerm The first {@link BytesRef} to compare
   * @param currentTerm The second {@link BytesRef} to compare
   * @return The number of common elements (from the start of each).
   */
  public static int bytesDifference(BytesRef priorTerm, BytesRef currentTerm) {
    int mismatch =
        Arrays.mismatch(
            priorTerm.bytes,
            priorTerm.offset,
            priorTerm.offset + priorTerm.length,
            currentTerm.bytes,
            currentTerm.offset,
            currentTerm.offset + currentTerm.length);
    if (mismatch < 0) {
      throw new IllegalArgumentException(
          "terms out of order: priorTerm=" + priorTerm + ",currentTerm=" + currentTerm);
    }
    return mismatch;
  }

  /**
   * Returns the length of {@code currentTerm} needed for use as a sort key. so that {@link
   * BytesRef#compareTo(BytesRef)} still returns the same result. This method assumes currentTerm
   * comes after priorTerm.
   */
  public static int sortKeyLength(final BytesRef priorTerm, final BytesRef currentTerm) {
    return bytesDifference(priorTerm, currentTerm) + 1;
  }

  private StringHelper() {}

  /**
   * Returns <code>true</code> iff the ref starts with the given prefix. Otherwise <code>false
   * </code>.
   *
   * @param ref the {@code byte[]} to test
   * @param prefix the expected prefix
   * @return Returns <code>true</code> iff the ref starts with the given prefix. Otherwise <code>
   *     false</code>.
   */
  public static boolean startsWith(byte[] ref, BytesRef prefix) {
    // not long enough to start with the prefix
    if (ref.length < prefix.length) {
      return false;
    }
    return Arrays.equals(
        ref, 0, prefix.length, prefix.bytes, prefix.offset, prefix.offset + prefix.length);
  }

  /**
   * Returns <code>true</code> iff the ref starts with the given prefix. Otherwise <code>false
   * </code>.
   *
   * @param ref the {@link BytesRef} to test
   * @param prefix the expected prefix
   * @return Returns <code>true</code> iff the ref starts with the given prefix. Otherwise <code>
   *     false</code>.
   */
  public static boolean startsWith(BytesRef ref, BytesRef prefix) {
    // not long enough to start with the prefix
    if (ref.length < prefix.length) {
      return false;
    }
    return Arrays.equals(
        ref.bytes,
        ref.offset,
        ref.offset + prefix.length,
        prefix.bytes,
        prefix.offset,
        prefix.offset + prefix.length);
  }

  /**
   * Returns <code>true</code> iff the ref ends with the given suffix. Otherwise <code>false</code>.
   *
   * @param ref the {@link BytesRef} to test
   * @param suffix the expected suffix
   * @return Returns <code>true</code> iff the ref ends with the given suffix. Otherwise <code>false
   *     </code>.
   */
  public static boolean endsWith(BytesRef ref, BytesRef suffix) {
    int startAt = ref.length - suffix.length;
    // not long enough to start with the suffix
    if (startAt < 0) {
      return false;
    }
    return Arrays.equals(
        ref.bytes,
        ref.offset + startAt,
        ref.offset + startAt + suffix.length,
        suffix.bytes,
        suffix.offset,
        suffix.offset + suffix.length);
  }

  /** Pass this as the seed to {@link #murmurhash3_x86_32}. */

  // Poached from Guava: set a different salt/seed
  // for each JVM instance, to frustrate hash key collision
  // denial of service attacks, and to catch any places that
  // somehow rely on hash function/order across JVM
  // instances:
  public static final int GOOD_FAST_HASH_SEED;

  static {
    String prop = System.getProperty("tests.seed");
    if (prop != null) {
      // So if there is a test failure that relied on hash
      // order, we remain reproducible based on the test seed:
      GOOD_FAST_HASH_SEED = prop.hashCode();
    } else {
      GOOD_FAST_HASH_SEED = (int) System.currentTimeMillis();
    }
  }

  /**
   * Returns the MurmurHash3_x86_32 hash. Original source/tests at
   * https://github.com/yonik/java_util/
   */
  @SuppressWarnings("fallthrough")
  public static int murmurhash3_x86_32(byte[] data, int offset, int len, int seed) {

    final int c1 = 0xcc9e2d51;
    final int c2 = 0x1b873593;

    int h1 = seed;
    int roundedEnd = offset + (len & 0xfffffffc); // round down to 4 byte block

    for (int i = offset; i < roundedEnd; i += 4) {
      // little endian load order
      int k1 = (int) BitUtil.VH_LE_INT.get(data, i);
      k1 *= c1;
      k1 = Integer.rotateLeft(k1, 15);
      k1 *= c2;

      h1 ^= k1;
      h1 = Integer.rotateLeft(h1, 13);
      h1 = h1 * 5 + 0xe6546b64;
    }

    // tail
    int k1 = 0;

    switch (len & 0x03) {
      case 3:
        k1 = (data[roundedEnd + 2] & 0xff) << 16;
      // fallthrough
      case 2:
        k1 |= (data[roundedEnd + 1] & 0xff) << 8;
      // fallthrough
      case 1:
        k1 |= (data[roundedEnd] & 0xff);
        k1 *= c1;
        k1 = Integer.rotateLeft(k1, 15);
        k1 *= c2;
        h1 ^= k1;
    }

    // finalization
    h1 ^= len;

    // fmix(h1);
    h1 ^= h1 >>> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >>> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >>> 16;

    return h1;
  }

  public static int murmurhash3_x86_32(BytesRef bytes, int seed) {
    return murmurhash3_x86_32(bytes.bytes, bytes.offset, bytes.length, seed);
  }

  /**
   * Generates 128-bit hash from the byte array with the given offset, length and seed.
   *
   * <p>The code is adopted from Apache Commons (<a
   * href="https://commons.apache.org/proper/commons-codec/jacoco/org.apache.commons.codec.digest/MurmurHash3.java.html">link</a>)
   *
   * @param data The input byte array
   * @param offset The first element of array
   * @param length The length of array
   * @param seed The initial seed value
   * @return The 128-bit hash (2 longs)
   */
  public static long[] murmurhash3_x64_128(
      final byte[] data, final int offset, final int length, final int seed) {
    // Use an unsigned 32-bit integer as the seed
    return murmurhash3_x64_128(data, offset, length, seed & 0xFFFFFFFFL);
  }

  @SuppressWarnings("fallthrough")
  private static long[] murmurhash3_x64_128(
      final byte[] data, final int offset, final int length, final long seed) {
    long h1 = seed;
    long h2 = seed;
    final int nblocks = length >> 4;

    // Constants for 128-bit variant
    final long C1 = 0x87c37b91114253d5L;
    final long C2 = 0x4cf5ad432745937fL;
    final int R1 = 31;
    final int R2 = 27;
    final int R3 = 33;
    final int M = 5;
    final int N1 = 0x52dce729;
    final int N2 = 0x38495ab5;

    // body
    for (int i = 0; i < nblocks; i++) {
      final int index = offset + (i << 4);
      long k1 = (long) BitUtil.VH_LE_LONG.get(data, index);
      long k2 = (long) BitUtil.VH_LE_LONG.get(data, index + 8);

      // mix functions for k1
      k1 *= C1;
      k1 = Long.rotateLeft(k1, R1);
      k1 *= C2;
      h1 ^= k1;
      h1 = Long.rotateLeft(h1, R2);
      h1 += h2;
      h1 = h1 * M + N1;

      // mix functions for k2
      k2 *= C2;
      k2 = Long.rotateLeft(k2, R3);
      k2 *= C1;
      h2 ^= k2;
      h2 = Long.rotateLeft(h2, R1);
      h2 += h1;
      h2 = h2 * M + N2;
    }

    // tail
    long k1 = 0;
    long k2 = 0;
    final int index = offset + (nblocks << 4);
    switch (length & 0x0F) {
      case 15:
        k2 ^= ((long) data[index + 14] & 0xff) << 48;
      case 14:
        k2 ^= ((long) data[index + 13] & 0xff) << 40;
      case 13:
        k2 ^= ((long) data[index + 12] & 0xff) << 32;
      case 12:
        k2 ^= ((long) data[index + 11] & 0xff) << 24;
      case 11:
        k2 ^= ((long) data[index + 10] & 0xff) << 16;
      case 10:
        k2 ^= ((long) data[index + 9] & 0xff) << 8;
      case 9:
        k2 ^= data[index + 8] & 0xff;
        k2 *= C2;
        k2 = Long.rotateLeft(k2, R3);
        k2 *= C1;
        h2 ^= k2;

      case 8:
        k1 ^= ((long) data[index + 7] & 0xff) << 56;
      case 7:
        k1 ^= ((long) data[index + 6] & 0xff) << 48;
      case 6:
        k1 ^= ((long) data[index + 5] & 0xff) << 40;
      case 5:
        k1 ^= ((long) data[index + 4] & 0xff) << 32;
      case 4:
        k1 ^= ((long) data[index + 3] & 0xff) << 24;
      case 3:
        k1 ^= ((long) data[index + 2] & 0xff) << 16;
      case 2:
        k1 ^= ((long) data[index + 1] & 0xff) << 8;
      case 1:
        k1 ^= data[index] & 0xff;
        k1 *= C1;
        k1 = Long.rotateLeft(k1, R1);
        k1 *= C2;
        h1 ^= k1;
    }

    // finalization
    h1 ^= length;
    h2 ^= length;

    h1 += h2;
    h2 += h1;

    h1 = fmix64(h1);
    h2 = fmix64(h2);

    h1 += h2;
    h2 += h1;

    return new long[] {h1, h2};
  }

  /**
   * Performs the final avalanche mix step of the 64-bit hash function.
   *
   * @param hash The current hash
   * @return The final hash
   */
  private static long fmix64(long hash) {
    hash ^= (hash >>> 33);
    hash *= 0xff51afd7ed558ccdL;
    hash ^= (hash >>> 33);
    hash *= 0xc4ceb9fe1a85ec53L;
    hash ^= (hash >>> 33);
    return hash;
  }

  /**
   * Generates 128-bit hash from the byte array with the given offset, length and seed.
   *
   * <p>The code is adopted from Apache Commons (<a
   * href="https://commons.apache.org/proper/commons-codec/jacoco/org.apache.commons.codec.digest/MurmurHash3.java.html">link</a>)
   *
   * @param data The input data
   * @return The 128-bit hash (2 longs)
   */
  public static long[] murmurhash3_x64_128(BytesRef data) {
    return murmurhash3_x64_128(data.bytes, data.offset, data.length, 104729);
  }

  // Holds 128 bit unsigned value:
  @SuppressWarnings("NonFinalStaticField")
  private static BigInteger nextId;

  private static final BigInteger mask128;
  private static final Object idLock = new Object();

  static {
    // 128 bit unsigned mask
    byte[] maskBytes128 = new byte[16];
    Arrays.fill(maskBytes128, (byte) 0xff);
    mask128 = new BigInteger(1, maskBytes128);

    String prop = System.getProperty("tests.seed");

    // State for xorshift128:
    long x0;
    long x1;

    if (prop != null) {
      // So if there is a test failure that somehow relied on this id,
      // we remain reproducible based on the test seed:
      if (prop.length() > 8) {
        prop = prop.substring(prop.length() - 8);
      }
      x0 = Long.parseLong(prop, 16);
      x1 = x0;
    } else {
      // seed from /dev/urandom, if its available
      try (DataInputStream is =
          new DataInputStream(Files.newInputStream(Paths.get("/dev/urandom")))) {
        x0 = is.readLong();
        x1 = is.readLong();
      } catch (Exception _) {
        // may not be available on this platform
        // fall back to lower quality randomness from 3 different sources:
        x0 = System.nanoTime();
        x1 = (long) StringHelper.class.hashCode() << 32;

        StringBuilder sb = new StringBuilder();
        // Properties can vary across JVM instances:
        try {
          Properties p = System.getProperties();
          for (String s : p.stringPropertyNames()) {
            sb.append(s);
            sb.append(p.getProperty(s));
          }
          x1 |= sb.toString().hashCode();
        } catch (SecurityException _) {
          // getting Properties requires wildcard read-write: may not be allowed
          x1 |= StringBuffer.class.hashCode();
        }
      }
    }

    // Use a few iterations of xorshift128 to scatter the seed
    // in case multiple Lucene instances starting up "near" the same
    // nanoTime, since we use ++ (mod 2^128) for full period cycle:
    for (int i = 0; i < 10; i++) {
      long s1 = x0;
      long s0 = x1;
      x0 = s0;
      s1 ^= s1 << 23; // a
      x1 = s1 ^ s0 ^ (s1 >>> 17) ^ (s0 >>> 26); // b, c
    }

    // 64-bit unsigned mask
    byte[] maskBytes64 = new byte[8];
    Arrays.fill(maskBytes64, (byte) 0xff);
    BigInteger mask64 = new BigInteger(1, maskBytes64);

    // First make unsigned versions of x0, x1:
    BigInteger unsignedX0 = BigInteger.valueOf(x0).and(mask64);
    BigInteger unsignedX1 = BigInteger.valueOf(x1).and(mask64);

    // Concatentate bits of x0 and x1, as unsigned 128 bit integer:
    nextId = unsignedX0.shiftLeft(64).or(unsignedX1);
  }

  /** length in bytes of an ID */
  public static final int ID_LENGTH = 16;

  /** Generates a non-cryptographic globally unique id. */
  public static byte[] randomId() {

    // NOTE: we don't use Java's UUID.randomUUID() implementation here because:
    //
    //   * It's overkill for our usage: it tries to be cryptographically
    //     secure, whereas for this use we don't care if someone can
    //     guess the IDs.
    //
    //   * It uses SecureRandom, which on Linux can easily take a long time
    //     (I saw ~ 10 seconds just running a Lucene test) when entropy
    //     harvesting is falling behind.
    //
    //   * It loses a few (6) bits to version and variant and it's not clear
    //     what impact that has on the period, whereas the simple ++ (mod 2^128)
    //     we use here is guaranteed to have the full period.

    byte[] bits;
    synchronized (idLock) {
      bits = nextId.toByteArray();
      nextId = nextId.add(BigInteger.ONE).and(mask128);
    }

    // toByteArray() always returns a sign bit, so it may require an extra byte (always zero)
    if (bits.length > ID_LENGTH) {
      assert bits.length == ID_LENGTH + 1;
      assert bits[0] == 0;
      return ArrayUtil.copyOfSubArray(bits, 1, bits.length);
    } else {
      byte[] result = new byte[ID_LENGTH];
      System.arraycopy(bits, 0, result, result.length - bits.length, bits.length);
      return result;
    }
  }

  /**
   * Helper method to render an ID as a string, for debugging
   *
   * <p>Returns the string {@code (null)} if the id is null. Otherwise, returns a string
   * representation for debugging. Never throws an exception. The returned string may indicate if
   * the id is definitely invalid.
   */
  public static String idToString(byte[] id) {
    if (id == null) {
      return "(null)";
    } else {
      StringBuilder sb = new StringBuilder();
      sb.append(new BigInteger(1, id).toString(Character.MAX_RADIX));
      if (id.length != ID_LENGTH) {
        sb.append(" (INVALID FORMAT)");
      }
      return sb.toString();
    }
  }

  /**
   * Just converts each int in the incoming {@link IntsRef} to each byte in the returned {@link
   * BytesRef}, throwing {@code IllegalArgumentException} if any int value is out of bounds for a
   * byte.
   */
  public static BytesRef intsRefToBytesRef(IntsRef ints) {
    byte[] bytes = new byte[ints.length];
    for (int i = 0; i < ints.length; i++) {
      int x = ints.ints[ints.offset + i];
      if (x < 0 || x > 255) {
        throw new IllegalArgumentException(
            "int at pos=" + i + " with value=" + x + " is out-of-bounds for byte");
      }
      bytes[i] = (byte) x;
    }

    return new BytesRef(bytes);
  }
}
