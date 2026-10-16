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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;

/**
 * The order of {@link HashedTerms} against {@link Arrays#sort(Object[])} on {@link BytesRef}s, for
 * keys that tie on their first 8 bytes, are prefixes of each other, hold 0x00 and 0xFF bytes, and
 * come in every order, and for sizes on both sides of every threshold of the sort.
 */
public class TestHashedTerms extends LuceneTestCase {

  /** Sizes around the thresholds of the sort: insertion sort, radix sort and comparison sort. */
  private static final int[] SIZES = {
    0, 1, 2, 3, 7, 8, 9, 31, 32, 33, 34, 63, 64, 65, 127, 128, 129, 130, 255, 256, 257, 1000, 4096,
    20000
  };

  private static int randomSize(Random random) {
    return random.nextInt(4) == 0
        ? TestUtil.nextInt(random, 0, 5000)
        : SIZES[random.nextInt(SIZES.length)];
  }

  /**
   * Returns a key with the given bytes, at a random offset of a larger array whose other bytes are
   * random, so that the sort must not read past the key or before it.
   */
  private static BytesRef place(Random random, byte[] bytes) {
    final int offset = random.nextInt(4) == 0 ? random.nextInt(9) : 0;
    final int slack = random.nextInt(4) == 0 ? random.nextInt(9) : 0;
    final byte[] buffer = new byte[offset + bytes.length + slack];
    random.nextBytes(buffer);
    System.arraycopy(bytes, 0, buffer, offset, bytes.length);
    return new BytesRef(buffer, offset, bytes.length);
  }

  /** Random keys from {@code next}, a fifth of them duplicates of earlier ones. */
  private static BytesRef[] randomKeys(Random random, int n, Function<Random, byte[]> next) {
    final BytesRef[] keys = new BytesRef[n];
    for (int i = 0; i < n; i++) {
      if (i > 0 && random.nextInt(5) == 0) {
        keys[i] = place(random, BytesRef.deepCopyOf(keys[random.nextInt(i)]).bytes);
      } else {
        keys[i] = place(random, next.apply(random));
      }
    }
    return keys;
  }

  private static byte[] randomBinary(Random random) {
    final byte[] bytes = new byte[random.nextInt(4) == 0 ? random.nextInt(40) : 16];
    random.nextBytes(bytes);
    return bytes;
  }

  /** Short keys over {0x00, 0x01, 0xFF}: many tie on their prefixes or are prefixes of others. */
  private static byte[] tinyAlphabet(Random random) {
    final byte[] alphabet = {0x00, 0x01, (byte) 0xFF};
    final byte[] bytes = new byte[random.nextInt(13)];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = alphabet[random.nextInt(alphabet.length)];
    }
    return bytes;
  }

  /** The distinct keys in unsigned byte order, as Arrays.sort and a pass to remove duplicates. */
  private static List<BytesRef> expected(BytesRef[] keys) {
    final BytesRef[] sorted = keys.clone();
    Arrays.sort(sorted);
    final List<BytesRef> distinct = new ArrayList<>();
    for (BytesRef key : sorted) {
      if (distinct.isEmpty() || distinct.get(distinct.size() - 1).bytesEquals(key) == false) {
        distinct.add(key);
      }
    }
    return distinct;
  }

  /** Checks that the keys, in their order and in sorted, reverse and nearly sorted orders, sort. */
  private static void assertSorts(Random random, BytesRef[] keys) throws IOException {
    final List<BytesRef> expected = expected(keys);
    final HashedTerms hashed = assertSorts(keys, expected);
    assertSorts(hashed, expected.toArray(new BytesRef[0]), expected); // in order
    final BytesRef[] sorted = keys.clone();
    Arrays.sort(sorted);
    assertSorts(hashed, sorted, expected); // in order, with duplicates
    final BytesRef[] reversed = expected.toArray(new BytesRef[0]);
    Collections.reverse(Arrays.asList(reversed));
    assertSorts(hashed, reversed, expected);
    Collections.reverse(Arrays.asList(sorted));
    assertSorts(hashed, sorted, expected); // in reverse order, with duplicates
    if (expected.size() > 1) {
      final BytesRef[] nearly = expected.toArray(new BytesRef[0]);
      final int swaps = random.nextBoolean() ? 1 : TestUtil.nextInt(random, 1, 10);
      for (int s = 0; s < swaps; s++) {
        int i = random.nextInt(nearly.length);
        int j = random.nextBoolean() ? Math.min(i + 1, nearly.length - 1) : random.nextInt(i + 1);
        BytesRef tmp = nearly[i];
        nearly[i] = nearly[j];
        nearly[j] = tmp;
      }
      assertSorts(hashed, nearly, expected);
      Collections.reverse(Arrays.asList(nearly));
      assertSorts(hashed, nearly, expected);
    }
    final List<BytesRef> shuffled = new ArrayList<>(Arrays.asList(keys));
    Collections.shuffle(shuffled, random);
    final HashedTerms fromCollection = new HashedTerms(shuffled);
    assertEquals(hashed, fromCollection);
    assertEquals(hashed.hashCode(), fromCollection.hashCode());
  }

  private static void assertSorts(HashedTerms hashed, BytesRef[] keys, List<BytesRef> expected)
      throws IOException {
    final HashedTerms other = assertSorts(keys, expected);
    assertEquals(hashed, other);
    assertEquals(other, hashed);
    assertEquals(hashed.hashCode(), other.hashCode());
  }

  private static HashedTerms assertSorts(BytesRef[] keys, List<BytesRef> expected)
      throws IOException {
    final BytesRef[] copy = keys.clone();
    final HashedTerms hashed = new HashedTerms(keys);
    assertArrayEquals(copy, keys); // the caller's array is not reordered
    assertEquals(expected.size(), hashed.size());
    assertEquals(expected.size(), hashed.hashes().length);
    assertEquals(expected.size(), hashed.mixes().length);
    assertEquals(expected.size(), hashed.filterMasks().length);
    final BytesRefIterator iterator = hashed.iterator();
    for (int i = 0; i < expected.size(); i++) {
      final BytesRef key = expected.get(i);
      if (key.equals(hashed.get(i)) == false) {
        fail("key " + i + " of " + expected.size() + ": " + key + " != " + hashed.get(i));
      }
      assertEquals(key, iterator.next());
      // what lookups take from the hash, computed once for every segment
      final long hash = TermHash.hash(key.bytes, key.offset, key.length);
      assertEquals(hash, hashed.hash(i));
      assertEquals(hash, hashed.hashes()[i]);
      assertEquals(TermHash.mix(hash), hashed.mixes()[i]);
      assertEquals(BlockedBloomFilter.mask(TermHash.mix(hash)), hashed.filterMasks()[i]);
    }
    assertNull(iterator.next());
    return hashed;
  }

  public void testRandomBinaryKeys() throws IOException {
    final Random random = random();
    for (int size : SIZES) {
      assertSorts(random, randomKeys(random, size, TestHashedTerms::randomBinary));
    }
    for (int iter = 0; iter < 5; iter++) {
      assertSorts(random, randomKeys(random, randomSize(random), TestHashedTerms::randomBinary));
    }
  }

  public void testTinyAlphabet() throws IOException {
    final Random random = random();
    for (int size : SIZES) {
      assertSorts(random, randomKeys(random, size, TestHashedTerms::tinyAlphabet));
    }
    for (int iter = 0; iter < 5; iter++) {
      assertSorts(random, randomKeys(random, randomSize(random), TestHashedTerms::tinyAlphabet));
    }
  }

  /**
   * Every key of up to 4 bytes over {0x00, 0x7F, 0x80, 0xFF}, around which signed and unsigned
   * orders differ, after a lead of random or zero bytes whose length puts these bytes before, at
   * and after the end of the first prefix, and the prefixes of that lead.
   */
  public void testExhaustive() throws IOException {
    final Random random = random();
    final byte[] alphabet = {0x00, 0x7F, (byte) 0x80, (byte) 0xFF};
    for (int leadLength : new int[] {0, 4, 7, 8, 9, 16}) {
      final byte[] lead = new byte[leadLength];
      if (random.nextBoolean()) {
        random.nextBytes(lead);
      }
      final List<BytesRef> keys = new ArrayList<>();
      for (int length = 0; length < leadLength; length++) {
        keys.add(place(random, Arrays.copyOf(lead, length)));
      }
      for (int length = 0; length <= 4; length++) {
        for (int code = 0; code < 1 << (2 * length); code++) {
          final byte[] bytes = Arrays.copyOf(lead, leadLength + length);
          for (int i = 0; i < length; i++) {
            bytes[leadLength + i] = alphabet[(code >>> (2 * i)) & 3];
          }
          keys.add(place(random, bytes));
        }
      }
      for (int iter = 0; iter < 3; iter++) {
        Collections.shuffle(keys, random);
        assertSorts(random, keys.toArray(new BytesRef[0]));
        assertSorts(random, keys.subList(0, random.nextInt(keys.size())).toArray(new BytesRef[0]));
      }
    }
  }

  /**
   * Keys of a few hosts and paths, sharing more than their first 8 bytes, with keys that are
   * prefixes of others: the URLs of a host and its path, and their scheme.
   */
  public void testUrls() throws IOException {
    final Random random = random();
    final int numHosts = TestUtil.nextInt(random, 1, 5);
    final String[] hosts = new String[numHosts];
    for (int h = 0; h < numHosts; h++) {
      hosts[h] = "https://" + (random.nextBoolean() ? "www." : "") + "host" + h + ".example.com";
    }
    final Function<Random, byte[]> url =
        r -> {
          final StringBuilder b = new StringBuilder(hosts[r.nextInt(numHosts)]);
          switch (r.nextInt(8)) {
            case 0 -> {} // a host
            case 1 -> b.setLength(r.nextInt(b.length() + 1)); // a prefix of one
            default -> {
              b.append("/items/");
              if (r.nextBoolean()) {
                b.append(TestUtil.randomSimpleString(r, 0, 3)).append('/');
              }
              b.append(r.nextInt(r.nextBoolean() ? 100 : 1_000_000));
            }
          }
          return b.toString().getBytes(StandardCharsets.UTF_8);
        };
    for (int iter = 0; iter < 5; iter++) {
      assertSorts(random, randomKeys(random, randomSize(random), url));
    }
  }

  /**
   * Keys that share more bytes than are sorted by their prefixes at all levels: prefixes of one
   * another, with or without a random byte after them, so most only differ in their length.
   */
  public void testKeysPrefixesOfOneAnother() throws IOException {
    final Random random = random();
    final byte[] buffer = new byte[200];
    random.nextBytes(buffer);
    if (random.nextBoolean()) {
      Arrays.fill(buffer, random.nextInt(buffer.length), buffer.length, (byte) 0); // zero padding
    }
    final Function<Random, byte[]> next =
        r -> {
          final int length = r.nextInt(buffer.length + 1);
          final byte[] bytes = Arrays.copyOf(buffer, length + (r.nextInt(4) == 0 ? 1 : 0));
          if (bytes.length > length) {
            bytes[length] = (byte) r.nextInt(256);
          }
          return bytes;
        };
    for (int iter = 0; iter < 5; iter++) {
      assertSorts(random, randomKeys(random, randomSize(random), next));
    }
  }

  /**
   * Keys that tie on their prefixes with one of them shorter, so zero-padded: such as {@code
   * [0x61]} and {@code [0x61, 0x00]}, and keys of zero bytes of every length.
   */
  public void testZeroPadding() throws IOException {
    final Random random = random();
    final Function<Random, byte[]> next =
        r -> {
          final byte[] bytes = new byte[r.nextInt(24)];
          if (bytes.length > 0 && r.nextBoolean()) {
            bytes[r.nextInt(bytes.length)] = (byte) (r.nextBoolean() ? 0x01 : 0xFF);
          }
          return bytes;
        };
    for (int iter = 0; iter < 5; iter++) {
      assertSorts(random, randomKeys(random, randomSize(random), next));
    }
  }

  /**
   * Many copies of one to a few keys, shorter or longer than 8 bytes, and sharing their first 8
   * bytes or more, or not.
   */
  public void testDuplicates() throws IOException {
    final Random random = random();
    for (int numDistinct : new int[] {1, 2, TestUtil.nextInt(random, 3, 6)}) {
      final byte[] shared = new byte[TestUtil.nextInt(random, 8, 24)];
      random.nextBytes(shared);
      final byte[][] distinct = new byte[numDistinct][];
      for (int i = 0; i < numDistinct; i++) {
        if (random.nextBoolean()) {
          distinct[i] = Arrays.copyOf(shared, shared.length + random.nextInt(8));
          for (int j = shared.length; j < distinct[i].length; j++) {
            distinct[i][j] = (byte) random.nextInt(256);
          }
        } else {
          distinct[i] = new byte[random.nextBoolean() ? random.nextInt(8) : random.nextInt(40)];
          random.nextBytes(distinct[i]);
        }
      }
      for (int size : new int[] {2, 33, 1000, TestUtil.nextInt(random, 2, 5000)}) {
        assertSorts(random, randomKeys(random, size, r -> distinct[r.nextInt(numDistinct)]));
      }
    }
  }

  /** Several groups of keys, every group sharing a long prefix, and keys with random bytes. */
  public void testGroups() throws IOException {
    final Random random = random();
    final byte[][] groups = new byte[TestUtil.nextInt(random, 1, 20)][];
    for (int g = 0; g < groups.length; g++) {
      groups[g] = new byte[TestUtil.nextInt(random, 0, 30)];
      random.nextBytes(groups[g]);
      if (g > 0 && random.nextBoolean()) {
        // a group sharing some of the bytes of another one
        byte[] other = groups[random.nextInt(g)];
        int shared = Math.min(Math.min(other.length, groups[g].length), random.nextInt(20));
        System.arraycopy(other, 0, groups[g], 0, shared);
      }
    }
    final Function<Random, byte[]> next =
        r -> {
          final byte[] group = groups[r.nextInt(groups.length)];
          final byte[] bytes = Arrays.copyOf(group, group.length + r.nextInt(12));
          for (int i = group.length; i < bytes.length; i++) {
            bytes[i] = (byte) r.nextInt(r.nextBoolean() ? 4 : 256);
          }
          return bytes;
        };
    for (int iter = 0; iter < 5; iter++) {
      assertSorts(random, randomKeys(random, randomSize(random), next));
    }
  }

  public void testEqualsAndHashCode() {
    final Random random = random();
    for (int iter = 0; iter < 10; iter++) {
      final BytesRef[] keys = randomKeys(random, randomSize(random), TestHashedTerms::randomBinary);
      final HashedTerms hashed = new HashedTerms(keys);
      final List<BytesRef> withDuplicates = new ArrayList<>(Arrays.asList(keys));
      for (int i = 0; i < keys.length && i < 100; i++) {
        withDuplicates.add(BytesRef.deepCopyOf(keys[random.nextInt(keys.length)]));
      }
      Collections.shuffle(withDuplicates, random);
      final HashedTerms other = new HashedTerms(withDuplicates);
      assertEquals(hashed, other);
      assertEquals(hashed.hashCode(), other.hashCode());
      assertEquals(hashed.hashCode(), hashed.hashCode());
      if (hashed.size() > 0) {
        // without one key
        final List<BytesRef> fewer = new ArrayList<>();
        final BytesRef removed = hashed.get(random.nextInt(hashed.size()));
        for (BytesRef key : keys) {
          if (key.bytesEquals(removed) == false) {
            fewer.add(key);
          }
        }
        assertFalse(hashed.equals(new HashedTerms(fewer)));
      }
    }
    assertEquals(new HashedTerms(), new HashedTerms(new ArrayList<>()));
    assertEquals(new HashedTerms().hashCode(), new HashedTerms(new ArrayList<>()).hashCode());
    assertFalse(new HashedTerms().equals(new HashedTerms(new BytesRef())));
  }

  /** The tag and control byte of every key, its memory use, and invalid arguments. */
  public void testAccessors() {
    final BytesRef[] keys =
        randomKeys(random(), randomSize(random()), TestHashedTerms::randomBinary);
    final HashedTerms hashed = new HashedTerms(keys);
    for (int i = 0; i < hashed.size(); i++) {
      final long hash = hashed.hash(i);
      assertEquals(TermHash.tag(hash), hashed.tag(i));
      assertEquals(TermHash.control(hash), hashed.control(i));
    }
    assertTrue(hashed.ramBytesUsed() > 0);
    expectThrows(NullPointerException.class, () -> new HashedTerms(new BytesRef("a"), null));
    expectThrows(IndexOutOfBoundsException.class, () -> hashed.get(hashed.size()));
  }

  /**
   * More keys than are sorted by insertion, longer than any term, sharing a long prefix, and each a
   * prefix of the next, so that they share 8 more bytes at every level of the sort. A sort that
   * recursed once per few bytes that keys share would overflow the stack of the thread this runs
   * on.
   */
  public void testLongCommonPrefix() throws Exception {
    final int prefixLength = 256 * 1024;
    final int numKeys = 130;
    final byte[] buffer = new byte[prefixLength + numKeys];
    random().nextBytes(buffer);
    final List<BytesRef> keys = new ArrayList<>();
    for (int i = 0; i < numKeys; i++) {
      keys.add(new BytesRef(buffer, 0, prefixLength + i)); // each one a prefix of the next
    }
    Collections.shuffle(keys, random());
    final AtomicReference<Object> result = new AtomicReference<>();
    final Thread thread =
        new Thread(
            null,
            () -> {
              try {
                result.set(new HashedTerms(keys));
              } catch (Throwable t) {
                result.set(t);
              }
            },
            "hashed-keys",
            256 * 1024);
    thread.start();
    thread.join();
    if (result.get() instanceof Throwable t) {
      throw new AssertionError(t);
    }
    final HashedTerms hashed = (HashedTerms) result.get();
    assertEquals(numKeys, hashed.size());
    for (int i = 0; i < numKeys; i++) {
      assertEquals(new BytesRef(buffer, 0, prefixLength + i), hashed.get(i));
    }
  }
}
