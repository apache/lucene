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
import java.util.Locale;
import java.util.Random;
import java.util.TreeSet;
import java.util.UUID;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOBooleanSupplier;

/**
 * Comparing keys with a field's smallest and largest terms through their first 8 bytes ({@link
 * HashTerms#prefix}), or through the 8 bytes after the prefix that the two terms share when they
 * share their first 8 bytes, decides exactly as comparing whole keys does: for short keys, the
 * terms and their prefixes, keys that differ from them only after 8 bytes or in trailing zero
 * bytes, edge bytes, and keys of schemes whose keys all share 8 bytes or more, such as URLs.
 */
public class TestPrimaryKeyHashRange extends LuceneTestCase {

  /** Bytes at the edges of signed and unsigned order, and of zero padding. */
  private static final byte[] EDGE_BYTES = {
    0x00, 0x01, 0x7F, (byte) 0x80, (byte) 0xFE, (byte) 0xFF
  };

  /** A key of mostly {@link #EDGE_BYTES}, of 6 to 10 bytes more often than not. */
  private static byte[] randomKey(Random random) {
    int length = random.nextInt(3) == 0 ? random.nextInt(20) : TestUtil.nextInt(random, 6, 10);
    byte[] key = new byte[length];
    for (int i = 0; i < length; i++) {
      key[i] =
          random.nextInt(4) == 0
              ? (byte) random.nextInt(256)
              : EDGE_BYTES[random.nextInt(EDGE_BYTES.length)];
    }
    return key;
  }

  /**
   * Keys close to {@code key}: the key, its prefixes, the key extended with zero bytes and with
   * other bytes, and the key with one byte changed.
   */
  private static List<byte[]> neighbors(byte[] key, Random random) {
    List<byte[]> neighbors = new ArrayList<>();
    neighbors.add(key.clone());
    for (int length = 0; length <= key.length + 9; length++) {
      neighbors.add(Arrays.copyOf(key, length)); // prefixes, and the key followed by zero bytes
    }
    for (byte b : EDGE_BYTES) {
      byte[] longer = Arrays.copyOf(key, key.length + 1);
      longer[key.length] = b;
      neighbors.add(longer);
    }
    for (int i = 0; i < Math.min(key.length, 12); i++) {
      for (int delta : new int[] {-1, 1, 0x80}) {
        byte[] changed = key.clone();
        changed[i] += (byte) delta;
        neighbors.add(changed);
      }
      byte[] changed = key.clone();
      changed[i] = EDGE_BYTES[random.nextInt(EDGE_BYTES.length)];
      neighbors.add(changed);
    }
    // the same first 8 bytes, then anything
    byte[] samePrefix = Arrays.copyOf(key, Math.min(key.length, Long.BYTES));
    byte[] suffix = randomKey(random);
    byte[] extended = Arrays.copyOf(samePrefix, samePrefix.length + suffix.length);
    System.arraycopy(suffix, 0, extended, samePrefix.length, suffix.length);
    neighbors.add(extended);
    return neighbors;
  }

  /**
   * {@code key} at a random offset of a larger array of random bytes, half of them 0xFF, so that
   * reading past the key shows.
   */
  private static BytesRef embed(byte[] key, Random random) {
    int offset = random.nextInt(5);
    byte[] bytes = new byte[offset + key.length + random.nextInt(12)];
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = random.nextBoolean() ? (byte) 0xFF : (byte) random.nextInt(256);
    }
    System.arraycopy(key, 0, bytes, offset, key.length);
    return new BytesRef(bytes, offset, key.length);
  }

  private static long prefix(BytesRef key) {
    return HashTerms.prefix(key.bytes, key.offset, key.length);
  }

  /** The first 8 bytes of every length of key, read big-endian, zero-padded and never past it. */
  public void testPrefix() {
    final Random random = random();
    for (int iter = 0; iter < 1000; iter++) {
      byte[] key = new byte[random.nextInt(20)];
      random.nextBytes(key);
      long expected = 0;
      for (int i = 0; i < Long.BYTES; i++) {
        expected = (expected << 8) | (i < key.length ? key[i] & 0xFF : 0);
      }
      assertEquals(expected, prefix(embed(key, random)));
    }
    assertEquals(0L, prefix(new BytesRef()));
    assertEquals(0xFF00000000000000L, prefix(new BytesRef(new byte[] {(byte) 0xFF})));
    assertEquals(0x6100000000000000L, prefix(new BytesRef(new byte[] {0x61, 0x00})));
    assertEquals(
        0x0102030405060708L, prefix(new BytesRef(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10})));
  }

  /**
   * The offset that ranges are checked from is 0 unless the smallest and largest terms share their
   * first 8 bytes, and then the length of the prefix that they share.
   */
  public void testRangeOffset() {
    assertEquals(0, rangeOffset("a", "b"));
    assertEquals(0, rangeOffset("abcdefg1", "abcdefg2"));
    assertEquals(0, rangeOffset("abcdefg", "abcdefgh"));
    assertEquals(8, rangeOffset("abcdefgh", "abcdefgh1"));
    assertEquals(8, rangeOffset("abcdefgh1", "abcdefgh2"));
    assertEquals(9, rangeOffset("abcdefghi", "abcdefghi")); // a single term
    assertEquals(20, rangeOffset("https://example.com/a1", "https://example.com/b2"));
    assertEquals(11, rangeOffset("0000000001234", "0000000001299"));
  }

  private static int rangeOffset(String min, String max) {
    return HashTerms.rangeOffset(new BytesRef(min), new BytesRef(max));
  }

  /**
   * A key of a scheme whose keys all start with the same 8 bytes or more: URLs, ARNs, UUID URNs or
   * zero-padded 16-digit numbers; keys close to each other share even more.
   */
  private static byte[] schemeKey(Random random, int scheme, int base) {
    final String key =
        switch (scheme) {
          case 0 -> "https://example.com/item/" + (base + random.nextInt(1000));
          case 1 -> "arn:aws:s3:::bucket/" + Integer.toHexString(base + random.nextInt(4096));
          case 2 ->
              "urn:uuid:" + new UUID(base, random.nextInt(random.nextBoolean() ? 16 : 1 << 20));
          default -> String.format(Locale.ROOT, "%016d", base + random.nextInt(2000));
        };
    return key.getBytes(StandardCharsets.UTF_8);
  }

  /**
   * On real segments, {@link HashTerms#outOfRange} decides exactly as whole-key comparisons with
   * the smallest and largest terms, and seekExact and prepareSeekExact find exactly the indexed
   * keys.
   */
  public void testRangeEquivalence() throws IOException {
    final Random random = random();
    final int iters = atLeast(10);
    for (int iter = 0; iter < iters; iter++) {
      // keys of their own, or neighbors of one key, so that the smallest and largest terms often
      // share their first 8 bytes, or are prefixes of each other
      final TreeSet<BytesRef> indexed = new TreeSet<>();
      final int numKeys = TestUtil.nextInt(random, 1, 20);
      final int kind = random.nextInt(3);
      if (kind == 0) {
        while (indexed.size() < numKeys) {
          indexed.add(new BytesRef(randomKey(random)));
        }
      } else if (kind == 1) {
        // keys that all share their first 8 bytes or more
        final int scheme = random.nextInt(4);
        final int base = random.nextInt(1_000_000);
        while (indexed.size() < numKeys) {
          indexed.add(new BytesRef(schemeKey(random, scheme, base)));
        }
      } else {
        List<byte[]> neighbors = neighbors(randomKey(random), random);
        for (int i = 0; i < numKeys; i++) {
          indexed.add(new BytesRef(neighbors.get(random.nextInt(neighbors.size()))));
        }
      }
      final BytesRef min = indexed.first();
      final BytesRef max = indexed.last();
      final List<byte[]> probes = new ArrayList<>();
      for (BytesRef term : new BytesRef[] {min, max}) {
        probes.addAll(neighbors(BytesRef.deepCopyOf(term).bytes, random));
      }
      for (BytesRef term : indexed) {
        probes.add(BytesRef.deepCopyOf(term).bytes);
      }
      for (int i = 0; i < 50; i++) {
        probes.add(randomKey(random));
      }
      // keys of every scheme, near the indexed ones and away from them
      for (int i = 0; i < 50; i++) {
        probes.add(schemeKey(random, random.nextInt(4), random.nextInt(1_000_000)));
      }
      checkRange(indexed, probes);
    }
  }

  /** Short keys around and inside a range whose terms are prefixes of each other. */
  public void testShortTermsThatArePrefixes() throws IOException {
    final TreeSet<BytesRef> indexed = new TreeSet<>();
    indexed.add(new BytesRef(new byte[] {0x61}));
    indexed.add(new BytesRef(new byte[] {0x61, 0x00, 0x00}));
    indexed.add(new BytesRef(new byte[] {0x61, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}));
    final List<byte[]> probes = new ArrayList<>();
    for (int length = 0; length <= 12; length++) {
      probes.add(Arrays.copyOf(new byte[] {0x61}, length));
      byte[] ones = Arrays.copyOf(new byte[] {0x61}, length);
      if (length > 1) {
        ones[length - 1] = 0x01;
      }
      probes.add(ones);
    }
    probes.add(new byte[] {0x60, (byte) 0xFF});
    probes.add(new byte[] {0x62});
    checkRange(indexed, probes);
  }

  private static void checkRange(TreeSet<BytesRef> indexed, List<byte[]> probeBytes)
      throws IOException {
    final Random random = random();
    final Codec codec = TestUtil.alwaysPostingsFormat(PrimaryKeyHashTestUtil.randomFormat(random));
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          new IndexWriterConfig(new MockAnalyzer(random))
              .setCodec(codec)
              .setMergePolicy(NoMergePolicy.INSTANCE)
              .setMaxBufferedDocs(indexed.size() + 1)
              .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        List<BytesRef> shuffled = new ArrayList<>(indexed);
        Collections.shuffle(shuffled, random);
        for (BytesRef key : shuffled) {
          Document doc = new Document();
          doc.add(new StringField("id", key, Field.Store.NO));
          writer.addDocument(doc);
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        final HashTerms terms = (HashTerms) getOnlyLeafReader(reader).terms("id");
        final BytesRef min = terms.getMin();
        final BytesRef max = terms.getMax();
        assertEquals(indexed.first(), min);
        assertEquals(indexed.last(), max);
        final List<BytesRef> probes = new ArrayList<>();
        for (byte[] probe : probeBytes) {
          probes.add(embed(probe, random));
        }
        for (BytesRef probe : probes) {
          boolean expected = probe.compareTo(min) < 0 || probe.compareTo(max) > 0;
          assertEquals(
              probe + " in [" + min + ", " + max + "]",
              expected,
              terms.outOfRange(probe.bytes, probe.offset, probe.length));
          boolean present = indexed.contains(probe);
          assertFalse(expected && present);
          TermsEnum termsEnum = terms.iterator();
          assertEquals(probe.toString(), present, termsEnum.seekExact(probe));
          IOBooleanSupplier supplier = terms.iterator().prepareSeekExact(probe);
          assertEquals(probe.toString(), present, supplier != null && supplier.get());
        }
      }
    }
  }
}
