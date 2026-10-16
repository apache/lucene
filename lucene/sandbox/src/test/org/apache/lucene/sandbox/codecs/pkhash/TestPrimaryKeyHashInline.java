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

import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.checkField;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.onlyFile;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.postingsOf;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.randomFilterBitsPerKey;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.randomFormat;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.withoutHeaderAndFooter;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.writeSingleSegment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.Inlining;
import org.apache.lucene.sandbox.search.PrimaryKeyHashInSetQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

/**
 * Fields whose keys all have the same length, of at most {@value
 * PrimaryKeyHashPostingsFormat#MAX_INLINE_KEY_LENGTH} bytes, may have them inline: a record of a
 * doc id and a key per slot, rather than slot words that point to records in term order; by default
 * only if they are short enough to take no more space. Checked: lookups of every kind, for keys of
 * every length that can be inline, of one document and of several, sharing bytes, in tiny and in
 * mixed segments; the same results as with records in term order; and the choice of the layout.
 */
public class TestPrimaryKeyHashInline extends LuceneTestCase {

  /** Distinct random keys of {@code length} bytes; at most 256 of one byte. */
  private static List<BytesRef> randomKeys(Random random, int length, int numKeys) {
    final Set<BytesRef> unique = new HashSet<>();
    final List<BytesRef> keys = new ArrayList<>();
    while (keys.size() < numKeys) {
      final byte[] bytes = new byte[length];
      random.nextBytes(bytes);
      final BytesRef key = new BytesRef(bytes);
      if (unique.add(key)) {
        keys.add(key);
      }
    }
    return keys;
  }

  /**
   * Fields of keys of 1 to 60 bytes, around the lengths that change how keys are compared (at most
   * 4 bytes, in the first 8 bytes of a record; up to 7, with them and an int in its last 8 bytes;
   * up to 12, with them and its last 8 bytes; then with the bytes between them too), at the most
   * that can be inline, and one byte more, which cannot be. Some keys have two documents.
   */
  public void testFixedLengthFields() throws IOException {
    final List<Integer> lengths =
        new ArrayList<>(List.of(1, 4, 5, 6, 8, 10, 12, 13, 16, 36, 60, 61));
    lengths.add(
        TestUtil.nextInt(random(), 1, PrimaryKeyHashPostingsFormat.MAX_INLINE_KEY_LENGTH + 4));
    for (int keyLength : lengths) {
      final Random random = random();
      final int numKeys =
          keyLength == 1 ? TestUtil.nextInt(random, 50, 200) : TestUtil.nextInt(random, 200, 3000);
      final List<BytesRef> keys = randomKeys(random, keyLength, numKeys);
      final List<BytesRef> docKeys = new ArrayList<>(keys);
      for (BytesRef key : keys) {
        if (random.nextInt(10) == 0) {
          docKeys.add(key);
        }
      }
      Collections.shuffle(docKeys, random);
      final int filterBitsPerKey = randomFilterBitsPerKey(random);
      final String message = keyLength + "-byte keys";
      try (Directory dir = newDirectory()) {
        writeSingleSegment(dir, randomFormat(random, filterBitsPerKey, Inlining.ALWAYS), docKeys);
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          final LeafReader leaf = getOnlyLeafReader(reader);
          final HashTerms terms = (HashTerms) leaf.terms("id");
          final boolean inline = keyLength <= PrimaryKeyHashPostingsFormat.MAX_INLINE_KEY_LENGTH;
          assertEquals(message, inline, terms.isInline());
          assertEquals(filterBitsPerKey != 0, terms.hasFilter());
          assertEquals(message, inline ? keyLength : -1, terms.keyLength());
          assertEquals(message, inline ? Integer.BYTES + keyLength : 0, terms.recordStride());
          // and keys of other lengths, one byte shorter and longer
          final List<BytesRef> probes = new ArrayList<>(keys);
          for (int i = 0; i < 100; i++) {
            final BytesRef key = keys.get(random.nextInt(keys.size()));
            probes.add(new BytesRef(key.bytes, key.offset, key.length - 1));
            probes.add(new BytesRef(Arrays.copyOf(BytesRef.deepCopyOf(key).bytes, key.length + 1)));
          }
          checkField(leaf, "id", probes, true);
        }
      }
    }
  }

  /**
   * The same documents, with updates, deletes and merges, written with inline keys and with records
   * in term order: the same results for every way of looking keys up, segment by segment and in the
   * whole index.
   */
  public void testSameResultsWithAndWithoutInlineKeys() throws IOException {
    final long seed = random().nextLong();
    final int filterBitsPerKey = randomFilterBitsPerKey(random());
    PrimaryKeyHashTestUtil.assertSameResults(
        randomFormat(new Random(seed), filterBitsPerKey, Inlining.NEVER),
        randomFormat(new Random(seed), filterBitsPerKey, Inlining.ALWAYS),
        random().nextInt(3), // binary UUIDs, UUID strings or numbers
        (expected, actual) -> {
          assertFalse(expected.isInline());
          assertTrue(actual.isInline());
        });
  }

  /**
   * By default, keys are inline only if they are no longer than {@link
   * HashFieldsConsumer#defaultMaxInlineKeyLength} at the field's load factor. The same keys, of one
   * length from 1 to 60 bytes, written with keys inline by default, whenever they can be, and
   * never: by default, the data and meta files are those of one of the other two, byte for byte,
   * the inline one if and only if the keys are that short. Fields of a few keys to a few thousand,
   * at any load factor, in partitions and spilled, with a filter or not.
   */
  public void testInlineOnlyIfNotLarger() throws IOException {
    assertEquals(
        31,
        HashFieldsConsumer.defaultMaxInlineKeyLength(
            PrimaryKeyHashPostingsFormat.DEFAULT_LOAD_FACTOR));
    assertEquals(13, HashFieldsConsumer.defaultMaxInlineKeyLength(0.5f));
    assertEquals(
        PrimaryKeyHashPostingsFormat.MAX_INLINE_KEY_LENGTH,
        HashFieldsConsumer.defaultMaxInlineKeyLength(0.9f));
    for (int iter = 0; iter < 5; iter++) {
      final Random random = random();
      final int keyLength =
          TestUtil.nextInt(random, 1, PrimaryKeyHashPostingsFormat.MAX_INLINE_KEY_LENGTH);
      int numKeys =
          random.nextBoolean()
              ? TestUtil.nextInt(random, 1, 50)
              : TestUtil.nextInt(random, 50, 3000);
      if (keyLength == 1) {
        numKeys = Math.min(numKeys, 256);
      }
      final List<BytesRef> keys = randomKeys(random, keyLength, numKeys);
      // the same options for every layout; randomFormat draws the load factor first
      final long seed = random.nextLong();
      final float loadFactor = 0.5f + 0.4f * new Random(seed).nextFloat();
      final int filterBitsPerKey = randomFilterBitsPerKey(random);
      final List<byte[]> files = new ArrayList<>();
      final boolean[] inline = new boolean[Inlining.values().length];
      for (Inlining inlining : Inlining.values()) {
        try (Directory dir = new ByteBuffersDirectory()) {
          final String data =
              writeSingleSegment(
                  dir, randomFormat(new Random(seed), filterBitsPerKey, inlining), keys);
          files.add(withoutHeaderAndFooter(dir, data));
          files.add(
              withoutHeaderAndFooter(
                  dir, onlyFile(dir, PrimaryKeyHashPostingsFormat.META_EXTENSION)));
          try (DirectoryReader reader = DirectoryReader.open(dir)) {
            inline[inlining.ordinal()] =
                ((HashTerms) getOnlyLeafReader(reader).terms("id")).isInline();
          }
        }
      }
      final boolean expected =
          keyLength <= HashFieldsConsumer.defaultMaxInlineKeyLength(loadFactor);
      final String message =
          String.format(
              Locale.ROOT, "%d keys of %d bytes, load factor %.3f", numKeys, keyLength, loadFactor);
      assertFalse(message, inline[Inlining.NEVER.ordinal()]);
      assertTrue(message, inline[Inlining.ALWAYS.ordinal()]);
      assertEquals(message, expected, inline[Inlining.IF_NOT_LARGER.ordinal()]);
      final int chosen = (expected ? Inlining.ALWAYS : Inlining.NEVER).ordinal();
      final int byDefault = Inlining.IF_NOT_LARGER.ordinal();
      assertArrayEquals(message, files.get(2 * chosen), files.get(2 * byDefault));
      assertArrayEquals(message, files.get(2 * chosen + 1), files.get(2 * byDefault + 1));
    }
  }

  /**
   * The layout chosen by default, at the default load factor, for keys of the lengths of common
   * ids: keys of 6, 10 and 16 bytes (sequence numbers, ASINs, binary UUIDs) are inline, and keys of
   * 36 bytes (UUID strings) are in term order. In one partition, and spilled in several.
   */
  public void testDefaultLayoutOfCommonKeyLengths() throws IOException {
    final int[] lengths = {6, 10, 16, 36};
    final boolean[] inline = {true, true, true, false};
    for (int i = 0; i < lengths.length; i++) {
      final List<BytesRef> keys =
          randomKeys(random(), lengths[i], TestUtil.nextInt(random(), 2000, 4000));
      for (int maxKeysPerPartition :
          new int[] {PrimaryKeyHashPostingsFormat.DEFAULT_MAX_KEYS_PER_PARTITION, 256}) {
        try (Directory dir = new ByteBuffersDirectory()) {
          writeSingleSegment(
              dir,
              PrimaryKeyHashTestUtil.format(
                  PrimaryKeyHashPostingsFormat.DEFAULT_LOAD_FACTOR,
                  maxKeysPerPartition,
                  randomFilterBitsPerKey(random()),
                  Inlining.IF_NOT_LARGER),
              keys);
          try (DirectoryReader reader = DirectoryReader.open(dir)) {
            final HashTerms terms = (HashTerms) getOnlyLeafReader(reader).terms("id");
            assertEquals(maxKeysPerPartition == 256, terms.numPartitions() > 1);
            assertEquals(lengths[i] + " bytes", inline[i], terms.isInline());
          }
        }
      }
    }
  }

  /**
   * A field of keys of one byte, all of which have several documents: every present key is sought
   * in the delegate.
   */
  public void testOnlyMultiDocumentKeys() throws IOException {
    final List<BytesRef> docKeys = new ArrayList<>();
    for (int i = 0; i < 300; i++) {
      docKeys.add(new BytesRef(Integer.toString(i % 7)));
    }
    try (Directory dir = newDirectory()) {
      writeSingleSegment(dir, randomFormat(random(), 0, Inlining.ALWAYS), docKeys);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        final LeafReader leaf = getOnlyLeafReader(reader);
        final HashTerms terms = (HashTerms) leaf.terms("id");
        assertTrue(terms.isInline());
        assertEquals(1, terms.keyLength());
        final HashedTerms keys = new HashedTerms(postingsOf(terms.getDelegate()).keySet());
        assertEquals(7, keys.size());
        final int[] docs = new int[keys.size()];
        terms.lookupBatch(keys, 0, keys.size(), docs);
        for (int doc : docs) {
          assertEquals(HashTerms.MULTI, doc);
        }
        final List<BytesRef> probes = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
          probes.add(new BytesRef(Integer.toString(i)));
        }
        checkField(leaf, "id", probes, true);
      }
    }
  }

  /**
   * A key of {@code length} bytes of one of four shapes: random bytes; the first 4 bytes of {@code
   * prefix}, which the heads of records hold, then bytes of a small alphabet if there are more than
   * 6 of them, so that keys differ in a byte or two; mostly zero bytes, as the records of empty
   * slots are; or {@code prefix} but for a byte, the first one after the head or the last one.
   */
  private static BytesRef adversarialKey(Random random, int length, int shape, byte[] prefix) {
    final byte[] bytes = new byte[length];
    switch (shape) {
      case 0 -> random.nextBytes(bytes);
      case 1 -> {
        final int from = Math.min(Integer.BYTES, length - 1);
        System.arraycopy(prefix, 0, bytes, 0, from);
        for (int i = from; i < length; i++) {
          bytes[i] = (byte) random.nextInt(length - from > 6 ? 3 : 256);
        }
      }
      case 2 -> {
        for (int i = 0; i < length; i++) {
          bytes[i] = random.nextInt(4) == 0 ? (byte) random.nextInt(2) : 0;
        }
      }
      default -> {
        System.arraycopy(prefix, 0, bytes, 0, length);
        bytes[random.nextBoolean() ? Math.min(Integer.BYTES, length - 1) : length - 1] =
            (byte) random.nextInt(256);
      }
    }
    return new BytesRef(bytes);
  }

  private static Document doc(BytesRef key) {
    Document doc = new Document();
    doc.add(new StringField("id", key, Field.Store.NO));
    return doc;
  }

  /**
   * Checks every way to look up {@code probes} in the id field of {@code reader}, segment by
   * segment and in the whole reader, against the delegate; and that every segment whose keys all
   * have the same length, of at most {@value PrimaryKeyHashPostingsFormat#MAX_INLINE_KEY_LENGTH}
   * bytes, has them inline.
   */
  private static void checkAllLookups(DirectoryReader reader, List<BytesRef> probes)
      throws IOException {
    for (LeafReaderContext context : reader.leaves()) {
      if (context.reader().terms("id") instanceof HashTerms table) {
        final Set<Integer> termLengths = new HashSet<>();
        for (BytesRef term : postingsOf(table.getDelegate()).keySet()) {
          termLengths.add(term.length);
        }
        final int length = termLengths.iterator().next();
        assertEquals(
            termLengths.size() == 1
                && length >= 1
                && length <= PrimaryKeyHashPostingsFormat.MAX_INLINE_KEY_LENGTH,
            table.isInline());
      }
      checkField(context.reader(), "id", probes, true);
    }
    TestPrimaryKeyHashLookup.checkLookups(reader, "id", probes);
    final IndexSearcher searcher = newSearcher(reader);
    final List<BytesRef> some = probes.subList(0, Math.min(probes.size(), 1000));
    assertEquals(
        searcher.count(new TermInSetQuery("id", some)),
        searcher.count(new PrimaryKeyHashInSetQuery("id", some)));
  }

  /**
   * Keys of one length that make lookups compare more than one record: keys that share their first
   * 4 bytes, whose records then differ after their heads, or only in their last byte; keys of
   * mostly zero bytes, as the records of empty slots are; and keys of 1 to 3 bytes, of which most
   * values are present, so that control bytes often match and the heads of records hold bytes of
   * the next record too. Then the absent keys of the same shapes, and for keys of 1 or 2 bytes,
   * every value or many. With updates, deletes, flushes and merges.
   */
  public void testKeysThatShareBytes() throws IOException {
    final Random random = random();
    final int[] lengths = {1, 2, 3, 4, 5, 7, 8, 9, 12, 13, 16, 59, 60};
    final int length =
        random.nextBoolean()
            ? lengths[random.nextInt(lengths.length)]
            : TestUtil.nextInt(random, 1, PrimaryKeyHashPostingsFormat.MAX_INLINE_KEY_LENGTH);
    final int shape = random.nextInt(4);
    final byte[] prefix = new byte[length];
    random.nextBytes(prefix);
    final List<BytesRef> keys = new ArrayList<>();
    final Set<BytesRef> all = new HashSet<>();
    try (Directory dir = newDirectory()) {
      final IndexWriterConfig iwc =
          new IndexWriterConfig(new MockAnalyzer(random))
              .setCodec(TestUtil.alwaysPostingsFormat(inlineFormat(random)))
              .setMaxBufferedDocs(TestUtil.nextInt(random, 2, 400))
              .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
              .setMergeScheduler(new SerialMergeScheduler());
      if (random.nextBoolean()) {
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
      } else {
        final LogDocMergePolicy mergePolicy = new LogDocMergePolicy();
        mergePolicy.setMergeFactor(TestUtil.nextInt(random, 2, 10));
        iwc.setMergePolicy(mergePolicy);
      }
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        final int numDocs = TestUtil.nextInt(random, 1, 2000);
        for (int i = 0; i < numDocs; i++) {
          final int op = keys.isEmpty() ? 0 : random.nextInt(10);
          final BytesRef key =
              op == 1
                  ? keys.get(random.nextInt(keys.size())) // another document of a key
                  : adversarialKey(random, length, shape, prefix);
          keys.add(key);
          all.add(key);
          if (op == 2) {
            writer.updateDocument(new Term("id", key), doc(key));
          } else {
            writer.addDocument(doc(key));
          }
          if (op == 3) {
            writer.deleteDocuments(new Term("id", keys.get(random.nextInt(keys.size()))));
          }
        }
        if (random.nextBoolean()) {
          writer.forceMerge(1);
        }
      }
      final List<BytesRef> probes = new ArrayList<>(all);
      for (int i = 0; i < 300; i++) {
        probes.add(adversarialKey(random, length, shape, prefix));
        probes.add(adversarialKey(random, length, random.nextInt(4), prefix));
      }
      if (length <= 2) {
        for (int i = 0; i < 256; i++) {
          final int value = length == 1 ? i : random.nextInt(1 << 16);
          probes.add(
              new BytesRef(
                  length == 1
                      ? new byte[] {(byte) value}
                      : new byte[] {(byte) value, (byte) (value >>> 8)}));
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        checkAllLookups(reader, probes);
      }
    }
  }

  /** A format with random options and inline keys, with a filter half of the time. */
  private static PrimaryKeyHashPostingsFormat inlineFormat(Random random) {
    return randomFormat(random, randomFilterBitsPerKey(random), Inlining.ALWAYS);
  }

  /**
   * Segments of a few keys each, whose tables have a few slots: keys are often at the last home
   * slot or in the tail after it, and their records the last of the field's slice.
   */
  public void testTinySegments() throws IOException {
    final Random random = random();
    final int length =
        TestUtil.nextInt(random, 1, PrimaryKeyHashPostingsFormat.MAX_INLINE_KEY_LENGTH);
    final Set<BytesRef> all = new HashSet<>();
    try (Directory dir = newDirectory()) {
      final IndexWriterConfig iwc =
          new IndexWriterConfig(new MockAnalyzer(random))
              .setCodec(TestUtil.alwaysPostingsFormat(inlineFormat(random)))
              .setMaxBufferedDocs(TestUtil.nextInt(random, 2, 12))
              .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
              .setMergePolicy(NoMergePolicy.INSTANCE);
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        final int numDocs = TestUtil.nextInt(random, 1, 200);
        for (int i = 0; i < numDocs; i++) {
          final BytesRef key = adversarialKey(random, length, 0, null);
          all.add(key);
          writer.addDocument(doc(key));
        }
      }
      final List<BytesRef> probes = new ArrayList<>(all);
      for (int i = 0; i < 200; i++) {
        probes.add(adversarialKey(random, length, 0, null));
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        checkAllLookups(reader, probes);
      }
    }
  }

  /**
   * Segments whose keys have different lengths, each its own, inline or not, and segments that
   * merges left with keys of several lengths, with updates and deletes across them: every key is
   * found in the segments of its length, and only there.
   */
  public void testSegmentsOfDifferentKeyLengths() throws IOException {
    final Random random = random();
    final int[] lengths = new int[TestUtil.nextInt(random, 2, 4)];
    for (int i = 0; i < lengths.length; i++) {
      lengths[i] =
          TestUtil.nextInt(random, 1, PrimaryKeyHashPostingsFormat.MAX_INLINE_KEY_LENGTH + 2);
    }
    final byte[] prefix = new byte[PrimaryKeyHashPostingsFormat.MAX_INLINE_KEY_LENGTH + 2];
    random.nextBytes(prefix);
    final List<BytesRef> keys = new ArrayList<>();
    final Set<BytesRef> all = new HashSet<>();
    try (Directory dir = newDirectory()) {
      final IndexWriterConfig iwc =
          new IndexWriterConfig(new MockAnalyzer(random))
              .setCodec(TestUtil.alwaysPostingsFormat(inlineFormat(random)))
              .setMaxBufferedDocs(100_000)
              .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
              .setMergePolicy(
                  random.nextInt(3) == 0 ? new LogDocMergePolicy() : NoMergePolicy.INSTANCE)
              .setMergeScheduler(new SerialMergeScheduler());
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        final int numSegments = TestUtil.nextInt(random, 2, 6);
        for (int s = 0; s < numSegments; s++) {
          final int length = lengths[random.nextInt(lengths.length)];
          final int numDocs = TestUtil.nextInt(random, 1, 300);
          for (int i = 0; i < numDocs; i++) {
            final int op = keys.isEmpty() ? 0 : random.nextInt(8);
            final BytesRef key =
                op == 1
                    ? keys.get(random.nextInt(keys.size()))
                    : adversarialKey(random, length, random.nextInt(4), prefix);
            keys.add(key);
            all.add(key);
            if (op == 2) {
              writer.updateDocument(new Term("id", key), doc(key));
            } else {
              writer.addDocument(doc(key));
            }
            if (op == 3) {
              writer.deleteDocuments(new Term("id", keys.get(random.nextInt(keys.size()))));
            }
          }
          writer.flush();
        }
      }
      final List<BytesRef> probes = new ArrayList<>(all);
      for (int i = 0; i < 300; i++) {
        final int length = lengths[random.nextInt(lengths.length)];
        probes.add(adversarialKey(random, length, random.nextInt(4), prefix));
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        checkAllLookups(reader, probes);
      }
    }
  }
}
