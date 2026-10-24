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
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.postingsOf;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.randomFilterBits;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.randomInlining;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.randomKey;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;

/**
 * The register-blocked Bloom filter of a field: its words and bits are those its documentation
 * gives, it never rules out a key that is there, however keys are looked up and the segment was
 * written, and its false positives are as rare as documented.
 */
public class TestPrimaryKeyHashFilter extends LuceneTestCase {

  /**
   * Words, bits and sizes are those the filter's documentation gives, computed independently: bits
   * 30 to 63 of the filter hash, as a fraction of 2^34, times the number of words, choose the word,
   * and bits {@code 6i} to {@code 6i + 5} the position of the key's bit {@code i}, neither
   * depending on the other's bits. Whatever the number of partitions, a key's bits are those of its
   * mix, which batches compute once for all fields. The mix and the filter hash, which filters on
   * disk depend on, have golden values.
   */
  public void testLayout() {
    final Random random = random();
    final long positionBits = (1L << BlockedBloomFilter.POSITION_HASH_BITS) - 1;
    for (int iter = 0; iter < 10_000; iter++) {
      final long filterHash = iter == 0 ? 0 : iter == 1 ? -1L : random.nextLong();
      final int numWords =
          random.nextBoolean()
              ? TestUtil.nextInt(random, 1, 100)
              : TestUtil.nextInt(random, 1, BlockedBloomFilter.MAX_WORDS);
      // the high 34 bits, times the number of words, then the bits above the low 34 of that
      final long expectedWord =
          BigInteger.valueOf(filterHash >>> 30)
              .multiply(BigInteger.valueOf(numWords))
              .shiftRight(34)
              .longValueExact();
      final int word = BlockedBloomFilter.word(filterHash, numWords);
      assertEquals(expectedWord, word);
      assertEquals((long) word * Long.BYTES, BlockedBloomFilter.wordOffset(filterHash, numWords));
      long expectedMask = 0;
      for (int i = 0; i < BlockedBloomFilter.K; i++) {
        expectedMask |= 1L << ((filterHash >>> (6 * i)) & 63);
      }
      final long mask = BlockedBloomFilter.mask(filterHash);
      assertEquals(expectedMask, mask);
      // the word does not depend on the low 30 bits, nor the bits on the others
      final long other = random.nextLong();
      assertEquals(
          word,
          BlockedBloomFilter.word((filterHash & ~positionBits) | (other & positionBits), numWords));
      assertEquals(
          mask, BlockedBloomFilter.mask((filterHash & positionBits) | (other & ~positionBits)));
      // the same value, taken as the hash of a key
      final int partitionBits =
          TestUtil.nextInt(random, 0, PrimaryKeyHashPostingsFormat.MAX_PARTITION_BITS);
      assertEquals(
          BlockedBloomFilter.mask(TermHash.mix(filterHash)),
          BlockedBloomFilter.mask(TermHash.filterHash(filterHash, partitionBits)));
    }
    assertEquals(
        BlockedBloomFilter.MAX_WORDS - 1,
        BlockedBloomFilter.word(-1L, BlockedBloomFilter.MAX_WORDS));
    // of the hash of "abc"
    assertEquals(0x8c123698fd5b7a1bL, TermHash.mix(0x18ce44bde5a6523dL));
    assertEquals(0x18d23698fd5b7a1bL, TermHash.filterHash(0x18ce44bde5a6523dL, 10));

    // as many words of 64 bits as the keys take, at least one
    assertEquals(1, BlockedBloomFilter.numWords(0, 8));
    assertEquals(1, BlockedBloomFilter.numWords(8, 8));
    assertEquals(2, BlockedBloomFilter.numWords(9, 8));
    assertEquals(157, BlockedBloomFilter.numWords(1000, 10));
    assertEquals(1L << 31, BlockedBloomFilter.numWords(1L << 34, 8));

    // a word lets a key through if and only if it has all of the key's bits, the sign bit included
    for (int iter = 0; iter < 1000; iter++) {
      final long mask = iter == 0 ? Long.MIN_VALUE : BlockedBloomFilter.mask(random.nextLong());
      assertEquals(1, BlockedBloomFilter.mayContain(-1L, mask));
      assertEquals(1, BlockedBloomFilter.mayContain(mask | random.nextLong(), mask));
      assertEquals(0, BlockedBloomFilter.mayContain(0, mask));
      for (long bits = mask; bits != 0; bits &= bits - 1) {
        assertEquals(0, BlockedBloomFilter.mayContain(mask & ~Long.lowestOneBit(bits), mask));
      }
    }
  }

  /** Per word, the rate at which an absent key passes: its fraction of set bits, to the power K. */
  private static double[] passRates(long[] words) {
    final double[] rates = new double[words.length];
    for (int w = 0; w < words.length; w++) {
      rates[w] = Math.pow(Long.bitCount(words[w]) / (double) Long.SIZE, BlockedBloomFilter.K);
    }
    return rates;
  }

  private static double mean(double[] values) {
    return Arrays.stream(values).sum() / values.length;
  }

  private static double standardError(double[] values) {
    final double mean = mean(values);
    final double sum = Arrays.stream(values).map(v -> (v - mean) * (v - mean)).sum();
    return Math.sqrt(sum / (values.length - 1) / values.length);
  }

  /**
   * At every number of bits per key: no added key is ruled out, absent keys pass as often as the
   * set bits say, and at the documented rate where {@link BlockedBloomFilter} gives one.
   */
  public void testFalsePositiveRates() {
    final Map<Integer, Double> documented =
        Map.of(4, 0.20, 6, 0.076, 8, 0.034, 10, 0.0178, 12, 0.0104, 16, 0.0043);
    final Random random = random();
    final int numProbes = 20_000;
    for (int bitsPerKey = PrimaryKeyHashPostingsFormat.MIN_FILTER_BITS_PER_KEY;
        bitsPerKey <= PrimaryKeyHashPostingsFormat.MAX_FILTER_BITS_PER_KEY;
        bitsPerKey++) {
      final int numKeys = TestUtil.nextInt(random, 20_000, 40_000);
      final long[] words = new long[(int) BlockedBloomFilter.numWords(numKeys, bitsPerKey)];
      final long[] hashes = new long[numKeys];
      for (int i = 0; i < numKeys; i++) {
        BytesRef key = randomKey(random, 0, i);
        hashes[i] = TermHash.hash(key.bytes, key.offset, key.length);
        BlockedBloomFilter.add(words, TermHash.filterHash(hashes[i], 0));
      }
      for (long hash : hashes) {
        assertTrue(passes(words, hash));
      }
      int passed = 0;
      for (int i = 0; i < numProbes; i++) {
        byte[] probe = new byte[17]; // never one of the keys
        random.nextBytes(probe);
        if (passes(words, TermHash.hash(probe, 0, probe.length))) {
          passed++;
        }
      }
      final double[] rates = passRates(words);
      final double expected = mean(rates);
      final String message =
          String.format(
              Locale.ROOT,
              "%d bits per key: %d of %d probes passed, the bits of the filter say %.5f",
              bitsPerKey,
              passed,
              numProbes,
              expected);
      assertEquals(
          message,
          expected,
          passed / (double) numProbes,
          6 * Math.sqrt(expected * (1 - expected) / numProbes) + 1.0 / numProbes);
      final Double rate = documented.get(bitsPerKey);
      if (rate != null) {
        assertEquals(message, rate, expected, 6 * standardError(rates) + 0.05 * rate);
      }
    }
  }

  /** Whether a key passes a filter of a single partition. */
  private static boolean passes(long[] words, long hash) {
    final long filterHash = TermHash.filterHash(hash, 0);
    final long word = words[BlockedBloomFilter.word(filterHash, words.length)];
    return BlockedBloomFilter.mayContain(word, BlockedBloomFilter.mask(filterHash)) == 1;
  }

  /** The words of the filter of a field, as written. */
  private static long[] readFilter(HashTerms terms) throws IOException {
    final RandomAccessInput in = terms.newSlice();
    final long[] words = new long[terms.filterWords()];
    for (int i = 0; i < words.length; i++) {
      words[i] = in.readLong(terms.filterOffset() + (long) i * Long.BYTES);
    }
    return words;
  }

  /**
   * Every field's filter is, word for word, the one built from all the delegate's terms at once,
   * with the number of words its size gives, at a multiple of 64 bytes, whether or not the keys
   * were spilled and partitioned, for keys of one document or more.
   */
  public void testFilterHoldsTheKeysOfTheField() throws IOException {
    final Random random = random();
    final int bitsPerKey = randomFilterBits(random);
    final int numDocs = TestUtil.nextInt(random, 1, 3000);
    final Codec codec =
        TestUtil.alwaysPostingsFormat(
            PrimaryKeyHashTestUtil.randomFormat(random, bitsPerKey, randomInlining(random)));
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          new IndexWriterConfig(new MockAnalyzer(random))
              .setCodec(codec)
              .setMergePolicy(NoMergePolicy.INSTANCE)
              .setMaxBufferedDocs(numDocs + 1)
              .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        final int shape = random.nextInt(5);
        for (int i = 0; i < numDocs; i++) {
          writer.addDocument(doc(randomKey(random, shape, i), i));
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        final LeafReader leaf = getOnlyLeafReader(reader);
        for (String field : new String[] {"id", "group"}) {
          final HashTerms terms = (HashTerms) leaf.terms(field);
          final NavigableMap<BytesRef, int[]> postings = postingsOf(terms.getDelegate());
          assertEquals(
              BlockedBloomFilter.numWords(postings.size(), bitsPerKey), terms.filterWords());
          // the slice starts at a multiple of 64 of the data file, and so does the filter
          assertEquals(0, terms.filterOffset() % PrimaryKeyHashPostingsFormat.CACHE_LINE_BYTES);
          final long[] expected = new long[terms.filterWords()];
          for (BytesRef key : postings.keySet()) {
            final long hash = TermHash.hash(key.bytes, key.offset, key.length);
            BlockedBloomFilter.add(expected, terms.filterHash(TermHash.mix(hash), hash));
          }
          assertArrayEquals(field, expected, readFilter(terms));
        }
      }
    }
  }

  /**
   * A partition's keys are in consecutive words, from its first word to the next partition's, at
   * most {@code ceil(numWords / numPartitions) + 1} of them; so the writer, which adds one
   * partition after the other, holds no more than that many words in heap, and writes, through a
   * temporary file that it deletes, the filter that adding all the keys at once gives.
   */
  public void testFilterIsBuiltOnePartitionAtATime() throws IOException {
    final Random random = random();
    for (int iter = 0; iter < 20; iter++) {
      final int partitionBits =
          iter == 0
              ? 0
              : TestUtil.nextInt(random, 0, PrimaryKeyHashPostingsFormat.MAX_PARTITION_BITS);
      final int numPartitions = 1 << partitionBits;
      final int numKeys =
          random.nextBoolean()
              ? TestUtil.nextInt(random, 1, 100)
              : TestUtil.nextInt(random, 1, 50_000);
      final int numWords = (int) BlockedBloomFilter.numWords(numKeys, randomFilterBits(random));
      final String message =
          numKeys + " keys, " + numWords + " words, " + partitionBits + " partition bits";
      // in partition order, as the writer gathers them, and in any order within a partition
      final Long[] hashes = new Long[numKeys];
      for (int i = 0; i < numKeys; i++) {
        hashes[i] = random.nextLong();
      }
      Arrays.sort(
          hashes,
          (a, b) ->
              Integer.compare(
                  TermHash.partition(a, partitionBits), TermHash.partition(b, partitionBits)));
      final long[] expected = new long[numWords];
      for (long hash : hashes) {
        final long filterHash = TermHash.filterHash(hash, partitionBits);
        BlockedBloomFilter.add(expected, filterHash);
        final int p = TermHash.partition(hash, partitionBits);
        final int word = BlockedBloomFilter.word(filterHash, numWords);
        assertTrue(message, word >= BlockedBloomFilter.firstWord(p, partitionBits, numWords));
        if (p + 1 < numPartitions) {
          assertTrue(message, word <= BlockedBloomFilter.firstWord(p + 1, partitionBits, numWords));
        }
      }
      final int padding = random.nextInt(64);
      final long start =
          IndexOutput.alignOffset(padding, PrimaryKeyHashPostingsFormat.CACHE_LINE_BYTES);
      try (Directory dir = newDirectory()) {
        try (HashFieldsConsumer.FilterWriter writer =
            new HashFieldsConsumer.FilterWriter(
                dir, "_0", IOContext.DEFAULT, numWords, partitionBits)) {
          assertEquals(message, numWords, writer.numWords());
          // the words of one partition, at most, and all of them only if there is one partition
          assertTrue(
              message + ": " + writer.windowWords(),
              writer.windowWords()
                  <= (numPartitions == 1
                      ? numWords
                      : Math.min(numWords, Math.ceilDiv(numWords, numPartitions) + 1)));
          int i = 0;
          for (int p = 0; p < numPartitions; p++) {
            while (i < numKeys && TermHash.partition(hashes[i], partitionBits) == p) {
              writer.add(hashes[i++]);
            }
            writer.finishPartition(p);
          }
          try (IndexOutput out = dir.createOutput("filter", IOContext.DEFAULT)) {
            out.writeBytes(new byte[padding], padding);
            assertEquals(message, start, writer.writeTo(out));
          }
        }
        for (String file : dir.listAll()) {
          assertFalse(message + ": " + file, file.endsWith(".tmp"));
        }
        final long[] actual = new long[expected.length];
        try (IndexInput in = dir.openInput("filter", IOContext.READONCE)) {
          assertEquals(start + (long) numWords * Long.BYTES, in.length());
          in.seek(start);
          in.readLongs(actual, 0, actual.length);
        }
        assertArrayEquals(message, expected, actual);
      }
    }
  }

  private static Document doc(BytesRef key, int seq) {
    Document doc = new Document();
    doc.add(new StringField("id", key, Field.Store.NO));
    doc.add(new StringField("group", Integer.toString(seq % 11), Field.Store.NO));
    return doc;
  }

  /**
   * No key that is there is ever ruled out, by any way of looking keys up, on segments with
   * partitions, spilled keys, keys of several documents, deletes, updates and merges.
   */
  public void testNoFalseNegatives() throws IOException {
    final Random random = random();
    final Codec codec =
        TestUtil.alwaysPostingsFormat(
            PrimaryKeyHashTestUtil.randomFormat(
                random, randomFilterBits(random), randomInlining(random)));
    final List<BytesRef> keys = new ArrayList<>();
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random)).setCodec(codec);
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        final int numDocs = atLeast(2000);
        final int shape = random.nextInt(6);
        for (int i = 0; i < numDocs; i++) {
          final int op = keys.isEmpty() ? 3 : random.nextInt(20);
          if (op == 0) {
            // a key of several documents, unless the copies are in different segments
            writer.addDocument(doc(keys.get(random.nextInt(keys.size())), i));
          } else if (op == 1) {
            BytesRef key = keys.get(random.nextInt(keys.size()));
            writer.updateDocument(new Term("id", key), doc(key, i));
          } else if (op == 2) {
            writer.deleteDocuments(new Term("id", keys.get(random.nextInt(keys.size()))));
          } else {
            BytesRef key = randomKey(random, shape == 5 ? random.nextInt(5) : shape, i);
            keys.add(key);
            writer.addDocument(doc(key, i));
          }
          if (random.nextInt(500) == 0) {
            writer.commit();
          }
        }
        if (random.nextBoolean()) {
          writer.forceMerge(TestUtil.nextInt(random, 1, 3));
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        boolean multi = false;
        for (LeafReaderContext context : reader.leaves()) {
          final LeafReader leaf = context.reader();
          for (String field : new String[] {"id", "group"}) {
            final HashTerms terms = (HashTerms) leaf.terms(field);
            assertTrue(terms.hasFilter());
            multi |= postingsOf(terms.getDelegate()).values().stream().anyMatch(d -> d.length > 1);
            // every way of looking keys up, against the delegate
            checkField(leaf, field, keys, true);
          }
        }
        // the group field has keys of several documents
        assertTrue(multi);
      }
    }
  }

  /**
   * With one partition, the most partitions, and a random number in between, no key that is there
   * is ruled out, whether the directory maps files or not: records in term order or inline keys, a
   * key of another length among keys of one length, keys of several documents, compound files or
   * not. Batches of any range of keys find what the delegate's postings hold in each of two
   * segments whose ranges of terms overlap.
   */
  public void testNoFalseNegativesAcrossPartitionCounts() throws IOException {
    final Random random = random();
    final int maxKeysPerPartition = 16;
    for (int partitionBits :
        new int[] {
          0,
          TestUtil.nextInt(random, 1, PrimaryKeyHashPostingsFormat.MAX_PARTITION_BITS - 1),
          PrimaryKeyHashPostingsFormat.MAX_PARTITION_BITS
        }) {
      final int numKeys =
          partitionBits == 0
              ? TestUtil.nextInt(random, 1, maxKeysPerPartition)
              : TestUtil.nextInt(
                  random,
                  (maxKeysPerPartition << (partitionBits - 1)) + 1,
                  maxKeysPerPartition << partitionBits);
      assertEquals(partitionBits, HashFieldsConsumer.partitionBits(numKeys, maxKeysPerPartition));
      // distinct keys, shuffled, so that the ranges of the two segments overlap
      final int numSecond = TestUtil.nextInt(random, 1, 64);
      final int shape = random.nextInt(5);
      final boolean otherLength = shape <= 2 && random.nextInt(3) == 0;
      final List<BytesRef> all = new ArrayList<>();
      final Set<BytesRef> distinct = new HashSet<>();
      while (all.size() < numKeys + numSecond) {
        BytesRef key = randomKey(random, shape, all.size());
        if (otherLength && all.isEmpty()) {
          final byte[] bytes = new byte[key.length + 1];
          random.nextBytes(bytes);
          key = new BytesRef(bytes);
        }
        if (distinct.add(key)) {
          all.add(key);
        }
      }
      Collections.shuffle(all, random);
      final List<BytesRef> second = all.subList(0, numSecond);
      final List<BytesRef> first = all.subList(numSecond, all.size());
      final PrimaryKeyHashPostingsFormat format =
          new PrimaryKeyHashPostingsFormat(
              TestUtil.getDefaultPostingsFormat(),
              0.5f + 0.4f * random.nextFloat(),
              maxKeysPerPartition,
              PrimaryKeyHashPostingsFormat.DEFAULT_MAX_PROBE_LIMIT,
              PrimaryKeyHashPostingsFormat.MAX_RECORDS_LENGTH,
              randomFilterBits(random),
              randomInlining(random));
      final Path path = createTempDir();
      try (Directory dir = new MMapDirectory(path)) {
        final IndexWriterConfig iwc =
            new IndexWriterConfig(new MockAnalyzer(random))
                .setCodec(TestUtil.alwaysPostingsFormat(format))
                .setMergePolicy(NoMergePolicy.INSTANCE)
                .setMaxBufferedDocs(2 * all.size() + 1)
                .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
                .setUseCompoundFile(random.nextBoolean());
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
          for (List<BytesRef> keys : List.of(first, second)) {
            for (BytesRef key : keys) {
              final int copies = random.nextInt(50) == 0 ? 2 : 1;
              for (int c = 0; c < copies; c++) {
                final Document doc = new Document();
                doc.add(new StringField("id", key, Field.Store.NO));
                writer.addDocument(doc);
              }
            }
            writer.commit();
          }
        }
      }

      // probes: every key, near misses, a key cut short and absent ones
      final List<BytesRef> probes = new ArrayList<>(all);
      for (int i = 0; i < Math.min(all.size(), 1000); i++) {
        final byte[] bytes = BytesRef.deepCopyOf(all.get(random.nextInt(all.size()))).bytes;
        if (bytes.length > 0) {
          final byte[] flipped = bytes.clone();
          flipped[random.nextInt(flipped.length)] ^= (byte) (1 + random.nextInt(255));
          probes.add(new BytesRef(flipped));
          probes.add(new BytesRef(bytes, 0, bytes.length - 1));
        }
        probes.add(new BytesRef(String.format(Locale.ROOT, "absent%d", i)));
      }
      try (Directory dir =
              random.nextBoolean() ? new MMapDirectory(path) : new NIOFSDirectory(path);
          DirectoryReader reader = DirectoryReader.open(dir)) {
        final String message = partitionBits + " partition bits, " + dir;
        assertEquals(message, 2, reader.leaves().size());
        for (LeafReaderContext context : reader.leaves()) {
          final HashTerms terms = (HashTerms) context.reader().terms("id");
          assertTrue(message, terms.hasFilter());
          if (context.ord == 0) {
            assertEquals(message, 1 << partitionBits, terms.numPartitions());
          }
          final NavigableMap<BytesRef, int[]> postings = postingsOf(terms.getDelegate());
          PrimaryKeyHashTestUtil.checkLookupBatch(terms, postings, probes);
        }
      }
    }
  }
}
