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
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.docs;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.format;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.lookupResult;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.postingsOf;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.randomFilterBits;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.randomFilterBitsPerKey;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.randomFormat;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.randomInlining;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.randomKey;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.writeSingleSegment;

import java.io.IOException;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntPredicate;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene104.Lucene104PostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.Inlining;
import org.apache.lucene.sandbox.search.PrimaryKeyHashInSetQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOBooleanSupplier;

/**
 * Lookups in the {@link HashTerms} of a segment agree with the delegate's terms dictionary, for
 * present, absent and nearly present keys, keys of several documents, keys crafted to collide, and
 * fields without a table. Readers are opened with {@link DirectoryReader#open}, as reader wrappers
 * would hide {@link HashTerms}.
 */
public class TestHashTerms extends LuceneTestCase {

  private static Document doc(BytesRef key, int seq) {
    Document doc = new Document();
    doc.add(new StringField("id", key, Field.Store.NO));
    doc.add(new StringField("seq", Integer.toString(seq), Field.Store.NO)); // a second primary key
    // keys of one byte, mostly of several documents
    doc.add(new StringField("group", Integer.toString(seq % 7), Field.Store.NO));
    return doc;
  }

  /** The key of {@code i}, of 7 digits. */
  private static BytesRef number(int i) {
    return new BytesRef(String.format(Locale.ROOT, "%07d", i));
  }

  /** The keys of the even numbers below {@code 2 * numKeys}: the odd ones between are absent. */
  private static List<BytesRef> evenNumbers(int numKeys) {
    final List<BytesRef> keys = new ArrayList<>();
    for (int i = 0; i < numKeys; i++) {
      keys.add(number(2 * i));
    }
    return keys;
  }

  private static HashTerms onlyTable(DirectoryReader reader) throws IOException {
    return (HashTerms) getOnlyLeafReader(reader).terms("id");
  }

  /** Enough keys and tiny partitions: the field is partitioned and its keys are spilled. */
  public void testManyPartitions() throws IOException {
    final Random random = random();
    final int maxKeysPerPartition = 16;
    final Codec codec =
        TestUtil.alwaysPostingsFormat(
            format(
                0.75f,
                maxKeysPerPartition,
                PrimaryKeyHashPostingsFormat.DEFAULT_FILTER_BITS_PER_KEY,
                Inlining.IF_NOT_LARGER));
    final int numKeys = TestUtil.nextInt(random, 2000, 5000);
    final List<BytesRef> keys = new ArrayList<>();
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          new IndexWriterConfig(new MockAnalyzer(random))
              .setCodec(codec)
              .setMaxBufferedDocs(numKeys + 1)
              .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < numKeys; i++) {
          BytesRef key = randomKey(random, random.nextInt(5), i);
          keys.add(key);
          writer.addDocument(doc(key, i));
        }
        writer.commit(); // one flushed segment
        if (random.nextBoolean()) {
          writer.addDocument(doc(new BytesRef("another"), numKeys));
          writer.forceMerge(1); // and a merged one
          keys.add(new BytesRef("another"));
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = getOnlyLeafReader(reader);
        int distinct = new HashSet<>(keys).size();
        int expectedPartitions =
            1 << HashFieldsConsumer.partitionBits(distinct, maxKeysPerPartition);
        assertTrue(expectedPartitions > 1);
        assertEquals(expectedPartitions, ((HashTerms) leaf.terms("id")).numPartitions());
        checkField(leaf, "id", keys, true);
        checkField(leaf, "seq", keys, true);
      }
    }
  }

  /**
   * Keys that all share one home slot. Up to the probe limit the table is built, every lookup scans
   * the shared run, and the run fills the tail as far as it goes past the capacity; one key more
   * and the field has no table. Either way results are the delegate's. The run is checked in term
   * order without a filter and inline with one, through which batches take another way.
   */
  public void testCollidingKeys() throws IOException {
    final int limit = PrimaryKeyHashPostingsFormat.DEFAULT_MAX_PROBE_LIMIT;
    checkCollidingKeys(limit + 1, 0, Inlining.NEVER);
    checkCollidingKeys(limit + 1, randomFilterBits(random()), Inlining.IF_NOT_LARGER);
    checkCollidingKeys(limit + 2, randomFilterBits(random()), randomInlining(random()));
  }

  private void checkCollidingKeys(int numKeys, int filterBitsPerKey, Inlining inlining)
      throws IOException {
    final Random random = random();
    // a single partition of this many slots
    final int capacity = (int) HashFieldsConsumer.capacity(numKeys, 0.75f);
    final int targetHome = random.nextInt(capacity);
    final List<BytesRef> keys = new ArrayList<>();
    final byte[] candidate = new byte[16];
    while (keys.size() < numKeys) {
      random.nextBytes(candidate);
      long hash = TermHash.hash(candidate, 0, candidate.length);
      if (TermHash.homeSlot(hash, 0, capacity) == targetHome) {
        keys.add(new BytesRef(candidate.clone()));
      }
    }
    try (Directory dir = newDirectory()) {
      writeSingleSegment(dir, format(0.75f, 1 << 20, filterBitsPerKey, inlining), keys);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = getOnlyLeafReader(reader);
        // n keys on one home slot need a displacement of n - 1
        boolean expectTable = numKeys - 1 <= PrimaryKeyHashPostingsFormat.DEFAULT_MAX_PROBE_LIMIT;
        if (expectTable) {
          HashTerms terms = (HashTerms) leaf.terms("id");
          assertEquals(numKeys - 1, terms.maxProbeOfPartition(0));
          assertEquals(filterBitsPerKey != 0, terms.hasFilter());
          assertEquals(
              Math.max(0, targetHome + numKeys - capacity),
              terms.slotCountOfPartition(0) - capacity);
        }
        List<BytesRef> probes = new ArrayList<>(keys);
        probes.add(new BytesRef("absent"));
        checkField(leaf, "id", probes, expectTable);
      }
    }
  }

  /**
   * A probe limit that can never be reached, a natural way to ask for no limit, must not keep
   * tables from being written; partitions are tiny, so that keys are spilled.
   */
  public void testUnreachableProbeLimit() throws IOException {
    final PostingsFormat format =
        new PrimaryKeyHashPostingsFormat(
            TestUtil.getDefaultPostingsFormat(),
            0.75f,
            16,
            Integer.MAX_VALUE,
            PrimaryKeyHashPostingsFormat.MAX_RECORDS_LENGTH,
            PrimaryKeyHashPostingsFormat.DEFAULT_FILTER_BITS_PER_KEY,
            Inlining.IF_NOT_LARGER);
    final List<BytesRef> keys = new ArrayList<>();
    for (int i = 0; i < 200; i++) {
      keys.add(new BytesRef("key" + i));
    }
    try (Directory dir = newDirectory()) {
      writeSingleSegment(dir, format, keys);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = getOnlyLeafReader(reader);
        checkField(leaf, "id", List.of(new BytesRef("absent")), true);
        assertTrue(((HashTerms) leaf.terms("id")).numPartitions() > 1);
      }
    }
  }

  /**
   * Three keys with the same home slot, control byte and tag, found by a birthday search. The
   * smallest and the largest are indexed, so that the third is within the range of the field's
   * terms: a lookup of the largest one first meets the smallest one's record, and a lookup of the
   * third meets both. The key comparison, not the hash, must decide.
   */
  public void testTagCollisions() throws IOException {
    final int capacity = (int) HashFieldsConsumer.capacity(2, 0.75f);
    assertTrue(capacity <= 4);
    final long salt = random().nextLong();
    final int numCandidates = 1 << 20;
    // (control, tag, home) in the high bits, candidate index in the low 32 bits
    final long[] codes = new long[numCandidates];
    for (int i = 0; i < numCandidates; i++) {
      byte[] key = candidateKey(salt, i);
      long hash = TermHash.hash(key, 0, key.length);
      long code =
          ((long) TermHash.control(hash) << (TermHash.TAG_BITS + 2))
              | ((long) TermHash.tag(hash) << 2)
              | TermHash.homeSlot(hash, 0, capacity); // < 2^26
      codes[i] = (code << 32) | i;
    }
    Arrays.sort(codes);
    int start = -1;
    for (int i = 2; i < numCandidates && start < 0; i++) {
      if (codes[i] >>> 32 == codes[i - 2] >>> 32) {
        start = i - 2;
      }
    }
    // about 300 such triples are expected among 2^20 candidates, so this practically never fails
    assertTrue("no three candidates share a slot, a control byte and a tag", start >= 0);
    final List<BytesRef> sorted = new ArrayList<>();
    for (int i = start; i < start + 3; i++) {
      sorted.add(new BytesRef(candidateKey(salt, (int) codes[i])));
    }
    Collections.sort(sorted);
    for (Inlining inlining : new Inlining[] {Inlining.NEVER, Inlining.ALWAYS}) {
      try (Directory dir = newDirectory()) {
        writeSingleSegment(
            dir, format(0.75f, 1 << 20, 0, inlining), List.of(sorted.get(0), sorted.get(2)));
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          HashTerms terms = onlyTable(reader);
          assertEquals(inlining == Inlining.ALWAYS, terms.isInline());
          assertEquals(1, terms.maxProbeOfPartition(0));
          checkField(getOnlyLeafReader(reader), "id", sorted, true);
        }
      }
    }
  }

  private static byte[] candidateKey(long salt, int i) {
    byte[] key = new byte[Long.BYTES];
    BitUtil.VH_BE_LONG.set(key, 0, salt + i);
    return key;
  }

  /**
   * Keys below a segment's smallest term or above its largest one, and keys of another length than
   * the inline keys of a field, are absent without any read of the data file, by every kind of
   * lookup but {@link HashTerms#lookupBatch}, which leaves the range to its callers. With a filter
   * and without.
   */
  public void testKeysThatCannotBeThereReadNothing() throws IOException {
    for (Inlining inlining : new Inlining[] {Inlining.NEVER, Inlining.ALWAYS}) {
      for (int filterBitsPerKey : new int[] {0, randomFilterBits(random())}) {
        checkKeysThatCannotBeThere(inlining, filterBitsPerKey);
      }
    }
  }

  private void checkKeysThatCannotBeThere(Inlining inlining, int filterBitsPerKey)
      throws IOException {
    final Random random = random();
    final int numKeys = TestUtil.nextInt(random, 500, 2000);
    final List<BytesRef> keys = new ArrayList<>();
    for (int i = 0; i < numKeys; i++) {
      keys.add(new BytesRef(String.format(Locale.ROOT, "%08d", 1000 + 2 * i)));
    }
    final List<BytesRef> none = new ArrayList<>();
    for (String key :
        new String[] {"", "0", "00000999", "00000000", "0001", "1", "9", "99999999"}) {
      none.add(new BytesRef(key));
    }
    // longer than the largest term, of which it is a prefix
    final BytesRef max = keys.getLast();
    none.add(new BytesRef(Arrays.copyOf(BytesRef.deepCopyOf(max).bytes, max.length + 1)));
    if (inlining == Inlining.ALWAYS) {
      // within the range of the terms: the first 7 bytes of a key, and a key followed by a digit
      for (int i = 0; i < 100; i++) {
        final String key = keys.get(TestUtil.nextInt(random, 5, numKeys - 2)).utf8ToString();
        none.add(new BytesRef(key.substring(0, 7)));
        none.add(new BytesRef(key + random.nextInt(10)));
      }
    }
    final PostingsFormat format =
        format(0.75f, random.nextBoolean() ? 1 << 20 : 64, filterBitsPerKey, inlining);
    try (ReadCountingDirectory dir = new ReadCountingDirectory(newDirectory())) {
      final String dataName = writeSingleSegment(dir, format, keys);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        final HashTerms terms = onlyTable(reader);
        assertEquals(inlining == Inlining.ALWAYS, terms.isInline());
        final IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);
        dir.resetReads();
        for (BytesRef key : none) {
          assertFalse(key.toString(), terms.iterator().seekExact(key));
          IOBooleanSupplier supplier = terms.iterator().prepareSeekExact(key);
          assertTrue(key.toString(), supplier == null || supplier.get() == false);
          assertEquals(0, searcher.count(new TermQuery(new Term("id", key))));
        }
        assertEquals(0, searcher.count(new PrimaryKeyHashInSetQuery("id", none)));
        assertEquals(0, dir.reads(dataName));
        // and every lookup, lookupBatch included, agrees with the delegate on them
        checkField(getOnlyLeafReader(reader), "id", none, true);
        assertTrue(dir.reads(dataName) > 0);
      }
    }
  }

  /**
   * A miss whose home slot is empty reads the control bytes from there and nothing else; exact
   * seeks never read the filter. A batch reads only the filter word of a key that the filter rules
   * out.
   */
  public void testMissAtAnEmptyHomeSlotReadsOnlyControlBytes() throws IOException {
    for (Inlining inlining : new Inlining[] {Inlining.NEVER, Inlining.ALWAYS}) {
      for (int filterBits : new int[] {0, randomFilterBits(random())}) {
        final int numKeys = TestUtil.nextInt(random(), 500, 2000);
        try (ReadCountingDirectory dir = new ReadCountingDirectory(newDirectory())) {
          final String dataName =
              writeSingleSegment(
                  dir, format(0.75f, 1 << 20, filterBits, inlining), evenNumbers(numKeys));
          try (DirectoryReader reader = DirectoryReader.open(dir)) {
            final HashTerms terms = onlyTable(reader);
            assertEquals(inlining == Inlining.ALWAYS, terms.isInline());
            final RandomAccessInput in = terms.newSlice();
            final List<BytesRef> misses = new ArrayList<>();
            final List<BytesRef> ruledOut = new ArrayList<>(); // wherever their home slot
            for (int i = 1; i < numKeys; i++) {
              final BytesRef miss = number(2 * i - 1);
              final long hash = TermHash.hash(miss.bytes, miss.offset, miss.length);
              final int p = terms.partition(hash);
              if (in.readByte(terms.controlOffset(p) + terms.homeSlot(hash, p))
                  == TermHash.EMPTY_CONTROL) {
                misses.add(miss);
              }
              if (terms.hasFilter()) {
                final long filterHash = terms.filterHash(TermHash.mix(hash), hash);
                final long word = in.readLong(terms.filterWordPosition(filterHash));
                if (BlockedBloomFilter.mayContain(word, BlockedBloomFilter.mask(filterHash)) == 0) {
                  ruledOut.add(miss);
                }
              }
            }
            assertTrue(misses.size() > numKeys / 10);
            dir.resetReads();
            for (BytesRef miss : misses) {
              assertFalse(terms.iterator().seekExact(miss));
            }
            assertEquals(misses.size(), dir.reads(dataName));
            // one read per key: its control word, or with a filter, its filter word
            final List<BytesRef> batchKeys = terms.hasFilter() ? ruledOut : misses;
            assertFalse(batchKeys.isEmpty());
            dir.resetReads();
            final HashedTerms batch = new HashedTerms(batchKeys);
            final int[] docs = new int[batch.size()];
            terms.lookupBatch(batch, 0, batch.size(), docs);
            for (int doc : docs) {
              assertEquals(HashTerms.ABSENT, doc);
            }
            assertEquals(batchKeys.size(), dir.reads(dataName));
          }
        }
      }
    }
  }

  /**
   * prepareSeekExact finds what seekExact finds, keys of several documents included, whether its
   * prefetch reports that the bytes are to be read, which defers the lookup to the supplier, or
   * that they are in memory, in which case the key is looked up right away: every miss then returns
   * null.
   */
  public void testPrepareSeekExact() throws IOException {
    for (Inlining inlining : new Inlining[] {Inlining.NEVER, Inlining.ALWAYS}) {
      final int numKeys = TestUtil.nextInt(random(), 1000, 3000);
      final PostingsFormat format =
          format(
              0.5f + 0.4f * random().nextFloat(),
              TestUtil.nextInt(random(), 16, 64),
              randomFilterBitsPerKey(random()),
              inlining);
      final List<BytesRef> docKeys = evenNumbers(numKeys);
      docKeys.addAll(evenNumbers(5)); // keys of two documents
      try (ReadCountingDirectory dir = new ReadCountingDirectory(newDirectory())) {
        writeSingleSegment(dir, format, docKeys);
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          final HashTerms terms = onlyTable(reader);
          assertTrue(terms.numPartitions() > 1);
          for (boolean prefetchReads : new boolean[] {true, false}) {
            dir.setPrefetchReads(prefetchReads);
            int nulls = 0;
            for (int i = 0; i < 2 * numKeys - 1; i++) {
              final BytesRef key = number(i);
              final boolean present = i % 2 == 0;
              final TermsEnum termsEnum = terms.iterator();
              final IOBooleanSupplier supplier = termsEnum.prepareSeekExact(key);
              if (supplier == null) {
                assertFalse(key.toString(), present || prefetchReads);
                nulls++;
              } else {
                assertEquals(prefetchReads, supplier.doDefer());
                assertEquals(key.toString(), present, supplier.get());
                if (present) {
                  assertEquals(key, termsEnum.term());
                }
              }
            }
            assertEquals(prefetchReads ? 0 : numKeys - 1, nulls);
          }
        }
      }
    }
  }

  /**
   * Postings enums handed back for reuse are not lost when the enum alternates between keys found
   * in the table and keys of several documents, sought in the delegate.
   */
  public void testPostingsReuseAcrossModes() throws IOException {
    final List<BytesRef> keys = new ArrayList<>();
    for (char ch = 'a'; ch <= 'z'; ch++) {
      keys.add(new BytesRef(Character.toString(ch)));
    }
    keys.add(new BytesRef("m")); // "m" has two documents: 12 and 26
    try (Directory dir = newDirectory()) {
      writeSingleSegment(
          dir, new PrimaryKeyHashPostingsFormat(new Lucene104PostingsFormat()), keys);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        TermsEnum termsEnum = onlyTable(reader).iterator();
        PostingsEnum postings = null;
        PostingsEnum delegatePostings = null;
        for (int iter = 0; iter < 10; iter++) {
          char single = (char) ('a' + random().nextInt(12)); // before "m"
          assertTrue(termsEnum.seekExact(new BytesRef(Character.toString(single))));
          postings = termsEnum.postings(postings, PostingsEnum.NONE);
          assertArrayEquals(new int[] {single - 'a'}, docs(postings));

          assertTrue(termsEnum.seekExact(new BytesRef("m")));
          postings = termsEnum.postings(postings, PostingsEnum.NONE);
          if (delegatePostings == null) {
            delegatePostings = postings;
          } else {
            assertSame(delegatePostings, postings);
          }
          assertArrayEquals(new int[] {12, 26}, docs(postings));
        }
      }
    }
  }

  /**
   * An enum that is not on a term, because it is fresh, a seek failed or it is exhausted, throws
   * {@link IllegalStateException} on calls that need a term. {@link HashTerms#lookupBatch} throws
   * on invalid ranges and on arrays too short for the results.
   */
  public void testUnpositionedEnumAndInvalidBatches() throws IOException {
    final List<BytesRef> keys = new ArrayList<>();
    for (int i = 0; i < 10; i += 2) {
      keys.add(new BytesRef("id" + i));
    }
    try (Directory dir = newDirectory()) {
      writeSingleSegment(dir, randomFormat(random()), keys);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        final HashTerms terms = onlyTable(reader);
        TermsEnum termsEnum = terms.iterator();
        assertUnpositioned(termsEnum);
        assertFalse(termsEnum.seekExact(new BytesRef("id5")));
        assertUnpositioned(termsEnum);
        assertTrue(termsEnum.seekExact(new BytesRef("id8")));
        assertNull(termsEnum.next());
        assertUnpositioned(termsEnum);
        assertTrue(termsEnum.seekExact(new BytesRef("id0")));
        assertNull(termsEnum.prepareSeekExact(new BytesRef("zz")));
        assertUnpositioned(termsEnum);

        final HashedTerms hashed = new HashedTerms(keys);
        terms.lookupBatch(hashed, 1, 1, new int[0]);
        expectThrows(IndexOutOfBoundsException.class, () -> terms.lookupBatch(hashed, 1, 0, null));
        expectThrows(
            IndexOutOfBoundsException.class,
            () -> terms.lookupBatch(hashed, 0, hashed.size() + 1, null));
        expectThrows(
            IllegalArgumentException.class, () -> terms.lookupBatch(hashed, 0, 2, new int[1]));
      }
    }
  }

  private static void assertUnpositioned(TermsEnum termsEnum) {
    expectThrows(IllegalStateException.class, termsEnum::term);
    expectThrows(IllegalStateException.class, termsEnum::docFreq);
    expectThrows(IllegalStateException.class, termsEnum::totalTermFreq);
    expectThrows(IllegalStateException.class, termsEnum::termState);
    expectThrows(IllegalStateException.class, () -> termsEnum.postings(null, PostingsEnum.NONE));
    expectThrows(IllegalStateException.class, () -> termsEnum.impacts(PostingsEnum.FREQS));
  }

  /**
   * A field gets a table only if its records take at most {@code maxRecordsLength} bytes, which is
   * the bound that readers check: a limit of exactly their length gives a table, one byte less does
   * not. Inline keys, whose records the writer lays out in term order before it copies them in slot
   * order, are bound the same.
   */
  public void testRecordsLengthLimit() throws IOException {
    final int numKeys = TestUtil.nextInt(random(), 1, 300);
    final List<BytesRef> keys = new ArrayList<>();
    for (int i = 0; i < numKeys; i++) {
      keys.add(new BytesRef(String.format(Locale.ROOT, "%016d", i)));
    }
    // records follow each other with nothing between them: a doc id, a one-byte length, the key
    final long recordsLength = numKeys * (Integer.BYTES + 1 + 16L);
    for (long limit : new long[] {recordsLength, recordsLength - 1, 0}) {
      for (Inlining inlining : new Inlining[] {Inlining.NEVER, Inlining.ALWAYS}) {
        final PostingsFormat format =
            new PrimaryKeyHashPostingsFormat(
                TestUtil.getDefaultPostingsFormat(),
                0.75f,
                TestUtil.nextInt(random(), 16, 64),
                PrimaryKeyHashPostingsFormat.DEFAULT_MAX_PROBE_LIMIT,
                limit,
                PrimaryKeyHashPostingsFormat.DEFAULT_FILTER_BITS_PER_KEY,
                inlining);
        try (Directory dir = newDirectory()) {
          writeSingleSegment(dir, format, keys);
          try (DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReader leaf = getOnlyLeafReader(reader);
            boolean expectTable = recordsLength <= limit;
            assertEquals("limit=" + limit, expectTable, leaf.terms("id") instanceof HashTerms);
            if (expectTable) {
              assertEquals(inlining == Inlining.ALWAYS, ((HashTerms) leaf.terms("id")).isInline());
            }
            checkField(leaf, "id", keys, expectTable);
          }
        }
      }
    }
  }

  /**
   * The format is used for some fields only, next to the default format, on segments with keys of
   * several documents, updates and deletes; a field whose keys mostly have several documents gets a
   * table too.
   */
  public void testPerFieldFormat() throws IOException {
    final Random random = random();
    final PrimaryKeyHashPostingsFormat format = randomFormat(random);
    final Codec codec =
        new Lucene104Codec() {
          @Override
          public PostingsFormat getPostingsFormatForField(String field) {
            return "seq".equals(field) ? super.getPostingsFormatForField(field) : format;
          }
        };
    final List<BytesRef> keys = new ArrayList<>();
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random)).setCodec(codec);
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        int numDocs = atLeast(200);
        for (int i = 0; i < numDocs; i++) {
          int op = keys.isEmpty() ? 3 : random.nextInt(20);
          if (op == 0) {
            // a key of several documents, unless the copies end up in different segments
            writer.addDocument(doc(keys.get(random.nextInt(keys.size())), i));
          } else if (op == 1) {
            BytesRef key = keys.get(random.nextInt(keys.size()));
            writer.updateDocument(new Term("id", key), doc(key, i));
          } else if (op == 2) {
            writer.deleteDocuments(new Term("id", keys.get(random.nextInt(keys.size()))));
          } else {
            BytesRef key = randomKey(random, random.nextInt(5), i);
            keys.add(key);
            writer.addDocument(doc(key, i));
          }
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        for (LeafReaderContext context : reader.leaves()) {
          LeafReader leaf = context.reader();
          checkField(leaf, "id", keys, true);
          checkField(leaf, "group", keys, true);
          assertFalse(leaf.terms("seq") instanceof HashTerms);
        }
      }
    }
  }

  /**
   * Keys of every length up to 130 bytes and some much longer, whose records cross cache lines or
   * are longer than one, and whose lengths take one to three bytes; and a field whose smallest key
   * is longer than a batch of records.
   */
  public void testKeysOfManyLengths() throws IOException {
    final Set<BytesRef> keys = new HashSet<>();
    for (int length = 0; length <= 130; length++) {
      for (int i = 0; i < 1 + random().nextInt(3); i++) {
        final byte[] bytes = new byte[length];
        random().nextBytes(bytes);
        keys.add(new BytesRef(bytes));
      }
    }
    for (int length : new int[] {300, 16383, 16384, IndexWriter.MAX_TERM_LENGTH}) {
      final byte[] bytes = new byte[length];
      random().nextBytes(bytes);
      keys.add(new BytesRef(bytes));
    }
    final List<BytesRef> shuffled = new ArrayList<>(keys);
    Collections.shuffle(shuffled, random());
    final List<BytesRef> longOnly = new ArrayList<>();
    for (int length : new int[] {5000, 5000, IndexWriter.MAX_TERM_LENGTH}) {
      final byte[] bytes = new byte[length];
      random().nextBytes(bytes);
      longOnly.add(new BytesRef(bytes));
    }
    for (List<BytesRef> segmentKeys : List.of(shuffled, longOnly)) {
      try (Directory dir = newDirectory()) {
        writeSingleSegment(dir, randomFormat(random()), segmentKeys);
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          checkField(getOnlyLeafReader(reader), "id", List.of(), true);
        }
      }
    }
  }

  /** One {@link HashTerms} shared by threads that each look keys up their own way. */
  public void testConcurrentLookups() throws Exception {
    final Codec codec = TestUtil.alwaysPostingsFormat(randomFormat(random()));
    final List<BytesRef> keys = new ArrayList<>();
    try (Directory dir = newDirectory()) {
      try (IndexWriter writer =
          new IndexWriter(dir, newIndexWriterConfig(new MockAnalyzer(random())).setCodec(codec))) {
        int numDocs = atLeast(500);
        for (int i = 0; i < numDocs; i++) {
          BytesRef key = randomKey(random(), random().nextInt(5), i);
          keys.add(key);
          writer.addDocument(doc(key, i));
          keys.add(randomKey(random(), random().nextInt(5), i)); // most likely absent
        }
        writer.forceMerge(1);
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        final HashTerms terms = onlyTable(reader);
        final NavigableMap<BytesRef, int[]> expected = postingsOf(terms.getDelegate());
        final HashedTerms hashedTerms = new HashedTerms(keys);
        final int[] expectedResults = new int[hashedTerms.size()];
        for (int i = 0; i < hashedTerms.size(); i++) {
          expectedResults[i] = lookupResult(expected.get(hashedTerms.get(i)));
        }
        Thread[] threads = new Thread[TestUtil.nextInt(random(), 2, 6)];
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final long seed = random().nextLong();
        for (int t = 0; t < threads.length; t++) {
          final Random threadRandom = new Random(seed + t);
          threads[t] =
              new Thread(
                  () -> {
                    try {
                      for (int iter = 0; iter < 20; iter++) {
                        if (threadRandom.nextBoolean()) {
                          int[] docs = new int[hashedTerms.size()];
                          terms.lookupBatch(hashedTerms, 0, hashedTerms.size(), docs);
                          assertArrayEquals(expectedResults, docs);
                        } else {
                          TermsEnum termsEnum = terms.iterator();
                          for (int i = 0; i < hashedTerms.size(); i++) {
                            assertEquals(
                                expected.containsKey(hashedTerms.get(i)),
                                termsEnum.seekExact(hashedTerms.get(i)));
                          }
                        }
                      }
                    } catch (Throwable e) {
                      failure.compareAndSet(null, e);
                    }
                  });
        }
        for (Thread thread : threads) {
          thread.start();
        }
        for (Thread thread : threads) {
          thread.join();
        }
        if (failure.get() != null) {
          throw new AssertionError("a lookup thread failed", failure.get());
        }
      }
    }
  }

  /**
   * Keys whose control bytes are equal: a run of keys of one home slot and one control byte, three
   * keys of the next home slot with that control byte, so that candidates of the run's keys hold
   * other keys, and a block of other keys after the run, which may run into the tail. Lookups must
   * go past every slot with their control byte that holds another key, across words of control
   * bytes, and stop at an empty slot or after MaxProbe + 1 slots; batches look the next candidates
   * of such keys up in rounds.
   */
  public void testControlCollisions() throws IOException {
    for (Inlining inlining : new Inlining[] {Inlining.NEVER, Inlining.ALWAYS}) {
      final Random random = random();
      final int numShared = TestUtil.nextInt(random, 10, 20);
      final int numBlock = TestUtil.nextInt(random, 10, 20);
      final int numKeys = numShared + 3 + numBlock;
      final float loadFactor = 0.5f + 0.4f * random.nextFloat();
      final int capacity = (int) HashFieldsConsumer.capacity(numKeys, loadFactor);
      // the block's home slot is the last one half of the time: then it runs into the tail
      final int home =
          random.nextBoolean() ? capacity - numShared - 1 : random.nextInt(capacity - numShared);
      final int control = TermHash.control(random.nextLong());
      final KeySearch search = new KeySearch(random, capacity);
      final List<BytesRef> keys = new ArrayList<>(search.find(numShared, home, c -> c == control));
      keys.addAll(search.find(3, home + 1, c -> c == control));
      keys.addAll(search.find(numBlock, home + numShared, c -> c != control));
      // absent keys of the run's home slot, with its control byte or another, and of the run
      final List<BytesRef> misses = new ArrayList<>(search.find(5, home, c -> c == control));
      misses.addAll(search.find(5, home, c -> c != control));
      final int lastHome = Math.min(capacity - 1, home + numKeys - 1);
      for (int i = 0; i < 20; i++) {
        misses.addAll(search.find(1, TestUtil.nextInt(random, home + 1, lastHome), _ -> true));
      }
      try (Directory dir = newDirectory()) {
        writeSingleSegment(dir, format(loadFactor, 1 << 20, 0, inlining), keys);
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          final HashTerms terms = onlyTable(reader);
          assertEquals(inlining == Inlining.ALWAYS, terms.isInline());
          assertEquals(1, terms.numPartitions());
          assertTrue(terms.maxProbeOfPartition(0) >= numShared);
          // the run and the keys of the next home slot in one chunk
          final HashedTerms sharing = new HashedTerms(keys.subList(0, numShared + 3));
          assertTrue(sharing.size() <= HashTerms.CHUNK_SIZE);
          final int[] docs = new int[sharing.size()];
          terms.lookupBatch(sharing, 0, sharing.size(), docs);
          for (int i = 0; i < sharing.size(); i++) {
            assertEquals(keys.indexOf(sharing.get(i)), docs[i]);
          }
          checkField(getOnlyLeafReader(reader), "id", misses, true);
        }
      }
    }
  }

  /** Finds random keys of 7 bytes by the home slot and the control byte of their hash. */
  private static final class KeySearch {
    private final Random random;
    private final int capacity;
    private final Set<BytesRef> found = new HashSet<>();

    KeySearch(Random random, int capacity) {
      this.random = random;
      this.capacity = capacity;
    }

    /** {@code count} new distinct keys with the given home slot and a matching control byte. */
    List<BytesRef> find(int count, int home, IntPredicate control) {
      final List<BytesRef> keys = new ArrayList<>();
      final byte[] bytes = new byte[Long.BYTES];
      while (keys.size() < count) {
        BitUtil.VH_LE_LONG.set(bytes, 0, random.nextLong());
        long hash = TermHash.hash(bytes, 0, 7);
        if (TermHash.homeSlot(hash, 0, capacity) == home && control.test(TermHash.control(hash))) {
          BytesRef key = new BytesRef(Arrays.copyOf(bytes, 7));
          if (found.add(key)) {
            keys.add(key);
          }
        }
      }
      return keys;
    }
  }

  /**
   * The scan of control bytes 8 at a time agrees with a scan of one byte at a time, on random
   * control bytes of several kinds, wherever they start.
   */
  public void testControlScan() throws IOException {
    try (Directory dir = newDirectory()) {
      for (int iter = 0; iter < 100; iter++) {
        final int slotCount = TestUtil.nextInt(random(), 1, 100);
        final int length = (int) PrimaryKeyHashPostingsFormat.controlLength(slotCount);
        final int offset = random().nextInt(16); // control bytes need no alignment
        final int want = TermHash.control(random().nextLong());
        final int kind = random().nextInt(4);
        final byte[] control = new byte[length];
        for (int i = 0; i < (kind == 3 ? length : slotCount); i++) {
          control[i] = (byte) randomControlByte(random(), kind, want);
        }
        final String name = "control" + iter;
        try (IndexOutput out = dir.createOutput(name, IOContext.DEFAULT)) {
          for (int i = 0; i < offset; i++) {
            out.writeByte((byte) random().nextInt(256));
          }
          out.writeBytes(control, length);
        }
        try (IndexInput in = dir.openInput(name, IOContext.DEFAULT)) {
          final RandomAccessInput slice = in.randomAccessSlice(0, offset + length);
          for (int q = 0; q < 100; q++) {
            final int from = random().nextInt(slotCount);
            final int limit = TestUtil.nextInt(random(), from + 1, slotCount);
            int expected = -1;
            for (int i = from; i < limit; i++) {
              int b = control[i] & 0xFF;
              if (b == TermHash.EMPTY_CONTROL || b == want) {
                expected = b == want ? i : -1;
                break;
              }
            }
            final long first = slice.readLong(offset + from);
            assertEquals(
                "from=" + from + " limit=" + limit + " " + Arrays.toString(control),
                expected,
                HashTerms.findCandidate(slice, offset, from, first, limit, HashTerms.repeat(want)));
          }
        }
      }
    }
  }

  private static int randomControlByte(Random random, int kind, int want) {
    return switch (kind) {
      case 0 -> { // as written
        int r = random.nextInt(10);
        yield r < 3 ? TermHash.EMPTY_CONTROL : r == 3 ? want : 0x80 | random.nextInt(128);
      }
      case 1 -> { // long runs without an empty slot
        int r = random.nextInt(40);
        yield r == 0 ? TermHash.EMPTY_CONTROL : r == 1 ? want : 0x80 | random.nextInt(128);
      }
      case 2 -> { // a byte below one of these is flagged by the borrow of 8-byte arithmetic
        int[] choices = {
          TermHash.EMPTY_CONTROL, want, want ^ 1, 0x01, 0x80, 0x81, 0xFF, want ^ 0x80, 0x7F
        };
        yield choices[random.nextInt(choices.length)];
      }
      default -> random.nextInt(256);
    };
  }

  /**
   * Every length and every position of a difference, so that no word of the compare is skipped; and
   * a record whose key length is not a valid VInt matches no key.
   */
  public void testKeyEquals() throws IOException {
    try (Directory dir = new ByteBuffersDirectory()) {
      byte[] stored = new byte[64];
      random().nextBytes(stored);
      Arrays.fill(stored, Integer.BYTES, Integer.BYTES + 5, (byte) 0xFF); // after a 4-byte doc id
      try (IndexOutput out = dir.createOutput("keys", IOContext.DEFAULT)) {
        out.writeBytes(stored, stored.length);
      }
      try (IndexInput in = dir.openInput("keys", IOContext.DEFAULT)) {
        RandomAccessInput slice = in.randomAccessSlice(0, in.length());
        assertEquals(
            HashTerms.NO_MATCH,
            HashTerms.matchRecord(slice, 0, slice.readLong(0), new byte[0], 0, 0));
        for (int length = 0; length <= 40; length++) {
          int position = random().nextInt(stored.length - length + 1);
          byte[] key = new byte[length + 2];
          System.arraycopy(stored, position, key, 1, length);
          assertTrue(HashTerms.keyEquals(slice, position, key, 1, length));
          for (int diff = 0; diff < length; diff++) {
            key[1 + diff] ^= (byte) (1 + random().nextInt(255));
            assertFalse(HashTerms.keyEquals(slice, position, key, 1, length));
            key[1 + diff] = stored[position + diff];
          }
        }
      }
    }
  }

  /**
   * Every length of a key that can be inline and is longer than 4 bytes, and every position of a
   * difference after the first 4 bytes, which the head of a record holds: no word of the compare is
   * skipped, and none is read past the record.
   */
  public void testRestEquals() throws IOException {
    try (Directory dir = new ByteBuffersDirectory()) {
      final byte[] stored = new byte[2 * 64];
      random().nextBytes(stored);
      try (IndexOutput out = dir.createOutput("records", IOContext.DEFAULT)) {
        out.writeBytes(stored, stored.length);
      }
      try (IndexInput in = dir.openInput("records", IOContext.DEFAULT)) {
        for (int length = Integer.BYTES + 1;
            length <= PrimaryKeyHashPostingsFormat.MAX_INLINE_KEY_LENGTH;
            length++) {
          // a record of a doc id and a key that ends where the file does, or anywhere before
          final int recordLength = Integer.BYTES + length;
          final int position =
              random().nextBoolean()
                  ? stored.length - recordLength
                  : random().nextInt(stored.length - recordLength + 1);
          final RandomAccessInput slice = in.randomAccessSlice(0, position + recordLength);
          final byte[] key = new byte[length + 2];
          System.arraycopy(stored, position + Integer.BYTES, key, 1, length);
          final long tail = slice.readLong(position + recordLength - Long.BYTES);
          assertTrue(HashTerms.restEquals(slice, position, tail, key, 1, length));
          for (int diff = Integer.BYTES; diff < length; diff++) {
            key[1 + diff] ^= (byte) (1 + random().nextInt(255));
            assertFalse(HashTerms.restEquals(slice, position, tail, key, 1, length));
            key[1 + diff] = stored[position + Integer.BYTES + diff];
          }
        }
      }
    }
  }

  /**
   * Counts the positional reads of the data files, which is how lookups read them; sequential
   * reads, such as those of headers and footers, are not counted. Prefetches return what {@link
   * #setPrefetchReads} says, so that tests choose whether the bytes are to be read or are in
   * memory.
   */
  private static final class ReadCountingDirectory extends FilterDirectory {
    private final Map<String, AtomicLong> reads = new ConcurrentHashMap<>();
    private volatile boolean prefetchReads;

    ReadCountingDirectory(Directory in) {
      super(in);
    }

    long reads(String file) {
      return counter(file).get();
    }

    void resetReads() {
      for (AtomicLong counter : reads.values()) {
        counter.set(0);
      }
    }

    void setPrefetchReads(boolean prefetchReads) {
      this.prefetchReads = prefetchReads;
    }

    private AtomicLong counter(String file) {
      return reads.computeIfAbsent(file, _ -> new AtomicLong());
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
      final IndexInput input = super.openInput(name, context);
      return name.endsWith("." + PrimaryKeyHashPostingsFormat.DATA_EXTENSION)
          ? new CountingInput(input, counter(name), this)
          : input;
    }
  }

  private static final class CountingInput extends FilterIndexInput {
    private final AtomicLong reads;
    private final ReadCountingDirectory dir;

    CountingInput(IndexInput in, AtomicLong reads, ReadCountingDirectory dir) {
      super("counting(" + in + ")", in);
      this.reads = reads;
      this.dir = dir;
    }

    @Override
    public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
      return new CountingInput(in.slice(sliceDescription, offset, length), reads, dir);
    }

    @Override
    public CountingInput clone() {
      return new CountingInput(in.clone(), reads, dir);
    }

    @Override
    public RandomAccessInput randomAccessSlice(long offset, long length) throws IOException {
      final RandomAccessInput slice = in.randomAccessSlice(offset, length);
      return new RandomAccessInput() {
        @Override
        public long length() {
          return slice.length();
        }

        @Override
        public byte readByte(long pos) throws IOException {
          reads.incrementAndGet();
          return slice.readByte(pos);
        }

        @Override
        public void readBytes(long pos, byte[] bytes, int offset, int length) throws IOException {
          reads.incrementAndGet();
          slice.readBytes(pos, bytes, offset, length);
        }

        @Override
        public short readShort(long pos) throws IOException {
          reads.incrementAndGet();
          return slice.readShort(pos);
        }

        @Override
        public int readInt(long pos) throws IOException {
          reads.incrementAndGet();
          return slice.readInt(pos);
        }

        @Override
        public long readLong(long pos) throws IOException {
          reads.incrementAndGet();
          return slice.readLong(pos);
        }

        @Override
        public boolean prefetch(long offset, long length) throws IOException {
          slice.prefetch(offset, length);
          return dir.prefetchReads;
        }
      };
    }
  }
}
