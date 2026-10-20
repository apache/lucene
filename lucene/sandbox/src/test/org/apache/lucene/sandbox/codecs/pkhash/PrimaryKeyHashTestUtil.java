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

import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.DATA_CODEC;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.DATA_EXTENSION;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.META_CODEC;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.META_EXTENSION;
import static org.apache.lucene.tests.util.LuceneTestCase.newDirectory;
import static org.apache.lucene.tests.util.LuceneTestCase.random;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.Inlining;
import org.apache.lucene.sandbox.search.PrimaryKeyHashInSetQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOBooleanSupplier;

/**
 * Formats, keys, indexes and checks shared by the tests of {@link PrimaryKeyHashPostingsFormat}.
 */
public final class PrimaryKeyHashTestUtil {

  private PrimaryKeyHashTestUtil() {}

  // ---- formats and keys ----

  /**
   * A format with random options: often tiny partitions, a filter half of the time, and keys of one
   * length inline as {@link #randomInlining} says.
   */
  public static PrimaryKeyHashPostingsFormat randomFormat(Random random) {
    return randomFormat(random, randomFilterBitsPerKey(random), randomInlining(random));
  }

  /** As {@link #randomFormat(Random)}, with the given filter and inlining. */
  static PrimaryKeyHashPostingsFormat randomFormat(
      Random random, int filterBitsPerKey, Inlining inlining) {
    return randomFormat(random, TestUtil.getDefaultPostingsFormat(), filterBitsPerKey, inlining);
  }

  /** As {@link #randomFormat(Random)}, with the given delegate, filter and inlining. */
  static PrimaryKeyHashPostingsFormat randomFormat(
      Random random, PostingsFormat delegate, int filterBitsPerKey, Inlining inlining) {
    final float loadFactor = 0.5f + 0.4f * random.nextFloat();
    final int maxKeysPerPartition =
        random.nextBoolean()
            ? TestUtil.nextInt(random, 16, 64)
            : TestUtil.nextInt(random, 16, 100_000);
    return new PrimaryKeyHashPostingsFormat(
        delegate,
        loadFactor,
        maxKeysPerPartition,
        PrimaryKeyHashPostingsFormat.DEFAULT_MAX_PROBE_LIMIT,
        PrimaryKeyHashPostingsFormat.MAX_RECORDS_LENGTH,
        filterBitsPerKey,
        inlining);
  }

  /** A format with the given options, with the default delegate and limits. */
  static PrimaryKeyHashPostingsFormat format(
      float loadFactor, int maxKeysPerPartition, int filterBitsPerKey, Inlining inlining) {
    return new PrimaryKeyHashPostingsFormat(
        TestUtil.getDefaultPostingsFormat(),
        loadFactor,
        maxKeysPerPartition,
        PrimaryKeyHashPostingsFormat.DEFAULT_MAX_PROBE_LIMIT,
        PrimaryKeyHashPostingsFormat.MAX_RECORDS_LENGTH,
        filterBitsPerKey,
        inlining);
  }

  /** Inline by default half of the time, else never or whenever possible. */
  static Inlining randomInlining(Random random) {
    return switch (random.nextInt(4)) {
      case 0 -> Inlining.NEVER;
      case 1 -> Inlining.ALWAYS;
      default -> Inlining.IF_NOT_LARGER;
    };
  }

  /** No filter half of the time, else {@link #randomFilterBits}. */
  static int randomFilterBitsPerKey(Random random) {
    return random.nextBoolean() ? 0 : randomFilterBits(random);
  }

  /** Any valid number of bits per key of a filter. */
  static int randomFilterBits(Random random) {
    return TestUtil.nextInt(
        random,
        PrimaryKeyHashPostingsFormat.MIN_FILTER_BITS_PER_KEY,
        PrimaryKeyHashPostingsFormat.MAX_FILTER_BITS_PER_KEY);
  }

  /**
   * A key of one of 5 shapes: binary UUIDs, UUID strings, zero-padded numbers, which all have a
   * fixed length, unicode strings and random binary keys.
   */
  public static BytesRef randomKey(Random random, int shape, int seq) {
    return switch (shape) {
      case 0 -> {
        byte[] bytes = new byte[16];
        random.nextBytes(bytes);
        yield new BytesRef(bytes);
      }
      case 1 -> new BytesRef(new UUID(random.nextLong(), random.nextLong()).toString());
      case 2 -> new BytesRef(String.format(Locale.ROOT, "%010d", seq));
      case 3 -> new BytesRef(TestUtil.randomUnicodeString(random, 20));
      default -> {
        int length =
            random.nextInt(50) == 0
                ? random.nextInt(IndexWriter.MAX_TERM_LENGTH + 1)
                : random.nextInt(40);
        byte[] bytes = new byte[length];
        random.nextBytes(bytes);
        yield new BytesRef(bytes);
      }
    };
  }

  // ---- indexes and files ----

  /** Indexes the keys in one segment, in order, one document each; returns its data file. */
  static String writeSingleSegment(Directory dir, PostingsFormat format, List<BytesRef> keys)
      throws IOException {
    final IndexWriterConfig iwc =
        new IndexWriterConfig(new MockAnalyzer(random()))
            .setCodec(TestUtil.alwaysPostingsFormat(format))
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setMaxBufferedDocs(keys.size() + 1)
            .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
            .setUseCompoundFile(false);
    try (IndexWriter writer = new IndexWriter(dir, iwc)) {
      for (BytesRef key : keys) {
        Document doc = new Document();
        doc.add(new StringField("id", key, Field.Store.NO));
        writer.addDocument(doc);
      }
    }
    return onlyFile(dir, DATA_EXTENSION);
  }

  /** The only file of the directory with the given extension. */
  static String onlyFile(Directory dir, String extension) throws IOException {
    String name = null;
    for (String file : dir.listAll()) {
      if (file.endsWith("." + extension)) {
        assertNull("one segment", name);
        name = file;
      }
    }
    assertNotNull(name);
    return name;
  }

  static byte[] readAll(Directory dir, String name) throws IOException {
    try (IndexInput in = dir.openInput(name, IOContext.READONCE)) {
      final byte[] bytes = new byte[Math.toIntExact(in.length())];
      in.readBytes(bytes, 0, bytes.length);
      return bytes;
    }
  }

  /** The index header length of a meta or data file of this format, given its name. */
  static int headerLength(String file) {
    // "_PrimaryKeyHash_0" for "_1_PrimaryKeyHash_0.hpd", "" for "_1.hpd"
    final String suffix = IndexFileNames.stripSegmentName(IndexFileNames.stripExtension(file));
    return CodecUtil.indexHeaderLength(
        file.endsWith("." + META_EXTENSION) ? META_CODEC : DATA_CODEC,
        suffix.isEmpty() ? "" : suffix.substring(1));
  }

  /**
   * The bytes of a meta or data file of this format but for its header, which holds the segment's
   * random id, and its footer, whose checksum covers the header.
   */
  static byte[] withoutHeaderAndFooter(Directory dir, String file) throws IOException {
    final byte[] bytes = readAll(dir, file);
    return Arrays.copyOfRange(bytes, headerLength(file), bytes.length - CodecUtil.footerLength());
  }

  /** The meta and data files of the two directories are the same but for headers and footers. */
  static void assertSameFiles(Directory expected, Directory actual) throws IOException {
    final List<String> files = formatFiles(expected);
    assertFalse(files.isEmpty());
    assertEquals(files, formatFiles(actual));
    for (String file : files) {
      assertArrayEquals(
          file, withoutHeaderAndFooter(expected, file), withoutHeaderAndFooter(actual, file));
    }
  }

  private static List<String> formatFiles(Directory dir) throws IOException {
    final List<String> files = new ArrayList<>();
    for (String file : dir.listAll()) {
      if (file.endsWith("." + META_EXTENSION) || file.endsWith("." + DATA_EXTENSION)) {
        files.add(file);
      }
    }
    return files;
  }

  /**
   * Writes an index of the given key shape from {@code seed} alone, flushes, merges, updates,
   * deletes and keys of several documents included, so that it can be written twice the same;
   * returns its keys.
   */
  private static List<BytesRef> writeSeeded(
      Directory dir, PostingsFormat format, long seed, int shape) throws IOException {
    final Random random = new Random(seed);
    final LogDocMergePolicy mergePolicy = new LogDocMergePolicy();
    mergePolicy.setMergeFactor(TestUtil.nextInt(random, 2, 10));
    final IndexWriterConfig iwc =
        new IndexWriterConfig(new MockAnalyzer(random))
            .setCodec(TestUtil.alwaysPostingsFormat(format))
            .setMaxBufferedDocs(TestUtil.nextInt(random, 50, 500))
            .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
            .setMergePolicy(mergePolicy)
            .setMergeScheduler(new SerialMergeScheduler());
    final List<BytesRef> keys = new ArrayList<>();
    try (IndexWriter writer = new IndexWriter(dir, iwc)) {
      final int numDocs = TestUtil.nextInt(random, 500, 3000);
      for (int i = 0; i < numDocs; i++) {
        final int op = keys.isEmpty() ? 0 : random.nextInt(20);
        final Document doc = new Document();
        doc.add(new TextField("body", "some text " + (i % 100), Field.Store.NO));
        if (op == 1 || op == 2 || op == 3) {
          final BytesRef key = keys.get(random.nextInt(keys.size()));
          doc.add(new StringField("id", key, Field.Store.NO));
          switch (op) {
            case 1 -> writer.updateDocument(new Term("id", key), doc);
            case 2 -> writer.deleteDocuments(new Term("id", key));
            default -> writer.addDocument(doc); // another document of the key
          }
        } else {
          final BytesRef key = randomKey(random, shape, i);
          keys.add(key);
          doc.add(new StringField("id", key, Field.Store.NO));
          writer.addDocument(doc);
        }
      }
    }
    return keys;
  }

  /**
   * Writes the same index of keys of the given shape with both formats and checks that their
   * segments have tables of the same sizes, which {@code checkTables} checks further, and that
   * every way to look keys up finds the same, segment by segment and in the whole index.
   */
  static void assertSameResults(
      PostingsFormat expectedFormat,
      PostingsFormat actualFormat,
      int shape,
      BiConsumer<HashTerms, HashTerms> checkTables)
      throws IOException {
    final Random random = random();
    final long seed = random.nextLong();
    try (Directory expectedDir = newDirectory();
        Directory actualDir = newDirectory()) {
      final List<BytesRef> keys = writeSeeded(expectedDir, expectedFormat, seed, shape);
      assertEquals(keys, writeSeeded(actualDir, actualFormat, seed, shape));
      final List<BytesRef> probes = new ArrayList<>(keys);
      for (int i = 0; i < 300; i++) {
        final byte[] bytes = BytesRef.deepCopyOf(keys.get(random.nextInt(keys.size()))).bytes;
        if (bytes.length > 0) {
          bytes[random.nextInt(bytes.length)] ^= (byte) (1 + random.nextInt(255));
        }
        probes.add(new BytesRef(bytes));
        probes.add(new BytesRef("absent" + i));
      }
      try (DirectoryReader expectedReader = DirectoryReader.open(expectedDir);
          DirectoryReader actualReader = DirectoryReader.open(actualDir)) {
        assertEquals(expectedReader.leaves().size(), actualReader.leaves().size());
        for (int l = 0; l < expectedReader.leaves().size(); l++) {
          final HashTerms expected =
              (HashTerms) expectedReader.leaves().get(l).reader().terms("id");
          final HashTerms actual = (HashTerms) actualReader.leaves().get(l).reader().terms("id");
          checkTables.accept(expected, actual);
          assertEquals(expected.numPartitions(), actual.numPartitions());
          for (int p = 0; p < expected.numPartitions(); p++) {
            assertEquals(expected.capacityOfPartition(p), actual.capacityOfPartition(p));
            assertEquals(expected.slotCountOfPartition(p), actual.slotCountOfPartition(p));
            assertEquals(expected.maxProbeOfPartition(p), actual.maxProbeOfPartition(p));
          }
          final TermsEnum expectedEnum = expected.iterator();
          final TermsEnum actualEnum = actual.iterator();
          for (BytesRef probe : probes) {
            final boolean found = expectedEnum.seekExact(probe);
            assertEquals(probe.toString(), found, actualEnum.seekExact(probe));
            if (found) {
              assertEquals(expectedEnum.docFreq(), actualEnum.docFreq());
              assertArrayEquals(docs(expectedEnum.postings(null)), docs(actualEnum.postings(null)));
            }
            final IOBooleanSupplier supplier = actual.iterator().prepareSeekExact(probe);
            assertEquals(probe.toString(), found, supplier != null && supplier.get());
          }
          final HashedTerms hashed = new HashedTerms(probes);
          final int[] expectedDocs = new int[hashed.size()];
          final int[] actualDocs = new int[hashed.size()];
          expected.lookupBatch(hashed, 0, hashed.size(), expectedDocs);
          actual.lookupBatch(hashed, 0, hashed.size(), actualDocs);
          assertArrayEquals(expectedDocs, actualDocs);
        }
        final List<BytesRef> some = probes.subList(0, Math.min(500, probes.size()));
        assertEquals(
            new IndexSearcher(expectedReader).count(new PrimaryKeyHashInSetQuery("id", some)),
            new IndexSearcher(actualReader).count(new PrimaryKeyHashInSetQuery("id", some)));
      }
    }
  }

  // ---- checks ----

  /** The postings of every term, read from {@code terms}' own ordered iteration. */
  static NavigableMap<BytesRef, int[]> postingsOf(Terms terms) throws IOException {
    final NavigableMap<BytesRef, int[]> postings = new TreeMap<>();
    final TermsEnum termsEnum = terms.iterator();
    PostingsEnum reuse = null;
    for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
      reuse = termsEnum.postings(reuse, PostingsEnum.NONE);
      postings.put(BytesRef.deepCopyOf(term), docs(reuse));
    }
    return postings;
  }

  static int[] docs(PostingsEnum postings) throws IOException {
    assertEquals(-1, postings.docID());
    final List<Integer> docs = new ArrayList<>();
    for (int doc = postings.nextDoc();
        doc != DocIdSetIterator.NO_MORE_DOCS;
        doc = postings.nextDoc()) {
      assertEquals(1, postings.freq());
      docs.add(doc);
    }
    assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.docID());
    return docs.stream().mapToInt(Integer::intValue).toArray();
  }

  /** What {@link HashTerms#lookupBatch} gives for a key of these documents, null if none. */
  static int lookupResult(int[] docs) {
    return docs == null ? HashTerms.ABSENT : docs.length == 1 ? docs[0] : HashTerms.MULTI;
  }

  private static int liveCount(int[] docs, Bits liveDocs) {
    if (docs == null) {
      return 0;
    }
    int count = 0;
    for (int doc : docs) {
      if (liveDocs == null || liveDocs.get(doc)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Checks every way to look keys of {@code field} up against the postings of the delegate, probing
   * every term of the field, near misses of them, and {@code extraProbes}.
   *
   * @param expectTable whether the field must have a hash table
   */
  static void checkField(
      LeafReader reader, String field, Collection<BytesRef> extraProbes, boolean expectTable)
      throws IOException {
    final Random random = random();
    final Terms terms = reader.terms(field);
    if (terms == null) {
      for (BytesRef probe : extraProbes) {
        assertEquals(0, reader.docFreq(new Term(field, probe)));
      }
      return;
    }
    assertEquals(expectTable, terms instanceof HashTerms);
    final Terms oracle = terms instanceof HashTerms hashTerms ? hashTerms.getDelegate() : terms;
    final NavigableMap<BytesRef, int[]> expected = postingsOf(oracle);

    final List<BytesRef> probes = new ArrayList<>(expected.keySet());
    final List<BytesRef> present = new ArrayList<>(expected.keySet());
    for (int i = 0; i < 200 && present.isEmpty() == false; i++) {
      final byte[] bytes = BytesRef.deepCopyOf(present.get(random.nextInt(present.size()))).bytes;
      if (bytes.length > 0) {
        final byte[] flipped = bytes.clone();
        flipped[random.nextInt(flipped.length)] ^= (byte) (1 + random.nextInt(255));
        probes.add(new BytesRef(flipped));
        probes.add(new BytesRef(bytes, 0, bytes.length - 1));
      }
      final byte[] extended = Arrays.copyOf(bytes, bytes.length + 1);
      extended[bytes.length] = (byte) random.nextInt(256);
      probes.add(new BytesRef(extended));
    }
    probes.addAll(extraProbes);
    probes.add(new BytesRef());
    Collections.shuffle(probes, random);

    final IndexSearcher searcher = new IndexSearcher(reader);
    TermsEnum termsEnum = terms.iterator();
    assertEquals(expectTable, termsEnum.preferSeekExact());
    PostingsEnum reuse = null;
    for (BytesRef probe : probes) {
      final int[] docs = expected.get(probe);
      final boolean found;
      switch (random.nextInt(3)) {
        case 0 -> found = termsEnum.seekExact(probe);
        case 1 -> {
          IOBooleanSupplier supplier = termsEnum.prepareSeekExact(probe);
          found = supplier != null && supplier.get();
        }
        default -> {
          termsEnum = terms.iterator();
          found = termsEnum.seekExact(probe);
        }
      }
      assertEquals(probe.toString(), docs != null, found);
      if (random.nextInt(10) == 0) {
        assertEquals(
            liveCount(docs, reader.getLiveDocs()),
            searcher.count(new TermQuery(new Term(field, probe))));
        assertEquals(docs == null ? 0 : docs.length, reader.docFreq(new Term(field, probe)));
      }
      if (found == false) {
        continue;
      }
      assertEquals(probe, termsEnum.term());
      assertEquals(docs.length, termsEnum.docFreq());
      assertEquals(docs.length, termsEnum.totalTermFreq());
      final int flags = random.nextBoolean() ? PostingsEnum.NONE : PostingsEnum.ALL;
      reuse = termsEnum.postings(random.nextBoolean() ? reuse : null, flags);
      assertArrayEquals(docs, docs(reuse));
      assertArrayEquals(docs, docs(termsEnum.impacts(PostingsEnum.FREQS)));

      // replay the term state on a fresh enum, as TermQuery does
      final TermState state = termsEnum.termState();
      final TermsEnum other = terms.iterator();
      other.seekExact(BytesRef.deepCopyOf(probe), state);
      assertEquals(probe, other.term());
      assertEquals(docs.length, other.docFreq());
      assertArrayEquals(docs, docs(other.postings(null, PostingsEnum.NONE)));

      // ordered operations carry on from the term that was found
      switch (random.nextInt(4)) {
        case 0 -> {
          final BytesRef next = termsEnum.next();
          assertEquals(expected.higherKey(probe), next);
          if (next != null) {
            assertArrayEquals(expected.get(next), docs(termsEnum.postings(null)));
          }
        }
        case 1 -> {
          // only for our own state: BlockTree returns its first term from next() on a fresh enum
          // positioned with seekExact(term, state)
          if (state instanceof HashTermState) {
            assertEquals(expected.higherKey(probe), other.next());
          }
        }
        case 2 -> {
          assertEquals(TermsEnum.SeekStatus.FOUND, termsEnum.seekCeil(probe));
          assertEquals(probe, termsEnum.term());
          assertArrayEquals(docs, docs(termsEnum.postings(null)));
        }
        default -> {}
      }
    }

    if (terms instanceof HashTerms hashTerms) {
      checkLookupBatch(hashTerms, expected, probes);
    }
  }

  /** {@link HashTerms#lookupBatch} on the whole batch, then on random ranges of it. */
  static void checkLookupBatch(
      HashTerms terms, NavigableMap<BytesRef, int[]> expected, List<BytesRef> probes)
      throws IOException {
    final Random random = random();
    final HashedTerms keys = new HashedTerms(probes);
    for (int iter = 0; iter < 5; iter++) {
      final int from = iter == 0 ? 0 : random.nextInt(keys.size() + 1);
      final int to = iter == 0 ? keys.size() : TestUtil.nextInt(random, from, keys.size());
      final int[] docs = new int[to - from + random.nextInt(3)];
      terms.lookupBatch(keys, from, to, docs);
      for (int i = from; i < to; i++) {
        assertEquals(
            keys.get(i).toString(), lookupResult(expected.get(keys.get(i))), docs[i - from]);
      }
    }
  }

  // ---- reads ----

  /**
   * Counts the positional reads of every data file of a directory, which is how lookups read it;
   * sequential reads, such as those of the header and footer when a segment is opened, are not
   * counted. Prefetches return what {@link #setPrefetchReads} says, so that tests choose whether
   * the bytes are to be read or are in memory already.
   */
  static final class ReadCountingDirectory extends FilterDirectory {
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
      return name.endsWith("." + DATA_EXTENSION)
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
    public boolean prefetch(long offset, long length) throws IOException {
      in.prefetch(offset, length);
      return dir.prefetchReads;
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

  // ---- the meta file ----

  /** The meta file of a segment, as fields that tests can read and change. */
  static final class Meta {
    byte[] header;
    String delegate;
    int hashFunction;
    final List<FieldMeta> fields = new ArrayList<>();

    static Meta read(Directory dir, String metaName) throws IOException {
      try (IndexInput in = dir.openInput(metaName, IOContext.READONCE)) {
        final Meta meta = new Meta();
        meta.header = new byte[headerLength(metaName)];
        in.readBytes(meta.header, 0, meta.header.length);
        meta.delegate = in.readString();
        meta.hashFunction = in.readVInt();
        for (int number = in.readInt(); number != -1; number = in.readInt()) {
          final FieldMeta field = new FieldMeta();
          field.number = number;
          field.numKeys = in.readVLong();
          field.layout = in.readByte();
          if (field.layout == PrimaryKeyHashPostingsFormat.LAYOUT_TERM_ORDER) {
            field.recordsOffset = in.readVLong();
            field.recordsLength = in.readVLong();
          } else {
            field.keyLength = in.readByte();
            field.controlOffset = in.readVLong();
          }
          field.partitionBits = in.readByte();
          final int numPartitions = 1 << field.partitionBits;
          field.capacities = new int[numPartitions];
          field.tails = new int[numPartitions];
          field.maxProbes = new int[numPartitions];
          for (int p = 0; p < numPartitions; p++) {
            field.capacities[p] = in.readVInt();
            field.tails[p] = in.readVInt();
            field.maxProbes[p] = in.readVInt();
          }
          field.filterBitsPerKey = in.readByte();
          if (field.filterBitsPerKey != 0) {
            field.filterOffset = in.readVLong();
            field.filterWords = in.readVInt();
          }
          meta.fields.add(field);
        }
        return meta;
      }
    }

    FieldMeta field(int number) {
      for (FieldMeta field : fields) {
        if (field.number == number) {
          return field;
        }
      }
      throw new AssertionError("no entry for field " + number);
    }

    /** Writes the fields as they are, whatever they say, and a valid footer. */
    void write(IndexOutput out) throws IOException {
      out.writeBytes(header, header.length);
      out.writeString(delegate);
      out.writeVInt(hashFunction);
      for (FieldMeta field : fields) {
        out.writeInt(field.number);
        out.writeVLong(field.numKeys);
        out.writeByte(field.layout);
        if (field.layout == PrimaryKeyHashPostingsFormat.LAYOUT_TERM_ORDER) {
          out.writeVLong(field.recordsOffset);
          out.writeVLong(field.recordsLength);
        } else {
          out.writeByte(field.keyLength);
          out.writeVLong(field.controlOffset);
        }
        out.writeByte(field.partitionBits);
        // as many partitions as were read, whatever partitionBits now says
        for (int p = 0; p < field.capacities.length; p++) {
          out.writeVInt(field.capacities[p]);
          out.writeVInt(field.tails[p]);
          out.writeVInt(field.maxProbes[p]);
        }
        out.writeByte(field.filterBitsPerKey);
        if (field.filterBitsPerKey != 0) {
          out.writeVLong(field.filterOffset);
          out.writeVInt(field.filterWords);
        }
      }
      out.writeInt(-1);
      CodecUtil.writeFooter(out);
    }
  }

  /** The entry of a field in the meta file. */
  static final class FieldMeta {
    int number;
    long numKeys;
    byte layout;
    long recordsOffset; // with records in term order
    long recordsLength;
    byte keyLength; // with inline keys
    long controlOffset;
    byte partitionBits;
    int[] capacities;
    int[] tails;
    int[] maxProbes;
    byte filterBitsPerKey;
    long filterOffset;
    int filterWords;

    /** Where the slots, or inline records, of the last partition end, as the file format says. */
    long slotsEnd() {
      final boolean inline = layout == PrimaryKeyHashPostingsFormat.LAYOUT_INLINE;
      final int stride = Integer.BYTES + keyLength;
      long end =
          inline
              ? controlOffset
              : IndexOutput.alignOffset(recordsOffset + recordsLength, Long.BYTES);
      for (int p = 0; p < capacities.length; p++) {
        final long slotCount = (long) capacities[p] + tails[p];
        end += PrimaryKeyHashPostingsFormat.controlLength(slotCount);
        end =
            inline
                ? IndexOutput.alignOffset(end, PrimaryKeyHashPostingsFormat.CACHE_LINE_BYTES)
                    + slotCount * stride
                    + PrimaryKeyHashPostingsFormat.inlinePadding(stride)
                : end + slotCount * Long.BYTES;
      }
      return end;
    }
  }
}
