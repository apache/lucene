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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterDirectoryReader;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SoftDeletesDirectoryReaderWrapper;
import org.apache.lucene.index.SoftDeletesRetentionMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.Inlining;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.index.AssertingDirectoryReader;
import org.apache.lucene.tests.index.PerThreadPKLookup;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOSupplier;

/**
 * {@link PrimaryKeyHashLookup} returns, for every key, single or in batches, what a plain loop of
 * {@link TermsEnum#seekExact(BytesRef)} over the segments, largest first, then a scan of the
 * postings for a live document, returns; and what {@link PerThreadPKLookup} returns. Indexes have
 * deletes, soft deletes, updates across segments, keys with several documents, segments of other
 * formats and many small segments; readers are wrapped in ways that hide the tables or change the
 * live documents.
 */
public class TestPrimaryKeyHashLookup extends LuceneTestCase {

  private static final String ID = "id";
  private static final String GROUP = "group";
  private static final String SEQ = "seq";
  private static final String SOFT = "soft";

  /** A value that lookups never write, for the entries of a result array past the keys. */
  private static final int UNWRITTEN = Integer.MIN_VALUE + 7;

  private static Document doc(BytesRef id, int seq) {
    Document doc = new Document();
    doc.add(new StringField(ID, id, Field.Store.NO));
    // keys of many documents each: almost every key of a segment has several documents
    doc.add(new StringField(GROUP, Integer.toString(seq % 7), Field.Store.NO));
    // one document each, to delete a single copy of a key
    doc.add(new StringField(SEQ, Integer.toString(seq), Field.Store.NO));
    return doc;
  }

  // ---- the oracle ----

  /** The segments of a reader in the order of PerThreadPKLookup: most live documents first. */
  private static List<LeafReaderContext> largestFirst(IndexReader reader) {
    List<LeafReaderContext> leaves = new ArrayList<>(reader.leaves());
    leaves.sort((a, b) -> Integer.compare(b.reader().numDocs(), a.reader().numDocs()));
    return leaves;
  }

  /**
   * The plain lookup of keys: {@code seekExact} in every segment, in order, in the delegate's terms
   * for a segment with a table so that the table is not used, then the first live document.
   */
  private static final class Oracle {
    private final List<LeafReaderContext> leaves;
    private final TermsEnum[] termsEnums;

    Oracle(IndexReader reader, String field) throws IOException {
      leaves = largestFirst(reader);
      termsEnums = new TermsEnum[leaves.size()];
      for (int i = 0; i < leaves.size(); i++) {
        Terms terms = leaves.get(i).reader().terms(field);
        if (terms instanceof HashTerms table) {
          terms = table.getDelegate();
        }
        termsEnums[i] = terms == null ? null : terms.iterator();
      }
    }

    int lookup(BytesRef key) throws IOException {
      for (int i = 0; i < leaves.size(); i++) {
        if (termsEnums[i] == null || termsEnums[i].seekExact(key) == false) {
          continue;
        }
        Bits live = leaves.get(i).reader().getLiveDocs();
        PostingsEnum postings = termsEnums[i].postings(null, PostingsEnum.NONE);
        for (int doc = postings.nextDoc();
            doc != DocIdSetIterator.NO_MORE_DOCS;
            doc = postings.nextDoc()) {
          if (live == null || live.get(doc)) {
            return leaves.get(i).docBase + doc;
          }
        }
      }
      return -1;
    }
  }

  // ---- probes ----

  /**
   * Keys to look up: the given keys, which include deleted ones, near misses of them, keys below
   * and above every term, and random ones.
   */
  private static List<BytesRef> probes(Collection<BytesRef> keys, Random random) {
    final List<BytesRef> present = new ArrayList<>(keys);
    final Set<BytesRef> probes = new LinkedHashSet<>(present);
    for (int i = 0; i < 300 && present.isEmpty() == false; i++) {
      byte[] bytes = BytesRef.deepCopyOf(present.get(random.nextInt(present.size()))).bytes;
      if (bytes.length > 0) {
        byte[] flipped = bytes.clone();
        flipped[random.nextInt(flipped.length)] ^= (byte) (1 + random.nextInt(255));
        probes.add(new BytesRef(flipped));
        probes.add(new BytesRef(bytes, 0, random.nextInt(bytes.length)));
      }
      byte[] extended = Arrays.copyOf(bytes, bytes.length + 1);
      extended[bytes.length] = (byte) random.nextInt(256);
      probes.add(new BytesRef(extended));
    }
    probes.add(new BytesRef());
    probes.add(new BytesRef(new byte[] {0}));
    for (int length : new int[] {1, 7, 8, 9, 20}) {
      byte[] high = new byte[length];
      Arrays.fill(high, (byte) 0xFF);
      probes.add(new BytesRef(high));
    }
    for (int i = 0; i < 50; i++) {
      probes.add(PrimaryKeyHashTestUtil.randomKey(random, random.nextInt(5), i));
    }
    List<BytesRef> list = new ArrayList<>(probes);
    Collections.shuffle(list, random);
    return list;
  }

  /**
   * The probe itself or, half of the time, a copy of its bytes at an offset of a larger array of
   * random bytes, so that keys neither all start at offset 0 nor all end where their array does.
   */
  private static BytesRef copyAtOffset(BytesRef key, Random random) {
    if (random.nextBoolean()) {
      return key;
    }
    int offset = random.nextInt(5);
    byte[] bytes = new byte[offset + key.length + random.nextInt(5)];
    random.nextBytes(bytes);
    System.arraycopy(key.bytes, key.offset, bytes, offset, key.length);
    return new BytesRef(bytes, offset, key.length);
  }

  // ---- the check ----

  /**
   * Checks every way to look up the probes in {@code field} of {@code reader} against the oracle
   * and {@link PerThreadPKLookup}: one key at a time, in batches of random ranges with duplicates,
   * and in batches whose sizes are around multiples of the chunk size, on the same instance in a
   * random order.
   *
   * @return the lookup, for further checks
   */
  static PrimaryKeyHashLookup checkLookups(IndexReader reader, String field, List<BytesRef> probes)
      throws IOException {
    final Random random = random();
    final PrimaryKeyHashLookup lookup = new PrimaryKeyHashLookup(reader, field);
    final Oracle oracle = new Oracle(reader, field);
    final PerThreadPKLookup perThread = new PerThreadPKLookup(reader, field);

    int numLeaves = 0;
    int numTables = 0;
    boolean anyFilter = false;
    for (LeafReaderContext context : reader.leaves()) {
      Terms terms = context.reader().terms(field);
      if (context.reader().numDocs() > 0 && terms != null) {
        numLeaves++;
        if (terms instanceof HashTerms table) {
          numTables++;
          anyFilter |= table.hasFilter();
        }
      }
    }
    assertEquals(numLeaves, lookup.numLeaves());
    assertEquals(numTables, lookup.numTables());
    final int chunk = lookup.chunkKeys();
    assertEquals(expectedChunkKeys(numTables, anyFilter), chunk);
    assertTrue(chunk == 1 || (long) chunk * numTables <= PrimaryKeyHashLookup.MAX_CHUNK_ENTRIES);
    assertEquals(0, lookup.scratchKeys());

    final Map<BytesRef, Integer> expected = new HashMap<>();
    for (BytesRef probe : probes) {
      int doc = oracle.lookup(probe);
      expected.put(probe, doc);
      assertEquals(probe.toString(), doc, perThread.lookup(probe));
    }

    // batch sizes around multiples of the chunk size, and random ones
    final List<Integer> sizes = new ArrayList<>();
    for (int m = 1; m <= 3; m++) {
      sizes.add(m * chunk - 1);
      sizes.add(m * chunk);
      sizes.add(m * chunk + 1);
    }
    for (int i = 0; i < 4; i++) {
      sizes.add(random.nextInt(probes.size() + 1));
    }
    sizes.add(probes.size());
    sizes.add(0);

    final List<Runnable> checks = new ArrayList<>();
    checks.add(
        () -> {
          for (BytesRef probe : probes) {
            int doc = lookupUnchecked(lookup, copyAtOffset(probe, random));
            assertEquals(probe.toString(), expected.get(probe).intValue(), doc);
          }
        });
    for (int size : sizes) {
      checks.add(() -> checkBatch(lookup, probes, Math.min(size, probes.size()), expected, random));
    }
    Collections.shuffle(checks, random);
    for (Runnable check : checks) {
      check.run();
    }
    // scratch space for a whole chunk, once a batch was looked up in tables
    assertEquals(numTables == 0 || probes.isEmpty() ? 0 : chunk, lookup.scratchKeys());
    return lookup;
  }

  /**
   * The keys of a chunk: as many as fit in {@link PrimaryKeyHashLookup#MAX_CHUNK_ENTRIES} entries,
   * at most {@link PrimaryKeyHashLookup#MAX_CHUNK_KEYS} unless a segment has a filter, and at least
   * one.
   */
  private static int expectedChunkKeys(int numTables, boolean anyFilter) {
    if (numTables == 0) {
      return PrimaryKeyHashLookup.MAX_CHUNK_KEYS;
    }
    final int fit = Math.max(1, PrimaryKeyHashLookup.MAX_CHUNK_ENTRIES / numTables);
    return anyFilter ? fit : Math.min(PrimaryKeyHashLookup.MAX_CHUNK_KEYS, fit);
  }

  private static int lookupUnchecked(PrimaryKeyHashLookup lookup, BytesRef key) {
    try {
      return lookup.lookup(key);
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  /**
   * A batch of {@code size} keys, random probes and repeats of them, at a random offset of a larger
   * array, into a result array longer than needed whose extra entries must stay unwritten.
   */
  private static void checkBatch(
      PrimaryKeyHashLookup lookup,
      List<BytesRef> probes,
      int size,
      Map<BytesRef, Integer> expected,
      Random random) {
    final int from = random.nextInt(4);
    final BytesRef[] keys = new BytesRef[from + size + random.nextInt(4)];
    final BytesRef[] originals = new BytesRef[keys.length];
    for (int i = 0; i < keys.length; i++) {
      BytesRef probe = probes.get(random.nextInt(probes.size()));
      originals[i] = probe;
      keys[i] = copyAtOffset(probe, random);
    }
    // with repeats of earlier keys of the batch
    for (int i = from + 1; i < from + size; i++) {
      if (random.nextInt(10) == 0) {
        int j = TestUtil.nextInt(random, from, i - 1);
        keys[i] = keys[j];
        originals[i] = originals[j];
      }
    }
    final int[] docs = new int[size + random.nextInt(3)];
    Arrays.fill(docs, UNWRITTEN);
    try {
      lookup.lookup(keys, from, from + size, docs);
    } catch (IOException e) {
      throw new AssertionError(e);
    }
    for (int i = 0; i < size; i++) {
      assertEquals(
          originals[from + i].toString(), expected.get(originals[from + i]).intValue(), docs[i]);
    }
    for (int i = size; i < docs.length; i++) {
      assertEquals(UNWRITTEN, docs[i]);
    }
  }

  // ---- indexes ----

  /** How {@link #writeRandomIndex} writes. */
  private record Options(
      boolean softDeletes, boolean retainSoftDeletes, boolean smallSegments, boolean mixedFormats) {

    static Options randomOptions(Random random) {
      boolean softDeletes = random.nextBoolean();
      return new Options(
          softDeletes,
          softDeletes && random.nextBoolean(),
          random.nextInt(3) == 0,
          random.nextInt(3) == 0);
    }
  }

  private static IndexWriterConfig config(Random random, Codec codec, Options options) {
    IndexWriterConfig iwc = newIndexWriterConfig(new MockAnalyzer(random)).setCodec(codec);
    if (options.smallSegments()) {
      iwc.setMergePolicy(NoMergePolicy.INSTANCE)
          .setMaxBufferedDocs(TestUtil.nextInt(random, 2, 30))
          .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
    }
    if (options.softDeletes()) {
      iwc.setSoftDeletesField(SOFT);
      if (options.retainSoftDeletes()) {
        // soft-deleted versions survive merges: their keys then have several documents
        iwc.setMergePolicy(
            new SoftDeletesRetentionMergePolicy(
                SOFT, MatchAllDocsQuery::new, iwc.getMergePolicy()));
      }
    }
    return iwc;
  }

  /**
   * Writes a random index in one or more sessions, each with the table format or, for mixed
   * formats, sometimes with the default codec, and returns the ids it ever held, deleted or not.
   * Ids are added, updated (in place or with a soft update, so that the old version is deleted in
   * whichever segment holds it), deleted (hard or soft), and added again without deleting the
   * previous copies, of which single copies are then deleted again by their sequence number.
   *
   * @param writerConsumer gets the writer of the last session before it is closed
   */
  private static List<BytesRef> writeRandomIndex(
      Directory dir, Random random, Options options, int numOps, WriterConsumer writerConsumer)
      throws IOException {
    final int shape = random.nextInt(6); // 5 mixes all shapes
    final Codec tableCodec =
        TestUtil.alwaysPostingsFormat(PrimaryKeyHashTestUtil.randomFormat(random));
    final List<BytesRef> ids = new ArrayList<>();
    final int numSessions = options.mixedFormats() ? TestUtil.nextInt(random, 2, 4) : 1;
    final boolean tablesFirst = random.nextBoolean();
    int seq = 0;
    for (int session = 0; session < numSessions; session++) {
      final Codec codec;
      if (options.mixedFormats()) {
        // the first two sessions have one format each, the others either
        final boolean tables = session < 2 ? tablesFirst == (session == 0) : random.nextBoolean();
        codec = tables ? tableCodec : TestUtil.getDefaultCodec();
      } else {
        codec = tableCodec;
      }
      try (IndexWriter writer = new IndexWriter(dir, config(random, codec, options))) {
        final int ops = numOps / numSessions;
        for (int op = 0; op < ops; op++) {
          final int action = ids.isEmpty() ? 0 : random.nextInt(100);
          if (action < 45) {
            BytesRef id =
                PrimaryKeyHashTestUtil.randomKey(
                    random, shape == 5 ? random.nextInt(5) : shape, ids.size());
            ids.add(id);
            writer.addDocument(doc(id, seq++));
          } else if (action < 62) {
            BytesRef id = ids.get(random.nextInt(ids.size()));
            if (options.softDeletes() && random.nextBoolean()) {
              writer.softUpdateDocument(
                  new Term(ID, id), doc(id, seq++), new NumericDocValuesField(SOFT, 1));
            } else {
              writer.updateDocument(new Term(ID, id), doc(id, seq++));
            }
          } else if (action < 70) {
            // another live copy: the key is no longer unique
            writer.addDocument(doc(ids.get(random.nextInt(ids.size())), seq++));
          } else if (action < 78) {
            BytesRef id = ids.get(random.nextInt(ids.size()));
            if (options.softDeletes() && random.nextBoolean()) {
              writer.updateDocValues(new Term(ID, id), new NumericDocValuesField(SOFT, 1));
            } else {
              writer.deleteDocuments(new Term(ID, id));
            }
          } else if (action < 84) {
            // one document, maybe one of several copies of its key
            writer.deleteDocuments(new Term(SEQ, Integer.toString(random.nextInt(seq))));
          } else if (action < 86) {
            writer.commit();
          } else if (action < 88) {
            writer.flush();
          } else if (action == 88 && options.smallSegments() == false && random.nextInt(5) == 0) {
            writer.forceMerge(TestUtil.nextInt(random, 1, 4));
          } else {
            BytesRef id =
                PrimaryKeyHashTestUtil.randomKey(
                    random, shape == 5 ? random.nextInt(5) : shape, ids.size());
            ids.add(id);
            writer.addDocument(doc(id, seq++));
          }
        }
        if (session == numSessions - 1) {
          writerConsumer.accept(writer);
        }
      }
    }
    return ids;
  }

  @FunctionalInterface
  private interface WriterConsumer {
    void accept(IndexWriter writer) throws IOException;
  }

  private static List<BytesRef> groups() {
    List<BytesRef> groups = new ArrayList<>();
    for (int i = 0; i < 8; i++) {
      groups.add(new BytesRef(Integer.toString(i)));
    }
    return groups;
  }

  // ---- tests ----

  /** Random indexes, read by NRT readers (which apply soft deletes) and by readers of a commit. */
  public void testRandomIndexes() throws IOException {
    final Random random = random();
    final Options options = Options.randomOptions(random);
    try (Directory dir = newDirectory()) {
      final List<BytesRef> ids =
          writeRandomIndex(
              dir,
              random,
              options,
              atLeast(1500),
              writer -> {
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                  checkLookups(reader, ID, probes(termsOf(reader, ID), random));
                  checkLookups(reader, GROUP, probes(groups(), random));
                }
                writer.commit();
              });
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        checkLookups(reader, ID, probes(ids, random));
        checkLookups(reader, SEQ, probes(termsOf(reader, SEQ), random));
        if (options.softDeletes()) {
          checkLookups(
              new SoftDeletesDirectoryReaderWrapper(reader, SOFT), ID, probes(ids, random));
        }
      }
    }
  }

  /** The distinct terms of a field of a reader, of deleted documents too. */
  private static List<BytesRef> termsOf(IndexReader reader, String field) throws IOException {
    Set<BytesRef> terms = new HashSet<>();
    for (LeafReaderContext context : reader.leaves()) {
      Terms t = context.reader().terms(field);
      if (t == null) {
        continue;
      }
      TermsEnum termsEnum = t.iterator();
      for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
        terms.add(BytesRef.deepCopyOf(term));
      }
    }
    return new ArrayList<>(terms);
  }

  /**
   * Segments written with the default codec next to segments with tables, and ids updated and
   * deleted across them.
   */
  public void testMixedFormats() throws IOException {
    final Random random = random();
    final Options options = new Options(random.nextBoolean(), false, random.nextBoolean(), true);
    try (Directory dir = newDirectory()) {
      final Codec tableCodec =
          TestUtil.alwaysPostingsFormat(PrimaryKeyHashTestUtil.randomFormat(random));
      final List<BytesRef> ids = new ArrayList<>();
      int seq = 0;
      // default codec, then tables, then default again: no merges, so that both stay
      final Codec[] codecs = {TestUtil.getDefaultCodec(), tableCodec, TestUtil.getDefaultCodec()};
      for (Codec codec : codecs) {
        IndexWriterConfig iwc =
            config(random, codec, options).setMergePolicy(NoMergePolicy.INSTANCE);
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
          final int numDocs = TestUtil.nextInt(random, 100, 400);
          for (int i = 0; i < numDocs; i++) {
            if (ids.isEmpty() == false && random.nextInt(4) == 0) {
              BytesRef id = ids.get(random.nextInt(ids.size()));
              if (random.nextInt(3) == 0) {
                writer.deleteDocuments(new Term(ID, id));
              } else {
                writer.updateDocument(new Term(ID, id), doc(id, seq++));
              }
            } else {
              BytesRef id = PrimaryKeyHashTestUtil.randomKey(random, random.nextInt(5), seq);
              ids.add(id);
              writer.addDocument(doc(id, seq++));
            }
            if (random.nextInt(100) == 0) {
              writer.flush();
            }
          }
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        PrimaryKeyHashLookup lookup = checkLookups(reader, ID, probes(ids, random));
        assertTrue(lookup.toString(), lookup.numTables() > 0);
        assertTrue(lookup.toString(), lookup.numTables() < lookup.numLeaves());
        checkLookups(reader, GROUP, probes(groups(), random));
      }
    }
  }

  /**
   * A field without a table, though its segments are of the table format: the field is then looked
   * up in its terms dictionary, while the id field of the same segments uses its table.
   */
  public void testFieldWithoutTable() throws IOException {
    final Random random = random();
    final PostingsFormat format = new PrimaryKeyHashPostingsFormat();
    // records may take at most 0 bytes: no field written with it gets a table
    final PostingsFormat noTable =
        new PrimaryKeyHashPostingsFormat(
            TestUtil.getDefaultPostingsFormat(),
            PrimaryKeyHashPostingsFormat.DEFAULT_LOAD_FACTOR,
            PrimaryKeyHashPostingsFormat.DEFAULT_MAX_KEYS_PER_PARTITION,
            PrimaryKeyHashPostingsFormat.DEFAULT_MAX_PROBE_LIMIT,
            0,
            true,
            PrimaryKeyHashPostingsFormat.DEFAULT_FILTER_BITS_PER_KEY,
            Inlining.IF_NOT_LARGER);
    final Codec codec =
        new Lucene104Codec() {
          @Override
          public PostingsFormat getPostingsFormatForField(String field) {
            return GROUP.equals(field) ? noTable : format;
          }
        };
    final List<BytesRef> ids = new ArrayList<>();
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          new IndexWriterConfig(new MockAnalyzer(random))
              .setCodec(codec)
              .setMergePolicy(NoMergePolicy.INSTANCE)
              .setMaxBufferedDocs(100_000)
              .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        // segments of 50 documents: each of the 7 groups has several documents in every segment
        final int numDocs = 50 * TestUtil.nextInt(random, 2, 8);
        for (int i = 0; i < numDocs; i++) {
          BytesRef id = PrimaryKeyHashTestUtil.randomKey(random, 1, i);
          ids.add(id);
          writer.addDocument(doc(id, i));
          if (i % 50 == 49) {
            writer.flush();
          }
          if (random.nextInt(10) == 0) {
            writer.deleteDocuments(new Term(SEQ, Integer.toString(random.nextInt(i + 1))));
          }
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        PrimaryKeyHashLookup groups = checkLookups(reader, GROUP, probes(groups(), random));
        assertEquals(0, groups.numTables());
        PrimaryKeyHashLookup lookup = checkLookups(reader, ID, probes(ids, random));
        assertEquals(lookup.numLeaves(), lookup.numTables());
      }
    }
  }

  /**
   * Reader wrappers: one that wraps terms and so hides the tables, one that hides documents without
   * wrapping terms, so that tables are used with the wrapper's live documents, soft deletes applied
   * by a wrapper, and the test framework's random wrappers.
   */
  public void testWrappedReaders() throws IOException {
    final Random random = random();
    final Options options =
        new Options(true, random.nextBoolean(), random.nextBoolean(), random.nextInt(4) == 0);
    try (Directory dir = newDirectory()) {
      final List<BytesRef> ids = writeRandomIndex(dir, random, options, atLeast(800), _ -> {});
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        final List<BytesRef> probes = probes(ids, random);
        final PrimaryKeyHashLookup plain = checkLookups(reader, ID, probes);

        PrimaryKeyHashLookup asserting =
            checkLookups(new AssertingDirectoryReader(reader), ID, probes);
        assertEquals(0, asserting.numTables());

        DirectoryReader hiding = new HidingDirectoryReader(reader, random.nextLong());
        PrimaryKeyHashLookup hidden = checkLookups(hiding, ID, probes);
        // same tables, other live documents (a segment whose documents are all hidden is skipped)
        assertTrue(hidden.numTables() <= plain.numTables());
        int hiddenTables = 0;
        for (LeafReaderContext context : hiding.leaves()) {
          if (context.reader().numDocs() > 0 && context.reader().terms(ID) instanceof HashTerms) {
            hiddenTables++;
          }
        }
        assertEquals(hiddenTables, hidden.numTables());

        checkLookups(new SoftDeletesDirectoryReaderWrapper(reader, SOFT), ID, probes);
        checkLookups(
            new HidingDirectoryReader(
                new SoftDeletesDirectoryReaderWrapper(reader, SOFT), random.nextLong()),
            ID,
            probes);
        for (int i = 0; i < 3; i++) {
          checkLookups(wrapReader(reader), ID, probes);
        }
        checkLookups(new MultiReader(reader, new AssertingDirectoryReader(reader)), ID, probes);
      }
    }
  }

  /** Hides a random subset of the live documents of every segment, without wrapping its terms. */
  private static final class HidingDirectoryReader extends FilterDirectoryReader {
    private final long seed;

    HidingDirectoryReader(DirectoryReader in, long seed) throws IOException {
      super(
          in,
          new SubReaderWrapper() {
            private final Random random = new Random(seed);

            @Override
            public LeafReader wrap(LeafReader reader) {
              return new HidingLeafReader(reader, random);
            }
          });
      this.seed = seed;
    }

    @Override
    protected DirectoryReader doWrapDirectoryReader(DirectoryReader in) throws IOException {
      return new HidingDirectoryReader(in, seed);
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
  }

  private static final class HidingLeafReader extends FilterLeafReader {
    private final FixedBitSet liveDocs;
    private final int numDocs;

    HidingLeafReader(LeafReader in, Random random) {
      super(in);
      final Bits inLiveDocs = in.getLiveDocs();
      // sometimes every document, so that a whole segment is skipped
      final int hideOneIn = random.nextInt(10) == 0 ? 1 : TestUtil.nextInt(random, 2, 5);
      liveDocs = new FixedBitSet(in.maxDoc());
      for (int doc = 0; doc < in.maxDoc(); doc++) {
        if ((inLiveDocs == null || inLiveDocs.get(doc)) && random.nextInt(hideOneIn) != 0) {
          liveDocs.set(doc);
        }
      }
      numDocs = liveDocs.cardinality();
    }

    @Override
    public Bits getLiveDocs() {
      return liveDocs;
    }

    @Override
    public int numDocs() {
      return numDocs;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
  }

  /**
   * More segments with a table than a chunk has entries, so that chunks have a single key, with ids
   * updated and deleted across segments.
   */
  public void testManySmallSegments() throws IOException {
    final Random random = random();
    final Codec codec = TestUtil.alwaysPostingsFormat(PrimaryKeyHashTestUtil.randomFormat(random));
    final int numSegments =
        PrimaryKeyHashLookup.MAX_CHUNK_ENTRIES + TestUtil.nextInt(random, 1, 40);
    final List<BytesRef> ids = new ArrayList<>();
    final List<BytesRef> anchors = new ArrayList<>();
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          new IndexWriterConfig(new MockAnalyzer(random))
              .setCodec(codec)
              .setMergePolicy(NoMergePolicy.INSTANCE)
              .setMaxBufferedDocs(100_000)
              .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
      final int shape = random.nextInt(5);
      int seq = 0;
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        for (int s = 0; s < numSegments; s++) {
          // an id that is never updated, so that every segment keeps a live document
          final BytesRef anchor = new BytesRef("anchor" + s);
          anchors.add(anchor);
          writer.addDocument(doc(anchor, seq++));
          final int numDocs = TestUtil.nextInt(random, 0, 3);
          for (int i = 0; i < numDocs; i++) {
            if (ids.isEmpty() == false && random.nextInt(3) == 0) {
              BytesRef id = ids.get(random.nextInt(ids.size()));
              writer.updateDocument(new Term(ID, id), doc(id, seq++));
            } else {
              BytesRef id = PrimaryKeyHashTestUtil.randomKey(random, shape, seq);
              ids.add(id);
              writer.addDocument(doc(id, seq++));
            }
          }
          writer.flush();
        }
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          assertEquals(numSegments, reader.leaves().size());
          ids.addAll(anchors);
          PrimaryKeyHashLookup lookup = checkLookups(reader, ID, probes(ids, random));
          assertEquals(numSegments, lookup.numTables());
          assertEquals(1, lookup.chunkKeys());
        }
      }
    }
  }

  /**
   * Chunks have as many keys as fit for 1 to 20 segments, and more with a filter, so that the
   * stages after the filters have entries to overlap; the segments' ranges of keys are disjoint, so
   * that most keys are out of the range of most segments, or not. Scratch space is allocated for
   * one key by the first lookup of one, and for a whole chunk by the first lookup of several keys,
   * which later batches of any size do not allocate again.
   */
  public void testChunksAndScratchSpace() throws IOException {
    final Random random = random();
    final boolean filter = random.nextBoolean();
    final Codec codec =
        TestUtil.alwaysPostingsFormat(
            PrimaryKeyHashTestUtil.randomFormat(
                random,
                filter ? PrimaryKeyHashTestUtil.randomFilterBits(random) : 0,
                PrimaryKeyHashTestUtil.randomInlining(random)));
    final int numSegments = TestUtil.nextInt(random, 1, 20);
    final boolean sequential = random.nextBoolean();
    final List<BytesRef> ids = new ArrayList<>();
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          new IndexWriterConfig(new MockAnalyzer(random))
              .setCodec(codec)
              .setMergePolicy(NoMergePolicy.INSTANCE)
              .setMaxBufferedDocs(100_000)
              .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
      int seq = 0;
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        for (int s = 0; s < numSegments; s++) {
          final int numDocs = TestUtil.nextInt(random, 1, 200);
          for (int i = 0; i < numDocs; i++) {
            BytesRef id =
                sequential
                    ? new BytesRef(String.format(Locale.ROOT, "%010d", seq))
                    : PrimaryKeyHashTestUtil.randomKey(random, 0, seq);
            ids.add(id);
            writer.addDocument(doc(id, seq++));
          }
          writer.flush();
        }
        for (int i = 0; i < seq / 10; i++) {
          writer.deleteDocuments(new Term(SEQ, Integer.toString(random.nextInt(seq))));
        }
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          final PrimaryKeyHashLookup lookup = new PrimaryKeyHashLookup(reader, ID);
          final int chunk = lookup.chunkKeys();
          assertEquals(expectedChunkKeys(lookup.numTables(), filter), chunk);
          final Oracle oracle = new Oracle(reader, ID);
          assertEquals(0, lookup.scratchKeys());
          assertEquals(oracle.lookup(ids.get(0)), lookup.lookup(ids.get(0)));
          assertEquals(1, lookup.scratchKeys());
          final List<BytesRef> probes = probes(ids, random);
          for (int size : new int[] {2, 3, chunk - 1, chunk, chunk + 1, 2 * chunk + 5, 1}) {
            final BytesRef[] keys = new BytesRef[size];
            for (int i = 0; i < size; i++) {
              keys[i] = probes.get(random.nextInt(probes.size()));
            }
            final int[] docs = new int[size];
            lookup.lookup(keys, 0, size, docs);
            for (int i = 0; i < size; i++) {
              assertEquals(keys[i].toString(), oracle.lookup(keys[i]), docs[i]);
            }
            assertEquals(chunk, lookup.scratchKeys());
          }
          assertEquals(
              oracle.lookup(ids.get(1 % ids.size())), lookup.lookup(ids.get(1 % ids.size())));
          assertEquals(chunk, lookup.scratchKeys());
          checkLookups(reader, ID, probes);
        }
      }
    }
  }

  /**
   * A key live in several segments resolves to its first live document in the largest segment by
   * live documents, which need not be the largest by documents; then, as its documents are deleted,
   * to the next.
   */
  public void testOrderOfSegments() throws IOException {
    final Random random = random();
    final Codec codec = TestUtil.alwaysPostingsFormat(PrimaryKeyHashTestUtil.randomFormat(random));
    final BytesRef key = new BytesRef("key");
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          new IndexWriterConfig(new MockAnalyzer(random))
              .setCodec(codec)
              .setMergePolicy(NoMergePolicy.INSTANCE)
              .setMaxBufferedDocs(100_000)
              .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        int seq = 0;
        // segment A: the most documents, but most of them deleted below; two copies of the key
        for (int i = 0; i < 20; i++) {
          writer.addDocument(doc(i == 3 || i == 7 ? key : new BytesRef("a" + i), seq++));
        }
        writer.flush();
        // segment B: 10 documents, one copy of the key
        for (int i = 0; i < 10; i++) {
          writer.addDocument(doc(i == 4 ? key : new BytesRef("b" + i), seq++));
        }
        writer.flush();
        // segment C: 6 documents, one copy of the key
        for (int i = 0; i < 6; i++) {
          writer.addDocument(doc(i == 1 ? key : new BytesRef("c" + i), seq++));
        }
        writer.flush();
        for (int i = 10; i < 20; i++) {
          writer.deleteDocuments(new Term(SEQ, Integer.toString(i))); // A has 10 live left
        }
        writer.deleteDocuments(new Term(SEQ, "9")); // A has 9: B comes first
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          assertEquals(3, reader.leaves().size());
          final LeafReader a = reader.leaves().get(0).reader();
          final LeafReader b = reader.leaves().get(1).reader();
          // A comes first by documents, B by live documents
          assertTrue(a.maxDoc() > b.maxDoc());
          assertTrue(a.numDocs() < b.numDocs());
          int docBaseB = reader.leaves().get(1).docBase;
          PrimaryKeyHashLookup lookup = new PrimaryKeyHashLookup(reader, ID);
          assertEquals(docBaseB + 4, lookup.lookup(key));
          assertEquals(docBaseB + 4, lookupOne(lookup, key));
        }
        writer.deleteDocuments(new Term(SEQ, Integer.toString(20 + 4))); // B's copy
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          // A, with 9 live documents, before C: its first live copy
          PrimaryKeyHashLookup lookup = new PrimaryKeyHashLookup(reader, ID);
          assertEquals(reader.leaves().get(0).docBase + 3, lookup.lookup(key));
          assertEquals(reader.leaves().get(0).docBase + 3, lookupOne(lookup, key));
        }
        writer.deleteDocuments(new Term(SEQ, "3"));
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          PrimaryKeyHashLookup lookup = new PrimaryKeyHashLookup(reader, ID);
          assertEquals(reader.leaves().get(0).docBase + 7, lookup.lookup(key));
        }
        writer.deleteDocuments(new Term(SEQ, "7"));
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          PrimaryKeyHashLookup lookup = new PrimaryKeyHashLookup(reader, ID);
          assertEquals(reader.leaves().get(2).docBase + 1, lookup.lookup(key));
          checkLookups(reader, ID, probes(termsOf(reader, ID), random));
        }
        writer.deleteDocuments(new Term(SEQ, Integer.toString(30 + 1)));
        try (DirectoryReader reader = DirectoryReader.open(writer)) {
          PrimaryKeyHashLookup lookup = new PrimaryKeyHashLookup(reader, ID);
          assertEquals(-1, lookup.lookup(key));
          assertEquals(-1, lookupOne(lookup, key));
        }
      }
    }
  }

  private static int lookupOne(PrimaryKeyHashLookup lookup, BytesRef key) throws IOException {
    int[] docs = new int[1];
    lookup.lookup(new BytesRef[] {key}, 0, 1, docs);
    return docs[0];
  }

  /** Readers without segments, and fields that no segment or only some segments have. */
  public void testEmptyReaderAndMissingField() throws IOException {
    final Random random = random();
    try (MultiReader empty = new MultiReader()) {
      PrimaryKeyHashLookup lookup = new PrimaryKeyHashLookup(empty, ID);
      assertEquals(0, lookup.numLeaves());
      assertEquals(-1, lookup.lookup(new BytesRef("a")));
      int[] docs = {UNWRITTEN, UNWRITTEN};
      lookup.lookup(new BytesRef[] {new BytesRef("a"), new BytesRef()}, 0, 2, docs);
      assertArrayEquals(new int[] {-1, -1}, docs);
    }
    final Codec codec = TestUtil.alwaysPostingsFormat(PrimaryKeyHashTestUtil.randomFormat(random));
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          new IndexWriterConfig(new MockAnalyzer(random))
              .setCodec(codec)
              .setMergePolicy(
                  new FilterMergePolicy(NoMergePolicy.INSTANCE) {
                    @Override
                    public boolean keepFullyDeletedSegment(
                        IOSupplier<CodecReader> readerIOSupplier) {
                      return true;
                    }
                  });
      List<BytesRef> others = new ArrayList<>();
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        writer.addDocument(doc(new BytesRef("x"), 0));
        writer.flush();
        // a segment without the id field
        for (int i = 0; i < 5; i++) {
          Document doc = new Document();
          BytesRef other = new BytesRef("o" + i);
          others.add(other);
          doc.add(new StringField("other", other, Field.Store.NO));
          writer.addDocument(doc);
        }
        writer.flush();
        // a segment whose documents are all deleted
        writer.addDocument(doc(new BytesRef("y"), 1));
        writer.flush();
        writer.deleteDocuments(new Term(ID, "y"));
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(3, reader.leaves().size());
        assertEquals(0, reader.leaves().get(2).reader().numDocs());
        PrimaryKeyHashLookup lookup = new PrimaryKeyHashLookup(reader, ID);
        assertEquals(1, lookup.numLeaves());
        assertEquals(reader.leaves().get(0).docBase, lookup.lookup(new BytesRef("x")));
        assertEquals(-1, lookup.lookup(new BytesRef("y")));
        checkLookups(reader, ID, probes(List.of(new BytesRef("x"), new BytesRef("y")), random));
        checkLookups(reader, "other", probes(others, random));
        PrimaryKeyHashLookup missing = checkLookups(reader, "missing", probes(others, random));
        assertEquals(0, missing.numLeaves());
      }
    }
  }

  public void testArguments() throws IOException {
    try (Directory dir = newDirectory()) {
      IndexWriterConfig iwc =
          new IndexWriterConfig(new MockAnalyzer(random()))
              .setCodec(
                  TestUtil.alwaysPostingsFormat(PrimaryKeyHashTestUtil.randomFormat(random())));
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        writer.addDocument(doc(new BytesRef("a"), 0));
        writer.addDocument(doc(new BytesRef("b"), 1));
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        expectThrows(NullPointerException.class, () -> new PrimaryKeyHashLookup(null, ID));
        expectThrows(NullPointerException.class, () -> new PrimaryKeyHashLookup(reader, null));
        PrimaryKeyHashLookup lookup = new PrimaryKeyHashLookup(reader, ID);
        assertEquals(1, lookup.numTables());
        expectThrows(NullPointerException.class, () -> lookup.lookup((BytesRef) null));
        final BytesRef[] keys = {new BytesRef("a"), new BytesRef("b"), new BytesRef("c")};
        final int[] docs = new int[3];
        expectThrows(IndexOutOfBoundsException.class, () -> lookup.lookup(keys, -1, 2, docs));
        expectThrows(IndexOutOfBoundsException.class, () -> lookup.lookup(keys, 2, 1, docs));
        expectThrows(IndexOutOfBoundsException.class, () -> lookup.lookup(keys, 0, 4, docs));
        expectThrows(IllegalArgumentException.class, () -> lookup.lookup(keys, 0, 3, new int[2]));
        expectThrows(NullPointerException.class, () -> lookup.lookup(null, 0, 0, docs));
        expectThrows(NullPointerException.class, () -> lookup.lookup(keys, 0, 0, null));
        final BytesRef[] withNull = {new BytesRef("a"), null, new BytesRef("b")};
        Arrays.fill(docs, UNWRITTEN);
        expectThrows(NullPointerException.class, () -> lookup.lookup(withNull, 0, 3, docs));
        assertArrayEquals(new int[] {UNWRITTEN, UNWRITTEN, UNWRITTEN}, docs);
        // a null key outside of the range is not looked at
        lookup.lookup(withNull, 2, 3, docs);
        assertEquals(1, docs[0]);
        lookup.lookup(keys, 1, 1, docs); // empty range
        assertEquals(1, docs[0]);
        lookup.lookup(keys, 0, 3, docs);
        assertArrayEquals(new int[] {0, 1, -1}, docs);
      }
    }
  }

  /**
   * Segments with and without filters, with inline keys and records in term order, side by side, as
   * after a change of format or a merge of segments of keys of other lengths; their ranges of keys
   * overlap, or not, so that a key is looked up in several segments, in one, or in none.
   */
  public void testSegmentsOfDifferentTables() throws IOException {
    final Random random = random();
    final int shape = random.nextInt(3); // keys of one length
    final boolean disjoint = random.nextBoolean();
    final List<BytesRef> ids = new ArrayList<>();
    final Set<Boolean> filters = new HashSet<>();
    final Set<Boolean> inline = new HashSet<>();
    try (Directory dir = newDirectory()) {
      final int numSegments = TestUtil.nextInt(random, 2, 12);
      int seq = 0;
      for (int s = 0; s < numSegments; s++) {
        // a filter and records in term order in the first segment, inline keys only in the second
        final boolean filter = s < 2 ? s == 0 : random.nextBoolean();
        final boolean inlineKeys = s < 2 ? s == 1 : random.nextBoolean();
        final IndexWriterConfig iwc =
            new IndexWriterConfig(new MockAnalyzer(random))
                .setCodec(
                    TestUtil.alwaysPostingsFormat(
                        PrimaryKeyHashTestUtil.randomFormat(
                            random,
                            filter ? PrimaryKeyHashTestUtil.randomFilterBits(random) : 0,
                            inlineKeys ? Inlining.ALWAYS : Inlining.NEVER)))
                .setMergePolicy(NoMergePolicy.INSTANCE)
                .setMaxBufferedDocs(100_000)
                .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
        // a session per segment, which is then written with the session's format
        try (IndexWriter writer = new IndexWriter(dir, iwc)) {
          final int numDocs = TestUtil.nextInt(random, 50, 500);
          for (int i = 0; i < numDocs; i++) {
            BytesRef id;
            if (ids.isEmpty() == false && random.nextInt(20) == 0) {
              id = ids.get(random.nextInt(ids.size())); // another copy, in this segment or not
            } else if (disjoint) {
              // the keys of segment s start with s: the ranges of segments do not overlap
              id = new BytesRef(String.format(Locale.ROOT, "%02d-%07d", s, seq));
              ids.add(id);
            } else {
              id = PrimaryKeyHashTestUtil.randomKey(random, shape, seq);
              ids.add(id);
            }
            writer.addDocument(doc(id, seq++));
          }
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        assertEquals(numSegments, reader.leaves().size());
        for (LeafReaderContext context : reader.leaves()) {
          final HashTerms table = (HashTerms) context.reader().terms(ID);
          filters.add(table.hasFilter());
          inline.add(table.isInline());
        }
        assertEquals(Set.of(true, false), filters);
        assertEquals(Set.of(true, false), inline);
        checkLookups(reader, ID, probes(ids, random));
      }
    }
  }
}
