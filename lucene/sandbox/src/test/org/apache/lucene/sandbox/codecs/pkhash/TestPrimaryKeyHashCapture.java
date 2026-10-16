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

import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.assertSameFiles;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.checkField;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.checkLookupBatch;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.postingsOf;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CompoundFormat;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90CompoundFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.SoftDeletesRetentionMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.Inlining;
import org.apache.lucene.search.DocAndFloatFeatureBuffer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.tests.codecs.asserting.AssertingCodec;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOBooleanSupplier;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.lucene.util.Version;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.CompiledAutomaton;
import org.junit.Before;

/**
 * Keys are captured during the delegate's own pass over the terms and postings. The files are the
 * same as when keys are read in a separate pass, also with delegates that read the postings in
 * unusual but complete ways; delegates that read fields in any other way get the separate pass.
 */
public class TestPrimaryKeyHashCapture extends LuceneTestCase {

  private static final String SOFT_DELETES = "soft";
  private static final List<String> DOCS_FIELDS = List.of("group", "id", "seq");

  /** Everything the index depends on, so that it can be written twice the same. */
  private record Setup(
      long seed,
      int numOps,
      int keyShape,
      int maxBufferedDocs,
      int mergeFactor,
      boolean indexSort,
      boolean softDeletes,
      float loadFactor,
      int maxKeysPerPartition,
      int filterBitsPerKey,
      Inlining inlining) {

    static Setup random(Random random) {
      return new Setup(
          random.nextLong(),
          atLeast(random, 300),
          random.nextInt(6), // 5 mixes all shapes
          random.nextBoolean()
              ? TestUtil.nextInt(random, 2, 20)
              : TestUtil.nextInt(random, 20, 400),
          TestUtil.nextInt(random, 2, 10),
          random.nextBoolean(),
          random.nextBoolean(),
          0.5f + 0.4f * random.nextFloat(),
          random.nextBoolean() ? TestUtil.nextInt(random, 16, 64) : 1 << 20,
          PrimaryKeyHashTestUtil.randomFilterBitsPerKey(random),
          PrimaryKeyHashTestUtil.randomInlining(random));
    }
  }

  /**
   * The bits per key of the filters of the segments that {@link #writeSegment} writes, and when
   * their fields have their keys inline, chosen per test: the segments that a test compares are
   * written with the same.
   */
  private int segmentFilterBitsPerKey;

  private Inlining segmentInlining;

  @Before
  public void chooseSegmentFormat() {
    segmentFilterBitsPerKey = PrimaryKeyHashTestUtil.randomFilterBitsPerKey(random());
    segmentInlining = PrimaryKeyHashTestUtil.randomInlining(random());
  }

  /** Collects the messages of {@link HashFieldsConsumer#INFO_STREAM_COMPONENT}. */
  private static final class Messages extends InfoStream {
    final List<String> messages = Collections.synchronizedList(new ArrayList<>());

    @Override
    public void message(String component, String message) {
      if (isEnabled(component)) {
        messages.add(message);
      }
    }

    @Override
    public boolean isEnabled(String component) {
      return HashFieldsConsumer.INFO_STREAM_COMPONENT.equals(component);
    }

    @Override
    public void close() {}
  }

  private static Document doc(BytesRef key, int seq, Random random) {
    Document doc = new Document();
    doc.add(new StringField("id", key, Field.Store.NO));
    doc.add(new StringField("seq", Integer.toString(seq), Field.Store.NO));
    doc.add(new StringField("group", Integer.toString(seq % 7), Field.Store.NO));
    // not DOCS: comes before the DOCS fields, which the delegate writes in between
    doc.add(new TextField("body", "some text " + (seq % 100), Field.Store.NO));
    doc.add(new NumericDocValuesField("sort", random.nextInt(100)));
    return doc;
  }

  /**
   * Writes the index of {@code setup}: adds, duplicates, updates, deletes (soft or hard), commits
   * and merges, all decided by the setup's seed.
   */
  private static void write(
      Directory dir, Setup setup, PostingsFormat delegate, boolean captureKeys, Messages messages)
      throws IOException {
    final PostingsFormat format =
        new PrimaryKeyHashPostingsFormat(
            delegate,
            setup.loadFactor(),
            setup.maxKeysPerPartition(),
            PrimaryKeyHashPostingsFormat.DEFAULT_MAX_PROBE_LIMIT,
            PrimaryKeyHashPostingsFormat.MAX_RECORDS_LENGTH,
            captureKeys,
            setup.filterBitsPerKey(),
            setup.inlining());
    // the files are compared outside of compound files
    final CompoundFormat compoundFormat = new Lucene90CompoundFormat();
    compoundFormat.setShouldUseCompoundFile(false);
    final Codec codec =
        new AssertingCodec() {
          @Override
          public PostingsFormat getPostingsFormatForField(String field) {
            return format;
          }

          @Override
          public CompoundFormat compoundFormat() {
            return compoundFormat;
          }
        };
    final LogDocMergePolicy logDocMergePolicy = new LogDocMergePolicy();
    logDocMergePolicy.setMergeFactor(setup.mergeFactor());
    final MergePolicy mergePolicy =
        setup.softDeletes()
            // soft-deleted versions are kept: their keys have several documents after merges
            ? new SoftDeletesRetentionMergePolicy(
                SOFT_DELETES, MatchAllDocsQuery::new, logDocMergePolicy)
            : logDocMergePolicy;
    final IndexWriterConfig iwc =
        new IndexWriterConfig()
            .setCodec(codec)
            .setMaxBufferedDocs(setup.maxBufferedDocs())
            .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
            .setMergePolicy(mergePolicy)
            .setMergeScheduler(new SerialMergeScheduler())
            .setUseCompoundFile(false)
            .setInfoStream(messages);
    if (setup.indexSort()) {
      iwc.setIndexSort(new Sort(new SortField("sort", SortField.Type.LONG)));
    }
    if (setup.softDeletes()) {
      iwc.setSoftDeletesField(SOFT_DELETES);
    }

    final Random random = new Random(setup.seed());
    try (IndexWriter writer = new IndexWriter(dir, iwc)) {
      final List<BytesRef> keys = new ArrayList<>();
      for (int i = 0; i < setup.numOps(); i++) {
        final int op = keys.isEmpty() ? 0 : random.nextInt(25);
        switch (op) {
          case 1 -> writer.addDocument(doc(keys.get(random.nextInt(keys.size())), i, random));
          case 2, 3 -> {
            BytesRef key = keys.get(random.nextInt(keys.size()));
            Term term = new Term("id", key);
            if (setup.softDeletes()) {
              writer.softUpdateDocument(
                  term, doc(key, i, random), new NumericDocValuesField(SOFT_DELETES, 1));
            } else {
              writer.updateDocument(term, doc(key, i, random));
            }
          }
          case 4 -> {
            Term term = new Term("id", keys.get(random.nextInt(keys.size())));
            if (setup.softDeletes()) {
              writer.updateDocValues(term, new NumericDocValuesField(SOFT_DELETES, 1));
            } else {
              writer.deleteDocuments(term);
            }
          }
          case 5 -> {
            if (random.nextInt(10) == 0) {
              writer.commit();
            }
          }
          case 6 -> {
            if (random.nextInt(20) == 0) {
              writer.forceMerge(TestUtil.nextInt(random, 1, 3));
            }
          }
          default -> {
            final BytesRef key;
            if (setup.keyShape() == AROUND_BATCH_SIZE) {
              key = keyAroundBatchSize(random);
            } else {
              int shape = setup.keyShape() == 5 ? random.nextInt(5) : setup.keyShape();
              key = PrimaryKeyHashTestUtil.randomKey(random, shape, i);
            }
            keys.add(key);
            writer.addDocument(doc(key, i, random));
          }
        }
      }
      if (random.nextBoolean()) {
        writer.forceMerge(1);
      }
    }
  }

  /**
   * The key shape of {@link Setup} of keys of about as many bytes as the records that the writer
   * holds before it writes them, or more, and short ones.
   */
  private static final int AROUND_BATCH_SIZE = 6;

  private static BytesRef keyAroundBatchSize(Random random) {
    final int batch = HashFieldsConsumer.RECORD_BATCH_BYTES;
    final int length =
        switch (random.nextInt(4)) {
          case 0 -> random.nextInt(40);
          case 1 -> batch - 30 + random.nextInt(40);
          case 2 -> TestUtil.nextInt(random, batch / 2 - 20, batch / 2 + 20);
          default -> TestUtil.nextInt(random, batch, IndexWriter.MAX_TERM_LENGTH);
        };
    final byte[] bytes = new byte[length];
    random.nextBytes(bytes);
    return new BytesRef(bytes);
  }

  /**
   * Keys captured during the delegate's pass give the same files as a separate pass, also keys of
   * about as many bytes as the records that a field's writer holds before it writes them, and more:
   * a record that does not fit in what is left of them is laid out once those are written, in more
   * bytes if it takes more. Every key is found.
   */
  public void testSameFilesAsSeparatePass() throws IOException {
    final Setup random = Setup.random(random());
    final Setup aroundBatchSize =
        new Setup(
            random.seed(),
            TestUtil.nextInt(random(), 50, 200),
            AROUND_BATCH_SIZE,
            random.maxBufferedDocs(),
            random.mergeFactor(),
            random.indexSort(),
            random.softDeletes(),
            random.loadFactor(),
            random.maxKeysPerPartition(),
            random.filterBitsPerKey(),
            random.inlining());
    for (Setup setup : List.of(random, aroundBatchSize)) {
      final Messages messages = new Messages();
      try (Directory separate = newDirectory();
          Directory captured = newDirectory()) {
        write(separate, setup, TestUtil.getDefaultPostingsFormat(), false, new Messages());
        write(captured, setup, TestUtil.getDefaultPostingsFormat(), true, messages);
        assertEquals(List.of(), messages.messages);
        assertSameFiles(separate, captured);
        try (DirectoryReader reader = DirectoryReader.open(captured)) {
          for (LeafReaderContext context : reader.leaves()) {
            LeafReader leaf = context.reader();
            checkField(leaf, "id", List.of(), leaf.terms("id") instanceof HashTerms);
          }
        }
      }
    }
  }

  /**
   * The same index written twice gives the same meta and data files, byte for byte, but for their
   * headers and footers. With 16 keys per partition, so that keys are spilled.
   */
  public void testSameFilesWrittenTwice() throws IOException {
    final Setup random = Setup.random(random());
    final Setup setup =
        new Setup(
            random.seed(),
            atLeast(1000),
            random.keyShape(),
            TestUtil.nextInt(random(), 100, 400),
            random.mergeFactor(),
            random.indexSort(),
            random.softDeletes(),
            random.loadFactor(),
            16,
            random.filterBitsPerKey(),
            random.inlining());
    final boolean captureKeys = random().nextBoolean();
    try (Directory first = newDirectory();
        Directory second = newDirectory()) {
      write(first, setup, TestUtil.getDefaultPostingsFormat(), captureKeys, new Messages());
      write(second, setup, TestUtil.getDefaultPostingsFormat(), captureKeys, new Messages());
      assertSameFiles(first, second);
      try (DirectoryReader reader = DirectoryReader.open(second)) {
        int spilled = 0;
        for (LeafReaderContext context : reader.leaves()) {
          if (context.reader().terms("id") instanceof HashTerms terms
              && terms.numPartitions() > 1) {
            spilled++;
          }
        }
        assertTrue(spilled > 0);
      }
    }
  }

  /**
   * Delegates that read the documents of a term with more than one enum, some of them once they
   * have moved to the next term, or read the fields again after writing them, get the same files.
   * Delegates that seek or iterate a field again before the end of its terms, iterate two fields at
   * once or a field not at all, or read documents in bulk get their fields' keys read in a separate
   * pass, and the tables are right.
   */
  public void testMeddlingDelegates() throws IOException {
    for (Meddling meddling :
        new Meddling[] {
          Meddling.TWO_ENUMS,
          Meddling.STALE_ENUM,
          Meddling.TWO_PASSES,
          Meddling.PEEK,
          Meddling.SEEK,
          Meddling.INTO_BIT_SET,
          Meddling.INTO_ARRAY,
          Meddling.INTERLEAVE,
          Meddling.NOT_ITERATED,
          Meddling.NOT_ITERATED_THEN_PEEK
        }) {
      final Setup setup = Setup.random(random());
      final Messages messages = new Messages();
      try (Directory separate = newDirectory();
          Directory captured = newDirectory()) {
        write(separate, setup, TestUtil.getDefaultPostingsFormat(), false, new Messages());
        write(captured, setup, meddling.wrap(TestUtil.getDefaultPostingsFormat()), true, messages);
        if (meddling.reasons.length == 0) {
          assertEquals(meddling.toString(), List.of(), messages.messages);
          assertSameFiles(separate, captured);
          continue;
        }
        assertSeparatePasses(meddling, messages);
        try (DirectoryReader expected = DirectoryReader.open(separate);
            DirectoryReader actual = DirectoryReader.open(captured)) {
          assertEquals(expected.leaves().size(), actual.leaves().size());
          for (int i = 0; i < expected.leaves().size(); i++) {
            LeafReader expectedLeaf = expected.leaves().get(i).reader();
            LeafReader actualLeaf = actual.leaves().get(i).reader();
            for (String field : DOCS_FIELDS) {
              boolean hasTable = expectedLeaf.terms(field) instanceof HashTerms;
              checkField(actualLeaf, field, List.of(), hasTable);
            }
          }
        }
      }
    }
  }

  /**
   * A delegate that seeks while it iterates a field, in any of the ways that a terms enum can seek:
   * the field's keys are read in a separate pass.
   */
  public void testAnySeekAbandonsTheCapture() throws IOException {
    for (Seek seek : Seek.values()) {
      final Setup setup = Setup.random(random());
      final Messages messages = new Messages();
      try (Directory dir = newDirectory()) {
        write(
            dir,
            setup,
            Meddling.SEEK.wrap(TestUtil.getDefaultPostingsFormat(), seek),
            true,
            messages);
        assertSeparatePasses(Meddling.SEEK, messages);
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          for (LeafReaderContext context : reader.leaves()) {
            LeafReader leaf = context.reader();
            checkField(leaf, "id", List.of(), leaf.terms("id") instanceof HashTerms);
          }
        }
      }
    }
  }

  private static void assertSeparatePasses(Meddling meddling, Messages messages) {
    assertFalse(meddling.toString(), messages.messages.isEmpty());
    for (String message : messages.messages) {
      assertTrue(message, message.contains("separate pass"));
      assertTrue(
          meddling + ": " + message, Arrays.stream(meddling.reasons).anyMatch(message::contains));
    }
  }

  /**
   * Delegates that read the postings of a segment, whose enums support {@code advance}, bulk reads
   * and the bounds of the terms, unlike those that flushes and merges write from: keys are captured
   * as long as no document can have been read without the capture seeing it. A delegate that reads
   * the documents of every term but the first with an enum of its own once it has moved to the next
   * term gets the same files: they are not taken for the next term's documents. Many keys of the
   * segment have two documents and come right before a key of one, which would otherwise be marked
   * as having several too.
   */
  public void testDelegateReadsPostingsOfSegment() throws IOException {
    withSegment(
        (segment, keys) -> {
          int multiThenSingle = 0;
          int previous = 0;
          for (int[] docs : postingsOf(segment.terms("id")).values()) {
            if (previous > 1 && docs.length == 1) {
              multiThenSingle++;
            }
            previous = docs.length;
          }
          assertTrue(multiThenSingle > 0);
          for (Meddling meddling :
              new Meddling[] {
                Meddling.ADVANCE,
                Meddling.STALE_ENUM,
                Meddling.MIN_MAX,
                Meddling.SKIP_ADVANCE,
                Meddling.NEXT_POSTINGS
              }) {
            checkRewrite(segment, keys, meddling);
          }
        });
  }

  /**
   * Writes the postings of {@code segment} again with {@code meddling}: if it reads all of them,
   * the files are the same as with a separate pass; else the fields' keys are read in a separate
   * pass, for its reasons, and the tables are right.
   */
  private void checkRewrite(CodecReader segment, List<BytesRef> keys, Meddling meddling)
      throws IOException {
    try (Directory separate = newDirectory();
        Directory captured = newDirectory()) {
      writeSegment(separate, segment, TestUtil.getDefaultPostingsFormat(), false, null);
      final Messages messages = new Messages();
      final SegmentInfo info =
          writeSegment(
              captured,
              segment,
              meddling.wrap(TestUtil.getDefaultPostingsFormat()),
              true,
              messages);
      if (meddling.reasons.length == 0) {
        assertEquals(meddling.toString(), List.of(), messages.messages);
        assertSameFiles(separate, captured);
      } else {
        assertSeparatePasses(meddling, messages);
        checkTables(captured, info, segment, keys);
      }
    }
  }

  /** The tables of a copy of {@code segment} written to {@code dir} hold its keys. */
  private static void checkTables(
      Directory dir, SegmentInfo info, CodecReader segment, List<BytesRef> keys)
      throws IOException {
    try (FieldsProducer producer =
        new PrimaryKeyHashPostingsFormat()
            .fieldsProducer(
                new SegmentReadState(dir, info, segment.getFieldInfos(), IOContext.DEFAULT))) {
      for (String field : DOCS_FIELDS) {
        HashTerms terms = (HashTerms) producer.terms(field);
        List<BytesRef> probes = new ArrayList<>(keys);
        probes.add(new BytesRef("absent"));
        checkLookupBatch(terms, postingsOf(segment.terms(field)), probes);
      }
    }
  }

  /** A test of a segment and the keys of its documents. */
  private interface SegmentTest {
    void test(CodecReader segment, List<BytesRef> keys) throws IOException;
  }

  /**
   * Runs {@code test} on a segment of at least 500 documents, written with the default codec, whose
   * postings, unlike those that flushes and merges write from, support {@code advance} and bulk
   * reads. Some keys have two documents. Keys are of every shape, or half of the time all binary
   * UUIDs, of 16 bytes, which may be inline.
   */
  private static void withSegment(SegmentTest test) throws IOException {
    withSegment(test, random().nextBoolean() ? -1 : 0);
  }

  /**
   * As {@link #withSegment(SegmentTest)}, with keys of the given shape, or of every shape if -1.
   */
  private static void withSegment(SegmentTest test, int shape) throws IOException {
    try (Directory source = newDirectory()) {
      final int numDocs = atLeast(500);
      final IndexWriterConfig iwc =
          new IndexWriterConfig()
              .setCodec(TestUtil.getDefaultCodec())
              .setMergePolicy(NoMergePolicy.INSTANCE)
              .setMaxBufferedDocs(numDocs + 1)
              .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH);
      final List<BytesRef> keys = new ArrayList<>();
      try (IndexWriter writer = new IndexWriter(source, iwc)) {
        final Random random = random();
        for (int i = 0; i < numDocs; i++) {
          // some keys twice
          BytesRef key =
              keys.isEmpty() == false && random.nextInt(10) == 0
                  ? keys.get(random.nextInt(keys.size()))
                  : PrimaryKeyHashTestUtil.randomKey(
                      random, shape < 0 ? random.nextInt(5) : shape, i);
          keys.add(key);
          writer.addDocument(doc(key, i, random));
        }
      }
      try (DirectoryReader reader = DirectoryReader.open(source)) {
        test.test((CodecReader) getOnlyLeafReader(reader), keys);
      }
    }
  }

  /**
   * A capture that ends with spilled keys, because the delegate seeks, deletes its temporary files,
   * the records of keys that may be inline included, and the separate pass writes the right tables.
   */
  public void testAbandonWithSpilledKeys() throws IOException {
    for (int shape : new int[] {-1, 0}) {
      withSegment(
          (segment, keys) -> {
            // the key fields have more than LATE_TERM terms, more than maxKeysPerPartition keys
            assertTrue(segment.terms("id").size() > Meddling.LATE_TERM);
            try (Directory dir = newDirectory()) {
              final Messages messages = new Messages();
              final SegmentInfo info =
                  writeSegment(
                      dir,
                      segment,
                      segment.getPostingsReader(),
                      Meddling.LATE_SEEK.wrap(TestUtil.getDefaultPostingsFormat()),
                      true,
                      16,
                      messages);
              assertSeparatePasses(Meddling.LATE_SEEK, messages);
              assertNoTemporaryFiles(dir);
              checkTables(dir, info, segment, keys);
            }
            // the directory had no file left open
          },
          shape);
    }
  }

  /**
   * A delegate that fails during a capture, after keys were spilled: the exception is thrown, and
   * the temporary files are closed and deleted, the records of keys that may be inline included.
   */
  public void testDelegateFailsDuringCapture() throws IOException {
    for (int shape : new int[] {-1, 0}) {
      withSegment(
          (segment, _) -> {
            try (Directory dir = newDirectory()) {
              final IOException e =
                  expectThrows(
                      IOException.class,
                      () ->
                          writeSegment(
                              dir,
                              segment,
                              segment.getPostingsReader(),
                              Meddling.THROW.wrap(TestUtil.getDefaultPostingsFormat()),
                              true,
                              16,
                              new Messages()));
              assertEquals(Meddling.THROWN, e.getMessage());
              assertNoTemporaryFiles(dir);
            }
            // the directory had no file left open
          },
          shape);
    }
  }

  private static void assertNoTemporaryFiles(Directory dir) throws IOException {
    for (String file : dir.listAll()) {
      assertFalse(file, file.contains("hashspill"));
      assertFalse(file, file.contains("hashrecords"));
      assertFalse(file, file.contains("hashfilter"));
    }
  }

  /**
   * Terms that do not come in increasing order, which no source of a write gives, but a capture
   * does not take for granted: the first terms enum of every field gives its first term twice, and
   * the delegate skips it the second time. The keys are read in a separate pass, from an enum that
   * gives them in order.
   */
  public void testTermsOutOfOrder() throws IOException {
    withSegment(
        (segment, keys) -> {
          try (Directory dir = newDirectory()) {
            final Messages messages = new Messages();
            final SegmentInfo info =
                writeSegment(
                    dir,
                    segment,
                    new SourceFields(segment.getPostingsReader(), true),
                    Meddling.DEDUP.wrap(TestUtil.getDefaultPostingsFormat()),
                    true,
                    PrimaryKeyHashPostingsFormat.DEFAULT_MAX_KEYS_PER_PARTITION,
                    messages);
            assertSeparatePasses(Meddling.DEDUP, messages);
            assertEquals(DOCS_FIELDS.size(), messages.messages.size());
            checkTables(dir, info, segment, keys);
          }
        });
  }

  /**
   * The postings enums that the delegate reuses are those of the source of the write, not the
   * capture's, which wrap them: the source can reuse them.
   */
  public void testSourceReusesItsPostingsEnums() throws IOException {
    withSegment(
        (segment, _) -> {
          try (Directory dir = newDirectory()) {
            final Messages messages = new Messages();
            final SourceFields fields = new SourceFields(segment.getPostingsReader(), false);
            writeSegment(
                dir,
                segment,
                fields,
                TestUtil.getDefaultPostingsFormat(),
                true,
                PrimaryKeyHashPostingsFormat.DEFAULT_MAX_KEYS_PER_PARTITION,
                messages);
            assertEquals(List.of(), messages.messages);
            assertTrue(fields.reused > 0);
            assertEquals(0, fields.foreign);
          }
        });
  }

  /**
   * Once a capture is over, nothing of the format keeps it reachable: neither the fields that the
   * delegate writes, which record how each capture ended, nor the postings enums of the captured
   * fields, which the delegate may keep to reuse. Checked once the delegate has written every
   * field, before the write is over, by following the references of the format's objects, which
   * reach each capture while it is in progress.
   */
  public void testCapturesAreNotKept() throws IOException {
    withSegment(
        (segment, _) -> {
          final List<TermsEnum> captures = new ArrayList<>();
          final PostingsFormat delegate = TestUtil.getDefaultPostingsFormat();
          final PostingsFormat checking =
              new PostingsFormat(delegate.getName()) {
                @Override
                public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
                  final FieldsConsumer in = delegate.fieldsConsumer(state);
                  return new FieldsConsumer() {
                    @Override
                    public void write(Fields fields, NormsProducer norms) throws IOException {
                      final RecordingFields recording = new RecordingFields(fields);
                      in.write(recording, norms);
                      assertFalse(recording.postings.isEmpty());
                      final List<Object> roots = new ArrayList<>(recording.postings);
                      roots.add(fields);
                      assertFalse(isReachable(recording.captures, roots));
                      captures.addAll(recording.captures);
                    }

                    @Override
                    public void close() throws IOException {
                      in.close();
                    }
                  };
                }

                @Override
                public FieldsProducer fieldsProducer(SegmentReadState state) {
                  throw new UnsupportedOperationException();
                }
              };
          try (Directory dir = newDirectory()) {
            final Messages messages = new Messages();
            writeSegment(dir, segment, checking, true, messages);
            assertEquals(List.of(), messages.messages);
          }
          assertEquals(DOCS_FIELDS.size(), captures.size());
        });
  }

  /**
   * Whether one of {@code targets} is reachable from {@code roots} through the fields of objects of
   * the format, and the collections and arrays that they hold.
   */
  @SuppressForbidden(reason = "follows the private fields of the format's objects")
  private static boolean isReachable(List<?> targets, List<?> roots) {
    final Set<Object> targetSet = Collections.newSetFromMap(new IdentityHashMap<>());
    targetSet.addAll(targets);
    final Set<Object> seen = Collections.newSetFromMap(new IdentityHashMap<>());
    final ArrayDeque<Object> stack = new ArrayDeque<>(roots);
    while (stack.isEmpty() == false) {
      final Object o = stack.pop();
      if (targetSet.contains(o)) {
        return true;
      }
      if (seen.add(o) == false) {
        continue;
      }
      final List<Object> references = new ArrayList<>();
      final Class<?> clazz = o.getClass();
      if (o instanceof Object[] array) {
        references.addAll(Arrays.asList(array));
      } else if (clazz.getName().startsWith("java.") && o instanceof Collection<?> collection) {
        references.addAll(collection);
      } else if (clazz.getName().startsWith("java.") && o instanceof Map<?, ?> map) {
        references.addAll(map.keySet());
        references.addAll(map.values());
      } else {
        for (Class<?> c = clazz; isFormatClass(c); c = c.getSuperclass()) {
          for (java.lang.reflect.Field field : c.getDeclaredFields()) {
            if (java.lang.reflect.Modifier.isStatic(field.getModifiers()) == false
                && field.getType().isPrimitive() == false) {
              field.setAccessible(true);
              try {
                references.add(field.get(o));
              } catch (IllegalAccessException e) {
                throw new AssertionError(e);
              }
            }
          }
        }
      }
      for (Object reference : references) {
        if (reference != null) {
          stack.push(reference);
        }
      }
    }
    return false;
  }

  /**
   * Whether {@code clazz} is a class of the format, rather than of the JDK, Lucene or this test.
   */
  private static boolean isFormatClass(Class<?> clazz) {
    return clazz != null
        && clazz.getPackageName().equals(HashFieldsConsumer.class.getPackageName())
        && clazz.getNestHost() != TestPrimaryKeyHashCapture.class;
  }

  /**
   * Fields that record the terms enums of the {@code DOCS} fields, which are the captures, and the
   * postings enums that those return. Each capture is reachable from the fields while it is in
   * progress.
   */
  private static final class RecordingFields extends FilterLeafReader.FilterFields {
    final List<TermsEnum> captures = new ArrayList<>();
    final Set<PostingsEnum> postings = Collections.newSetFromMap(new IdentityHashMap<>());

    RecordingFields(Fields in) {
      super(in);
    }

    @Override
    public Terms terms(String field) {
      final Terms terms = in.terms(field);
      if (terms == null || DOCS_FIELDS.contains(field) == false) {
        return terms;
      }
      return new FilterLeafReader.FilterTerms(terms) {
        @Override
        public TermsEnum iterator() throws IOException {
          final TermsEnum termsEnum = in.iterator();
          captures.add(termsEnum);
          assertTrue(isReachable(List.of(termsEnum), List.of(RecordingFields.this.in)));
          return new FilterLeafReader.FilterTermsEnum(termsEnum) {
            @Override
            public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
              final PostingsEnum postingsEnum = in.postings(reuse, flags);
              postings.add(postingsEnum);
              return postingsEnum;
            }
          };
        }
      };
    }
  }

  /**
   * The fields of a segment as the source of a write. They record the postings enums that their
   * terms enums return, and count the calls to {@code postings} that are given one of them to
   * reuse, and those given another enum. If {@code repeatFirstTerm}, the first terms enum of each
   * field gives its first term twice in a row.
   */
  private static final class SourceFields extends FilterLeafReader.FilterFields {
    private final boolean repeatFirstTerm;
    private final Set<String> iterated = new HashSet<>();
    private final Set<PostingsEnum> postingsEnums =
        Collections.newSetFromMap(new IdentityHashMap<>());
    int reused;
    int foreign;

    SourceFields(Fields in, boolean repeatFirstTerm) {
      super(in);
      this.repeatFirstTerm = repeatFirstTerm;
    }

    @Override
    public Terms terms(String field) {
      final Terms terms = in.terms(field);
      if (terms == null) {
        return null;
      }
      return new FilterLeafReader.FilterTerms(terms) {
        @Override
        public TermsEnum iterator() throws IOException {
          final boolean repeat = repeatFirstTerm && iterated.add(field);
          return new FilterLeafReader.FilterTermsEnum(in.iterator()) {
            private int calls;

            @Override
            public BytesRef next() throws IOException {
              if (repeat && calls++ == 1) {
                return in.term(); // the first term again
              }
              return in.next();
            }

            @Override
            public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
              if (reuse != null) {
                if (postingsEnums.contains(reuse)) {
                  reused++;
                } else {
                  foreign++;
                }
              }
              final PostingsEnum postings = in.postings(reuse, flags);
              postingsEnums.add(postings);
              return postings;
            }
          };
        }
      };
    }
  }

  /** Writes the postings of {@code segment} again, as a segment "_0" of {@code dir}. */
  private SegmentInfo writeSegment(
      Directory dir,
      CodecReader segment,
      PostingsFormat delegate,
      boolean captureKeys,
      InfoStream infoStream)
      throws IOException {
    return writeSegment(
        dir,
        segment,
        segment.getPostingsReader(),
        delegate,
        captureKeys,
        PrimaryKeyHashPostingsFormat.DEFAULT_MAX_KEYS_PER_PARTITION,
        infoStream);
  }

  /**
   * Writes {@code fields}, the postings of {@code segment} or a view of them, as a segment "_0" of
   * {@code dir}.
   */
  private SegmentInfo writeSegment(
      Directory dir,
      CodecReader segment,
      Fields fields,
      PostingsFormat delegate,
      boolean captureKeys,
      int maxKeysPerPartition,
      InfoStream infoStream)
      throws IOException {
    final PostingsFormat format =
        new PrimaryKeyHashPostingsFormat(
            delegate,
            PrimaryKeyHashPostingsFormat.DEFAULT_LOAD_FACTOR,
            maxKeysPerPartition,
            PrimaryKeyHashPostingsFormat.DEFAULT_MAX_PROBE_LIMIT,
            PrimaryKeyHashPostingsFormat.MAX_RECORDS_LENGTH,
            captureKeys,
            segmentFilterBitsPerKey,
            segmentInlining);
    final SegmentInfo info =
        new SegmentInfo(
            dir,
            Version.LATEST,
            Version.LATEST,
            "_0",
            segment.maxDoc(),
            false,
            false,
            TestUtil.getDefaultCodec(),
            Collections.emptyMap(),
            StringHelper.randomId(),
            new HashMap<>(),
            null);
    final SegmentWriteState state =
        new SegmentWriteState(
            infoStream, dir, info, segment.getFieldInfos(), null, IOContext.DEFAULT);
    try (FieldsConsumer consumer = format.fieldsConsumer(state)) {
      consumer.write(fields, segment.getNormsReader());
    }
    return info;
  }

  /** How a test delegate reads the fields that it is given, before or while writing them. */
  private enum Meddling {
    /** Reads every term's documents with an enum of its own first. */
    TWO_ENUMS,
    /**
     * Reads the first document of every term with an enum of its own first, and its other documents
     * once it has moved to the next term.
     */
    STALE_ENUM,
    /** Reads the smallest and the largest term of every field before iterating its terms. */
    MIN_MAX,
    /** Reads every field again after writing them all, as a bloom filter would. */
    TWO_PASSES,
    /** Reads documents with {@code advance(doc + 1)}. */
    ADVANCE,
    /** Reads the first terms and documents of every field first. */
    PEEK("iterated another field", "iterated the terms more than once"),
    /** Seeks to the second term when it gets there. */
    SEEK("seeks"),
    /** Seeks to the {@link #LATE_TERM}th term of a {@code DOCS} field when it gets there. */
    LATE_SEEK("seeks"),
    /**
     * Throws an {@link IOException} with the message {@link #THROWN} at the {@link #LATE_TERM}th
     * term of a {@code DOCS} field.
     */
    THROW,
    /** Skips a term that the terms enum gives twice in a row. */
    DEDUP("not in increasing order"),
    /** Advances an enum of its own past the start of every term. */
    SKIP_ADVANCE("did not read enough documents"),
    /** Reads the documents of every term into a bit set with an enum of its own first. */
    INTO_BIT_SET("did not read enough documents"),
    /** Reads the documents of every term into an array with an enum of its own first. */
    INTO_ARRAY("did not read enough documents"),
    /** Reads the postings of every term into a buffer with an enum of its own first. */
    NEXT_POSTINGS("did not read enough documents"),
    /** Reads the first term of the next field before writing each field. */
    INTERLEAVE("iterated another field", "iterated the terms more than once"),
    /** Reads the terms of {@code DOCS} fields through {@link Terms#intersect}. */
    NOT_ITERATED("did not iterate"),
    /** Like {@link #NOT_ITERATED}, then reads the first term of a field. */
    NOT_ITERATED_THEN_PEEK("did not iterate", "did not reach the end");

    /** A term of a field after the keys that fit in heap with 16 keys per partition. */
    static final int LATE_TERM = 100;

    static final String THROWN = "the delegate failed";

    final String[] reasons;

    Meddling(String... reasons) {
      this.reasons = reasons;
    }

    /**
     * A format that writes with {@code delegate} after or while reading the fields this way, with a
     * random {@link Seek}. It has the delegate's name, so that the index is read by the delegate.
     */
    PostingsFormat wrap(PostingsFormat delegate) {
      final Seek[] seeks = Seek.values();
      return wrap(delegate, seeks[random().nextInt(seeks.length)]);
    }

    /** As {@link #wrap(PostingsFormat)}, seeking with {@code seek}. */
    PostingsFormat wrap(PostingsFormat delegate, Seek seek) {
      return new PostingsFormat(delegate.getName()) {
        @Override
        public FieldsConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
          return new MeddlingFieldsConsumer(
              delegate.fieldsConsumer(state), state, Meddling.this, seek);
        }

        @Override
        public FieldsProducer fieldsProducer(SegmentReadState state) throws IOException {
          return delegate.fieldsProducer(state);
        }
      };
    }
  }

  /** How {@link Meddling#SEEK} and {@link Meddling#LATE_SEEK} seek to the term they are on. */
  private enum Seek {
    SEEK_EXACT,
    SEEK_CEIL,
    PREPARE_SEEK_EXACT,
    TERM_STATE,
    /** By ord, if the terms enum has ords, else as {@link #SEEK_EXACT}. */
    ORD;

    void seek(TermsEnum termsEnum, BytesRef term) throws IOException {
      switch (this) {
        case SEEK_EXACT -> assertTrue(termsEnum.seekExact(term));
        case SEEK_CEIL -> assertEquals(TermsEnum.SeekStatus.FOUND, termsEnum.seekCeil(term));
        case PREPARE_SEEK_EXACT -> {
          final IOBooleanSupplier supplier = termsEnum.prepareSeekExact(term);
          assertNotNull(supplier);
          assertTrue(supplier.get());
        }
        case TERM_STATE -> termsEnum.seekExact(term, termsEnum.termState());
        case ORD -> {
          final long ord;
          try {
            ord = termsEnum.ord();
          } catch (UnsupportedOperationException _) {
            // merges, and segments of the default codec, have no ords
            assertTrue(termsEnum.seekExact(term));
            return;
          }
          termsEnum.seekExact(ord);
        }
      }
    }
  }

  private static final CompiledAutomaton ALL =
      new CompiledAutomaton(Automata.makeAnyBinary(), false, false, true);

  private static final class MeddlingFieldsConsumer extends FieldsConsumer {
    private final FieldsConsumer in;
    private final SegmentWriteState state;
    private final Meddling meddling;
    private final Seek seek;

    MeddlingFieldsConsumer(
        FieldsConsumer in, SegmentWriteState state, Meddling meddling, Seek seek) {
      this.in = in;
      this.state = state;
      this.meddling = meddling;
      this.seek = seek;
    }

    private boolean isDocs(String field) {
      FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
      return fieldInfo != null && fieldInfo.getIndexOptions() == IndexOptions.DOCS;
    }

    /** Reads up to {@code maxTerms} terms of the field, and all of their documents. */
    private static void read(Fields fields, String field, int maxTerms) throws IOException {
      Terms terms = fields.terms(field);
      if (terms == null) {
        return;
      }
      TermsEnum termsEnum = terms.iterator();
      PostingsEnum postings = null;
      for (int i = 0; i < maxTerms && termsEnum.next() != null; i++) {
        postings = termsEnum.postings(postings, PostingsEnum.NONE);
        while (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {}
      }
    }

    @Override
    public void write(Fields fields, NormsProducer norms) throws IOException {
      switch (meddling) {
        case PEEK -> {
          for (String field : fields) {
            read(fields, field, 3);
          }
          in.write(fields, norms);
        }
        case TWO_PASSES -> {
          in.write(fields, norms);
          for (String field : fields) {
            read(fields, field, Integer.MAX_VALUE);
          }
        }
        case NOT_ITERATED_THEN_PEEK -> {
          in.write(new MeddlingFields(fields), norms);
          for (String field : fields) {
            if (isDocs(field)) {
              read(fields, field, 1);
              break;
            }
          }
        }
        // $CASES-OMITTED$
        default -> in.write(new MeddlingFields(fields), norms);
      }
    }

    @Override
    public void close() throws IOException {
      in.close();
    }

    private final class MeddlingFields extends FilterLeafReader.FilterFields {
      MeddlingFields(Fields in) {
        super(in);
      }

      @Override
      public Terms terms(String field) {
        Terms terms = in.terms(field);
        return terms == null ? null : new MeddlingTerms(in, field, terms);
      }
    }

    private final class MeddlingTerms extends FilterLeafReader.FilterTerms {
      /** The fields as the meddling consumer was given them. */
      private final Fields fields;

      private final String field;

      MeddlingTerms(Fields fields, String field, Terms in) {
        super(in);
        this.fields = fields;
        this.field = field;
      }

      @Override
      public TermsEnum iterator() throws IOException {
        switch (meddling) {
          case INTERLEAVE -> {
            TermsEnum termsEnum = in.iterator();
            boolean after = false;
            for (String other : fields) {
              if (after && isDocs(other)) {
                read(fields, other, 1);
                break;
              }
              after |= other.equals(field);
            }
            return termsEnum;
          }
          case NOT_ITERATED, NOT_ITERATED_THEN_PEEK -> {
            return isDocs(field) ? in.intersect(ALL, null) : in.iterator();
          }
          case MIN_MAX -> {
            in.getMin();
            in.getMax();
            return new MeddlingTermsEnum(in.iterator(), isDocs(field));
          }
          // $CASES-OMITTED$
          default -> {
            return new MeddlingTermsEnum(in.iterator(), isDocs(field));
          }
        }
      }
    }

    private final class MeddlingTermsEnum extends FilterLeafReader.FilterTermsEnum {
      private int numTerms;

      /** For {@link Meddling#DEDUP}: the previous term. */
      private final BytesRefBuilder previous = new BytesRefBuilder();

      /** For {@link Meddling#STALE_ENUM}: the enum of the previous term, and its first document. */
      private PostingsEnum stale;

      private int staleDoc;

      /** Whether the terms are those of a {@code DOCS} field, which the format captures. */
      private final boolean docs;

      MeddlingTermsEnum(TermsEnum in, boolean docs) {
        super(in);
        this.docs = docs;
      }

      @Override
      public BytesRef next() throws IOException {
        BytesRef term = in.next();
        if (meddling == Meddling.DEDUP
            && term != null
            && numTerms > 0
            && term.equals(previous.get())) {
          term = in.next();
        }
        if (term == null) {
          return null;
        }
        numTerms++;
        switch (meddling) {
          case SEEK, LATE_SEEK -> {
            if (meddling == Meddling.SEEK
                ? numTerms == 2
                : docs && numTerms == Meddling.LATE_TERM) {
              seek.seek(in, BytesRef.deepCopyOf(term));
              term = in.term();
            }
          }
          case THROW -> {
            if (docs && numTerms == Meddling.LATE_TERM) {
              throw new IOException(Meddling.THROWN);
            }
          }
          case STALE_ENUM -> {
            if (stale != null && staleDoc != DocIdSetIterator.NO_MORE_DOCS) {
              while (stale.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {}
            }
            stale = null;
          }
          case DEDUP -> previous.copyBytes(term);
          // $CASES-OMITTED$
          default -> {}
        }
        return term;
      }

      @Override
      public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
        // first, the documents read with an enum of its own
        final PostingsEnum own =
            switch (meddling) {
              case TWO_ENUMS, STALE_ENUM, SKIP_ADVANCE, INTO_BIT_SET, INTO_ARRAY, NEXT_POSTINGS ->
                  in.postings(null, flags);
              // $CASES-OMITTED$
              default -> null;
            };
        switch (meddling) {
          case TWO_ENUMS -> {
            while (own.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {}
          }
          case STALE_ENUM -> {
            stale = own;
            staleDoc = own.nextDoc();
          }
          case SKIP_ADVANCE -> own.advance(1);
          case INTO_BIT_SET -> {
            if (own.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
              own.intoBitSet(
                  DocIdSetIterator.NO_MORE_DOCS, new FixedBitSet(state.segmentInfo.maxDoc()), 0);
            }
          }
          case INTO_ARRAY -> {
            if (own.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
              own.intoArray(DocIdSetIterator.NO_MORE_DOCS, new int[4]);
            }
          }
          case NEXT_POSTINGS -> {
            if (own.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
              own.nextPostings(DocIdSetIterator.NO_MORE_DOCS, new DocAndFloatFeatureBuffer());
            }
          }
          // $CASES-OMITTED$
          default -> {}
        }
        if (reuse instanceof AdvancingPostingsEnum advancing) {
          reuse = advancing.unwrap();
        }
        PostingsEnum postings = in.postings(reuse, flags);
        return meddling == Meddling.ADVANCE ? new AdvancingPostingsEnum(postings) : postings;
      }
    }

    /** Reads every document with {@code advance}, never past the next one. */
    private static final class AdvancingPostingsEnum extends FilterLeafReader.FilterPostingsEnum {
      private int doc = -1;

      AdvancingPostingsEnum(PostingsEnum in) {
        super(in);
      }

      @Override
      public int nextDoc() throws IOException {
        if (doc != DocIdSetIterator.NO_MORE_DOCS) {
          doc = in.advance(doc + 1);
        }
        return doc;
      }
    }
  }
}
