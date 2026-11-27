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

import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.readAll;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.BiConsumer;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.Inlining;
import org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.FieldMeta;
import org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.Meta;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;

/**
 * Corrupted files must fail when the segment is opened, or at the latest in {@code checkIntegrity};
 * meta data that passes its checksum but is inconsistent must be rejected on open, by the check
 * that its message names.
 */
public class TestPrimaryKeyHashCorruption extends LuceneTestCase {

  /** A tampering of the meta data, given it and the entry of the id field. */
  private interface Tamper extends BiConsumer<Meta, FieldMeta> {}

  /** A tampering, and what the message of the exception must contain, or null for any message. */
  private record Case(String message, Tamper tamper) {}

  /**
   * A segment of 200 documents whose id field has a filter unless the bits are 0; if {@code
   * fixedLength}, its keys all have 5 bytes and are inline, else they are in term order. With a
   * DOCS field that has no terms.
   */
  private static final class Index implements Closeable {
    final Directory dir = new ByteBuffersDirectory();
    final String metaName;
    final String dataName;
    final int idField;
    final int bodyField;
    final int emptyField;

    Index(int filterBitsPerKey, boolean fixedLength) throws IOException {
      final IndexWriterConfig iwc =
          new IndexWriterConfig(new MockAnalyzer(random()))
              .setCodec(
                  TestUtil.alwaysPostingsFormat(
                      PrimaryKeyHashTestUtil.format(
                          0.75f, 16, filterBitsPerKey, Inlining.IF_NOT_LARGER)))
              .setMergePolicy(NoMergePolicy.INSTANCE)
              .setUseCompoundFile(false);
      final FieldType docsOnly = new FieldType();
      docsOnly.setIndexOptions(IndexOptions.DOCS);
      try (IndexWriter writer = new IndexWriter(dir, iwc)) {
        for (int i = 0; i < 200; i++) {
          Document doc = new Document();
          doc.add(new StringField("id", id(i, fixedLength), Field.Store.NO));
          doc.add(new TextField("body", "text " + i, Field.Store.NO));
          doc.add(new Field("empty", new CannedTokenStream(), docsOnly));
          writer.addDocument(doc);
        }
      }
      metaName = PrimaryKeyHashTestUtil.onlyFile(dir, PrimaryKeyHashPostingsFormat.META_EXTENSION);
      dataName = PrimaryKeyHashTestUtil.onlyFile(dir, PrimaryKeyHashPostingsFormat.DATA_EXTENSION);
      try (DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = getOnlyLeafReader(reader);
        assertEquals(fixedLength, ((HashTerms) leaf.terms("id")).isInline());
        assertNull(leaf.terms("empty"));
        idField = leaf.getFieldInfos().fieldInfo("id").number;
        bodyField = leaf.getFieldInfos().fieldInfo("body").number;
        emptyField = leaf.getFieldInfos().fieldInfo("empty").number;
      }
    }

    FieldMeta idEntry() throws IOException {
      return Meta.read(dir, metaName).field(idField);
    }

    /** A copy of the index in which {@code name} has the given content. */
    Directory copyWith(String name, byte[] content) throws IOException {
      Directory copy = new ByteBuffersDirectory();
      for (String file : dir.listAll()) {
        if (file.equals(name)) {
          try (IndexOutput out = copy.createOutput(file, IOContext.DEFAULT)) {
            out.writeBytes(content, content.length);
          }
        } else {
          copy.copyFrom(dir, file, file, IOContext.DEFAULT);
        }
      }
      return copy;
    }

    /** A copy of the index whose meta file is written again, with a valid checksum, tampered. */
    Directory tampered(Tamper tamper) throws IOException {
      final Meta meta = Meta.read(dir, metaName);
      tamper.accept(meta, meta.field(idField));
      try (Directory scratch = new ByteBuffersDirectory()) {
        try (IndexOutput out = scratch.createOutput("meta", IOContext.DEFAULT)) {
          meta.write(out);
        }
        return copyWith(metaName, readAll(scratch, "meta"));
      }
    }

    void assertRejected(List<Case> cases) throws IOException {
      for (Case c : cases) {
        try (Directory tampered = tampered(c.tamper())) {
          CorruptIndexException e =
              expectThrows(
                  CorruptIndexException.class, () -> DirectoryReader.open(tampered).close());
          if (c.message() != null) {
            assertTrue(c.message() + ": " + e.getMessage(), e.getMessage().contains(c.message()));
          }
        }
      }
    }

    /**
     * The meta data, read and written again by this test, opens and finds {@code present} but not
     * {@code absent}: the tests' parser and writer round-trip.
     */
    void assertOpens(String present, String absent) throws IOException {
      try (Directory tampered = tampered((_, _) -> {});
          DirectoryReader reader = DirectoryReader.open(tampered)) {
        LeafReader leaf = getOnlyLeafReader(reader);
        assertTrue(leaf.terms("id") instanceof HashTerms);
        assertEquals(1, leaf.docFreq(new Term("id", present)));
        assertEquals(0, leaf.docFreq(new Term("id", absent)));
        leaf.checkIntegrity();
      }
    }

    @Override
    public void close() throws IOException {
      dir.close();
    }
  }

  /** The id of document {@code i}: "id7", or "id007" if ids have a fixed length. */
  private static String id(int i, boolean fixedLength) {
    return fixedLength ? String.format(Locale.ROOT, "id%03d", i) : "id" + i;
  }

  private static Case rejected(String message, Tamper tamper) {
    return new Case(message, tamper);
  }

  /** The last partition of a field. */
  private static int last(FieldMeta field) {
    return field.capacities.length - 1;
  }

  /** Gives the id field's entry a filter of 8 bits per key, right after its slots. */
  private static void addFilter(Meta meta, FieldMeta field) {
    field.filterBitsPerKey = 8;
    field.filterOffset = IndexOutput.alignOffset(field.slotsEnd(), 64);
    field.filterWords = Math.toIntExact(Math.ceilDiv(field.numKeys * 8, 64));
  }

  /** As many keys as the slots of all partitions can take, and one more. */
  private static void tooManyKeys(Meta meta, FieldMeta field) {
    field.numKeys = Arrays.stream(field.capacities).sum() - field.capacities.length + 1;
    field.recordsLength = Math.max(field.recordsLength, field.numKeys * 5);
  }

  public void testCorruptedFiles() throws IOException {
    try (Index index = new Index(0, false)) {
      final int headerLength = PrimaryKeyHashTestUtil.headerLength(index.dataName);
      final byte[] meta = readAll(index.dir, index.metaName);
      final byte[] data = readAll(index.dir, index.dataName);
      final byte[] flippedMeta = meta.clone();
      flippedMeta[random().nextInt(meta.length)] ^= (byte) (1 + random().nextInt(255));
      for (byte[] bytes :
          List.of(flippedMeta, Arrays.copyOf(meta, random().nextInt(meta.length)))) {
        try (Directory dir = index.copyWith(index.metaName, bytes)) {
          expectThrows(IOException.class, () -> DirectoryReader.open(dir).close());
        }
      }
      final int truncated = TestUtil.nextInt(random(), headerLength, data.length - 1);
      try (Directory dir = index.copyWith(index.dataName, Arrays.copyOf(data, truncated))) {
        expectThrows(CorruptIndexException.class, () -> DirectoryReader.open(dir).close());
      }
      // the data file is only verified in full by checkIntegrity, also that of the merge instance
      final byte[] flipped = data.clone();
      flipped[
              TestUtil.nextInt(
                  random(), headerLength, data.length - CodecUtil.footerLength() - 1)] ^=
          (byte) (1 + random().nextInt(255));
      try (Directory dir = index.copyWith(index.dataName, flipped);
          DirectoryReader reader = DirectoryReader.open(dir)) {
        LeafReader leaf = getOnlyLeafReader(reader);
        expectThrows(CorruptIndexException.class, leaf::checkIntegrity);
        FieldsProducer merging = ((CodecReader) leaf).getPostingsReader().getMergeInstance();
        expectThrows(CorruptIndexException.class, () -> merging.checkIntegrity(null));
      }
      // files of any version other than the current one, the last field of the codec header
      final int versionOffset = CodecUtil.headerLength(PrimaryKeyHashPostingsFormat.META_CODEC) - 4;
      final int current = PrimaryKeyHashPostingsFormat.VERSION_CURRENT;
      try (Directory dir =
          index.tampered((m, _) -> BitUtil.VH_BE_INT.set(m.header, versionOffset, current - 1))) {
        expectThrows(IndexFormatTooOldException.class, () -> DirectoryReader.open(dir).close());
      }
      try (Directory dir =
          index.tampered((m, _) -> BitUtil.VH_BE_INT.set(m.header, versionOffset, current + 1))) {
        expectThrows(IndexFormatTooNewException.class, () -> DirectoryReader.open(dir).close());
      }
    }
  }

  /** A field whose records are in term order, in several partitions, without a filter. */
  public void testTamperedMeta() throws IOException {
    try (Index index = new Index(0, false)) {
      index.assertOpens("id7", "id7x");
      final FieldMeta id = index.idEntry();
      assertTrue("several partitions", id.capacities.length > 1);
      // a tail longer than the largest displacement, in a partition that keeps 2 home slots
      int longTail = -1;
      for (int p = 0; p < id.capacities.length && longTail < 0; p++) {
        if (id.capacities[p] - (id.maxProbes[p] + 1 - id.tails[p]) >= 2) {
          longTail = p;
        }
      }
      assertTrue(longTail >= 0);
      final int p = longTail;
      // the most slots whose control bytes fit in an array, as the writer holds them
      final long maxSlotCount = ((long) ArrayUtil.MAX_ARRAY_LENGTH & -Long.BYTES) - Long.BYTES;
      final List<Case> cases = new ArrayList<>();
      cases.add(rejected("unknown hash function", (m, _) -> m.hashFunction++));
      cases.add(rejected("invalid field number", (_, f) -> f.number = 1000));
      cases.add(rejected("invalid field number", (_, f) -> f.number = -2));
      cases.add(rejected("has index options", (_, f) -> f.number = index.bodyField));
      cases.add(rejected("duplicate field", (m, f) -> m.fields.add(f)));
      cases.add(rejected("has a hash table but no terms", (_, f) -> f.number = index.emptyField));
      cases.add(rejected("invalid numKeys=", (_, f) -> f.numKeys = 0));
      cases.add(rejected("invalid layout 2", (_, f) -> f.layout = 2));
      cases.add(rejected("invalid layout -1", (_, f) -> f.layout = -1));
      final String records = "invalid records offset=";
      cases.add(rejected(records, (_, f) -> f.recordsOffset = 0));
      cases.add(rejected(records, (_, f) -> f.recordsOffset = 1L << 57));
      cases.add(rejected(records, (_, f) -> f.recordsLength = 4));
      cases.add(rejected(records, (_, f) -> f.numKeys = f.recordsLength / 5 + 1));
      cases.add(
          rejected(
              records,
              (_, f) -> f.recordsLength = PrimaryKeyHashPostingsFormat.MAX_RECORDS_LENGTH + 1));
      // Records start at a cache line. Moved within one, keeping their end, nothing else is wrong.
      for (int shift :
          new int[] {1, Long.BYTES, PrimaryKeyHashPostingsFormat.CACHE_LINE_BYTES / 2}) {
        cases.add(
            rejected(
                records,
                (_, f) -> {
                  f.recordsOffset += shift;
                  f.recordsLength -= shift;
                }));
      }
      cases.add(rejected("invalid partitionBits", (_, f) -> f.partitionBits = 11));
      cases.add(rejected("invalid partitionBits", (_, f) -> f.partitionBits = -1));
      // Partitions whose sizes are inconsistent. Tails changed together with capacities keep the
      // number of slots, so that only the checks of the tail itself can reject them.
      final String partition = "invalid partition " + p + " capacity=";
      cases.add(rejected(partition, (_, f) -> f.capacities[p] = 1));
      cases.add(rejected(partition, (_, f) -> f.maxProbes[p] = -1));
      cases.add(rejected(partition, (_, f) -> f.maxProbes[p] = f.capacities[p] + f.tails[p]));
      cases.add(
          rejected(
              partition,
              (_, f) -> {
                f.capacities[p] += f.tails[p] + 1;
                f.tails[p] = -1;
              }));
      cases.add(
          rejected(
              partition,
              (_, f) -> {
                final int shift = f.maxProbes[p] + 1 - f.tails[p];
                f.capacities[p] -= shift;
                f.tails[p] += shift;
              }));
      // More slots than the writer can hold the control bytes of in one array are rejected by the
      // slot count check, before the length of the data is checked; the most it can hold pass it.
      for (long slotCount : new long[] {maxSlotCount + 1, Integer.MAX_VALUE + 2L, maxSlotCount}) {
        cases.add(
            rejected(
                slotCount > maxSlotCount
                    ? "invalid partition " + last(id) + " slotCount=" + slotCount
                    : "exceed the data",
                (_, f) -> {
                  f.capacities[last(f)] = Math.toIntExact(slotCount - 3);
                  f.tails[last(f)] = 3;
                  f.maxProbes[last(f)] = 3;
                }));
      }
      cases.add(rejected("exceed the data", (_, f) -> f.capacities[last(f)] += 1_000_000));
      cases.add(rejected("slots are too few", TestPrimaryKeyHashCorruption::tooManyKeys));
      // a filter, which the data has no room for
      cases.add(rejected("exceed the data", TestPrimaryKeyHashCorruption::addFilter));
      index.assertRejected(cases);
    }
  }

  /**
   * The filter of a field starts at the first multiple of 64 after its slots and has as many words
   * as its keys take at its number of bits per key: read with another number of words, it would
   * look for keys in other words than the writer put them in, and rule out keys that are there.
   */
  public void testTamperedFilter() throws IOException {
    // at most 12 bits per key, so that 16 bits per key take more words
    final int bitsPerKey =
        TestUtil.nextInt(random(), PrimaryKeyHashPostingsFormat.MIN_FILTER_BITS_PER_KEY, 12);
    try (Index index = new Index(bitsPerKey, false)) {
      index.assertOpens("id7", "id7x");
      final FieldMeta id = index.idEntry();
      assertEquals(bitsPerKey, id.filterBitsPerKey);
      final List<Case> cases = new ArrayList<>();
      for (int invalid : new int[] {-1, 1, 3, 17, 127}) {
        cases.add(
            rejected("invalid filter bitsPerKey=", (_, f) -> f.filterBitsPerKey = (byte) invalid));
      }
      final String layout = "invalid filter offset=";
      // another valid number of bits per key, which takes more words
      cases.add(rejected(layout, (_, f) -> f.filterBitsPerKey = (byte) (bitsPerKey + 1)));
      for (int shift : new int[] {1, -1, id.filterWords, -id.filterWords}) {
        cases.add(rejected(layout, (_, f) -> f.filterWords += shift));
      }
      for (long shift : new long[] {1, Long.BYTES, -Long.BYTES, 64, -64}) {
        cases.add(rejected(layout, (_, f) -> f.filterOffset += shift));
      }
      // consistent with the number of keys and the slots, but past the end of the data
      final int maxBits = PrimaryKeyHashPostingsFormat.MAX_FILTER_BITS_PER_KEY;
      cases.add(
          rejected(
              "exceed the data",
              (_, f) -> {
                f.filterBitsPerKey = (byte) maxBits;
                f.filterWords = Math.toIntExact(Math.ceilDiv(f.numKeys * maxBits, 64));
              }));
      index.assertRejected(cases);
    }
  }

  /**
   * A field with inline keys: its key length, its start, which must be at a cache line after the
   * header, and the sizes of its partitions. Another key length reads records with another stride,
   * which the smallest and largest terms, of the length of all keys, or the end of the data tell.
   */
  public void testTamperedInlineMeta() throws IOException {
    try (Index index = new Index(0, true)) {
      index.assertOpens("id007", "id7");
      final FieldMeta id = index.idEntry();
      assertEquals(PrimaryKeyHashPostingsFormat.LAYOUT_INLINE, id.layout);
      assertEquals(5, id.keyLength);
      final int headerLength = PrimaryKeyHashTestUtil.headerLength(index.dataName);
      final List<Case> cases = new ArrayList<>();
      for (int keyLength :
          new int[] {0, -1, PrimaryKeyHashPostingsFormat.MAX_INLINE_KEY_LENGTH + 1}) {
        cases.add(
            rejected("invalid key length " + keyLength, (_, f) -> f.keyLength = (byte) keyLength));
      }
      cases.add(
          rejected(
              "invalid key length 4, the field has a term of 5 bytes", (_, f) -> f.keyLength = 4));
      cases.add(rejected(null, (_, f) -> f.keyLength = 6));
      for (long shift :
          new long[] {1, Long.BYTES, -Long.BYTES, -id.controlOffset, -headerLength, 1L << 57}) {
        cases.add(
            rejected(
                "invalid controlOffset=" + (id.controlOffset + shift),
                (_, f) -> f.controlOffset += shift));
      }
      cases.add(rejected("exceed the data", (_, f) -> f.capacities[last(f)] += 1_000_000));
      cases.add(rejected("exceed the data", TestPrimaryKeyHashCorruption::addFilter));
      index.assertRejected(cases);
    }
  }
}
