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
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.format;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.randomFilterBits;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.randomFilterBitsPerKey;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashTestUtil.randomInlining;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.Inlining;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexOutput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * Fields with more keys than fit in one partition, whose keys are spilled to temporary files, as
 * are their filter and the records of keys that may be inline: every key is found, no temporary
 * file is left behind, and a corrupted temporary file fails the flush.
 */
public class TestPrimaryKeyHashSpill extends LuceneTestCase {

  private static final int MAX_KEYS_PER_PARTITION = 16;

  private static Codec codec(int filterBitsPerKey, Inlining inlining) {
    return TestUtil.alwaysPostingsFormat(
        format(0.75f, MAX_KEYS_PER_PARTITION, filterBitsPerKey, inlining));
  }

  /** One flushed segment, written with {@code codec} when the writer is committed. */
  private static IndexWriterConfig config(int numDocs, Codec codec) {
    return new IndexWriterConfig(new MockAnalyzer(random()))
        .setCodec(codec)
        .setMergePolicy(NoMergePolicy.INSTANCE)
        .setMaxBufferedDocs(numDocs + 1)
        .setRAMBufferSizeMB(IndexWriterConfig.DISABLE_AUTO_FLUSH)
        .setUseCompoundFile(false);
  }

  /** Distinct keys of one length: binary UUIDs, UUID strings or numbers. */
  private static List<BytesRef> randomKeys(int numKeys) {
    final List<BytesRef> keys = new ArrayList<>();
    final int shape = random().nextInt(3);
    for (int i = 0; i < numKeys; i++) {
      keys.add(PrimaryKeyHashTestUtil.randomKey(random(), shape, i));
    }
    return keys;
  }

  /** Keys {@code %0nd} of {@code n} digits. */
  private static List<BytesRef> numbers(int numKeys, int digits) {
    final List<BytesRef> keys = new ArrayList<>();
    for (int i = 0; i < numKeys; i++) {
      keys.add(new BytesRef(String.format(Locale.ROOT, "%0" + digits + "d", i)));
    }
    return keys;
  }

  private static void addKeys(IndexWriter writer, List<BytesRef> keys) throws IOException {
    for (BytesRef key : keys) {
      Document doc = new Document();
      doc.add(new StringField("id", key, Field.Store.NO));
      writer.addDocument(doc);
    }
  }

  private static void assertNoTemporaryFiles(Directory dir) throws IOException {
    for (String file : dir.listAll()) {
      assertFalse(file, file.endsWith(".tmp"));
    }
  }

  /**
   * Spilled keys that may be inline: a key of another length, first in term order or after records
   * are written to a temporary file, writes the records to the data file instead, which is then the
   * same as without inline keys.
   */
  public void testSpilledInlineKeys() throws IOException {
    final int numKeys = TestUtil.nextInt(random(), 1000, 3000);
    final List<BytesRef> keys = numbers(numKeys, 12);
    final int filterBitsPerKey = randomFilterBitsPerKey(random());
    // no key of another length, or one that comes first in term order, in the middle, or last
    final BytesRef[] others = {
      null,
      new BytesRef("0"),
      new BytesRef(keys.get(numKeys / 2).utf8ToString() + "x"),
      new BytesRef("9")
    };
    for (BytesRef other : others) {
      final List<BytesRef> withOther = new ArrayList<>(keys);
      if (other != null) {
        withOther.add(other);
      }
      final byte[][] data = new byte[Inlining.values().length][];
      for (Inlining inlining : new Inlining[] {Inlining.NEVER, Inlining.ALWAYS}) {
        try (Directory dir = new ByteBuffersDirectory()) {
          try (IndexWriter writer =
              new IndexWriter(dir, config(withOther.size(), codec(filterBitsPerKey, inlining)))) {
            addKeys(writer, withOther);
            writer.commit();
          }
          assertNoTemporaryFiles(dir);
          final String dataName =
              PrimaryKeyHashTestUtil.onlyFile(dir, PrimaryKeyHashPostingsFormat.DATA_EXTENSION);
          data[inlining.ordinal()] = PrimaryKeyHashTestUtil.withoutHeaderAndFooter(dir, dataName);
          try (DirectoryReader reader = DirectoryReader.open(dir)) {
            LeafReader leaf = getOnlyLeafReader(reader);
            HashTerms terms = (HashTerms) leaf.terms("id");
            assertTrue(terms.numPartitions() > 1);
            assertEquals(inlining == Inlining.ALWAYS && other == null, terms.isInline());
            checkField(leaf, "id", withOther, true);
          }
        }
      }
      final byte[] never = data[Inlining.NEVER.ordinal()];
      final byte[] always = data[Inlining.ALWAYS.ordinal()];
      if (other != null) {
        assertArrayEquals(never, always);
      } else {
        // 4 + 12 bytes per slot rather than 4 + 1 + 12 per key and 8 per slot
        assertTrue(always.length + " vs " + never.length, always.length < never.length);
      }
    }
  }

  /**
   * A partition after the first one whose keys cannot be placed within the probe limit leaves the
   * field without a table, with records in term order and with keys that may be inline; no
   * temporary file is left behind.
   */
  public void testLaterPartitionOverProbeLimit() throws IOException {
    final Random random = random();
    final int maxProbeLimit = TestUtil.nextInt(random, 0, 4);
    final float loadFactor = 0.75f;
    final int perPartition = 10;
    final int partitionBits = 2;
    assertEquals(
        partitionBits,
        HashFieldsConsumer.partitionBits(perPartition << partitionBits, MAX_KEYS_PER_PARTITION));
    final int capacity = (int) HashFieldsConsumer.capacity(perPartition, loadFactor);
    final int home = random.nextInt(capacity);
    // keys of 8 bytes, as many in every partition: in partition 0, each at its home slot, so that
    // the first partition is filled; in partition 1, maxProbeLimit + 2 with the same home slot, one
    // more than can be placed
    final Set<BytesRef> keys = new HashSet<>();
    final int[] counts = new int[1 << partitionBits];
    final Set<Integer> firstHomes = new HashSet<>();
    int colliding = 0;
    while (keys.size() < perPartition << partitionBits) {
      final byte[] bytes = new byte[Long.BYTES];
      random.nextBytes(bytes);
      final long hash = TermHash.hash(bytes, 0, bytes.length);
      final int p = TermHash.partition(hash, partitionBits);
      final int keyHome = TermHash.homeSlot(hash, partitionBits, capacity);
      final boolean atHome = p == 1 && keyHome == home;
      if (counts[p] == perPartition
          || (p == 0 && firstHomes.contains(keyHome))
          || (p == 1 && colliding < maxProbeLimit + 2 && atHome == false)
          || keys.add(new BytesRef(bytes)) == false) {
        continue;
      }
      counts[p]++;
      if (p == 0) {
        firstHomes.add(keyHome);
      }
      if (atHome) {
        colliding++;
      }
    }
    for (Inlining inlining : new Inlining[] {Inlining.NEVER, Inlining.ALWAYS}) {
      try (Directory dir = newDirectory()) {
        final Codec codec =
            TestUtil.alwaysPostingsFormat(
                new PrimaryKeyHashPostingsFormat(
                    TestUtil.getDefaultPostingsFormat(),
                    loadFactor,
                    MAX_KEYS_PER_PARTITION,
                    maxProbeLimit,
                    PrimaryKeyHashPostingsFormat.MAX_RECORDS_LENGTH,
                    randomFilterBitsPerKey(random),
                    inlining));
        try (IndexWriter writer = new IndexWriter(dir, config(keys.size(), codec))) {
          addKeys(writer, new ArrayList<>(keys));
          writer.commit();
        }
        assertNoTemporaryFiles(dir);
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
          final LeafReader leaf = getOnlyLeafReader(reader);
          assertFalse(inlining.toString(), leaf.terms("id") instanceof HashTerms);
          checkField(leaf, "id", new ArrayList<>(keys), false);
        }
      }
    }
  }

  /**
   * A bit flipped in a temporary file must fail the flush, as what is built or copied from it would
   * otherwise be covered by the data file's valid checksum: the records of keys that may be inline
   * (copied in slot order if they are, or in term order after a key of another length that comes
   * after more records than the writer holds in heap), the filter of a field of several partitions,
   * and a spill file, one that keys are spilled to as they are added or one it is split into.
   */
  public void testCorruptedTemporaryFiles() throws IOException {
    for (boolean sameLength : new boolean[] {true, false}) {
      final int numKeys = TestUtil.nextInt(random(), 1000, 3000);
      final List<BytesRef> keys = numbers(numKeys, 10);
      assertTrue(15 * (numKeys / 2) > HashFieldsConsumer.RECORD_BATCH_BYTES);
      if (sameLength == false) {
        // right after the key numKeys / 2, which it starts with
        keys.add(new BytesRef(String.format(Locale.ROOT, "%011d", 10L * (numKeys / 2))));
      }
      assertFlushFails(
          keys, codec(randomFilterBitsPerKey(random()), Inlining.ALWAYS), "hashrecords", 0);
    }
    final int numKeys = TestUtil.nextInt(random(), 200, 2000);
    final int partitionBits = HashFieldsConsumer.partitionBits(numKeys, MAX_KEYS_PER_PARTITION);
    assertTrue(partitionBits > 0);
    assertFlushFails(
        randomKeys(numKeys),
        codec(randomFilterBits(random()), randomInlining(random())),
        "hashfilter",
        0);
    // Files 0 to 31 take keys as they are added, 32 and up are those split from them if there are
    // more than 32 partitions. Corrupt one of the first files, or of the first split ones, so that
    // other files follow it if it has no keys.
    final int spillFiles = 1 << HashFieldsConsumer.SPLIT_BITS;
    final int target =
        partitionBits > HashFieldsConsumer.SPLIT_BITS && random().nextBoolean()
            ? spillFiles + random().nextInt(1 << (partitionBits - HashFieldsConsumer.SPLIT_BITS))
            : random().nextInt(spillFiles - 1);
    assertFlushFails(
        randomKeys(numKeys),
        codec(randomFilterBitsPerKey(random()), randomInlining(random())),
        "hashspill",
        target);
  }

  /**
   * Writes the keys in one segment, flipping a bit in the temporary file whose name contains {@code
   * kind} that is created after {@code skip} others, or in the next one if it has no data: the
   * flush must fail.
   */
  private static void assertFlushFails(List<BytesRef> keys, Codec codec, String kind, int skip)
      throws IOException {
    final AtomicInteger created = new AtomicInteger();
    final AtomicBoolean corrupted = new AtomicBoolean();
    final Directory raw = new ByteBuffersDirectory();
    final Directory dir =
        new FilterDirectory(raw) {
          @Override
          public IndexOutput createTempOutput(String prefix, String suffix, IOContext context)
              throws IOException {
            IndexOutput out = super.createTempOutput(prefix, suffix, context);
            if (out.getName().contains(kind) == false
                || created.getAndIncrement() < skip
                || corrupted.get()) {
              return out;
            }
            return new FilterIndexOutput("corrupting(" + out + ")", out.getName(), out) {
              private boolean closed;

              @Override
              public void close() throws IOException {
                if (closed) {
                  return;
                }
                closed = true;
                out.close();
                long dataLength = raw.fileLength(getName()) - CodecUtil.footerLength();
                if (dataLength > 0 && corrupted.compareAndSet(false, true)) {
                  flipBit(raw, getName(), dataLength);
                }
              }
            };
          }
        };
    try (dir) {
      IndexWriter writer = new IndexWriter(dir, config(keys.size(), codec));
      try {
        addKeys(writer, keys);
        expectThrows(CorruptIndexException.class, writer::commit);
      } finally {
        IOUtils.closeWhileHandlingException(writer);
      }
      assertTrue(kind, corrupted.get());
    }
  }

  /** Flips one bit in the data (not the footer) of a file once it is written. */
  private static void flipBit(Directory dir, String name, long dataLength) throws IOException {
    final byte[] bytes = PrimaryKeyHashTestUtil.readAll(dir, name);
    bytes[random().nextInt((int) dataLength)] ^= (byte) (1 << random().nextInt(8));
    dir.deleteFile(name);
    try (IndexOutput output = dir.createOutput(name, IOContext.DEFAULT)) {
      output.writeBytes(bytes, bytes.length);
    }
  }
}
