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

import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.CACHE_LINE_BYTES;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.DATA_CODEC;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.DATA_EXTENSION;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.LAYOUT_INLINE;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.LAYOUT_TERM_ORDER;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.MAX_INLINE_KEY_LENGTH;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.MAX_PARTITION_BITS;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.META_CODEC;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.META_EXTENSION;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.MULTI_DOC;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.VERSION_CURRENT;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.controlLength;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.inlinePadding;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsConsumer;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.Inlining;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * Writes the delegate's postings, then, for every {@link IndexOptions#DOCS} field, the records and
 * hash table described in {@link PrimaryKeyHashPostingsFormat}, and the field's filter if it has
 * one.
 *
 * <p>The keys of a field are read once the delegate has written it, in a pass of their own over the
 * field's terms, in order, and over the postings of each term up to its second document.
 *
 * <p>Statistics such as {@code docFreq()} cannot be read from the {@link Fields} given to {@link
 * #write} (they throw on merge views), so a key's documents are counted from its postings: a key is
 * skipped if it has no document (merges keep terms whose documents were all deleted), and marked as
 * multi-document if it has a second one. Deleted documents are never filtered out: the table must
 * agree with the delegate's postings, which keep them until they are merged away.
 */
final class HashFieldsConsumer extends FieldsConsumer {

  /**
   * Spilled keys are written, as they are added, to 2^SPLIT_BITS files, by the top SPLIT_BITS bits
   * of their hash. A field of more than 2^SPLIT_BITS partitions splits each of these files by the
   * next bits of the hash when it gathers the first partition of the file. Few files are open at
   * once, and since {@link PrimaryKeyHashPostingsFormat#MAX_PARTITION_BITS} is at most twice
   * SPLIT_BITS, every key is spilled at most twice.
   */
  static final int SPLIT_BITS = 5;

  /**
   * The bytes of (hash, word) pairs that are buffered per spill file being written, and written in
   * one call: 64 pairs.
   */
  private static final int SPILL_BLOCK_BYTES = 1024;

  /** The number of (hash, word) pairs read from a spill file at a time. */
  private static final int SPILL_READ_PAIRS = 256;

  /**
   * The bytes of records that the writer of a field holds before it writes them, in one call, or
   * more if a record takes more.
   */
  static final int RECORD_BATCH_BYTES = 4096;

  /** The number of longs written at a time by {@link #writeLongs}. */
  private static final int LONGS_BATCH = 1024;

  /** Zero bytes, as many as the padding before inline records can take. */
  private static final byte[] ZEROS = new byte[CACHE_LINE_BYTES];

  /**
   * The number of slots whose inline records are read from the temporary file of a field's records
   * before any of them is written to the data file.
   */
  static final int INLINE_BATCH_SLOTS = 64;

  private final FieldsConsumer delegate;
  private final SegmentWriteState state;
  private final float loadFactor;
  private final int maxKeysPerPartition;
  private final int maxProbeLimit;
  private final long maxRecordsLength;
  private final int filterBitsPerKey;

  /** The longest keys that may be inline, 0 if keys are never inline. */
  private final int maxInlineKeyLength;

  private IndexOutput meta, data;
  private boolean closed;

  HashFieldsConsumer(
      FieldsConsumer delegate,
      String delegateName,
      SegmentWriteState state,
      float loadFactor,
      int maxKeysPerPartition,
      int maxProbeLimit,
      long maxRecordsLength,
      int filterBitsPerKey,
      Inlining inlining)
      throws IOException {
    this.delegate = delegate;
    this.state = state;
    this.loadFactor = loadFactor;
    this.maxKeysPerPartition = maxKeysPerPartition;
    this.maxProbeLimit = maxProbeLimit;
    this.maxRecordsLength = maxRecordsLength;
    this.filterBitsPerKey = filterBitsPerKey;
    this.maxInlineKeyLength =
        switch (inlining) {
          case NEVER -> 0;
          case IF_NOT_LARGER -> defaultMaxInlineKeyLength(loadFactor);
          case ALWAYS -> MAX_INLINE_KEY_LENGTH;
        };
    String metaName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, META_EXTENSION);
    String dataName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, DATA_EXTENSION);
    try {
      meta = state.directory.createOutput(metaName, state.context);
      CodecUtil.writeIndexHeader(
          meta, META_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      meta.writeString(delegateName);
      meta.writeVInt(TermHash.XXH3_64_ID);
      data = state.directory.createOutput(dataName, state.context);
      CodecUtil.writeIndexHeader(
          data, DATA_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, meta, data);
      throw t;
    }
  }

  @Override
  public void write(Fields fields, NormsProducer norms) throws IOException {
    // The delegate writes first; it may have opened its files when it was created.
    delegate.write(fields, norms);
    for (String field : fields) {
      final FieldInfo fieldInfo = state.fieldInfos.fieldInfo(field);
      if (fieldInfo.getIndexOptions() != IndexOptions.DOCS) {
        continue;
      }
      final Terms terms = fields.terms(field);
      if (terms != null) {
        writeField(fieldInfo, terms);
      }
    }
  }

  /**
   * The longest keys that are inline by default at the given load factor. Per key, inline keys of
   * {@code L} bytes take {@code (5 + L) / loadFactor} bytes, since every slot takes a record and a
   * control byte, and keys in term order take a record of {@code 5 + L} bytes plus {@code 9 /
   * loadFactor} bytes of slots and control bytes, which is no less while {@code L <= (4 + 5 *
   * loadFactor) / (1 - loadFactor)}: 31 at the default load factor. At most {@link
   * PrimaryKeyHashPostingsFormat#MAX_INLINE_KEY_LENGTH}.
   */
  static int defaultMaxInlineKeyLength(float loadFactor) {
    final double lf = loadFactor;
    return (int) Math.min(MAX_INLINE_KEY_LENGTH, (4 + 5 * lf) / (1 - lf));
  }

  /**
   * Writes {@code values[from:to]} to {@code out} as little-endian longs, as {@link
   * IndexOutput#writeLong} does, with one call per {@value #LONGS_BATCH} longs rather than one per
   * long, since merges check their rate limit on every call.
   */
  static void writeLongs(IndexOutput out, long[] values, int from, int to) throws IOException {
    final byte[] batch = new byte[Math.min(to - from, LONGS_BATCH) * Long.BYTES];
    for (int start = from; start < to; start += LONGS_BATCH) {
      final int n = Math.min(LONGS_BATCH, to - start);
      for (int i = 0; i < n; i++) {
        BitUtil.VH_LE_LONG.set(batch, i * Long.BYTES, values[start + i]);
      }
      out.writeBytes(batch, n * Long.BYTES);
    }
  }

  /** Reads the keys of a field, and writes its records and table. */
  private void writeField(FieldInfo fieldInfo, Terms terms) throws IOException {
    try (FieldWriter writer = new FieldWriter(fieldInfo)) {
      final TermsEnum termsEnum = terms.iterator();
      PostingsEnum postings = null;
      for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
        postings = termsEnum.postings(postings, PostingsEnum.NONE);
        int doc = postings.nextDoc();
        if (doc == DocIdSetIterator.NO_MORE_DOCS) {
          continue;
        }
        if (postings.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
          doc = MULTI_DOC;
        }
        writer.add(term, doc);
      }
      writer.finish();
    }
  }

  /**
   * Writes the records of a field's keys, in term order, as they are added, and keeps their hashes,
   * from which {@link #finish} writes the field's table, its filter and its meta entry. Records
   * start at a multiple of {@link PrimaryKeyHashPostingsFormat#CACHE_LINE_BYTES}, each right after
   * the previous one.
   *
   * <p>If the first key is short enough for the keys to be inline (no longer than the field's
   * maximum inline key length, {@link #defaultMaxInlineKeyLength} unless the format's {@link
   * PrimaryKeyHashPostingsFormat.Inlining} says otherwise), the records are written to a temporary
   * file rather than to the data file, laid out as they would be there. If every key of the field
   * has the first key's length, {@link #finish} copies them from that file to the data file in slot
   * order, as inline records, and the field has no records in term order. Otherwise, the first key
   * of another length copies them to the data file, where they keep their offsets, and the records
   * that follow are written there, as for any other field.
   *
   * <p>Records are laid out in heap, in a batch of about {@value #RECORD_BATCH_BYTES} bytes that is
   * written in one call rather than one call per value, since merges check their rate limit on
   * every call.
   */
  private final class FieldWriter implements Closeable {
    private final FieldInfo fieldInfo;
    // Number of keys per value of the top MAX_PARTITION_BITS bits of the hash, from which the size
    // of every partition can be derived whatever the number of partitions turns out to be.
    private final long[] topBitsCounts = new long[1 << MAX_PARTITION_BITS];
    private final KeyBuffer keys = new KeyBuffer();
    private long numKeys;
    private boolean tableable = true;

    /** Where the records are written: the data file or the temporary file; null before a key. */
    private IndexOutput records;

    /** Where the records start in {@link #records}: a multiple of 64, 0 in the temporary file. */
    private long recordsOffset;

    /** The temporary file that the records are written to, while the keys may be inline. */
    private IndexOutput buffer;

    private String bufferName;

    /** The length of every key, while the records are written to the temporary file; else -1. */
    private int keyLength = -1;

    /**
     * The records that are not written to {@link #records} yet, the first {@link #batchEnd} bytes.
     */
    private byte[] batch = new byte[RECORD_BATCH_BYTES];

    /** The bytes of records written to {@link #records}, from {@link #recordsOffset} on. */
    private long written;

    private int batchEnd;

    FieldWriter(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
    }

    /**
     * Adds the next key in term order, with its only document, or {@link
     * PrimaryKeyHashPostingsFormat#MULTI_DOC}: lays out its record right after those of the keys
     * added so far.
     */
    void add(BytesRef term, int doc) throws IOException {
      final int length = term.length;
      // a doc id, a VInt length of at most 5 bytes, the key
      final int maxRecordLength = Integer.BYTES + 5 + length;
      if (batchEnd + maxRecordLength > batch.length) {
        writeBatch();
        if (maxRecordLength > batch.length) {
          batch = new byte[ArrayUtil.oversize(maxRecordLength, Byte.BYTES)];
        }
      }
      // hash the caller's bytes, before the copy
      final long hash = TermHash.hash(term.bytes, term.offset, length);
      final long recordOffset = written + batchEnd;
      int at = batchEnd;
      BitUtil.VH_LE_INT.set(batch, at, doc);
      at += Integer.BYTES;
      int v = length;
      while ((v & ~0x7F) != 0) {
        batch[at++] = (byte) ((v & 0x7F) | 0x80);
        v >>>= 7;
      }
      batch[at++] = (byte) v;
      System.arraycopy(term.bytes, term.offset, batch, at, length);
      at += length;

      if (records == null) {
        startRecords(length);
      } else if (buffer != null && length != keyLength) {
        moveRecordsToData();
      }
      batchEnd = at;
      numKeys++;

      if (tableable) {
        if (written + batchEnd > maxRecordsLength) {
          // more than 1TB of records for one field of one segment: serve it from the delegate
          tableable = false;
          return;
        }
        topBitsCounts[(int) (hash >>> (Long.SIZE - MAX_PARTITION_BITS))]++;
        keys.add(
            hash,
            ((recordOffset + 1) << TermHash.POINTER_SHIFT) | TermHash.tagAndControlBits(hash));
      }
    }

    /** Writes the records of the batch, those of the keys added, to {@link #records}. */
    private void writeBatch() throws IOException {
      if (batchEnd != 0) {
        records.writeBytes(batch, 0, batchEnd);
        written += batchEnd;
        batchEnd = 0;
      }
    }

    /** Starts the records with the first key, of {@code length} bytes. */
    private void startRecords(int length) throws IOException {
      if (length >= 1 && length <= maxInlineKeyLength) {
        buffer =
            state.directory.createTempOutput(state.segmentInfo.name, "hashrecords", state.context);
        bufferName = buffer.getName();
        records = buffer;
        recordsOffset = 0;
        keyLength = length;
      } else {
        records = data;
        recordsOffset = data.alignFilePointer(CACHE_LINE_BYTES);
      }
    }

    /**
     * Copies the records written to the temporary file to the data file, from a multiple of 64 on,
     * once a key of another length shows that the field's keys cannot be inline. The records keep
     * their offsets from the start of the records, which the slot words hold; the batch is written
     * to the data file after them.
     */
    private void moveRecordsToData() throws IOException {
      final long length = buffer.getFilePointer();
      assert length == written;
      closeBuffer();
      recordsOffset = data.alignFilePointer(CACHE_LINE_BYTES);
      try (ChecksumIndexInput in = state.directory.openChecksumInput(bufferName)) {
        data.copyBytes(in, length);
        // A mismatch fails the field before the data file has a footer, so it is never read.
        CodecUtil.checkFooter(in);
      }
      deleteBuffer();
      records = data;
      keyLength = -1;
    }

    /** Closes the temporary file of the records, with a footer, so that it can be read. */
    private void closeBuffer() throws IOException {
      CodecUtil.writeFooter(buffer);
      final IndexOutput buffer = this.buffer;
      this.buffer = null;
      buffer.close();
    }

    private void deleteBuffer() throws IOException {
      final String name = bufferName;
      bufferName = null;
      state.directory.deleteFile(name);
    }

    /**
     * Writes the field's table, right after its records, or with its records if its keys are
     * inline, then its filter, and its meta entry, if it has keys and a table can be built.
     */
    void finish() throws IOException {
      if (numKeys == 0) {
        assert records == null;
        return;
      }
      writeBatch();
      final long recordsLength = written;
      assert records.getFilePointer() - recordsOffset == recordsLength;
      assert tableable == false || recordsLength <= maxRecordsLength;

      final boolean inline = buffer != null;
      final Table table;
      if (tableable == false) {
        table = null;
      } else if (inline) {
        table = writeInlineTable();
      } else {
        table = writeTable(keys, numKeys, topBitsCounts, null);
      }
      if (table == null) {
        // The field gets no meta entry and is served by the delegate alone. The records and
        // partitions that were written to the data file, if any, are left unused there.
        return;
      }
      meta.writeInt(fieldInfo.number);
      meta.writeVLong(numKeys);
      if (inline) {
        meta.writeByte((byte) LAYOUT_INLINE);
        meta.writeByte((byte) keyLength);
        meta.writeVLong(table.controlOffset);
      } else {
        // the reader finds the first partition there
        assert table.controlOffset
            == IndexOutput.alignOffset(recordsOffset + recordsLength, Long.BYTES);
        meta.writeByte((byte) LAYOUT_TERM_ORDER);
        meta.writeVLong(recordsOffset);
        meta.writeVLong(recordsLength);
      }
      meta.writeByte((byte) table.partitionBits);
      for (int p = 0; p < table.capacities.length; p++) {
        meta.writeVInt(table.capacities[p]);
        meta.writeVInt(table.tails[p]);
        meta.writeVInt(table.maxProbes[p]);
      }
      meta.writeByte((byte) table.filterBitsPerKey);
      if (table.filterBitsPerKey != 0) {
        meta.writeVLong(table.filterOffset);
        meta.writeVInt(table.filterWords);
      }
    }

    /**
     * Writes the table of a field whose keys are inline, from the records in its temporary file,
     * which is checked against its checksum first, so that corruption is never written into the
     * data file, and deleted after.
     */
    private Table writeInlineTable() throws IOException {
      closeBuffer();
      final Table table;
      // read at the positions of the slot words of every partition in turn
      try (IndexInput in =
          state.directory.openInput(bufferName, state.context.withHints(DataAccessHint.RANDOM))) {
        CodecUtil.checksumEntireFile(in);
        table = writeTable(keys, numKeys, topBitsCounts, new InlineRecords(in, keyLength));
      }
      deleteBuffer();
      return table;
    }

    @Override
    public void close() throws IOException {
      // if the field failed, the exception is suppressed and IndexWriter deletes every file it
      // created, the temporary file of the records included
      final String name = bufferName;
      bufferName = null;
      final IndexOutput buffer = this.buffer;
      this.buffer = null;
      IOUtils.close(
          keys,
          buffer,
          () -> {
            if (name != null) {
              IOUtils.deleteFiles(state.directory, List.of(name));
            }
          });
    }
  }

  /**
   * Writes the inline records of the partitions of a field, in slot order, from its records in term
   * order in a temporary file, which the slot words point to: the record of slot {@code i} is the
   * doc id and the key of the record that the word of slot {@code i} points to, and that of an
   * empty slot is zero bytes. Holds the records of {@link #INLINE_BATCH_SLOTS} slots in heap, at
   * most 4 KB.
   */
  private final class InlineRecords {
    private final RandomAccessInput records;
    private final int keyLength;
    private final byte[] batch;

    /**
     * @param in the temporary file of the records, whose checksum was verified
     */
    InlineRecords(IndexInput in, int keyLength) throws IOException {
      assert keyLength >= 1 && keyLength <= MAX_INLINE_KEY_LENGTH;
      this.records = in.randomAccessSlice(0, in.length());
      this.keyLength = keyLength;
      this.batch = new byte[INLINE_BATCH_SLOTS * (Integer.BYTES + keyLength)];
    }

    /**
     * Writes the records of the slots whose words are {@code words}, from the first multiple of 64
     * on, so that records whose length divides 64 never cross a cache line. Zero bytes follow them
     * so that 8 bytes can be read from the start of every record.
     *
     * <p>The records of {@link #INLINE_BATCH_SLOTS} slots at a time are read, in a loop that does
     * nothing else, so that the cache misses of their random reads overlap, then written at once.
     */
    void write(long[] words) throws IOException {
      final int recordLength = Integer.BYTES + keyLength;
      final long position = data.getFilePointer();
      writeZeros((int) (IndexOutput.alignOffset(position, CACHE_LINE_BYTES) - position));
      for (int from = 0; from < words.length; from += INLINE_BATCH_SLOTS) {
        final int n = Math.min(INLINE_BATCH_SLOTS, words.length - from);
        for (int i = 0; i < n; i++) {
          final long word = words[from + i];
          final int at = i * recordLength;
          if (word == 0) {
            Arrays.fill(batch, at, at + recordLength, (byte) 0);
            continue;
          }
          // a record in term order: doc id, a key length of one byte, key
          final long recordPosition = HashTerms.recordPosition(word);
          assert records.readByte(recordPosition + Integer.BYTES) == keyLength;
          BitUtil.VH_LE_INT.set(batch, at, records.readInt(recordPosition));
          records.readBytes(
              recordPosition + Integer.BYTES + 1, batch, at + Integer.BYTES, keyLength);
        }
        data.writeBytes(batch, 0, n * recordLength);
      }
      writeZeros(inlinePadding(recordLength));
    }

    /**
     * Writes zero bytes in one call rather than one per byte, since merges check their rate limit
     * on every call.
     */
    private void writeZeros(int count) throws IOException {
      if (count != 0) {
        data.writeBytes(ZEROS, count);
      }
    }
  }

  /**
   * What the meta file needs to know about a field's slot tables and its filter: {@code
   * controlOffset} is where the control bytes of the first partition start, and {@code
   * filterBitsPerKey} is 0 if it has no filter.
   */
  private record Table(
      int partitionBits,
      long controlOffset,
      int[] capacities,
      int[] tails,
      int[] maxProbes,
      int filterBitsPerKey,
      long filterOffset,
      int filterWords) {}

  /**
   * Writes the control bytes and slots of a field, one partition at a time, then its filter, or
   * returns {@code null} if the field cannot have a table. Partitions written before that is
   * detected are left unused. If {@code inline} is not null, the field's keys are inline: the slots
   * of each partition are its records in slot order, which {@code inline} writes, rather than slot
   * words.
   *
   * <p>The filter is built as the partitions are gathered, which reads every key once, so it takes
   * no pass over the keys of its own, spilled or not, and one partition at a time, see {@link
   * FilterWriter}.
   */
  private Table writeTable(KeyBuffer keys, long numKeys, long[] topBitsCounts, InlineRecords inline)
      throws IOException {
    assert keys.size == numKeys;
    final int partitionBits = partitionBits(numKeys, maxKeysPerPartition);
    final int numPartitions = 1 << partitionBits;
    final int[] capacities = new int[numPartitions];
    final int[] tails = new int[numPartitions];
    final int[] maxProbes = new int[numPartitions];

    final long[] counts = new long[numPartitions];
    final int bucketsPerPartition = topBitsCounts.length >> partitionBits;
    for (int b = 0; b < topBitsCounts.length; b++) {
      counts[b / bucketsPerPartition] += topBitsCounts[b];
    }
    // inline records take the place of the records in term order, which start at a cache line
    final long controlOffset =
        data.alignFilePointer(inline == null ? Long.BYTES : CACHE_LINE_BYTES);
    try (FilterWriter filter = newFilter(numKeys, partitionBits)) {
      for (int p = 0; p < numPartitions; p++) {
        final long count = counts[p];
        final long capacity = capacity(count, loadFactor);
        // The tail after the last home slot is never longer than the largest displacement, which
        // is at most maxProbeLimit and less than count. The control bytes are held in one array,
        // which may have more entries than there are slots (padding follows the last control
        // byte), so it is the array that must fit.
        if (controlLength(capacity + Math.min(maxProbeLimit, count)) > ArrayUtil.MAX_ARRAY_LENGTH) {
          return null;
        }
        final int n = (int) count;
        final int[] homes = new int[n];
        final long[] words = keys.gather(p, partitionBits, (int) capacity, n, homes, filter);
        final Slots slots = fill((int) capacity, words, homes, n, maxProbeLimit);
        if (slots == null) {
          return null;
        }
        // the reader computes the offsets of the partitions from their numbers of slots
        assert slots.control.length == controlLength(slots.words.length);
        data.writeBytes(slots.control, slots.control.length);
        if (inline == null) {
          writeLongs(data, slots.words, 0, slots.words.length);
        } else {
          inline.write(slots.words);
        }
        capacities[p] = (int) capacity;
        tails[p] = slots.words.length - (int) capacity;
        maxProbes[p] = slots.maxProbe;
        if (filter != null) {
          filter.finishPartition(p);
        }
      }
      if (filter == null) {
        return new Table(partitionBits, controlOffset, capacities, tails, maxProbes, 0, 0, 0);
      }
      final long filterOffset = filter.writeTo(data);
      return new Table(
          partitionBits,
          controlOffset,
          capacities,
          tails,
          maxProbes,
          filterBitsPerKey,
          filterOffset,
          filter.numWords());
    }
  }

  /**
   * The writer of the filter of a field of {@code numKeys} keys and {@code partitionBits} partition
   * bits, or {@code null} if fields get no filter or if it would have more than {@link
   * BlockedBloomFilter#MAX_WORDS} words.
   */
  private FilterWriter newFilter(long numKeys, int partitionBits) {
    if (filterBitsPerKey == 0) {
      return null;
    }
    final long numWords = BlockedBloomFilter.numWords(numKeys, filterBitsPerKey);
    if (numWords > BlockedBloomFilter.MAX_WORDS) {
      return null;
    }
    return new FilterWriter(
        state.directory, state.segmentInfo.name, state.context, (int) numWords, partitionBits);
  }

  /**
   * Builds the filter of a field one partition at a time, as the partitions are gathered, and holds
   * in heap only the words that the keys of one partition are in, about {@code filterBitsPerKey /
   * 8} bytes per key of a partition, rather than the whole filter.
   *
   * <p>The top bits of a key's filter hash are its partition ({@link TermHash#filterHash(long,
   * int)}), so the keys of partition {@code p} are in the words from {@link
   * BlockedBloomFilter#firstWord} of {@code p} to that of {@code p + 1}, which may also hold keys
   * of the next partitions. Once the keys of a partition are added, the words before the first word
   * of the next one are final. A field of one partition holds all its words until they are written
   * after its slots. A field of several partitions writes its final words, after every partition,
   * to a temporary file, whose checksum is verified when it is copied to the data file after the
   * slots: the filter is in the data file where the reader expects it, and its writing takes one
   * more sequential write and read of {@code filterBitsPerKey / 8} bytes per key, rather than heap
   * for the whole filter.
   */
  static final class FilterWriter implements Closeable {
    private final Directory directory;
    private final String tempPrefix;
    private final IOContext context;
    private final int numWords;
    private final int partitionBits;
    private final int numPartitions;

    /** The words from {@link #windowStart} on. */
    private final long[] window;

    private int windowStart;

    /** The partition whose keys are being added. */
    private int partition;

    /** The final words, if the field has several partitions, until they are copied. */
    private IndexOutput temp;

    private String tempName;

    FilterWriter(
        Directory directory,
        String tempPrefix,
        IOContext context,
        int numWords,
        int partitionBits) {
      assert numWords >= 1 && numWords <= BlockedBloomFilter.MAX_WORDS : numWords;
      this.directory = directory;
      this.tempPrefix = tempPrefix;
      this.context = context;
      this.numWords = numWords;
      this.partitionBits = partitionBits;
      this.numPartitions = 1 << partitionBits;
      // the words of one partition, at most; all of them if there is a single partition
      final long windowWords =
          numPartitions == 1
              ? numWords
              : Math.min(numWords, Math.ceilDiv(numWords, numPartitions) + 1L);
      this.window = new long[Math.toIntExact(windowWords)];
    }

    /** The number of words of the filter. */
    int numWords() {
      return numWords;
    }

    /** The number of words held in heap. For tests. */
    int windowWords() {
      return window.length;
    }

    /**
     * Adds a key of the partition being built, given its hash. Its filter hash takes its top bits
     * from that partition rather than from the hash, which are the same unless the hash was read
     * from a corrupt spill file, whose checksum is verified before the filter is written: either
     * way, the key's word is in the window.
     */
    void add(long hash) {
      final long partitionHash =
          partitionBits == 0 ? 0 : (long) partition << (Long.SIZE - partitionBits);
      BlockedBloomFilter.add(
          window,
          windowStart,
          numWords,
          TermHash.filterHash(TermHash.mix(hash), partitionHash, partitionBits));
    }

    /**
     * Ends partition {@code p}, whose keys were all added: writes the words that no later partition
     * has keys in to the temporary file, and moves on to partition {@code p + 1}.
     */
    void finishPartition(int p) throws IOException {
      assert p == partition : p + " != " + partition;
      partition++;
      if (numPartitions == 1) {
        return; // written by writeTo
      }
      final int end =
          partition < numPartitions
              ? BlockedBloomFilter.firstWord(partition, partitionBits, numWords)
              : numWords;
      final int done = end - windowStart;
      if (temp == null) {
        temp = directory.createTempOutput(tempPrefix, "hashfilter", context);
        tempName = temp.getName();
      }
      writeLongs(temp, window, 0, done);
      if (end < numWords) {
        // word end may hold keys of p already, and the next partition starts there
        window[0] = window[done];
        Arrays.fill(window, 1, done + 1, 0L);
      }
      windowStart = end;
    }

    /**
     * Writes the filter to {@code out} once every partition is finished, from the first multiple of
     * {@value PrimaryKeyHashPostingsFormat#CACHE_LINE_BYTES} bytes on, as the records are, so that
     * its words fill whole cache lines and none crosses one, and returns where it starts.
     */
    long writeTo(IndexOutput out) throws IOException {
      assert partition == numPartitions : partition + " of " + numPartitions + " partitions";
      final long offset = out.alignFilePointer(CACHE_LINE_BYTES);
      if (temp == null) {
        assert numPartitions == 1 && window.length == numWords;
        writeLongs(out, window, 0, window.length);
        return offset;
      }
      CodecUtil.writeFooter(temp);
      temp.close();
      temp = null;
      try (ChecksumIndexInput in = directory.openChecksumInput(tempName)) {
        out.copyBytes(in, (long) numWords * Long.BYTES);
        // A mismatch fails the field before the data file has a footer, so it is never read.
        CodecUtil.checkFooter(in);
      }
      final String name = tempName;
      tempName = null;
      directory.deleteFile(name);
      return offset;
    }

    @Override
    public void close() throws IOException {
      // if the field failed, the exception is suppressed and IndexWriter deletes every file it
      // created, this one included
      final String name = tempName;
      tempName = null;
      final IndexOutput temp = this.temp;
      this.temp = null;
      IOUtils.close(
          temp,
          () -> {
            if (name != null) {
              IOUtils.deleteFiles(directory, List.of(name));
            }
          });
    }
  }

  /**
   * The number of partition bits for a field: enough that partitions hold about {@code
   * maxKeysPerPartition} keys on average, at most {@link
   * PrimaryKeyHashPostingsFormat#MAX_PARTITION_BITS}.
   */
  static int partitionBits(long numKeys, int maxKeysPerPartition) {
    long partitions = (numKeys + maxKeysPerPartition - 1) / maxKeysPerPartition;
    int bits = partitions <= 1 ? 0 : Long.SIZE - Long.numberOfLeadingZeros(partitions - 1);
    return Math.min(bits, MAX_PARTITION_BITS);
  }

  /** The number of slots of a partition of {@code n} keys; always more than {@code n}. */
  static long capacity(long n, float loadFactor) {
    long capacity = (long) Math.ceil(n / (double) loadFactor);
    return Math.max(Math.max(capacity, n + 1), 2);
  }

  /**
   * The control bytes and the slot words of a partition, and the largest displacement of a key from
   * its home slot. {@code control} holds {@link PrimaryKeyHashPostingsFormat#controlLength} bytes
   * for the {@code words.length} slots, padding included.
   */
  record Slots(byte[] control, long[] words, int maxProbe) {}

  /**
   * Places the keys of a partition by linear probing, in the order of their home slots (see {@link
   * PrimaryKeyHashPostingsFormat} for why), and returns the partition's control bytes and slots, or
   * {@code null} if a key would be more than {@code maxProbeLimit} slots from its home slot. {@code
   * words} holds, for each key, its record pointer and {@link TermHash#tagAndControlBits}; in a
   * slot, the control bits are moved to the slot's control byte, leaving the pointer and tag. The
   * keys of a home slot take its slots in the order they are given; lookups do not depend on that
   * order.
   *
   * <p>Inserted in the order of their home slots, keys with home slot {@code h} take the slots from
   * {@code h} or from right after the previously inserted key, whichever is later. Nothing wraps
   * around: keys that run past the last home slot go to a tail of extra slots after it, so there
   * are {@code capacity} slots plus that tail, which is never longer than the largest displacement.
   * The keys along the slots are therefore in the order of their home slots.
   *
   * <p>Where the keys of every home slot start and end follows from their number alone, so the
   * slots are sized, and the probe limit checked, before they are allocated. Each key is then
   * written straight to the next slot of its home slot.
   */
  static Slots fill(int capacity, long[] words, int[] homes, int n, int maxProbeLimit) {
    assert n < capacity;

    // the number of keys of every home slot, then the next slot that a key of it takes
    final int[] next = new int[capacity];
    for (int i = 0; i < n; i++) {
      next[homes[i]]++;
    }

    // The keys of home slot h take the slots from h or from where the keys of the previous home
    // slots end, whichever is later, and the last of them is the farthest from h. Without a branch:
    // a home slot without keys moves that end up to it, which changes neither where the keys of the
    // next home slots start nor the largest distance, which a key of an earlier one had.
    long end = 0;
    long largest = 0;
    for (int home = 0; home < capacity; home++) {
      final long start = Math.max(home, end);
      end = start + next[home];
      next[home] = (int) start;
      largest = Math.max(largest, end - 1 - home);
    }
    if (largest > maxProbeLimit) {
      return null;
    }
    assert end <= capacity + largest;

    final int slotCount = (int) Math.max(capacity, end);
    final long[] slots = new long[slotCount];
    // zero, the control byte of an empty slot, up to the end of the padding
    final byte[] control = new byte[(int) controlLength(slotCount)];
    for (int i = 0; i < n; i++) {
      final int home = homes[i];
      final int slot = next[home]++;
      final long word = words[i];
      // nothing between the control bits and the pointer
      assert (word & ZERO_BITS & ~TermHash.tagAndControlBits(-1L)) == 0;
      control[slot] = (byte) TermHash.control(word);
      slots[slot] = word & ~ZERO_BITS;
    }
    return new Slots(control, slots, (int) largest);
  }

  /** The bits of a slot word between the tag and the pointer, which are zero. */
  private static final long ZERO_BITS = ((1L << TermHash.POINTER_SHIFT) - 1) & ~TermHash.TAG_MASK;

  @Override
  public void close() throws IOException {
    if (closed) {
      return;
    }
    closed = true;
    boolean success = false;
    try {
      delegate.close();
      meta.writeInt(-1); // end of fields
      CodecUtil.writeFooter(meta);
      CodecUtil.writeFooter(data);
      success = true;
    } finally {
      if (success) {
        IOUtils.close(meta, data);
      } else {
        IOUtils.closeWhileHandlingException(meta, data);
      }
      meta = data = null;
    }
  }

  /**
   * The {@code (hash, word)} pairs of the keys of one field, where a word holds the record pointer
   * and {@link TermHash#tagAndControlBits} of the hash as {@link #fill} takes them, in heap until
   * there are more than {@code maxKeysPerPartition} of them, then in temporary files. In heap, the
   * field has a single partition, gathered with no copy.
   *
   * <p>Spilled, the pairs are written in term order to {@code 2^}{@link #SPLIT_BITS} files, by the
   * top {@link #SPLIT_BITS} bits of their hash, as they are added, a block of {@value
   * #SPILL_BLOCK_BYTES} bytes per file at a time: a field of at most {@code 2^}{@link #SPLIT_BITS}
   * partitions gathers each of its partitions from consecutive files, with no other pass. A field
   * of more partitions splits each file by the next bits of the hash into files of a partition
   * each, when the first of these partitions is gathered, so that only a few files exist at once;
   * each is deleted once it is read. Every file has a checksum footer, which is verified when it is
   * read, so that corruption is never written into the data file.
   *
   * <p>The pairs of a partition gathered from several files are in term order within each file, and
   * the files come in the order of the top bits of their hashes, not in term order, so the keys of
   * the few home slots that keys of two files can share are in file order in the slots.
   */
  private final class KeyBuffer implements Closeable {
    private long[] hashes = new long[16];
    private long[] words = new long[16];
    private long size;
    private boolean spilled;

    /** The files that the pairs are spilled to, once spilled, until a partition is gathered. */
    private SpillWriter spill;

    /** The pairs read from a spill file at a time, hash then word, once spilled. */
    private long[] pairs;

    /**
     * The files that are left to gather, in partition order: the first ones hold the pairs of the
     * next partition to gather, and maybe of more.
     */
    private final ArrayDeque<SpillFile> pending = new ArrayDeque<>();

    /**
     * {@code count} pairs whose hash starts with the {@code bits}-bit {@code prefix}, in term
     * order.
     */
    private record SpillFile(String name, int prefix, int bits, long count) {}

    void add(long hash, long word) throws IOException {
      if (spilled == false && size == maxKeysPerPartition) {
        startSpilling();
      }
      if (spilled) {
        spill.add(TermHash.partition(hash, SPLIT_BITS), hash, word);
      } else {
        int i = (int) size;
        if (i == hashes.length) {
          hashes = ArrayUtil.grow(hashes, i + 1);
          words = ArrayUtil.growExact(words, hashes.length);
        }
        hashes[i] = hash;
        words[i] = word;
      }
      size++;
    }

    private void startSpilling() throws IOException {
      spill = new SpillWriter(SPLIT_BITS);
      spilled = true;
      pairs = new long[2 * SPILL_READ_PAIRS];
      for (int i = 0; i < size; i++) {
        spill.add(TermHash.partition(hashes[i], SPLIT_BITS), hashes[i], words[i]);
      }
      hashes = words = null;
    }

    /**
     * Returns the slot words of the {@code n} keys of partition {@code p}, in term order within
     * each spill file they are read from (in the first {@code n} entries of the returned array),
     * fills {@code homes} with their home slots, and adds the keys to {@code filter}, unless it is
     * null. Partitions must be gathered in order.
     */
    long[] gather(int p, int partitionBits, int capacity, int n, int[] homes, FilterWriter filter)
        throws IOException {
      if (spilled == false) {
        assert partitionBits == 0 && n == size;
        for (int i = 0; i < n; i++) {
          homes[i] = TermHash.homeSlot(hashes[i], 0, capacity);
        }
        if (filter != null) {
          for (int i = 0; i < n; i++) {
            filter.add(hashes[i]);
          }
        }
        return words;
      }
      final int numFiles = partitionFiles(p, partitionBits, n);
      final long[] words = new long[n];
      final long[] pairs = this.pairs;
      int i = 0;
      for (int f = 0; f < numFiles; f++) {
        final SpillFile file = pending.getFirst();
        try (ChecksumIndexInput in = state.directory.openChecksumInput(file.name)) {
          for (long left = file.count; left > 0; ) {
            final int m = (int) Math.min(left, SPILL_READ_PAIRS);
            in.readLongs(pairs, 0, 2 * m);
            for (int j = 0; j < 2 * m; j += 2, i++) {
              final long hash = pairs[j];
              words[i] = pairs[j + 1];
              homes[i] = TermHash.homeSlot(hash, partitionBits, capacity);
              if (filter != null) {
                // Corruption fails the footer check below, before the filter can be written.
                filter.add(hash);
              }
            }
            left -= m;
          }
          CodecUtil.checkFooter(in);
        }
        pending.removeFirst();
        state.directory.deleteFile(file.name);
      }
      assert i == n;
      return words;
    }

    /**
     * The number of files that the pairs of partition {@code p}, of {@code count} keys, are in,
     * which are the first pending files once the files before them are split as far as that takes.
     * Partitions must be gathered in order.
     */
    private int partitionFiles(int p, int partitionBits, long count) throws IOException {
      if (spill != null) {
        final SpillWriter spill = this.spill;
        this.spill = null;
        try (spill) {
          spill.finish();
        }
        for (int f = 0; f < spill.counts.length; f++) {
          pending.add(new SpillFile(spill.name(f), f, SPLIT_BITS, spill.counts[f]));
        }
      }
      SpillFile file = pending.getFirst();
      while (file.bits < partitionBits) {
        split(file, Math.min(SPLIT_BITS, partitionBits - file.bits));
        file = pending.getFirst();
      }
      int numFiles = 0;
      long total = 0;
      for (SpillFile next : pending) {
        if (next.bits < partitionBits || next.prefix >>> (next.bits - partitionBits) != p) {
          break;
        }
        numFiles++;
        total += next.count;
      }
      assert numFiles > 0 && total == count
          : numFiles + " files of " + total + " pairs for partition " + p + " of " + count;
      return numFiles;
    }

    /**
     * Replaces {@code file}, the first pending file, with the {@code 2^bits} files of its pairs
     * split by the next {@code bits} bits of the hash.
     */
    private void split(SpillFile file, int bits) throws IOException {
      assert pending.getFirst() == file;
      final int childBits = file.bits + bits;
      final long[] pairs = this.pairs;
      final SpillWriter children = new SpillWriter(bits);
      try (children) {
        try (ChecksumIndexInput in = state.directory.openChecksumInput(file.name)) {
          for (long left = file.count; left > 0; ) {
            final int m = (int) Math.min(left, SPILL_READ_PAIRS);
            in.readLongs(pairs, 0, 2 * m);
            for (int j = 0; j < 2 * m; j += 2) {
              final long hash = pairs[j];
              // masked so that a corrupt hash cannot fail before the checksum is verified
              final int c = TermHash.partition(hash, childBits) & ((1 << bits) - 1);
              children.add(c, hash, pairs[j + 1]);
            }
            left -= m;
          }
          CodecUtil.checkFooter(in);
        }
        children.finish();
      }
      pending.removeFirst();
      for (int c = children.counts.length - 1; c >= 0; c--) {
        pending.addFirst(
            new SpillFile(
                children.name(c), (file.prefix << bits) | c, childBits, children.counts[c]));
      }
      state.directory.deleteFile(file.name);
    }

    @Override
    public void close() throws IOException {
      // if the field failed, the exception is suppressed and IndexWriter deletes every file it
      // created, these ones included
      final List<String> files = new ArrayList<>();
      for (SpillFile file : pending) {
        files.add(file.name);
      }
      IOUtils.close(spill, () -> IOUtils.deleteFiles(state.directory, files));
      spill = null;
      pending.clear();
    }
  }

  /**
   * Writes {@code (hash, word)} pairs to {@code 2^bits} new spill files, each pair to the file that
   * {@link #add} is given, and holds a block of {@value #SPILL_BLOCK_BYTES} bytes of pairs of each
   * file, which it writes in one call once full. Closed before it is finished, it deletes its
   * files.
   */
  private final class SpillWriter implements Closeable {
    private final IndexOutput[] outputs;
    private final String[] names;
    private final byte[] blocks;

    /** Where the pairs of file {@code f} that are not written yet end in {@link #blocks}. */
    private final int[] ends;

    /** The number of pairs of every file. */
    final long[] counts;

    private boolean finished;

    SpillWriter(int bits) throws IOException {
      final int numFiles = 1 << bits;
      outputs = new IndexOutput[numFiles];
      names = new String[numFiles];
      blocks = new byte[numFiles * SPILL_BLOCK_BYTES];
      ends = new int[numFiles];
      counts = new long[numFiles];
      boolean success = false;
      try {
        for (int f = 0; f < numFiles; f++) {
          outputs[f] =
              state.directory.createTempOutput(state.segmentInfo.name, "hashspill", state.context);
          names[f] = outputs[f].getName();
          ends[f] = f * SPILL_BLOCK_BYTES;
        }
        success = true;
      } finally {
        if (success == false) {
          IOUtils.closeWhileHandlingException(this);
        }
      }
    }

    /** The name of file {@code f}. */
    String name(int f) {
      return names[f];
    }

    void add(int f, long hash, long word) throws IOException {
      int end = ends[f];
      BitUtil.VH_LE_LONG.set(blocks, end, hash);
      BitUtil.VH_LE_LONG.set(blocks, end + Long.BYTES, word);
      end += 2 * Long.BYTES;
      if (end == (f + 1) * SPILL_BLOCK_BYTES) {
        end = f * SPILL_BLOCK_BYTES;
        outputs[f].writeBytes(blocks, end, SPILL_BLOCK_BYTES);
      }
      ends[f] = end;
      counts[f]++;
    }

    /** Writes the rest of the pairs of every file and its footer, and closes it. */
    void finish() throws IOException {
      for (int f = 0; f < outputs.length; f++) {
        final int start = f * SPILL_BLOCK_BYTES;
        outputs[f].writeBytes(blocks, start, ends[f] - start);
        CodecUtil.writeFooter(outputs[f]);
      }
      IOUtils.close(outputs);
      finished = true;
    }

    @Override
    public void close() throws IOException {
      if (finished) {
        return;
      }
      finished = true;
      final List<String> files = new ArrayList<>();
      for (String name : names) {
        if (name != null) {
          files.add(name);
        }
      }
      IOUtils.close(
          () -> IOUtils.close(outputs), () -> IOUtils.deleteFiles(state.directory, files));
    }
  }
}
