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
 * Writes the delegate's postings, then, for every {@link IndexOptions#DOCS} field, the records,
 * hash table and filter described in {@link PrimaryKeyHashPostingsFormat}.
 *
 * <p>The keys of a field are read in a pass of their own once the delegate has written it. Their
 * documents are counted from the postings, up to the second, because statistics such as {@code
 * docFreq()} throw on the merge views given to {@link #write}. A key without documents is skipped
 * (merges keep terms whose documents were all deleted). Deleted documents are not filtered out: the
 * table must agree with the delegate's postings.
 */
final class HashFieldsConsumer extends FieldsConsumer {

  /**
   * Spilled keys are written to 2^SPLIT_BITS files by the top SPLIT_BITS bits of their hash. Since
   * {@link PrimaryKeyHashPostingsFormat#MAX_PARTITION_BITS} is at most twice SPLIT_BITS, every key
   * is spilled at most twice.
   */
  static final int SPLIT_BITS = 5;

  /** Bytes of (hash, word) pairs buffered per spill file and written in one call: 64 pairs. */
  private static final int SPILL_BLOCK_BYTES = 1024;

  /** The number of (hash, word) pairs read from a spill file at a time. */
  private static final int SPILL_READ_PAIRS = 256;

  /** Bytes of records buffered and written in one call, or more if a record takes more. */
  static final int RECORD_BATCH_BYTES = 4096;

  /** The number of longs written at a time by {@link #writeLongs}. */
  private static final int LONGS_BATCH = 1024;

  /** Zero bytes, as many as the padding before inline records can take. */
  private static final byte[] ZEROS = new byte[CACHE_LINE_BYTES];

  /** The number of slots whose inline records are read before any of them is written. */
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
   * The longest keys that are inline by default. Per key, inline keys of {@code L} bytes take
   * {@code (5 + L) / loadFactor} bytes and keys in term order {@code 5 + L + 9 / loadFactor}, which
   * is no less while {@code L <= (4 + 5 * loadFactor) / (1 - loadFactor)}: 31 at the default load
   * factor. At most {@link PrimaryKeyHashPostingsFormat#MAX_INLINE_KEY_LENGTH}.
   */
  static int defaultMaxInlineKeyLength(float loadFactor) {
    final double lf = loadFactor;
    return (int) Math.min(MAX_INLINE_KEY_LENGTH, (4 + 5 * lf) / (1 - lf));
  }

  /**
   * Writes {@code values[from:to]} to {@code out} as little-endian longs, with one call per {@value
   * #LONGS_BATCH} longs rather than one per long, since merges check their rate limit on every
   * call.
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
   * Writes the records of a field's keys in term order as they are added, and keeps their hashes,
   * from which {@link #finish} writes the field's table, filter and meta entry.
   *
   * <p>If the first key is short enough to be inline, the records are written to a temporary file,
   * laid out as they would be in the data file. If every key has its length, {@link #finish} copies
   * them to the data file in slot order, as inline records. Otherwise, the first key of another
   * length moves them to the data file, where they keep their offsets.
   */
  private final class FieldWriter implements Closeable {
    private final FieldInfo fieldInfo;
    // Keys per value of the top MAX_PARTITION_BITS bits of the hash, from which the size of every
    // partition follows whatever the number of partitions turns out to be.
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

    /** The records not written to {@link #records} yet: the first {@link #batchEnd} bytes. */
    private byte[] batch = new byte[RECORD_BATCH_BYTES];

    /** The bytes of records written to {@link #records}, from {@link #recordsOffset} on. */
    private long written;

    private int batchEnd;

    FieldWriter(FieldInfo fieldInfo) {
      this.fieldInfo = fieldInfo;
    }

    /**
     * Adds the next key in term order, with its only document or {@link
     * PrimaryKeyHashPostingsFormat#MULTI_DOC}.
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

    /** Writes the batch to {@link #records}. */
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
     * Copies the records from the temporary file to the data file, once a key of another length
     * shows that the keys cannot be inline. They keep their offsets, which the slot words hold.
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

    /** Writes the footer of the temporary file of the records and closes it. */
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

    /** Writes the field's table, filter and meta entry, if it has keys and a table can be built. */
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
        // No meta entry: the delegate alone serves the field. Records and partitions already
        // written to the data file are left unused there.
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
     * Writes the table of a field whose keys are inline from its temporary file, whose checksum is
     * verified first so that corruption is never written into the data file.
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
      // if the field failed, the exception is suppressed and IndexWriter deletes this file
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
   * Writes the inline records of a field in slot order, from its records in term order in a
   * temporary file, which the slot words point to. Holds the records of {@link #INLINE_BATCH_SLOTS}
   * slots in heap, at most 4 KB.
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
     * Writes the records of the slots whose words are {@code words}, from a multiple of 64 so that
     * records whose length divides 64 never cross a cache line, then the padding. The records of
     * {@link #INLINE_BATCH_SLOTS} slots are read in a loop that does nothing else, so that the
     * cache misses of their random reads overlap.
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

    /** Writes {@code count} zero bytes in one call. */
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
   * returns {@code null} if the field cannot have a table. If {@code inline} is not null, it writes
   * each partition's records in slot order in place of the slot words. The filter is built as the
   * partitions are gathered, with no pass of its own over the keys.
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
        // The tail is never longer than the largest displacement, which is at most maxProbeLimit
        // and less than count. The control bytes, padding included, must fit in one array.
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
   * The writer of the filter of a field, or {@code null} if fields get no filter or if it would
   * have more than {@link BlockedBloomFilter#MAX_WORDS} words.
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
   * Builds the filter of a field one partition at a time, as the partitions are gathered, holding
   * in heap only the words of one partition, about {@code filterBitsPerKey / 8} bytes per key of a
   * partition.
   *
   * <p>The top bits of a key's filter hash are its partition ({@link TermHash#filterHash(long,
   * int)}), so the keys of partition {@code p} are in the words from {@link
   * BlockedBloomFilter#firstWord} of {@code p} to that of {@code p + 1}, which may also hold keys
   * of the next partitions. Once a partition is finished, the words before the first word of the
   * next one are final. With several partitions, they are written to a temporary file, which is
   * copied to the data file after the slots, where the reader expects the filter.
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
     * from that partition rather than from the hash, which differ only if the hash was read from a
     * corrupt spill file: either way, the key's word is in the window.
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
     * Writes the filter to {@code out} once every partition is finished, from a multiple of {@value
     * PrimaryKeyHashPostingsFormat#CACHE_LINE_BYTES} bytes so that its words fill whole cache
     * lines, and returns where it starts.
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
      // if the field failed, the exception is suppressed and IndexWriter deletes this file
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
   * Places the keys of a partition by linear probing, in the order of their home slots, and returns
   * the partition's control bytes and slots, or {@code null} if a key would be more than {@code
   * maxProbeLimit} slots from its home slot. {@code words} holds, for each key, its record pointer
   * and {@link TermHash#tagAndControlBits}; in a slot, the control bits are moved to the slot's
   * control byte. The keys of a home slot take its slots in the order they are given; lookups do
   * not depend on that order.
   *
   * <p>Nothing wraps around: keys that run past the last home slot go to a tail of extra slots,
   * never longer than the largest displacement. Where the keys of every home slot start and end
   * follows from their number alone, so the slots are sized, and the probe limit checked, before
   * they are allocated.
   */
  static Slots fill(int capacity, long[] words, int[] homes, int n, int maxProbeLimit) {
    assert n < capacity;

    // the number of keys of every home slot, then the next slot that a key of it takes
    final int[] next = new int[capacity];
    for (int i = 0; i < n; i++) {
      next[homes[i]]++;
    }

    // The keys of home slot h take the slots from h or from where the keys of the previous home
    // slots end, whichever is later, and the last of them is the farthest from h. A home slot
    // without keys needs no branch: moving that end up to it changes neither where later keys
    // start nor the largest distance.
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
   * The {@code (hash, word)} pairs of the keys of one field, with words as {@link #fill} takes
   * them, in heap until there are more than {@code maxKeysPerPartition} of them, then in temporary
   * files. In heap, the field has a single partition, gathered with no copy.
   *
   * <p>Spilled, the pairs are written in term order to {@code 2^}{@link #SPLIT_BITS} files by the
   * top bits of their hash: a field of at most that many partitions gathers each partition from
   * consecutive files. A field of more partitions splits each file by the next bits of the hash
   * when the first partition of that file is gathered, so that only a few files exist at once. Each
   * file is deleted once read, and its checksum is verified so that corruption is never written
   * into the data file.
   *
   * <p>A partition gathered from several files has its pairs in term order within each file and the
   * files in hash order, so the keys of a home slot shared by two files are in file order.
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
     * Returns the slot words of the {@code n} keys of partition {@code p} in the first {@code n}
     * entries of the returned array, fills {@code homes} with their home slots, and adds the keys
     * to {@code filter} unless it is null. Partitions must be gathered in order.
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
     * The number of files that hold the pairs of partition {@code p}, of {@code count} keys: the
     * first pending files, once split as far as that takes. Partitions must be gathered in order.
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
      // if the field failed, the exception is suppressed and IndexWriter deletes these files
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
   * Writes {@code (hash, word)} pairs to {@code 2^bits} new spill files, buffering a block of
   * {@value #SPILL_BLOCK_BYTES} bytes per file. Closed before it is finished, it deletes its files.
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
