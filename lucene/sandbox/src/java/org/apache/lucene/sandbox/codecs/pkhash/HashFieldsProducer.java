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
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.MAX_FILTER_BITS_PER_KEY;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.MAX_INLINE_KEY_LENGTH;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.MAX_PARTITION_BITS;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.MAX_RECORDS_LENGTH;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.META_CODEC;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.META_EXTENSION;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.MIN_FILTER_BITS_PER_KEY;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.VERSION_CURRENT;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.VERSION_START;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.controlLength;
import static org.apache.lucene.sandbox.codecs.pkhash.PrimaryKeyHashPostingsFormat.inlinePadding;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.Terms;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * Reads the files written by {@link HashFieldsConsumer}: opens the delegate by the name stored in
 * the meta file, reads and validates the entries of the fields that have a table, and returns a
 * {@link HashTerms} for each of them; other fields are served by the delegate. The meta file is
 * verified against its checksum when it is opened; the data file only has its header and footer
 * checked then, and is fully verified by {@link #checkIntegrity}.
 */
final class HashFieldsProducer extends FieldsProducer {

  /** Far beyond any real file; keeps offset arithmetic on corrupt meta data from overflowing. */
  private static final long MAX_DATA_OFFSET = 1L << 56;

  private final FieldsProducer delegate;
  private final IndexInput data;
  private final Map<String, HashTerms> tables;

  HashFieldsProducer(SegmentReadState state) throws IOException {
    final String metaName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, META_EXTENSION);
    final List<FieldEntry> entries = new ArrayList<>();
    String delegateName = null;
    try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaName)) {
      Throwable priorE = null;
      try {
        CodecUtil.checkIndexHeader(
            meta,
            META_CODEC,
            VERSION_START,
            VERSION_CURRENT,
            state.segmentInfo.getId(),
            state.segmentSuffix);
        delegateName = meta.readString();
        int hashFunction = meta.readVInt();
        if (hashFunction != TermHash.XXH3_64_ID) {
          throw new CorruptIndexException("unknown hash function " + hashFunction, meta);
        }
        readFields(meta, state.fieldInfos, state.segmentSuffix, entries);
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(meta, priorE);
      }
    }

    final String dataName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, DATA_EXTENSION);
    IndexInput data = null;
    FieldsProducer delegate = null;
    try {
      // lookups read a few bytes at random positions
      data =
          state.directory.openInput(
              dataName, state.context.withHints(FileTypeHint.DATA, DataAccessHint.RANDOM));
      CodecUtil.checkIndexHeader(
          data,
          DATA_CODEC,
          VERSION_START,
          VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      // The data file is too costly to verify in full on open, but this checks the structure of
      // its footer, which detects truncation.
      CodecUtil.retrieveChecksum(data);
      final long dataEnd = data.length() - CodecUtil.footerLength();
      for (FieldEntry entry : entries) {
        entry.validateRanges(dataEnd, data);
      }

      delegate = PostingsFormat.forName(delegateName).fieldsProducer(state);
      final Map<String, HashTerms> tables = new HashMap<>();
      for (FieldEntry entry : entries) {
        final Terms delegateTerms = delegate.terms(entry.field);
        final BytesRef min = delegateTerms == null ? null : delegateTerms.getMin();
        final BytesRef max = delegateTerms == null ? null : delegateTerms.getMax();
        if (min == null || max == null) {
          throw new CorruptIndexException(
              "field \"" + entry.field + "\" has a hash table but no terms", data);
        }
        if (entry.keyLength != 0) {
          checkKeyLength(entry, min, data);
          checkKeyLength(entry, max, data);
        }
        tables.put(entry.field, new HashTerms(delegateTerms, min, max, entry, data));
      }
      this.tables = tables;
      this.data = data;
      this.delegate = delegate;
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, data, delegate);
      throw t;
    }
  }

  /** A merge instance: the delegate's merge instance, without tables, and the same data file. */
  private HashFieldsProducer(FieldsProducer delegate, IndexInput data) {
    this.delegate = delegate;
    this.data = data;
    this.tables = Map.of();
  }

  private static void readFields(
      ChecksumIndexInput meta,
      FieldInfos fieldInfos,
      String segmentSuffix,
      List<FieldEntry> entries)
      throws IOException {
    final long minOffset = CodecUtil.indexHeaderLength(DATA_CODEC, segmentSuffix);
    final Set<Integer> fieldNumbers = new HashSet<>();
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
      FieldInfo info = fieldNumber < 0 ? null : fieldInfos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("invalid field number " + fieldNumber, meta);
      }
      if (info.getIndexOptions() != IndexOptions.DOCS) {
        throw new CorruptIndexException(
            "field \"" + info.name + "\" has index options " + info.getIndexOptions(), meta);
      }
      if (fieldNumbers.add(fieldNumber) == false) {
        throw new CorruptIndexException("duplicate field \"" + info.name + "\"", meta);
      }
      FieldEntry entry = new FieldEntry(info.name);
      entry.numKeys = meta.readVLong();
      if (entry.numKeys < 1) {
        throw new CorruptIndexException(
            "field \"" + info.name + "\": invalid numKeys=" + entry.numKeys, meta);
      }
      final int layout = meta.readByte();
      if (layout == LAYOUT_TERM_ORDER) {
        readTermOrderTable(meta, info, entry, minOffset);
      } else if (layout == LAYOUT_INLINE) {
        readInlineTable(meta, info, entry, minOffset);
      } else {
        throw new CorruptIndexException(
            "field \"" + info.name + "\": invalid layout " + layout, meta);
      }
      entries.add(entry);
    }
  }

  /**
   * Reads the table of a field whose records are in term order: where they are, then the
   * partitions, whose control bytes and slots follow the records, and the filter.
   */
  private static void readTermOrderTable(
      ChecksumIndexInput meta, FieldInfo info, FieldEntry entry, long minOffset)
      throws IOException {
    final long recordsOffset = meta.readVLong();
    final long recordsLength = meta.readVLong();
    // A record takes at least 5 bytes: doc, one-byte length, no key bytes. Records start at a
    // cache line.
    if (recordsOffset < minOffset
        || recordsOffset > MAX_DATA_OFFSET
        || recordsOffset % CACHE_LINE_BYTES != 0
        || recordsLength < 5
        || recordsLength / 5 < entry.numKeys
        || recordsLength > MAX_RECORDS_LENGTH) {
      throw new CorruptIndexException(
          "field \""
              + info.name
              + "\": invalid records offset="
              + recordsOffset
              + " length="
              + recordsLength,
          meta);
    }
    entry.sliceOffset = recordsOffset;
    final long end =
        readPartitions(
            meta,
            info,
            entry,
            IndexOutput.alignOffset(recordsOffset + recordsLength, Long.BYTES),
            0);
    entry.sliceLength = readFilter(meta, info, entry, end) - entry.sliceOffset;
  }

  /**
   * Reads the table of a field whose keys are inline: their length, where the field starts, then
   * the partitions and the filter.
   */
  private static void readInlineTable(
      ChecksumIndexInput meta, FieldInfo info, FieldEntry entry, long minOffset)
      throws IOException {
    final int keyLength = meta.readByte();
    if (keyLength < 1 || keyLength > MAX_INLINE_KEY_LENGTH) {
      throw new CorruptIndexException(
          "field \"" + info.name + "\": invalid key length " + keyLength, meta);
    }
    entry.keyLength = keyLength;
    final long controlOffset = meta.readVLong();
    if (controlOffset < minOffset
        || controlOffset > MAX_DATA_OFFSET
        || controlOffset % CACHE_LINE_BYTES != 0) {
      throw new CorruptIndexException(
          "field \"" + info.name + "\": invalid controlOffset=" + controlOffset, meta);
    }
    entry.sliceOffset = controlOffset;
    final long end = readPartitions(meta, info, entry, controlOffset, keyLength);
    entry.sliceLength = readFilter(meta, info, entry, end) - entry.sliceOffset;
  }

  /**
   * Reads and checks the partitions of a field, whose first control bytes start at {@code offset},
   * and returns where the last partition ends. Each partition's offsets follow from its number of
   * slots, as the file format gives them. For records in term order ({@code keyLength} 0), the
   * slots of a partition are slot words, right after its control bytes. For inline keys, the slots
   * are records of {@code 4 + keyLength} bytes, from the first multiple of 64 after the control
   * bytes, followed by {@link PrimaryKeyHashPostingsFormat#inlinePadding} zero bytes.
   */
  private static long readPartitions(
      ChecksumIndexInput meta, FieldInfo info, FieldEntry entry, long offset, int keyLength)
      throws IOException {
    entry.partitionBits = meta.readByte();
    if (entry.partitionBits < 0 || entry.partitionBits > MAX_PARTITION_BITS) {
      throw new CorruptIndexException(
          "field \"" + info.name + "\": invalid partitionBits " + entry.partitionBits, meta);
    }
    final boolean inline = keyLength != 0;
    final int slotLength = inline ? Integer.BYTES + keyLength : Long.BYTES;
    final int numPartitions = 1 << entry.partitionBits;
    entry.controlOffsets = new long[numPartitions];
    final long[] slotsOffsets = new long[numPartitions];
    entry.capacities = new int[numPartitions];
    entry.slotCounts = new int[numPartitions];
    entry.maxProbes = new int[numPartitions];
    long totalCapacity = 0;
    for (int p = 0; p < numPartitions; p++) {
      int capacity = meta.readVInt();
      int tail = meta.readVInt();
      int maxProbe = meta.readVInt();
      long slotCount = capacity + (long) tail;
      // the last slot holds a key whose home slot is at most capacity - 1, so tail <= maxProbe
      if (capacity < 2 || maxProbe < 0 || tail < 0 || tail > maxProbe || maxProbe >= slotCount) {
        throw new CorruptIndexException(
            "field \""
                + info.name
                + "\": invalid partition "
                + p
                + " capacity="
                + capacity
                + " tail="
                + tail
                + " maxProbe="
                + maxProbe,
            meta);
      }
      // The writer holds a partition's control bytes in one array, which has more entries than
      // the partition has slots (8 zero bytes and padding follow the last control byte), so it
      // is the array whose length bounds the number of slots.
      if (controlLength(slotCount) > ArrayUtil.MAX_ARRAY_LENGTH) {
        throw new CorruptIndexException(
            "field \""
                + info.name
                + "\": invalid partition "
                + p
                + " slotCount="
                + slotCount
                + ", too many for its control bytes to fit in an array",
            meta);
      }
      final long slotsOffset =
          inline
              ? IndexOutput.alignOffset(offset + controlLength(slotCount), CACHE_LINE_BYTES)
              : offset + controlLength(slotCount);
      // stored relative to the start of the field's slice
      entry.controlOffsets[p] = offset - entry.sliceOffset;
      slotsOffsets[p] = slotsOffset - entry.sliceOffset;
      entry.capacities[p] = capacity;
      entry.slotCounts[p] = (int) slotCount;
      entry.maxProbes[p] = maxProbe;
      offset = slotsOffset + slotCount * slotLength + (inline ? inlinePadding(slotLength) : 0);
      totalCapacity += capacity;
    }
    // every partition has more slots than keys
    if (totalCapacity - numPartitions < entry.numKeys) {
      throw new CorruptIndexException(
          "field \""
              + info.name
              + "\": "
              + totalCapacity
              + " slots are too few for "
              + entry.numKeys
              + " keys",
          meta);
    }
    if (inline) {
      entry.recordsOffsets = slotsOffsets;
    } else {
      entry.slotsOffsets = slotsOffsets;
    }
    return offset;
  }

  /**
   * Checks that the smallest or the largest term of an inline field, which is one of its keys, has
   * the key length that the meta data gives. Otherwise records would be read with another stride
   * than they were written with.
   */
  private static void checkKeyLength(FieldEntry entry, BytesRef term, IndexInput data)
      throws CorruptIndexException {
    if (term.length != entry.keyLength) {
      throw new CorruptIndexException(
          "field \""
              + entry.field
              + "\": invalid key length "
              + entry.keyLength
              + ", the field has a term of "
              + term.length
              + " bytes",
          data);
    }
  }

  /**
   * Reads the filter entry of a field whose slots end at {@code slotsEnd}, and returns where its
   * filter ends, or {@code slotsEnd} if it has none. Its offset and number of words follow from the
   * end of the slots, the number of keys and the number of bits per key, and must be those: a
   * filter read with another number of words would put keys in other words than the writer did, and
   * rule out keys that are there.
   */
  private static long readFilter(
      ChecksumIndexInput meta, FieldInfo info, FieldEntry entry, long slotsEnd) throws IOException {
    final int bitsPerKey = meta.readByte();
    if (bitsPerKey == 0) {
      return slotsEnd;
    }
    if (bitsPerKey < MIN_FILTER_BITS_PER_KEY || bitsPerKey > MAX_FILTER_BITS_PER_KEY) {
      throw new CorruptIndexException(
          "field \"" + info.name + "\": invalid filter bitsPerKey=" + bitsPerKey, meta);
    }
    final long offset = meta.readVLong();
    final int numWords = meta.readVInt();
    final long expectedOffset = IndexOutput.alignOffset(slotsEnd, CACHE_LINE_BYTES);
    // numKeys is at most MAX_RECORDS_LENGTH / 5, or less than the slots of 2^10 partitions of
    // fewer than 2^31 slots each, so this does not overflow
    final long expectedWords = BlockedBloomFilter.numWords(entry.numKeys, bitsPerKey);
    if (offset != expectedOffset
        || numWords != expectedWords
        || numWords > BlockedBloomFilter.MAX_WORDS) {
      throw new CorruptIndexException(
          "field \""
              + info.name
              + "\": invalid filter offset="
              + offset
              + " words="
              + numWords
              + ", expected offset="
              + expectedOffset
              + " words="
              + expectedWords
              + " for "
              + entry.numKeys
              + " keys at "
              + bitsPerKey
              + " bits per key",
          meta);
    }
    entry.filterOffset = offset - entry.sliceOffset;
    entry.filterWords = numWords;
    return offset + (long) numWords * Long.BYTES;
  }

  @Override
  public Iterator<String> iterator() {
    return delegate.iterator();
  }

  @Override
  public Terms terms(String field) {
    HashTerms terms = tables.get(field);
    return terms != null ? terms : delegate.terms(field);
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @Override
  public void checkIntegrity(MergePolicy.OneMerge merge) throws IOException {
    delegate.checkIntegrity(merge);
    CodecUtil.checksumEntireFile(data, merge);
  }

  /**
   * Returns a producer of the delegate's merge instance, without tables: merges only iterate terms
   * and postings, which the tables do not speed up. Its {@link #checkIntegrity} still verifies the
   * data file.
   */
  @Override
  public FieldsProducer getMergeInstance() {
    return new HashFieldsProducer(delegate.getMergeInstance(), data);
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(data, delegate);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()
        + "(fieldsWithTable="
        + tables.size()
        + ", delegate="
        + delegate
        + ")";
  }

  /**
   * The meta data of one field. Offsets of control bytes, slots, inline records and the filter are
   * relative to {@link #sliceOffset}.
   */
  static final class FieldEntry {
    final String field;
    long numKeys;

    /**
     * The length of every key if the keys are inline, else 0: the records are in term order, and
     * {@link #slotsOffsets} gives where the slots are.
     */
    int keyLength;

    /** Where the field's slice of the data file starts: its records, or its first control bytes. */
    long sliceOffset;

    int partitionBits;
    long[] controlOffsets;

    /** Where the slot words of every partition start, or null if the keys are inline. */
    long[] slotsOffsets;

    /** Where the inline records of every partition start, or null if they are in term order. */
    long[] recordsOffsets;

    int[] capacities;
    int[] slotCounts;
    int[] maxProbes;

    /** Where the filter starts, and its number of words; 0 if the field has no filter. */
    long filterOffset;

    int filterWords;

    /**
     * The length of the field's slice of the data file: its records, control bytes, slots and
     * filter, or its control bytes, inline records and filter.
     */
    long sliceLength;

    FieldEntry(String field) {
      this.field = field;
    }

    void validateRanges(long dataEnd, IndexInput data) throws CorruptIndexException {
      if (sliceOffset + sliceLength > dataEnd) {
        throw new CorruptIndexException(
            "field \""
                + field
                + "\": records, control bytes, slots and filter ["
                + sliceOffset
                + ", "
                + (sliceOffset + sliceLength)
                + ") exceed the data, which ends at "
                + dataEnd,
            data);
      }
    }
  }
}
