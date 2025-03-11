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
package org.apache.lucene.codecs.lucene90;

import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.SKIP_INDEX_JUMP_LENGTH_PER_LEVEL;
import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.SKIP_INDEX_MAX_LEVEL;
import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.TERMS_DICT_BLOCK_LZ4_SHIFT;

import java.io.IOException;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.internal.hppc.IntObjectHashMap;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/** reader for {@link Lucene90DocValuesFormat} */
final class Lucene90DocValuesProducer extends DocValuesProducer {
  private final IntObjectHashMap<NumericDocValuesProducer> numerics;
  private final IntObjectHashMap<BinaryDocValuesProducer> binaries;
  private final IntObjectHashMap<SortedDocValuesProducer> sorted;
  private final IntObjectHashMap<SortedSetDocValuesProducer> sortedSets;
  private final IntObjectHashMap<SortedNumericDocValuesProducer> sortedNumerics;
  private final IntObjectHashMap<DocValuesSkipperEntry> skippers;
  private final IndexInput data;
  private final int maxDoc;
  private int version = -1;
  private final boolean merging;

  /** expert: instantiates a new reader */
  Lucene90DocValuesProducer(
      SegmentReadState state,
      String dataCodec,
      String dataExtension,
      String metaCodec,
      String metaExtension)
      throws IOException {
    String metaName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);
    this.maxDoc = state.segmentInfo.maxDoc();
    numerics = new IntObjectHashMap<>();
    binaries = new IntObjectHashMap<>();
    sorted = new IntObjectHashMap<>();
    sortedSets = new IntObjectHashMap<>();
    sortedNumerics = new IntObjectHashMap<>();
    skippers = new IntObjectHashMap<>();
    merging = false;

    // read in the entries from the metadata file.
    try (ChecksumIndexInput in = state.directory.openChecksumInput(metaName)) {
      Throwable priorE = null;

      try {
        version =
            CodecUtil.checkIndexHeader(
                in,
                metaCodec,
                Lucene90DocValuesFormat.VERSION_START,
                Lucene90DocValuesFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix);

        readFields(in, state.fieldInfos);

      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(in, priorE);
      }
    }

    String dataName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, dataExtension);
    // Doc-values have a forward-only access pattern, so pass ReadAdvice.NORMAL to perform
    // readahead.
    this.data =
        state.directory.openInput(dataName, state.context.withReadAdvice(ReadAdvice.NORMAL));
    boolean success = false;
    try {
      final int version2 =
          CodecUtil.checkIndexHeader(
              data,
              dataCodec,
              Lucene90DocValuesFormat.VERSION_START,
              Lucene90DocValuesFormat.VERSION_CURRENT,
              state.segmentInfo.getId(),
              state.segmentSuffix);
      if (version != version2) {
        throw new CorruptIndexException(
            "Format versions mismatch: meta=" + version + ", data=" + version2, data);
      }

      // NOTE: data file is too costly to verify checksum against all the bytes on open,
      // but for now we at least verify proper structure of the checksum footer: which looks
      // for FOOTER_MAGIC + algorithmID. This is cheap and can detect some forms of corruption
      // such as file truncation.
      CodecUtil.retrieveChecksum(data);

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this.data);
      }
    }
  }

  // Used for cloning
  private Lucene90DocValuesProducer(
      IntObjectHashMap<NumericDocValuesProducer> numerics,
      IntObjectHashMap<BinaryDocValuesProducer> binaries,
      IntObjectHashMap<SortedDocValuesProducer> sorted,
      IntObjectHashMap<SortedSetDocValuesProducer> sortedSets,
      IntObjectHashMap<SortedNumericDocValuesProducer> sortedNumerics,
      IntObjectHashMap<DocValuesSkipperEntry> skippers,
      IndexInput data,
      int maxDoc,
      int version,
      boolean merging) {
    this.numerics = numerics;
    this.binaries = binaries;
    this.sorted = sorted;
    this.sortedSets = sortedSets;
    this.sortedNumerics = sortedNumerics;
    this.skippers = skippers;
    this.data = data.clone();
    this.maxDoc = maxDoc;
    this.version = version;
    this.merging = merging;
  }

  @Override
  public DocValuesProducer getMergeInstance() {
    return new Lucene90DocValuesProducer(
        numerics,
        binaries,
        sorted,
        sortedSets,
        sortedNumerics,
        skippers,
        data,
        maxDoc,
        version,
        true);
  }

  private void readFields(IndexInput meta, FieldInfos infos) throws IOException {
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
      FieldInfo info = infos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      }
      byte type = meta.readByte();
      if (info.docValuesSkipIndexType() != DocValuesSkipIndexType.NONE) {
        skippers.put(info.number, readDocValueSkipperMeta(meta));
      }
      if (type == Lucene90DocValuesFormat.NUMERIC) {
        numerics.put(info.number, readNumeric(meta));
      } else if (type == Lucene90DocValuesFormat.BINARY) {
        binaries.put(info.number, readBinary(meta));
      } else if (type == Lucene90DocValuesFormat.SORTED) {
        sorted.put(info.number, readSorted(meta));
      } else if (type == Lucene90DocValuesFormat.SORTED_SET) {
        sortedSets.put(info.number, readSortedSet(meta));
      } else if (type == Lucene90DocValuesFormat.SORTED_NUMERIC) {
        sortedNumerics.put(info.number, readSortedNumeric(meta));
      } else {
        throw new CorruptIndexException("invalid type: " + type, meta);
      }
    }
  }

  private NumericDocValuesProducer readNumeric(IndexInput meta) throws IOException {
    NumericEntry entry = new NumericEntry();
    readNumeric(meta, entry);
    return NumericDocValuesProducer.create(entry);
  }

  private DocValuesSkipperEntry readDocValueSkipperMeta(IndexInput meta) throws IOException {
    long offset = meta.readLong();
    long length = meta.readLong();
    long maxValue = meta.readLong();
    long minValue = meta.readLong();
    int docCount = meta.readInt();
    int maxDocID = meta.readInt();

    return new DocValuesSkipperEntry(offset, length, minValue, maxValue, docCount, maxDocID);
  }

  private void readNumeric(IndexInput meta, NumericEntry entry) throws IOException {
    entry.docsWithFieldOffset = meta.readLong();
    entry.docsWithFieldLength = meta.readLong();
    entry.jumpTableEntryCount = meta.readShort();
    entry.denseRankPower = meta.readByte();
    entry.numValues = meta.readLong();
    int tableSize = meta.readInt();
    if (tableSize > 256) {
      throw new CorruptIndexException("invalid table size: " + tableSize, meta);
    }
    if (tableSize >= 0) {
      entry.table = new long[tableSize];
      for (int i = 0; i < tableSize; ++i) {
        entry.table[i] = meta.readLong();
      }
    }
    if (tableSize < -1) {
      entry.blockShift = -2 - tableSize;
    } else {
      entry.blockShift = -1;
    }
    entry.bitsPerValue = meta.readByte();
    entry.minValue = meta.readLong();
    entry.gcd = meta.readLong();
    entry.valuesOffset = meta.readLong();
    entry.valuesLength = meta.readLong();
    entry.valueJumpTableOffset = meta.readLong();
  }

  private BinaryDocValuesProducer readBinary(IndexInput meta) throws IOException {
    final BinaryEntry entry = new BinaryEntry();
    entry.dataOffset = meta.readLong();
    entry.dataLength = meta.readLong();
    entry.docsWithFieldOffset = meta.readLong();
    entry.docsWithFieldLength = meta.readLong();
    entry.jumpTableEntryCount = meta.readShort();
    entry.denseRankPower = meta.readByte();
    entry.numDocsWithField = meta.readInt();
    entry.minLength = meta.readInt();
    entry.maxLength = meta.readInt();
    if (entry.minLength < entry.maxLength) {
      entry.addressesOffset = meta.readLong();

      // Old count of uncompressed addresses
      long numAddresses = entry.numDocsWithField + 1L;

      final int blockShift = meta.readVInt();
      entry.addressesMeta = DirectMonotonicReader.loadMeta(meta, numAddresses, blockShift);
      entry.addressesLength = meta.readLong();
    }
    return BinaryDocValuesProducer.create(entry);
  }

  private SortedDocValuesProducer readSorted(IndexInput meta) throws IOException {
    SortedEntry entry = new SortedEntry();
    entry.ordsEntry = new NumericEntry();
    readNumeric(meta, entry.ordsEntry);
    entry.termsDictEntry = new TermsDictEntry();
    readTermDict(meta, entry.termsDictEntry);
    return SortedDocValuesProducer.create(entry);
  }

  private SortedSetDocValuesProducer readSortedSet(IndexInput meta) throws IOException {
    SortedSetEntry entry = new SortedSetEntry();
    byte multiValued = meta.readByte();
    switch (multiValued) {
      case 0: // singlevalued
        entry.singleValueEntry = readSorted(meta);
        return SortedSetDocValuesProducer.create(entry);
      case 1: // multivalued
        break;
      default:
        throw new CorruptIndexException("Invalid multiValued flag: " + multiValued, meta);
    }
    entry.ordsEntry = new SortedNumericEntry();
    readSortedNumeric(meta, entry.ordsEntry);
    entry.termsDictEntry = new TermsDictEntry();
    readTermDict(meta, entry.termsDictEntry);
    return SortedSetDocValuesProducer.create(entry);
  }

  private static void readTermDict(IndexInput meta, TermsDictEntry entry) throws IOException {
    entry.termsDictSize = meta.readVLong();
    final int blockShift = meta.readInt();
    final long addressesSize =
        (entry.termsDictSize + (1L << TERMS_DICT_BLOCK_LZ4_SHIFT) - 1)
            >>> TERMS_DICT_BLOCK_LZ4_SHIFT;
    entry.termsAddressesMeta = DirectMonotonicReader.loadMeta(meta, addressesSize, blockShift);
    entry.maxTermLength = meta.readInt();
    entry.maxBlockLength = meta.readInt();
    entry.termsDataOffset = meta.readLong();
    entry.termsDataLength = meta.readLong();
    entry.termsAddressesOffset = meta.readLong();
    entry.termsAddressesLength = meta.readLong();
    entry.termsDictIndexShift = meta.readInt();
    final long indexSize =
        (entry.termsDictSize + (1L << entry.termsDictIndexShift) - 1) >>> entry.termsDictIndexShift;
    entry.termsIndexAddressesMeta = DirectMonotonicReader.loadMeta(meta, 1 + indexSize, blockShift);
    entry.termsIndexOffset = meta.readLong();
    entry.termsIndexLength = meta.readLong();
    entry.termsIndexAddressesOffset = meta.readLong();
    entry.termsIndexAddressesLength = meta.readLong();
  }

  private SortedNumericDocValuesProducer readSortedNumeric(IndexInput meta) throws IOException {
    SortedNumericEntry entry = new SortedNumericEntry();
    readSortedNumeric(meta, entry);
    return SortedNumericDocValuesProducer.create(entry);
  }

  private SortedNumericEntry readSortedNumeric(IndexInput meta, SortedNumericEntry entry)
      throws IOException {
    readNumeric(meta, entry);
    entry.numDocsWithField = meta.readInt();
    if (entry.numDocsWithField != entry.numValues) {
      entry.addressesOffset = meta.readLong();
      final int blockShift = meta.readVInt();
      entry.addressesMeta =
          DirectMonotonicReader.loadMeta(meta, entry.numDocsWithField + 1, blockShift);
      entry.addressesLength = meta.readLong();
    }
    return entry;
  }

  @Override
  public void close() throws IOException {
    data.close();
  }

  private record DocValuesSkipperEntry(
      long offset, long length, long minValue, long maxValue, int docCount, int maxDocId) {}

  public static class NumericEntry {
    long[] table;
    int blockShift;
    byte bitsPerValue;
    long docsWithFieldOffset;
    long docsWithFieldLength;
    short jumpTableEntryCount;
    byte denseRankPower;
    long numValues;
    long minValue;
    long gcd;
    long valuesOffset;
    long valuesLength;
    long valueJumpTableOffset; // -1 if no jump-table
  }

  public static class BinaryEntry {
    long dataOffset;
    long dataLength;
    long docsWithFieldOffset;
    long docsWithFieldLength;
    short jumpTableEntryCount;
    byte denseRankPower;
    int numDocsWithField;
    int minLength;
    int maxLength;
    long addressesOffset;
    long addressesLength;
    DirectMonotonicReader.Meta addressesMeta;
  }

  public static class TermsDictEntry {
    long termsDictSize;
    DirectMonotonicReader.Meta termsAddressesMeta;
    int maxTermLength;
    long termsDataOffset;
    long termsDataLength;
    long termsAddressesOffset;
    long termsAddressesLength;
    int termsDictIndexShift;
    DirectMonotonicReader.Meta termsIndexAddressesMeta;
    long termsIndexOffset;
    long termsIndexLength;
    long termsIndexAddressesOffset;
    long termsIndexAddressesLength;

    int maxBlockLength;
  }

  public static class SortedEntry {
    NumericEntry ordsEntry;
    TermsDictEntry termsDictEntry;
  }

  public static class SortedSetEntry {
    SortedDocValuesProducer singleValueEntry;
    SortedNumericEntry ordsEntry;
    TermsDictEntry termsDictEntry;
  }

  public static class SortedNumericEntry extends NumericEntry {
    int numDocsWithField;
    DirectMonotonicReader.Meta addressesMeta;
    long addressesOffset;
    long addressesLength;
  }

  @Override
  public NumericDocValues getNumeric(FieldInfo field) throws IOException {
    NumericDocValuesProducer entry = numerics.get(field.number);
    return entry.getNumeric(data, maxDoc, merging);
  }

  @Override
  public BinaryDocValues getBinary(FieldInfo field) throws IOException {
    BinaryDocValuesProducer entry = binaries.get(field.number);
    return entry.getBinary(data, maxDoc, merging);
  }

  @Override
  public SortedDocValues getSorted(FieldInfo field) throws IOException {
    SortedDocValuesProducer entry = sorted.get(field.number);
    return entry.getSorted(data, maxDoc, merging);
  }

  @Override
  public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
    SortedNumericDocValuesProducer entry = sortedNumerics.get(field.number);
    return entry.getSortedNumeric(data, maxDoc, merging);
  }

  @Override
  public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
    SortedSetDocValuesProducer entry = sortedSets.get(field.number);
    return entry.getSortedSet(data, maxDoc, merging);
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(data);
  }

  @Override
  public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
    final DocValuesSkipperEntry entry = skippers.get(field.number);

    final IndexInput input = data.slice("doc value skipper", entry.offset, entry.length);
    // Prefetch the first page of data. Following pages are expected to get prefetched through
    // read-ahead.
    if (input.length() > 0) {
      input.prefetch(0, 1);
    }
    // TODO: should we write to disk the actual max level for this segment?
    return new DocValuesSkipper() {
      final int[] minDocID = new int[SKIP_INDEX_MAX_LEVEL];
      final int[] maxDocID = new int[SKIP_INDEX_MAX_LEVEL];

      {
        for (int i = 0; i < SKIP_INDEX_MAX_LEVEL; i++) {
          minDocID[i] = maxDocID[i] = -1;
        }
      }

      final long[] minValue = new long[SKIP_INDEX_MAX_LEVEL];
      final long[] maxValue = new long[SKIP_INDEX_MAX_LEVEL];
      final int[] docCount = new int[SKIP_INDEX_MAX_LEVEL];
      int levels = 1;

      @Override
      public void advance(int target) throws IOException {
        if (target > entry.maxDocId) {
          // skipper is exhausted
          for (int i = 0; i < SKIP_INDEX_MAX_LEVEL; i++) {
            minDocID[i] = maxDocID[i] = DocIdSetIterator.NO_MORE_DOCS;
          }
        } else {
          // find next interval
          assert target > maxDocID[0] : "target must be bigger that current interval";
          while (true) {
            levels = input.readByte();
            assert levels <= SKIP_INDEX_MAX_LEVEL && levels > 0
                : "level out of range [" + levels + "]";
            boolean valid = true;
            // check if current interval is competitive or we can jump to the next position
            for (int level = levels - 1; level >= 0; level--) {
              if ((maxDocID[level] = input.readInt()) < target) {
                input.skipBytes(SKIP_INDEX_JUMP_LENGTH_PER_LEVEL[level]); // the jump for the level
                valid = false;
                break;
              }
              minDocID[level] = input.readInt();
              maxValue[level] = input.readLong();
              minValue[level] = input.readLong();
              docCount[level] = input.readInt();
            }
            if (valid) {
              // adjust levels
              while (levels < SKIP_INDEX_MAX_LEVEL && maxDocID[levels] >= target) {
                levels++;
              }
              break;
            }
          }
        }
      }

      @Override
      public int numLevels() {
        return levels;
      }

      @Override
      public int minDocID(int level) {
        return minDocID[level];
      }

      @Override
      public int maxDocID(int level) {
        return maxDocID[level];
      }

      @Override
      public long minValue(int level) {
        return minValue[level];
      }

      @Override
      public long maxValue(int level) {
        return maxValue[level];
      }

      @Override
      public int docCount(int level) {
        return docCount[level];
      }

      @Override
      public long minValue() {
        return entry.minValue;
      }

      @Override
      public long maxValue() {
        return entry.maxValue;
      }

      @Override
      public int docCount() {
        return entry.docCount;
      }
    };
  }
}
