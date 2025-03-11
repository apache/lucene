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

import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.TERMS_DICT_BLOCK_LZ4_SHIFT;

import java.io.IOException;
import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.compress.LZ4;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/** Reads sorted doc values values for {@link Lucene90DocValuesFormat} */
abstract class SortedDocValuesProducer {

  public abstract SortedDocValues getSorted(IndexInput data, int maxDoc, boolean merging)
      throws IOException;

  public static SortedDocValuesProducer create(Lucene90DocValuesProducer.SortedEntry sortedEntry)
      throws IOException {
    Lucene90DocValuesProducer.NumericEntry numericEntry = sortedEntry.ordsEntry;
    Lucene90DocValuesProducer.TermsDictEntry termsDictEntry = sortedEntry.termsDictEntry;
    // Specialize the common case for ordinals: single block of packed integers.
    // final Lucene90DocValuesProducer.NumericEntry ordsEntry = entry.ordsEntry;
    if (numericEntry.blockShift < 0 // single block
        && numericEntry.bitsPerValue > 0) { // more than 1 value
      if (numericEntry.gcd != 1 || numericEntry.minValue != 0 || numericEntry.table != null) {
        throw new IllegalStateException("Ordinals shouldn't use GCD, offset or table compression");
      }
      if (numericEntry.docsWithFieldOffset == -1) { // dense
        return new DenseSingleBlockSortedDocValuesProducer(numericEntry, termsDictEntry);
      } else if (numericEntry.docsWithFieldOffset >= 0) { // sparse but non-empty
        return new SparseSingleBlockSortedDocValuesProducer(numericEntry, termsDictEntry);
      }
    }
    return new OrdsSortedDocValuesProducer(numericEntry, termsDictEntry);
  }

  private static class DenseSingleBlockSortedDocValuesProducer extends SortedDocValuesProducer {

    private final long valuesOffset;
    private final long valuesLength;
    private final byte bitsPerValue;
    private final long numValues;
    private final Lucene90DocValuesProducer.TermsDictEntry termsDictEntry;

    private DenseSingleBlockSortedDocValuesProducer(
        Lucene90DocValuesProducer.NumericEntry numericEntry,
        Lucene90DocValuesProducer.TermsDictEntry termsDictEntry) {
      this.valuesOffset = numericEntry.valuesOffset;
      this.valuesLength = numericEntry.valuesLength;
      this.bitsPerValue = numericEntry.bitsPerValue;
      this.numValues = numericEntry.numValues;
      this.termsDictEntry = termsDictEntry;
    }

    @Override
    public SortedDocValues getSorted(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      final RandomAccessInput slice = data.randomAccessSlice(valuesOffset, valuesLength);
      // Prefetch the first page of data. Following pages are expected to get prefetched through
      // read-ahead.
      if (slice.length() > 0) {
        slice.prefetch(0, 1);
      }
      final LongValues values =
          NumericDocValuesProducer.getDirectReaderInstance(
              slice, bitsPerValue, 0L, numValues, merging);

      return new BaseSortedDocValues(termsDictEntry, data, merging) {

        private int doc = -1;

        @Override
        public int ordValue() {
          return (int) values.get(doc);
        }

        @Override
        public boolean advanceExact(int target) {
          doc = target;
          return true;
        }

        @Override
        public int docID() {
          return doc;
        }

        @Override
        public int nextDoc() {
          return advance(doc + 1);
        }

        @Override
        public int advance(int target) {
          if (target >= maxDoc) {
            return doc = NO_MORE_DOCS;
          }
          return doc = target;
        }

        @Override
        public long cost() {
          return maxDoc;
        }
      };
    }
  }

  private static class SparseSingleBlockSortedDocValuesProducer extends SortedDocValuesProducer {

    private final long valuesOffset;
    private final long valuesLength;
    private final byte bitsPerValue;
    private final long numValues;
    private final long docsWithFieldOffset;
    private final long docsWithFieldLength;
    private final int jumpTableEntryCount;
    private final byte denseRankPower;
    private final Lucene90DocValuesProducer.TermsDictEntry termsDictEntry;

    private SparseSingleBlockSortedDocValuesProducer(
        Lucene90DocValuesProducer.NumericEntry numericEntry,
        Lucene90DocValuesProducer.TermsDictEntry termsDictEntry) {
      this.valuesOffset = numericEntry.valuesOffset;
      this.valuesLength = numericEntry.valuesLength;
      this.bitsPerValue = numericEntry.bitsPerValue;
      this.numValues = numericEntry.numValues;
      this.docsWithFieldOffset = numericEntry.docsWithFieldOffset;
      this.docsWithFieldLength = numericEntry.docsWithFieldLength;
      this.jumpTableEntryCount = numericEntry.jumpTableEntryCount;
      this.denseRankPower = numericEntry.denseRankPower;
      this.termsDictEntry = termsDictEntry;
    }

    @Override
    public SortedDocValues getSorted(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      final RandomAccessInput slice = data.randomAccessSlice(valuesOffset, valuesLength);
      // Prefetch the first page of data. Following pages are expected to get prefetched through
      // read-ahead.
      if (slice.length() > 0) {
        slice.prefetch(0, 1);
      }
      final LongValues values =
          NumericDocValuesProducer.getDirectReaderInstance(
              slice, bitsPerValue, 0L, numValues, merging);

      final IndexedDISI disi =
          new IndexedDISI(
              data,
              docsWithFieldOffset,
              docsWithFieldLength,
              jumpTableEntryCount,
              denseRankPower,
              numValues);

      return new BaseSortedDocValues(termsDictEntry, data, merging) {
        @Override
        public int ordValue() {
          return (int) values.get(disi.index());
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          return disi.advanceExact(target);
        }

        @Override
        public int docID() {
          return disi.docID();
        }

        @Override
        public int nextDoc() throws IOException {
          return disi.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
          return disi.advance(target);
        }

        @Override
        public long cost() {
          return disi.cost();
        }
      };
    }
  }

  private static class OrdsSortedDocValuesProducer extends SortedDocValuesProducer {

    private final NumericDocValuesProducer numeric;
    private final Lucene90DocValuesProducer.TermsDictEntry entry;

    private OrdsSortedDocValuesProducer(
        Lucene90DocValuesProducer.NumericEntry numericEntry,
        Lucene90DocValuesProducer.TermsDictEntry termsDictEntry) {
      this.numeric = NumericDocValuesProducer.create(numericEntry);
      this.entry = termsDictEntry;
    }

    @Override
    public SortedDocValues getSorted(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      final NumericDocValues ords = numeric.getNumeric(data, maxDoc, merging);
      return new BaseSortedDocValues(entry, data, merging) {

        @Override
        public int ordValue() throws IOException {
          return (int) ords.longValue();
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          return ords.advanceExact(target);
        }

        @Override
        public int docID() {
          return ords.docID();
        }

        @Override
        public int nextDoc() throws IOException {
          return ords.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
          return ords.advance(target);
        }

        @Override
        public long cost() {
          return ords.cost();
        }
      };
    }
  }

  private abstract static class BaseSortedDocValues extends SortedDocValues {

    final IndexInput data;
    final Lucene90DocValuesProducer.TermsDictEntry entry;
    final TermsEnum termsEnum;
    final boolean merging;

    BaseSortedDocValues(
        Lucene90DocValuesProducer.TermsDictEntry entry, IndexInput data, boolean merging)
        throws IOException {
      this.entry = entry;
      this.data = data;
      this.merging = merging;
      this.termsEnum = termsEnum();
    }

    @Override
    public int getValueCount() {
      return Math.toIntExact(entry.termsDictSize);
    }

    @Override
    public BytesRef lookupOrd(int ord) throws IOException {
      termsEnum.seekExact(ord);
      return termsEnum.term();
    }

    @Override
    public int lookupTerm(BytesRef key) throws IOException {
      TermsEnum.SeekStatus status = termsEnum.seekCeil(key);
      switch (status) {
        case FOUND:
          return Math.toIntExact(termsEnum.ord());
        case NOT_FOUND:
        case END:
        default:
          return Math.toIntExact(-1L - termsEnum.ord());
      }
    }

    @Override
    public TermsEnum termsEnum() throws IOException {
      return new TermsDict(entry, data, merging);
    }
  }

  static class TermsDict extends BaseTermsEnum {
    static final int LZ4_DECOMPRESSOR_PADDING = 7;

    final Lucene90DocValuesProducer.TermsDictEntry entry;
    final LongValues blockAddresses;
    final IndexInput bytes;
    final long blockMask;
    final LongValues indexAddresses;
    final RandomAccessInput indexBytes;
    final BytesRef term;
    final BytesRef blockBuffer;
    final ByteArrayDataInput blockInput;
    long ord = -1;
    long currentCompressedBlockStart = -1;
    long currentCompressedBlockEnd = -1;

    TermsDict(Lucene90DocValuesProducer.TermsDictEntry entry, IndexInput data, boolean merging)
        throws IOException {
      this.entry = entry;
      RandomAccessInput addressesSlice =
          data.randomAccessSlice(entry.termsAddressesOffset, entry.termsAddressesLength);
      blockAddresses =
          DirectMonotonicReader.getInstance(entry.termsAddressesMeta, addressesSlice, merging);
      bytes = data.slice("terms", entry.termsDataOffset, entry.termsDataLength);
      blockMask = (1L << TERMS_DICT_BLOCK_LZ4_SHIFT) - 1;
      RandomAccessInput indexAddressesSlice =
          data.randomAccessSlice(entry.termsIndexAddressesOffset, entry.termsIndexAddressesLength);
      indexAddresses =
          DirectMonotonicReader.getInstance(
              entry.termsIndexAddressesMeta, indexAddressesSlice, merging);
      indexBytes = data.randomAccessSlice(entry.termsIndexOffset, entry.termsIndexLength);
      term = new BytesRef(entry.maxTermLength);

      // add the max term length for the dictionary
      // add 7 padding bytes can help decompression run faster.
      int bufferSize = entry.maxBlockLength + entry.maxTermLength + LZ4_DECOMPRESSOR_PADDING;
      blockBuffer = new BytesRef(new byte[bufferSize], 0, bufferSize);
      blockInput = new ByteArrayDataInput();
    }

    @Override
    public BytesRef next() throws IOException {
      if (++ord >= entry.termsDictSize) {
        return null;
      }

      if ((ord & blockMask) == 0L) {
        decompressBlock();
      } else {
        DataInput input = blockInput;
        final int token = Byte.toUnsignedInt(input.readByte());
        int prefixLength = token & 0x0F;
        int suffixLength = 1 + (token >>> 4);
        if (prefixLength == 15) {
          prefixLength += input.readVInt();
        }
        if (suffixLength == 16) {
          suffixLength += input.readVInt();
        }
        term.length = prefixLength + suffixLength;
        input.readBytes(term.bytes, prefixLength, suffixLength);
      }
      return term;
    }

    @Override
    public void seekExact(long ord) throws IOException {
      if (ord < 0 || ord >= entry.termsDictSize) {
        throw new IndexOutOfBoundsException();
      }
      // Signed shift since ord is -1 when the terms enum is not positioned
      final long currentBlockIndex = this.ord >> TERMS_DICT_BLOCK_LZ4_SHIFT;
      final long blockIndex = ord >> TERMS_DICT_BLOCK_LZ4_SHIFT;
      if (ord < this.ord || blockIndex != currentBlockIndex) {
        // The looked up ord is before the current ord or belongs to a different block, seek again
        final long blockAddress = blockAddresses.get(blockIndex);
        bytes.seek(blockAddress);
        this.ord = (blockIndex << TERMS_DICT_BLOCK_LZ4_SHIFT) - 1;
      }
      // Scan to the looked up ord
      while (this.ord < ord) {
        next();
      }
    }

    private BytesRef getTermFromIndex(long index) throws IOException {
      assert index >= 0 && index <= (entry.termsDictSize - 1) >>> entry.termsDictIndexShift;
      final long start = indexAddresses.get(index);
      term.length = (int) (indexAddresses.get(index + 1) - start);
      indexBytes.readBytes(start, term.bytes, 0, term.length);
      return term;
    }

    private long seekTermsIndex(BytesRef text) throws IOException {
      long lo = 0L;
      long hi = (entry.termsDictSize - 1) >> entry.termsDictIndexShift;
      while (lo <= hi) {
        final long mid = (lo + hi) >>> 1;
        getTermFromIndex(mid);
        final int cmp = term.compareTo(text);
        if (cmp <= 0) {
          lo = mid + 1;
        } else {
          hi = mid - 1;
        }
      }

      assert hi < 0 || getTermFromIndex(hi).compareTo(text) <= 0;
      assert hi == ((entry.termsDictSize - 1) >> entry.termsDictIndexShift)
          || getTermFromIndex(hi + 1).compareTo(text) > 0;
      assert hi < 0 ^ entry.termsDictSize > 0; // return -1 iff empty term dict

      return hi;
    }

    private BytesRef getFirstTermFromBlock(long block) throws IOException {
      assert block >= 0 && block <= (entry.termsDictSize - 1) >>> TERMS_DICT_BLOCK_LZ4_SHIFT;
      final long blockAddress = blockAddresses.get(block);
      bytes.seek(blockAddress);
      term.length = bytes.readVInt();
      bytes.readBytes(term.bytes, 0, term.length);
      return term;
    }

    private long seekBlock(BytesRef text) throws IOException {
      long index = seekTermsIndex(text);
      if (index == -1L) {
        // empty terms dict
        this.ord = 0;
        return -2L;
      }

      long ordLo = index << entry.termsDictIndexShift;
      long ordHi = Math.min(entry.termsDictSize, ordLo + (1L << entry.termsDictIndexShift)) - 1L;

      long blockLo = ordLo >>> TERMS_DICT_BLOCK_LZ4_SHIFT;
      long blockHi = ordHi >>> TERMS_DICT_BLOCK_LZ4_SHIFT;

      while (blockLo <= blockHi) {
        final long blockMid = (blockLo + blockHi) >>> 1;
        getFirstTermFromBlock(blockMid);
        final int cmp = term.compareTo(text);
        if (cmp <= 0) {
          blockLo = blockMid + 1;
        } else {
          blockHi = blockMid - 1;
        }
      }

      assert blockHi < 0 || getFirstTermFromBlock(blockHi).compareTo(text) <= 0;
      assert blockHi == ((entry.termsDictSize - 1) >>> TERMS_DICT_BLOCK_LZ4_SHIFT)
          || getFirstTermFromBlock(blockHi + 1).compareTo(text) > 0;

      // read the block only if term dict is not empty
      assert entry.termsDictSize > 0;
      // reset ord and bytes to the ceiling block even if
      // text is before the first term (blockHi == -1)
      final long block = Math.max(blockHi, 0);
      final long blockAddress = blockAddresses.get(block);
      this.ord = block << TERMS_DICT_BLOCK_LZ4_SHIFT;
      bytes.seek(blockAddress);
      decompressBlock();

      return blockHi;
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
      final long block = seekBlock(text);
      if (block == -2) {
        // empty terms dict
        assert entry.termsDictSize == 0;
        return SeekStatus.END;
      } else if (block == -1) {
        // before the first term
        return SeekStatus.NOT_FOUND;
      }

      while (true) {
        int cmp = term.compareTo(text);
        if (cmp == 0) {
          return SeekStatus.FOUND;
        } else if (cmp > 0) {
          return SeekStatus.NOT_FOUND;
        }
        if (next() == null) {
          return SeekStatus.END;
        }
      }
    }

    private void decompressBlock() throws IOException {
      // The first term is kept uncompressed, so no need to decompress block if only
      // look up the first term when doing seek block.
      term.length = bytes.readVInt();
      bytes.readBytes(term.bytes, 0, term.length);
      long offset = bytes.getFilePointer();
      if (offset < entry.termsDataLength - 1) {
        // Avoid decompress again if we are reading a same block.
        if (currentCompressedBlockStart != offset) {
          blockBuffer.offset = term.length;
          blockBuffer.length = bytes.readVInt();
          // Decompress the remaining of current block, using the first term as a dictionary
          System.arraycopy(term.bytes, 0, blockBuffer.bytes, 0, blockBuffer.offset);
          LZ4.decompress(bytes, blockBuffer.length, blockBuffer.bytes, blockBuffer.offset);
          currentCompressedBlockStart = offset;
          currentCompressedBlockEnd = bytes.getFilePointer();
        } else {
          // Skip decompression but need to re-seek to block end.
          bytes.seek(currentCompressedBlockEnd);
        }

        // Reset the buffer.
        blockInput.reset(blockBuffer.bytes, blockBuffer.offset, blockBuffer.length);
      }
    }

    @Override
    public BytesRef term() throws IOException {
      return term;
    }

    @Override
    public long ord() throws IOException {
      return ord;
    }

    @Override
    public long totalTermFreq() throws IOException {
      return -1L;
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public int docFreq() throws IOException {
      throw new UnsupportedOperationException();
    }
  }
}
