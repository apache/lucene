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

import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.NUMERIC_BLOCK_SHIFT;
import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.NUMERIC_BLOCK_SIZE;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.compress.LZ4;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

/** writer for {@link Lucene90DocValuesFormat} */
final class Lucene90DocValuesConsumer extends DocValuesConsumer {

  IndexOutput data, meta;
  final int maxDoc;
  private byte[] termsDictBuffer;

  /** expert: Creates a new writer */
  public Lucene90DocValuesConsumer(
      SegmentWriteState state,
      String dataCodec,
      String dataExtension,
      String metaCodec,
      String metaExtension)
      throws IOException {
    this.termsDictBuffer = new byte[1 << 14];
    boolean success = false;
    try {
      String dataName =
          IndexFileNames.segmentFileName(
              state.segmentInfo.name, state.segmentSuffix, dataExtension);
      data = state.directory.createOutput(dataName, state.context);
      CodecUtil.writeIndexHeader(
          data,
          dataCodec,
          Lucene90DocValuesFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      String metaName =
          IndexFileNames.segmentFileName(
              state.segmentInfo.name, state.segmentSuffix, metaExtension);
      meta = state.directory.createOutput(metaName, state.context);
      CodecUtil.writeIndexHeader(
          meta,
          metaCodec,
          Lucene90DocValuesFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      maxDoc = state.segmentInfo.maxDoc();
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @Override
  public void close() throws IOException {
    boolean success = false;
    try {
      if (meta != null) {
        meta.writeInt(-1); // write EOF marker
        CodecUtil.writeFooter(meta); // write checksum
      }
      if (data != null) {
        CodecUtil.writeFooter(data); // write checksum
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(data, meta);
      } else {
        IOUtils.closeWhileHandlingException(data, meta);
      }
      meta = data = null;
    }
  }

  @Override
  public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeByte(Lucene90DocValuesFormat.NUMERIC);

    writeValues(
        field,
        new EmptyDocValuesProducer() {
          @Override
          public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
            return DocValues.singleton(valuesProducer.getNumeric(field));
          }
        });
  }

  private long[] writeValues(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    int numDocsWithValue = 0;
    long numValues = 0;

    SortedNumericDocValues values = valuesProducer.getSortedNumeric(field);
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      numDocsWithValue++;
      final int count = values.docValueCount();
      numValues += count;
    }

    if (numDocsWithValue == 0) { // meta[-2, 0]: No documents with values
      meta.writeLong(-2); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else if (numDocsWithValue == maxDoc) { // meta[-1, 0]: All documents have values
      meta.writeLong(-1); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else { // meta[data.offset, data.length]: IndexedDISI structure for documents with values
      long offset = data.getFilePointer();
      meta.writeLong(offset); // docsWithFieldOffset
      values = valuesProducer.getSortedNumeric(field);
      final short jumpTableEntryCount =
          IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
      meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
      meta.writeShort(jumpTableEntryCount);
      meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
    }
    meta.writeLong(numValues);

    if (numValues > 0) {
      meta.writeInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
      final ByteBuffersDataOutput indexOut = new ByteBuffersDataOutput();
      final DirectMonotonicWriter indexWriter =
          DirectMonotonicWriter.getInstance(
              meta,
              new ByteBuffersIndexOutput(indexOut, "temp-dv-index", "temp-dv-index"),
              1L + ((numValues - 1) >>> NUMERIC_BLOCK_SHIFT),
              DIRECT_MONOTONIC_BLOCK_SHIFT);

      final long[] buffer = new long[NUMERIC_BLOCK_SIZE];
      int bufferSize = 0;

      final long valuesDataOffset = data.getFilePointer();

      final DocValuesEncoder encoder = new DocValuesEncoder();
      values = valuesProducer.getSortedNumeric(field);
      for (int doc = values.nextDoc();
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = values.nextDoc()) {
        final int count = values.docValueCount();
        for (int i = 0; i < count; ++i) {
          buffer[bufferSize++] = values.nextValue();
          if (bufferSize == NUMERIC_BLOCK_SIZE) {
            indexWriter.add(data.getFilePointer() - valuesDataOffset);
            encoder.encode(buffer, data);
            bufferSize = 0;
          }
        }
      }
      if (bufferSize > 0) {
        indexWriter.add(data.getFilePointer() - valuesDataOffset);
        // Fill unused slots in the block with zeroes rather than junk
        Arrays.fill(buffer, bufferSize, NUMERIC_BLOCK_SIZE, 0L);
        encoder.encode(buffer, data);
      }

      final long valuesDataLength = data.getFilePointer() - valuesDataOffset;

      indexWriter.finish();
      final long indexDataOffset = data.getFilePointer();
      data.copyBytes(indexOut.toDataInput(), indexOut.size());
      meta.writeLong(indexDataOffset);
      meta.writeLong(data.getFilePointer() - indexDataOffset);

      meta.writeLong(valuesDataOffset);
      meta.writeLong(valuesDataLength);
    }

    return new long[] {numDocsWithValue, numValues};
  }

  @Override
  public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    meta.writeInt(field.number);
    meta.writeByte(Lucene90DocValuesFormat.BINARY);

    BinaryDocValues values = valuesProducer.getBinary(field);
    long start = data.getFilePointer();
    meta.writeLong(start); // dataOffset
    int numDocsWithField = 0;
    int minLength = Integer.MAX_VALUE;
    int maxLength = 0;
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      numDocsWithField++;
      BytesRef v = values.binaryValue();
      int length = v.length;
      data.writeBytes(v.bytes, v.offset, v.length);
      minLength = Math.min(length, minLength);
      maxLength = Math.max(length, maxLength);
    }
    assert numDocsWithField <= maxDoc;
    meta.writeLong(data.getFilePointer() - start); // dataLength

    if (numDocsWithField == 0) {
      meta.writeLong(-2); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else if (numDocsWithField == maxDoc) {
      meta.writeLong(-1); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else {
      long offset = data.getFilePointer();
      meta.writeLong(offset); // docsWithFieldOffset
      values = valuesProducer.getBinary(field);
      final short jumpTableEntryCount =
          IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
      meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
      meta.writeShort(jumpTableEntryCount);
      meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
    }

    meta.writeInt(numDocsWithField);
    meta.writeInt(minLength);
    meta.writeInt(maxLength);
    if (maxLength > minLength) {
      start = data.getFilePointer();
      meta.writeLong(start);
      meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);

      final DirectMonotonicWriter writer =
          DirectMonotonicWriter.getInstance(
              meta, data, numDocsWithField + 1, DIRECT_MONOTONIC_BLOCK_SHIFT);
      long addr = 0;
      writer.add(addr);
      values = valuesProducer.getBinary(field);
      for (int doc = values.nextDoc();
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = values.nextDoc()) {
        addr += values.binaryValue().length;
        writer.add(addr);
      }
      writer.finish();
      meta.writeLong(data.getFilePointer() - start);
    }
  }

  @Override
  public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    meta.writeInt(field.number);
    meta.writeByte(Lucene90DocValuesFormat.SORTED);
    doAddSortedField(field, valuesProducer);
  }

  private void doAddSortedField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    writeValues(
        field,
        new EmptyDocValuesProducer() {
          @Override
          public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
            SortedDocValues sorted = valuesProducer.getSorted(field);
            NumericDocValues sortedOrds =
                new NumericDocValues() {
                  @Override
                  public long longValue() throws IOException {
                    return sorted.ordValue();
                  }

                  @Override
                  public boolean advanceExact(int target) throws IOException {
                    return sorted.advanceExact(target);
                  }

                  @Override
                  public int docID() {
                    return sorted.docID();
                  }

                  @Override
                  public int nextDoc() throws IOException {
                    return sorted.nextDoc();
                  }

                  @Override
                  public int advance(int target) throws IOException {
                    return sorted.advance(target);
                  }

                  @Override
                  public long cost() {
                    return sorted.cost();
                  }
                };
            return DocValues.singleton(sortedOrds);
          }
        });
    addTermsDict(DocValues.singleton(valuesProducer.getSorted(field)));
  }

  private void addTermsDict(SortedSetDocValues values) throws IOException {
    final long size = values.getValueCount();
    meta.writeVLong(size);

    int blockMask = Lucene90DocValuesFormat.TERMS_DICT_BLOCK_LZ4_MASK;
    int shift = Lucene90DocValuesFormat.TERMS_DICT_BLOCK_LZ4_SHIFT;

    meta.writeInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
    ByteBuffersDataOutput addressBuffer = new ByteBuffersDataOutput();
    ByteBuffersIndexOutput addressOutput =
        new ByteBuffersIndexOutput(addressBuffer, "temp", "temp");
    long numBlocks = (size + blockMask) >>> shift;
    DirectMonotonicWriter writer =
        DirectMonotonicWriter.getInstance(
            meta, addressOutput, numBlocks, DIRECT_MONOTONIC_BLOCK_SHIFT);

    BytesRefBuilder previous = new BytesRefBuilder();
    long ord = 0;
    long start = data.getFilePointer();
    int maxLength = 0, maxBlockLength = 0;
    TermsEnum iterator = values.termsEnum();

    LZ4.FastCompressionHashTable ht = new LZ4.FastCompressionHashTable();
    ByteArrayDataOutput bufferedOutput = new ByteArrayDataOutput(termsDictBuffer);

    for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
      if ((ord & blockMask) == 0) {
        if (bufferedOutput.getPosition() > 0) {
          maxBlockLength =
              Math.max(maxBlockLength, compressAndGetTermsDictBlockLength(bufferedOutput, ht));
          bufferedOutput.reset(termsDictBuffer);
        }

        writer.add(data.getFilePointer() - start);
        data.writeVInt(term.length);
        data.writeBytes(term.bytes, term.offset, term.length);
      } else {
        final int prefixLength = StringHelper.bytesDifference(previous.get(), term);
        final int suffixLength = term.length - prefixLength;
        assert suffixLength > 0; // terms are unique
        // Will write (suffixLength + 1 byte + 2 vint) bytes. Grow the buffer in need.
        bufferedOutput = maybeGrowBuffer(bufferedOutput, suffixLength + 11);
        bufferedOutput.writeByte(
            (byte) (Math.min(prefixLength, 15) | (Math.min(15, suffixLength - 1) << 4)));
        if (prefixLength >= 15) {
          bufferedOutput.writeVInt(prefixLength - 15);
        }
        if (suffixLength >= 16) {
          bufferedOutput.writeVInt(suffixLength - 16);
        }
        bufferedOutput.writeBytes(term.bytes, term.offset + prefixLength, suffixLength);
      }
      maxLength = Math.max(maxLength, term.length);
      previous.copyBytes(term);
      ++ord;
    }
    // Compress and write out the last block
    if (bufferedOutput.getPosition() > 0) {
      maxBlockLength =
          Math.max(maxBlockLength, compressAndGetTermsDictBlockLength(bufferedOutput, ht));
    }

    writer.finish();
    meta.writeInt(maxLength);
    // Write one more int for storing max block length.
    meta.writeInt(maxBlockLength);
    meta.writeLong(start);
    meta.writeLong(data.getFilePointer() - start);
    start = data.getFilePointer();
    addressBuffer.copyTo(data);
    meta.writeLong(start);
    meta.writeLong(data.getFilePointer() - start);

    // Now write the reverse terms index
    writeTermsIndex(values);
  }

  private int compressAndGetTermsDictBlockLength(
      ByteArrayDataOutput bufferedOutput, LZ4.FastCompressionHashTable ht) throws IOException {
    int uncompressedLength = bufferedOutput.getPosition();
    data.writeVInt(uncompressedLength);
    long before = data.getFilePointer();
    LZ4.compress(termsDictBuffer, 0, uncompressedLength, data, ht);
    int compressedLength = (int) (data.getFilePointer() - before);
    // Block length will be used for creating buffer for decompression, one corner case is that
    // compressed length might be bigger than un-compressed length, so just return the bigger one.
    return Math.max(uncompressedLength, compressedLength);
  }

  private ByteArrayDataOutput maybeGrowBuffer(ByteArrayDataOutput bufferedOutput, int termLength) {
    int pos = bufferedOutput.getPosition(), originalLength = termsDictBuffer.length;
    if (pos + termLength >= originalLength - 1) {
      termsDictBuffer = ArrayUtil.grow(termsDictBuffer, originalLength + termLength);
      bufferedOutput = new ByteArrayDataOutput(termsDictBuffer, pos, termsDictBuffer.length - pos);
    }
    return bufferedOutput;
  }

  private void writeTermsIndex(SortedSetDocValues values) throws IOException {
    final long size = values.getValueCount();
    meta.writeInt(Lucene90DocValuesFormat.TERMS_DICT_REVERSE_INDEX_SHIFT);
    long start = data.getFilePointer();

    long numBlocks =
        1L
            + ((size + Lucene90DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK)
                >>> Lucene90DocValuesFormat.TERMS_DICT_REVERSE_INDEX_SHIFT);
    ByteBuffersDataOutput addressBuffer = new ByteBuffersDataOutput();
    DirectMonotonicWriter writer;
    try (ByteBuffersIndexOutput addressOutput =
        new ByteBuffersIndexOutput(addressBuffer, "temp", "temp")) {
      writer =
          DirectMonotonicWriter.getInstance(
              meta, addressOutput, numBlocks, DIRECT_MONOTONIC_BLOCK_SHIFT);
      TermsEnum iterator = values.termsEnum();
      BytesRefBuilder previous = new BytesRefBuilder();
      long offset = 0;
      long ord = 0;
      for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
        if ((ord & Lucene90DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK) == 0) {
          writer.add(offset);
          final int sortKeyLength;
          if (ord == 0) {
            // no previous term: no bytes to write
            sortKeyLength = 0;
          } else {
            sortKeyLength = StringHelper.sortKeyLength(previous.get(), term);
          }
          offset += sortKeyLength;
          data.writeBytes(term.bytes, term.offset, sortKeyLength);
        } else if ((ord & Lucene90DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK)
            == Lucene90DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK) {
          previous.copyBytes(term);
        }
        ++ord;
      }
      writer.add(offset);
      writer.finish();
      meta.writeLong(start);
      meta.writeLong(data.getFilePointer() - start);
      start = data.getFilePointer();
      addressBuffer.copyTo(data);
      meta.writeLong(start);
      meta.writeLong(data.getFilePointer() - start);
    }
  }

  @Override
  public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeByte(Lucene90DocValuesFormat.SORTED_NUMERIC);
    doAddSortedNumericField(field, valuesProducer);
  }

  private void doAddSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    long[] stats = writeValues(field, valuesProducer);
    int numDocsWithField = Math.toIntExact(stats[0]);
    long numValues = stats[1];
    assert numValues >= numDocsWithField;

    meta.writeInt(numDocsWithField);
    if (numValues > numDocsWithField) {
      long start = data.getFilePointer();
      meta.writeLong(start);
      meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);

      final DirectMonotonicWriter addressesWriter =
          DirectMonotonicWriter.getInstance(
              meta, data, numDocsWithField + 1L, DIRECT_MONOTONIC_BLOCK_SHIFT);
      long addr = 0;
      addressesWriter.add(addr);
      SortedNumericDocValues values = valuesProducer.getSortedNumeric(field);
      for (int doc = values.nextDoc();
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = values.nextDoc()) {
        addr += values.docValueCount();
        addressesWriter.add(addr);
      }
      addressesWriter.finish();
      meta.writeLong(data.getFilePointer() - start);
    }
  }

  @Override
  public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeByte(Lucene90DocValuesFormat.SORTED_SET);

    SortedSetDocValues values = valuesProducer.getSortedSet(field);
    int numDocsWithField = 0;
    long numOrds = 0;
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      numDocsWithField++;
      for (long ord = values.nextOrd();
          ord != SortedSetDocValues.NO_MORE_ORDS;
          ord = values.nextOrd()) {
        numOrds++;
      }
    }

    if (numDocsWithField == numOrds) {
      meta.writeByte((byte) 0); // multiValued (0 = singleValued)
      doAddSortedField(
          field,
          new EmptyDocValuesProducer() {
            @Override
            public SortedDocValues getSorted(FieldInfo field) throws IOException {
              return SortedSetSelector.wrap(
                  valuesProducer.getSortedSet(field), SortedSetSelector.Type.MIN);
            }
          });
      return;
    }
    meta.writeByte((byte) 1); // multiValued (1 = multiValued)

    doAddSortedNumericField(
        field,
        new EmptyDocValuesProducer() {
          @Override
          public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
            SortedSetDocValues values = valuesProducer.getSortedSet(field);
            return new SortedNumericDocValues() {

              long[] ords = LongsRef.EMPTY_LONGS;
              int i, docValueCount;

              @Override
              public long nextValue() throws IOException {
                return ords[i++];
              }

              @Override
              public int docValueCount() {
                return docValueCount;
              }

              @Override
              public boolean advanceExact(int target) throws IOException {
                throw new UnsupportedOperationException();
              }

              @Override
              public int docID() {
                return values.docID();
              }

              @Override
              public int nextDoc() throws IOException {
                int doc = values.nextDoc();
                if (doc != NO_MORE_DOCS) {
                  docValueCount = 0;
                  for (long ord = values.nextOrd();
                      ord != SortedSetDocValues.NO_MORE_ORDS;
                      ord = values.nextOrd()) {
                    ords = ArrayUtil.grow(ords, docValueCount + 1);
                    ords[docValueCount++] = ord;
                  }
                  i = 0;
                }
                return doc;
              }

              @Override
              public int advance(int target) throws IOException {
                throw new UnsupportedOperationException();
              }

              @Override
              public long cost() {
                return values.cost();
              }
            };
          }
        });

    addTermsDict(values);
  }
}
