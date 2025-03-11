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

import java.io.IOException;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/** Reads binary doc values values for {@link Lucene90DocValuesFormat} */
abstract class BinaryDocValuesProducer {

  public abstract BinaryDocValues getBinary(IndexInput data, int maxDoc, boolean merging)
      throws IOException;

  private static final BinaryDocValuesProducer EMPTY =
      new BinaryDocValuesProducer() {
        @Override
        public BinaryDocValues getBinary(IndexInput data, int maxDoc, boolean merging) {
          return DocValues.emptyBinary();
        }
      };

  public static BinaryDocValuesProducer create(Lucene90DocValuesProducer.BinaryEntry entry)
      throws IOException {
    if (entry.minLength < entry.maxLength) {
      if (entry.docsWithFieldOffset == -2) {
        return EMPTY;
      } else if (entry.docsWithFieldOffset == -1) {
        return new DenseVariableLengthBinaryDocValuesProducer(entry);
      } else {
        return new SparseVariableLengthBinaryDocValuesProducer(entry);
      }
    } else {
      if (entry.docsWithFieldOffset == -2) {
        return EMPTY;
      } else if (entry.docsWithFieldOffset == -1) {
        return new DenseFixedLengthBinaryDocValuesProducer(entry);
      } else {
        return new SparseFixedLengthBinaryDocValuesProducer(entry);
      }
    }
  }

  private abstract static class BaseBinaryDocValuesProducer extends BinaryDocValuesProducer {
    private final long dataOffset;
    private final long dataLength;
    protected final int maxLength;

    protected BaseBinaryDocValuesProducer(long dataOffset, long dataLength, int maxLength) {
      this.dataOffset = dataOffset;
      this.dataLength = dataLength;
      this.maxLength = maxLength;
    }

    public RandomAccessInput getByteSlice(IndexInput data) throws IOException {
      final RandomAccessInput bytesSlice = data.randomAccessSlice(dataOffset, dataLength);
      // Prefetch the first page of data. Following pages are expected to get prefetched through
      // read-ahead.
      if (bytesSlice.length() > 0) {
        bytesSlice.prefetch(0, 1);
      }
      return bytesSlice;
    }
  }

  private abstract static class VariableLengthBinaryDocValuesProducer
      extends BaseBinaryDocValuesProducer {
    private final long docsWithFieldOffset;
    private final long docsWithFieldLength;
    private final short jumpTableEntryCount;
    private final byte denseRankPower;
    private final int numDocsWithField;

    protected VariableLengthBinaryDocValuesProducer(Lucene90DocValuesProducer.BinaryEntry entry) {
      super(entry.dataOffset, entry.dataLength, entry.maxLength);
      this.docsWithFieldOffset = entry.docsWithFieldOffset;
      this.docsWithFieldLength = entry.docsWithFieldLength;
      this.jumpTableEntryCount = entry.jumpTableEntryCount;
      this.denseRankPower = entry.denseRankPower;
      this.numDocsWithField = entry.numDocsWithField;
    }

    public IndexedDISI getIndexedDISI(IndexInput data) throws IOException {
      return new IndexedDISI(
          data,
          docsWithFieldOffset,
          docsWithFieldLength,
          jumpTableEntryCount,
          denseRankPower,
          numDocsWithField);
    }
  }

  private static class DenseFixedLengthBinaryDocValuesProducer extends BaseBinaryDocValuesProducer {

    protected DenseFixedLengthBinaryDocValuesProducer(Lucene90DocValuesProducer.BinaryEntry entry) {
      super(entry.dataOffset, entry.dataLength, entry.maxLength);
    }

    @Override
    public BinaryDocValues getBinary(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      RandomAccessInput bytesSlice = getByteSlice(data);
      final int length = maxLength;
      return new DenseBinaryDocValues(maxDoc) {
        final BytesRef bytes = new BytesRef(new byte[length], 0, length);

        @Override
        public BytesRef binaryValue() throws IOException {
          bytesSlice.readBytes((long) doc * length, bytes.bytes, 0, length);
          return bytes;
        }
      };
    }
  }

  private static class DenseVariableLengthBinaryDocValuesProducer
      extends BaseBinaryDocValuesProducer {
    final long addressesOffset;
    final long addressesLength;
    final DirectMonotonicReader.Meta addressesMeta;

    protected DenseVariableLengthBinaryDocValuesProducer(
        Lucene90DocValuesProducer.BinaryEntry entry) {
      super(entry.dataOffset, entry.dataLength, entry.maxLength);
      this.addressesOffset = entry.addressesOffset;
      this.addressesLength = entry.addressesLength;
      this.addressesMeta = entry.addressesMeta;
    }

    @Override
    public BinaryDocValues getBinary(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      final RandomAccessInput bytesSlice = getByteSlice(data);
      // variable length
      final RandomAccessInput addressesData =
          data.randomAccessSlice(addressesOffset, addressesLength);
      // Prefetch the first page of data. Following pages are expected to get prefetched through
      // read-ahead.
      if (addressesData.length() > 0) {
        addressesData.prefetch(0, 1);
      }
      final LongValues addresses =
          DirectMonotonicReader.getInstance(addressesMeta, addressesData, merging);
      return new DenseBinaryDocValues(maxDoc) {
        final BytesRef bytes = new BytesRef(new byte[maxLength], 0, maxLength);

        @Override
        public BytesRef binaryValue() throws IOException {
          long startOffset = addresses.get(doc);
          bytes.length = (int) (addresses.get(doc + 1L) - startOffset);
          bytesSlice.readBytes(startOffset, bytes.bytes, 0, bytes.length);
          return bytes;
        }
      };
    }
  }

  private static class SparseFixedLengthBinaryDocValuesProducer
      extends VariableLengthBinaryDocValuesProducer {

    protected SparseFixedLengthBinaryDocValuesProducer(
        Lucene90DocValuesProducer.BinaryEntry entry) {
      super(entry);
    }

    @Override
    public BinaryDocValues getBinary(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      final RandomAccessInput bytesSlice = getByteSlice(data);
      final IndexedDISI disi = getIndexedDISI(data);
      final int length = maxLength;
      return new SparseBinaryDocValues(disi) {
        final BytesRef bytes = new BytesRef(new byte[length], 0, length);

        @Override
        public BytesRef binaryValue() throws IOException {
          bytesSlice.readBytes((long) disi.index() * length, bytes.bytes, 0, length);
          return bytes;
        }
      };
    }
  }

  private static class SparseVariableLengthBinaryDocValuesProducer
      extends VariableLengthBinaryDocValuesProducer {

    private final long addressesOffset;
    private final long addressesLength;
    private final DirectMonotonicReader.Meta addressesMeta;

    protected SparseVariableLengthBinaryDocValuesProducer(
        Lucene90DocValuesProducer.BinaryEntry entry) {
      super(entry);
      this.addressesOffset = entry.addressesOffset;
      this.addressesLength = entry.addressesLength;
      this.addressesMeta = entry.addressesMeta;
    }

    @Override
    public BinaryDocValues getBinary(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      final RandomAccessInput bytesSlice = getByteSlice(data);
      final IndexedDISI disi = getIndexedDISI(data);
      final RandomAccessInput addressesData =
          data.randomAccessSlice(addressesOffset, addressesLength);
      // Prefetch the first page of data. Following pages are expected to get prefetched through
      // read-ahead.
      if (addressesData.length() > 0) {
        addressesData.prefetch(0, 1);
      }
      final LongValues addresses = DirectMonotonicReader.getInstance(addressesMeta, addressesData);

      return new SparseBinaryDocValues(disi) {
        final BytesRef bytes = new BytesRef(new byte[maxLength], 0, maxLength);

        @Override
        public BytesRef binaryValue() throws IOException {
          final int index = disi.index();
          long startOffset = addresses.get(index);
          bytes.length = (int) (addresses.get(index + 1L) - startOffset);
          bytesSlice.readBytes(startOffset, bytes.bytes, 0, bytes.length);
          return bytes;
        }
      };
    }
  }

  private abstract static class DenseBinaryDocValues extends BinaryDocValues {

    final int maxDoc;
    int doc = -1;

    DenseBinaryDocValues(int maxDoc) {
      this.maxDoc = maxDoc;
    }

    @Override
    public int nextDoc() {
      return advance(doc + 1);
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public long cost() {
      return maxDoc;
    }

    @Override
    public int advance(int target) {
      if (target >= maxDoc) {
        return doc = NO_MORE_DOCS;
      }
      return doc = target;
    }

    @Override
    public boolean advanceExact(int target) {
      doc = target;
      return true;
    }
  }

  private abstract static class SparseBinaryDocValues extends BinaryDocValues {

    private final IndexedDISI disi;

    SparseBinaryDocValues(IndexedDISI disi) {
      this.disi = disi;
    }

    @Override
    public int nextDoc() throws IOException {
      return disi.nextDoc();
    }

    @Override
    public int docID() {
      return disi.docID();
    }

    @Override
    public long cost() {
      return disi.cost();
    }

    @Override
    public int advance(int target) throws IOException {
      return disi.advance(target);
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      return disi.advanceExact(target);
    }
  }
}
