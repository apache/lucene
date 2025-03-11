package org.apache.lucene.codecs.lucene90;

import java.io.IOException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectReader;

/** Reads numeric doc values values for {@link Lucene90DocValuesFormat} */
abstract class NumericDocValuesProducer {

  public abstract NumericDocValues getNumeric(IndexInput data, int maxDoc, boolean merging)
      throws IOException;

  private static final NumericDocValuesProducer EMPTY =
      new NumericDocValuesProducer() {
        @Override
        public NumericDocValues getNumeric(IndexInput data, int maxDoc, boolean merging) {
          return DocValues.emptyNumeric();
        }
      };

  public static NumericDocValuesProducer create(Lucene90DocValuesProducer.NumericEntry entry) {
    if (entry.docsWithFieldOffset == -2) {
      return EMPTY;
    } else if (entry.docsWithFieldOffset == -1) {
      if (entry.bitsPerValue == 0) {
        return new DenseSingleValueNumericDocValuesProducer(entry.minValue);
      } else {
        if (entry.blockShift >= 0) {
          return new DenseVaryingVPDocValuesProducer(entry);
        }
        if (entry.table != null) {
          return new DenseTableDocValuesProducer(entry);
        }
        if (entry.gcd == 1 && entry.minValue == 0) {
          return new DenseCommonOrdinalsDocValuesProducer(entry);
        }
        return new DenseNumericDocValuesProducer(entry);
      }
    } else {
      if (entry.bitsPerValue == 0) {
        return new SparseSingleValueNumericDocValuesProducer(entry);
      } else {
        if (entry.blockShift >= 0) {
          return new SparseVaryingVPDocValuesProducer(entry);
        }
        if (entry.table != null) {
          return new SparseTableDocValuesProducer(entry);
        }
        if (entry.gcd == 1 && entry.minValue == 0) {
          return new SparseCommonOrdinalsDocValuesProducer(entry);
        }
        return new SparseNumericDocValuesProducer(entry);
      }
    }
  }

  private static class DenseSingleValueNumericDocValuesProducer extends NumericDocValuesProducer {
    private final long value;

    private DenseSingleValueNumericDocValuesProducer(long value) {
      this.value = value;
    }

    @Override
    public NumericDocValues getNumeric(IndexInput data, int maxDoc, boolean merging) {
      return new DenseNumericDocValues(maxDoc) {
        @Override
        public long longValue() {
          return value;
        }
      };
    }
  }

  private static class SparseSingleValueNumericDocValuesProducer extends NumericDocValuesProducer {
    private final long value;
    private final long docsWithFieldOffset;
    private final long docsWithFieldLength;
    private final short jumpTableEntryCount;
    private final byte denseRankPower;
    private final long numValues;

    private SparseSingleValueNumericDocValuesProducer(
        Lucene90DocValuesProducer.NumericEntry entry) {
      this.value = entry.minValue;
      this.docsWithFieldOffset = entry.docsWithFieldOffset;
      this.docsWithFieldLength = entry.docsWithFieldLength;
      this.jumpTableEntryCount = entry.jumpTableEntryCount;
      this.denseRankPower = entry.denseRankPower;
      this.numValues = entry.numValues;
    }

    @Override
    public NumericDocValues getNumeric(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      final IndexedDISI disi =
          new IndexedDISI(
              data,
              docsWithFieldOffset,
              docsWithFieldLength,
              jumpTableEntryCount,
              denseRankPower,
              numValues);
      return new SparseNumericDocValues(disi) {
        @Override
        public long longValue() {
          return value;
        }
      };
    }
  }

  private abstract static class BaseNumericDocValuesProducer extends NumericDocValuesProducer {
    protected final long valuesOffset;
    private final long valuesLength;

    protected BaseNumericDocValuesProducer(long valuesOffset, long valuesLength) {
      this.valuesOffset = valuesOffset;
      this.valuesLength = valuesLength;
    }

    protected final RandomAccessInput getSlice(IndexInput data) throws IOException {
      final RandomAccessInput slice = data.randomAccessSlice(valuesOffset, valuesLength);
      // Prefetch the first page of data. Following pages are expected to get prefetched through
      // read-ahead.
      if (slice.length() > 0) {
        slice.prefetch(0, 1);
      }
      return slice;
    }
  }

  // dense but split into blocks of different bits per value
  private static class DenseVaryingVPDocValuesProducer extends BaseNumericDocValuesProducer {
    private final long valueJumpTableOffset;
    private final int blockShift;
    private final long gcd;
    private final long numValues;

    private DenseVaryingVPDocValuesProducer(Lucene90DocValuesProducer.NumericEntry entry) {
      super(entry.valuesOffset, entry.valuesLength);
      this.valueJumpTableOffset = entry.valueJumpTableOffset;
      this.blockShift = entry.blockShift;
      this.gcd = entry.gcd;
      this.numValues = entry.numValues;
    }

    @Override
    public NumericDocValues getNumeric(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      // dense but split into blocks of different bits per value
      return new DenseNumericDocValues(maxDoc) {
        final RandomAccessInput slice = getSlice(data);
        final VaryingBPVReader vBPVReader =
            new VaryingBPVReader(
                slice,
                data,
                merging,
                valueJumpTableOffset,
                blockShift,
                gcd,
                valuesOffset,
                numValues);

        @Override
        public long longValue() throws IOException {
          return vBPVReader.getLongValue(doc);
        }
      };
    }
  }

  private static class DenseTableDocValuesProducer extends BaseNumericDocValuesProducer {

    private final long[] table;
    private final byte bitsPerValue;
    private final long numValues;

    private DenseTableDocValuesProducer(Lucene90DocValuesProducer.NumericEntry entry) {
      super(entry.valuesOffset, entry.valuesLength);
      this.table = entry.table;
      this.bitsPerValue = entry.bitsPerValue;
      this.numValues = entry.numValues;
    }

    @Override
    public NumericDocValues getNumeric(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      final RandomAccessInput slice = getSlice(data);
      final LongValues values =
          getDirectReaderInstance(slice, bitsPerValue, 0L, numValues, merging);
      final long[] table = this.table;
      return new DenseNumericDocValues(maxDoc) {
        @Override
        public long longValue() {
          return table[(int) values.get(doc)];
        }
      };
    }
  }

  // Common case for ordinals, which are encoded as numerics
  private static class DenseCommonOrdinalsDocValuesProducer extends BaseNumericDocValuesProducer {

    private final byte bitsPerValue;
    private final long numValues;

    private DenseCommonOrdinalsDocValuesProducer(Lucene90DocValuesProducer.NumericEntry entry) {
      super(entry.valuesOffset, entry.valuesLength);
      this.bitsPerValue = entry.bitsPerValue;
      this.numValues = entry.numValues;
    }

    @Override
    public NumericDocValues getNumeric(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      final RandomAccessInput slice = getSlice(data);
      final LongValues values =
          getDirectReaderInstance(slice, bitsPerValue, 0L, numValues, merging);
      return new DenseNumericDocValues(maxDoc) {
        @Override
        public long longValue() {
          return values.get(doc);
        }
      };
    }
  }

  private static class DenseNumericDocValuesProducer extends BaseNumericDocValuesProducer {
    private final byte bitsPerValue;
    private final long numValues;
    private final long minValue;
    private final long gcd;

    private DenseNumericDocValuesProducer(Lucene90DocValuesProducer.NumericEntry entry) {
      super(entry.valuesOffset, entry.valuesLength);
      this.bitsPerValue = entry.bitsPerValue;
      this.numValues = entry.numValues;
      this.minValue = entry.minValue;
      this.gcd = entry.gcd;
    }

    @Override
    public NumericDocValues getNumeric(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      final RandomAccessInput slice = getSlice(data);
      final LongValues values =
          getDirectReaderInstance(slice, bitsPerValue, 0L, numValues, merging);

      final long mul = gcd;
      final long delta = minValue;
      return new DenseNumericDocValues(maxDoc) {
        @Override
        public long longValue() {
          return mul * values.get(doc) + delta;
        }
      };
    }
  }

  private abstract static class BaseSparseNumericDocValuesProducer
      extends BaseNumericDocValuesProducer {
    private final long docsWithFieldOffset;
    private final long docsWithFieldLength;
    private final short jumpTableEntryCount;
    private final byte denseRankPower;
    protected final long numValues;

    protected BaseSparseNumericDocValuesProducer(Lucene90DocValuesProducer.NumericEntry entry) {
      super(entry.valuesOffset, entry.valuesLength);
      this.docsWithFieldOffset = entry.docsWithFieldOffset;
      this.docsWithFieldLength = entry.docsWithFieldLength;
      this.jumpTableEntryCount = entry.jumpTableEntryCount;
      this.denseRankPower = entry.denseRankPower;
      this.numValues = entry.numValues;
    }

    protected IndexedDISI getIndexedDISI(IndexInput data) throws IOException {
      return new IndexedDISI(
          data,
          docsWithFieldOffset,
          docsWithFieldLength,
          jumpTableEntryCount,
          denseRankPower,
          numValues);
    }
  }

  // sparse and split into blocks of different bits per value
  private static class SparseVaryingVPDocValuesProducer extends BaseSparseNumericDocValuesProducer {
    private final long valueJumpTableOffset;
    private final int blockShift;
    private final long gcd;

    private SparseVaryingVPDocValuesProducer(Lucene90DocValuesProducer.NumericEntry entry) {
      super(entry);
      this.valueJumpTableOffset = entry.valueJumpTableOffset;
      this.blockShift = entry.blockShift;
      this.gcd = entry.gcd;
    }

    @Override
    public NumericDocValues getNumeric(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      final IndexedDISI disi = getIndexedDISI(data);
      final RandomAccessInput slice = getSlice(data);
      return new SparseNumericDocValues(disi) {
        final VaryingBPVReader vBPVReader =
            new VaryingBPVReader(
                slice,
                data,
                merging,
                valueJumpTableOffset,
                blockShift,
                gcd,
                valuesOffset,
                numValues);

        @Override
        public long longValue() throws IOException {
          final int index = disi.index();
          return vBPVReader.getLongValue(index);
        }
      };
    }
  }

  private static class SparseTableDocValuesProducer extends BaseSparseNumericDocValuesProducer {
    private final long[] table;
    private final byte bitsPerValue;

    private SparseTableDocValuesProducer(Lucene90DocValuesProducer.NumericEntry entry) {
      super(entry);
      this.table = entry.table;
      this.bitsPerValue = entry.bitsPerValue;
    }

    @Override
    public NumericDocValues getNumeric(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      final long[] table = this.table;
      final IndexedDISI disi = getIndexedDISI(data);
      final RandomAccessInput slice = getSlice(data);
      final LongValues values =
          getDirectReaderInstance(slice, bitsPerValue, 0L, numValues, merging);
      return new SparseNumericDocValues(disi) {
        @Override
        public long longValue() {
          return table[(int) values.get(disi.index())];
        }
      };
    }
  }

  private static class SparseCommonOrdinalsDocValuesProducer
      extends BaseSparseNumericDocValuesProducer {

    private final byte bitsPerValue;

    private SparseCommonOrdinalsDocValuesProducer(Lucene90DocValuesProducer.NumericEntry entry) {
      super(entry);
      this.bitsPerValue = entry.bitsPerValue;
    }

    @Override
    public NumericDocValues getNumeric(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      final IndexedDISI disi = getIndexedDISI(data);
      final RandomAccessInput slice = getSlice(data);
      final LongValues values =
          getDirectReaderInstance(slice, bitsPerValue, 0L, numValues, merging);
      return new SparseNumericDocValues(disi) {
        @Override
        public long longValue() {
          return values.get(disi.index());
        }
      };
    }
  }

  private static class SparseNumericDocValuesProducer extends BaseSparseNumericDocValuesProducer {
    private final byte bitsPerValue;
    private final long minValue;
    private final long gcd;

    private SparseNumericDocValuesProducer(Lucene90DocValuesProducer.NumericEntry entry) {
      super(entry);
      this.bitsPerValue = entry.bitsPerValue;
      this.minValue = entry.minValue;
      this.gcd = entry.gcd;
    }

    @Override
    public NumericDocValues getNumeric(IndexInput data, int maxDoc, boolean merging)
        throws IOException {

      final IndexedDISI disi = getIndexedDISI(data);
      final RandomAccessInput slice = getSlice(data);
      final LongValues values =
          getDirectReaderInstance(slice, bitsPerValue, 0L, numValues, merging);
      final long mul = gcd;
      final long delta = minValue;
      return new SparseNumericDocValues(disi) {
        @Override
        public long longValue() throws IOException {
          return mul * values.get(disi.index()) + delta;
        }
      };
    }
  }

  private abstract static class DenseNumericDocValues extends NumericDocValues {
    final int maxDoc;
    int doc = -1;

    DenseNumericDocValues(int maxDoc) {
      this.maxDoc = maxDoc;
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
    public boolean advanceExact(int target) {
      doc = target;
      return true;
    }

    @Override
    public long cost() {
      return maxDoc;
    }
  }

  private abstract static class SparseNumericDocValues extends NumericDocValues {

    final IndexedDISI disi;

    SparseNumericDocValues(IndexedDISI disi) {
      this.disi = disi;
    }

    @Override
    public int advance(int target) throws IOException {
      return disi.advance(target);
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      return disi.advanceExact(target);
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
  }

  static LongValues getDirectReaderInstance(
      RandomAccessInput slice, int bitsPerValue, long offset, long numValues, boolean merging) {
    if (merging) {
      return DirectReader.getMergeInstance(slice, bitsPerValue, offset, numValues);
    } else {
      return DirectReader.getInstance(slice, bitsPerValue, offset);
    }
  }

  /**
   * Reader for longs split into blocks of different bits per values. The longs are requested by
   * index and must be accessed in monotonically increasing order.
   */
  // Note: The order requirement could be removed as the jump-tables allow for backwards iteration
  // Note 2: The rankSlice is only used if an advance of > 1 block is called. Its construction could
  // be lazy
  static class VaryingBPVReader {
    final RandomAccessInput slice; // 2 slices to avoid cache thrashing when using rank
    final RandomAccessInput rankSlice;
    final long valueJumpTableOffset;
    final long valuesOffset;
    final long numValues;
    final int shift;
    final long mul;
    final int mask;
    final boolean merging;

    long block = -1;
    long delta;
    long offset;
    long blockEndOffset;
    LongValues values;

    VaryingBPVReader(
        RandomAccessInput slice,
        IndexInput data,
        boolean merging,
        long valueJumpTableOffset,
        int blockShift,
        long gcd,
        long valuesOffset,
        long numValues)
        throws IOException {

      this.slice = slice;
      this.rankSlice =
          valueJumpTableOffset == -1
              ? null
              : data.randomAccessSlice(valueJumpTableOffset, data.length() - valueJumpTableOffset);

      if (rankSlice != null && rankSlice.length() > 0) {
        // Prefetch the first page of data. Following pages are expected to get prefetched through
        // read-ahead.
        rankSlice.prefetch(0, 1);
      }
      this.merging = merging;
      this.valueJumpTableOffset = valueJumpTableOffset;
      this.valuesOffset = valuesOffset;
      this.numValues = numValues;
      shift = blockShift;
      mul = gcd;
      mask = (1 << shift) - 1;
    }

    long getLongValue(long index) throws IOException {
      final long block = index >>> shift;
      if (this.block != block) {
        int bitsPerValue;
        do {
          // If the needed block is the one directly following the current block, it is cheaper to
          // avoid the cache
          if (rankSlice != null && block != this.block + 1) {
            blockEndOffset = rankSlice.readLong(block * Long.BYTES) - valuesOffset;
            this.block = block - 1;
          }
          offset = blockEndOffset;
          bitsPerValue = slice.readByte(offset++);
          delta = slice.readLong(offset);
          offset += Long.BYTES;
          if (bitsPerValue == 0) {
            blockEndOffset = offset;
          } else {
            final int length = slice.readInt(offset);
            offset += Integer.BYTES;
            blockEndOffset = offset + length;
          }
          this.block++;
        } while (this.block != block);
        final int numValues =
            Math.toIntExact(Math.min(1 << shift, this.numValues - (block << shift)));
        values =
            bitsPerValue == 0
                ? LongValues.ZEROES
                : getDirectReaderInstance(slice, bitsPerValue, offset, numValues, merging);
      }
      return mul * values.get(index & mask) + delta;
    }
  }
}
