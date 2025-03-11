package org.apache.lucene.codecs.lucene90;

import java.io.IOException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;

/** Reads sorted numeric doc values values for {@link Lucene90DocValuesFormat} */
abstract class SortedNumericDocValuesProducer {

  public abstract SortedNumericDocValues getSortedNumeric(
      IndexInput data, int maxDoc, boolean merging) throws IOException;

  public static SortedNumericDocValuesProducer create(
      Lucene90DocValuesProducer.SortedNumericEntry entry) {
    if (entry.numValues == entry.numDocsWithField) {
      return new SingletonNumericDocValuesProducer(NumericDocValuesProducer.create(entry));
    }
    if (entry.docsWithFieldOffset == -1) {
      // dense
      return new DenseSortedNumericDocValuesProducer(entry);
    } else {
      // sparse
      return new SparseSortedNumericDocValuesProducer(entry);
    }
  }

  private static class SingletonNumericDocValuesProducer extends SortedNumericDocValuesProducer {
    final NumericDocValuesProducer delegate;

    private SingletonNumericDocValuesProducer(NumericDocValuesProducer delegate) {
      this.delegate = delegate;
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      NumericDocValues docValues = delegate.getNumeric(data, maxDoc, merging);
      return DocValues.singleton(docValues);
    }
  }

  private abstract static class BaseSortedNumericDocValuesProducer
      extends SortedNumericDocValuesProducer {

    private final long addressesOffset;
    private final long addressesLength;

    protected BaseSortedNumericDocValuesProducer(long addressesOffset, long addressesLength) {
      this.addressesOffset = addressesOffset;
      this.addressesLength = addressesLength;
    }

    protected RandomAccessInput addressesInput(IndexInput data) throws IOException {
      final RandomAccessInput addressesInput =
          data.randomAccessSlice(addressesOffset, addressesLength);
      // Prefetch the first page of data. Following pages are expected to get prefetched through
      // read-ahead.
      if (addressesInput.length() > 0) {
        addressesInput.prefetch(0, 1);
      }
      return addressesInput;
    }
  }

  private static class DenseSortedNumericDocValuesProducer
      extends BaseSortedNumericDocValuesProducer {

    private final DirectMonotonicReader.Meta addressesMeta;
    private final LongValuesProducer valuesProducer;

    private DenseSortedNumericDocValuesProducer(
        Lucene90DocValuesProducer.SortedNumericEntry entry) {
      super(entry.addressesOffset, entry.addressesLength);
      this.addressesMeta = entry.addressesMeta;
      this.valuesProducer = LongValuesProducer.create(entry);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      final RandomAccessInput addressesInput = addressesInput(data);
      final LongValues addresses =
          DirectMonotonicReader.getInstance(addressesMeta, addressesInput, merging);
      final LongValues values = valuesProducer.getLongValues(data, maxDoc, merging);
      return new SortedNumericDocValues() {

        int doc = -1;
        long start, end;
        int count;

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
          start = addresses.get(target);
          end = addresses.get(target + 1L);
          count = (int) (end - start);
          return doc = target;
        }

        @Override
        public boolean advanceExact(int target) {
          start = addresses.get(target);
          end = addresses.get(target + 1L);
          count = (int) (end - start);
          doc = target;
          return true;
        }

        @Override
        public long nextValue() {
          return values.get(start++);
        }

        @Override
        public int docValueCount() {
          return count;
        }
      };
    }
  }

  private static class SparseSortedNumericDocValuesProducer
      extends BaseSortedNumericDocValuesProducer {
    private final DirectMonotonicReader.Meta addressesMeta;
    private final long docsWithFieldOffset;
    private final long docsWithFieldLength;
    private final int jumpTableEntryCount;
    private final byte denseRankPower;
    private final int numDocsWithField;
    private final LongValuesProducer valuesProducer;

    private SparseSortedNumericDocValuesProducer(
        Lucene90DocValuesProducer.SortedNumericEntry entry) {
      super(entry.addressesOffset, entry.addressesLength);
      this.addressesMeta = entry.addressesMeta;
      this.docsWithFieldOffset = entry.docsWithFieldOffset;
      this.docsWithFieldLength = entry.docsWithFieldLength;
      this.jumpTableEntryCount = entry.jumpTableEntryCount;
      this.denseRankPower = entry.denseRankPower;
      this.numDocsWithField = entry.numDocsWithField;
      this.valuesProducer = LongValuesProducer.create(entry);
    }

    @Override
    public SortedNumericDocValues getSortedNumeric(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      final RandomAccessInput addressesInput = addressesInput(data);
      final LongValues addresses =
          DirectMonotonicReader.getInstance(addressesMeta, addressesInput, merging);
      final LongValues values = valuesProducer.getLongValues(data, maxDoc, merging);
      final IndexedDISI disi =
          new IndexedDISI(
              data,
              docsWithFieldOffset,
              docsWithFieldLength,
              jumpTableEntryCount,
              denseRankPower,
              numDocsWithField);
      return new SortedNumericDocValues() {

        boolean set;
        long start, end;
        int count;

        @Override
        public int nextDoc() throws IOException {
          set = false;
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
          set = false;
          return disi.advance(target);
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          set = false;
          return disi.advanceExact(target);
        }

        @Override
        public long nextValue() {
          set();
          return values.get(start++);
        }

        @Override
        public int docValueCount() {
          set();
          return count;
        }

        private void set() {
          if (set == false) {
            final int index = disi.index();
            start = addresses.get(index);
            end = addresses.get(index + 1L);
            count = (int) (end - start);
            set = true;
          }
        }
      };
    }
  }
}
