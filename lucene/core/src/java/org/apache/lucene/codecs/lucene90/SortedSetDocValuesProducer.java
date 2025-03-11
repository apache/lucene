package org.apache.lucene.codecs.lucene90;

import java.io.IOException;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectReader;

/** Reads sorted set doc values values for {@link Lucene90DocValuesFormat} */
abstract class SortedSetDocValuesProducer {

  public abstract SortedSetDocValues getSortedSet(IndexInput data, int maxDoc, boolean merging)
      throws IOException;

  public static SortedSetDocValuesProducer create(Lucene90DocValuesProducer.SortedSetEntry entry)
      throws IOException {
    if (entry.singleValueEntry != null) {
      return new SingletonSortedDocValuesProducer(entry.singleValueEntry);
    }
    // Specialize the common case for ordinals: single block of packed integers.
    Lucene90DocValuesProducer.SortedNumericEntry ordsEntry = entry.ordsEntry;
    if (ordsEntry.blockShift < 0 && ordsEntry.bitsPerValue > 0) {
      if (ordsEntry.gcd != 1 || ordsEntry.minValue != 0 || ordsEntry.table != null) {
        throw new IllegalStateException("Ordinals shouldn't use GCD, offset or table compression");
      }
      if (ordsEntry.docsWithFieldOffset == -1) { // dense
        return new DenseSingleBlockSortedSetDocValuesProducer(entry);
      } else if (ordsEntry.docsWithFieldOffset >= 0) { // sparse but non-empty
        return new SparseSingleBlockSortedSetDocValuesProducer(entry);
      }
    }
    return new OrdsSortedSetDocValuesProducer(entry.ordsEntry, entry.termsDictEntry);
  }

  private static class SingletonSortedDocValuesProducer extends SortedSetDocValuesProducer {
    final SortedDocValuesProducer delegate;

    private SingletonSortedDocValuesProducer(SortedDocValuesProducer delegate) {
      this.delegate = delegate;
    }

    @Override
    public SortedSetDocValues getSortedSet(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      SortedDocValues docValues = delegate.getSorted(data, maxDoc, merging);
      return DocValues.singleton(docValues);
    }
  }

  private static class DenseSingleBlockSortedSetDocValuesProducer
      extends SortedSetDocValuesProducer {

    private final long addressesOffset;
    private final long addressesLength;
    private final DirectMonotonicReader.Meta addressesMeta;
    private final long valuesOffset;
    private final long valuesLength;
    private final byte bitsPerValue;
    private final Lucene90DocValuesProducer.TermsDictEntry termsDictEntry;

    private DenseSingleBlockSortedSetDocValuesProducer(
        Lucene90DocValuesProducer.SortedSetEntry sortedEntry) {
      Lucene90DocValuesProducer.SortedNumericEntry ordsEntry = sortedEntry.ordsEntry;
      this.addressesOffset = ordsEntry.addressesOffset;
      this.addressesLength = ordsEntry.addressesLength;
      this.addressesMeta = ordsEntry.addressesMeta;
      this.valuesOffset = ordsEntry.valuesOffset;
      this.valuesLength = ordsEntry.valuesLength;
      this.bitsPerValue = ordsEntry.bitsPerValue;
      this.termsDictEntry = sortedEntry.termsDictEntry;
    }

    @Override
    public SortedSetDocValues getSortedSet(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      final RandomAccessInput addressesInput =
          data.randomAccessSlice(addressesOffset, addressesLength);
      // Prefetch the first page of data. Following pages are expected to get prefetched through
      // read-ahead.
      if (addressesInput.length() > 0) {
        addressesInput.prefetch(0, 1);
      }
      final LongValues addresses = DirectMonotonicReader.getInstance(addressesMeta, addressesInput);

      final RandomAccessInput slice = data.randomAccessSlice(valuesOffset, valuesLength);
      // Prefetch the first page of data. Following pages are expected to get prefetched through
      // read-ahead.
      if (slice.length() > 0) {
        slice.prefetch(0, 1);
      }
      final LongValues values = DirectReader.getInstance(slice, bitsPerValue);

      return new BaseSortedSetDocValues(termsDictEntry, data, merging) {

        private int doc = -1;
        private long curr;
        private int count;

        @Override
        public long nextOrd() {
          return values.get(curr++);
        }

        @Override
        public boolean advanceExact(int target) {
          curr = addresses.get(target);
          long end = addresses.get(target + 1L);
          count = (int) (end - curr);
          doc = target;
          return true;
        }

        @Override
        public int docValueCount() {
          return count;
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
          curr = addresses.get(target);
          long end = addresses.get(target + 1L);
          count = (int) (end - curr);
          return doc = target;
        }

        @Override
        public long cost() {
          return maxDoc;
        }
      };
    }
  }

  private static class SparseSingleBlockSortedSetDocValuesProducer
      extends SortedSetDocValuesProducer {

    private final long addressesOffset;
    private final long addressesLength;
    private final DirectMonotonicReader.Meta addressesMeta;
    private final long valuesOffset;
    private final long valuesLength;
    private final byte bitsPerValue;
    private final long docsWithFieldOffset;
    private final long docsWithFieldLength;
    private final int jumpTableEntryCount;
    private final byte denseRankPower;
    private final Lucene90DocValuesProducer.TermsDictEntry termsDictEntry;
    private final long numValues;

    private SparseSingleBlockSortedSetDocValuesProducer(
        Lucene90DocValuesProducer.SortedSetEntry sortedEntry) {
      Lucene90DocValuesProducer.SortedNumericEntry ordsEntry = sortedEntry.ordsEntry;
      this.addressesOffset = ordsEntry.addressesOffset;
      this.addressesLength = ordsEntry.addressesLength;
      this.addressesMeta = ordsEntry.addressesMeta;
      this.valuesOffset = ordsEntry.valuesOffset;
      this.valuesLength = ordsEntry.valuesLength;
      this.bitsPerValue = ordsEntry.bitsPerValue;
      this.termsDictEntry = sortedEntry.termsDictEntry;
      this.docsWithFieldOffset = ordsEntry.docsWithFieldOffset;
      this.docsWithFieldLength = ordsEntry.docsWithFieldLength;
      this.jumpTableEntryCount = ordsEntry.jumpTableEntryCount;
      this.denseRankPower = ordsEntry.denseRankPower;
      this.numValues = ordsEntry.numValues;
    }

    @Override
    public SortedSetDocValues getSortedSet(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      final RandomAccessInput addressesInput =
          data.randomAccessSlice(addressesOffset, addressesLength);
      // Prefetch the first page of data. Following pages are expected to get prefetched through
      // read-ahead.
      if (addressesInput.length() > 0) {
        addressesInput.prefetch(0, 1);
      }
      final LongValues addresses = DirectMonotonicReader.getInstance(addressesMeta, addressesInput);

      final RandomAccessInput slice = data.randomAccessSlice(valuesOffset, valuesLength);
      // Prefetch the first page of data. Following pages are expected to get prefetched through
      // read-ahead.
      if (slice.length() > 0) {
        slice.prefetch(0, 1);
      }
      final LongValues values = DirectReader.getInstance(slice, bitsPerValue);

      final IndexedDISI disi =
          new IndexedDISI(
              data,
              docsWithFieldOffset,
              docsWithFieldLength,
              jumpTableEntryCount,
              denseRankPower,
              numValues);

      return new BaseSortedSetDocValues(termsDictEntry, data, merging) {

        boolean set;
        long curr;
        int count;

        @Override
        public long nextOrd() {
          set();
          return values.get(curr++);
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
          set = false;
          return disi.advanceExact(target);
        }

        @Override
        public int docValueCount() {
          set();
          return count;
        }

        @Override
        public int docID() {
          return disi.docID();
        }

        @Override
        public int nextDoc() throws IOException {
          set = false;
          return disi.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
          set = false;
          return disi.advance(target);
        }

        @Override
        public long cost() {
          return disi.cost();
        }

        private void set() {
          if (set == false) {
            final int index = disi.index();
            curr = addresses.get(index);
            long end = addresses.get(index + 1L);
            count = (int) (end - curr);
            set = true;
          }
        }
      };
    }
  }

  private static class OrdsSortedSetDocValuesProducer extends SortedSetDocValuesProducer {

    private final SortedNumericDocValuesProducer numeric;
    private final Lucene90DocValuesProducer.TermsDictEntry entry;

    private OrdsSortedSetDocValuesProducer(
        Lucene90DocValuesProducer.SortedNumericEntry numericEntry,
        Lucene90DocValuesProducer.TermsDictEntry termsDictEntry) {
      this.numeric = SortedNumericDocValuesProducer.create(numericEntry);
      this.entry = termsDictEntry;
    }

    @Override
    public SortedSetDocValues getSortedSet(IndexInput data, int maxDoc, boolean merging)
        throws IOException {
      final SortedNumericDocValues ords = numeric.getSortedNumeric(data, maxDoc, merging);
      return new BaseSortedSetDocValues(entry, data, merging) {

        @Override
        public long nextOrd() throws IOException {
          return ords.nextValue();
        }

        @Override
        public int docValueCount() {
          return ords.docValueCount();
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

  private abstract static class BaseSortedSetDocValues extends SortedSetDocValues {

    final Lucene90DocValuesProducer.TermsDictEntry entry;
    final IndexInput data;
    final TermsEnum termsEnum;
    final boolean merging;

    BaseSortedSetDocValues(
        Lucene90DocValuesProducer.TermsDictEntry entry, IndexInput data, boolean merging)
        throws IOException {
      this.entry = entry;
      this.data = data;
      this.merging = merging;
      this.termsEnum = termsEnum();
    }

    @Override
    public long getValueCount() {
      return entry.termsDictSize;
    }

    @Override
    public BytesRef lookupOrd(long ord) throws IOException {
      termsEnum.seekExact(ord);
      return termsEnum.term();
    }

    @Override
    public long lookupTerm(BytesRef key) throws IOException {
      TermsEnum.SeekStatus status = termsEnum.seekCeil(key);
      return switch (status) {
        case FOUND -> termsEnum.ord();
        default -> -1L - termsEnum.ord();
      };
    }

    @Override
    public TermsEnum termsEnum() throws IOException {
      return new SortedDocValuesProducer.TermsDict(entry, data, merging);
    }
  }
}
