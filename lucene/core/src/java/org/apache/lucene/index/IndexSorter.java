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

package org.apache.lucene.index;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.packed.PackedInts;

/**
 * Handles how documents should be sorted in an index, both within a segment and between segments.
 *
 * <p>Implementers must provide the following methods:
 *
 * <ul>
 *   <li>{@link #getDocComparator(LeafReader,int)} - an object that determines how documents within
 *       a segment are to be sorted
 *   <li>{@link #getComparableValues(List)} - an object that reads and compares the sort value of
 *       documents across segments, used for merge sorting. The default implementation adapts {@link
 *       #getComparableProviders(List)}, so sorts whose value can be expressed as a {@code long}
 *       only need to implement the latter.
 *   <li>{@link #getProviderName()} - the SPI-registered name of a {@link SortFieldProvider} to
 *       serialize the sort
 * </ul>
 *
 * <p>The companion {@link SortFieldProvider} should be registered with SPI via {@code
 * META-INF/services}
 */
public interface IndexSorter {

  /** Used for sorting documents across segments */
  interface ComparableProvider {
    /**
     * Returns a long so that the natural ordering of long values matches the ordering of doc IDs
     * for the given comparator
     */
    long getAsComparableLong(int docID) throws IOException;
  }

  /**
   * Caches the sort value of the current document of every segment being merged, and compares them.
   *
   * <p>This is the merge-sort counterpart of {@link DocComparator}. Most sorts reduce a document to
   * a single sortable {@code long} and should implement {@link #getComparableProviders(List)}
   * instead; the {@link #fromComparableProviders} adapter exposes them through this interface with
   * no change in behaviour. Sorts whose value cannot be represented as a {@code long} (for example
   * raw bytes) implement {@link #getComparableValues(List)} directly.
   */
  interface ComparableValues {
    /**
     * Reads the value of the current document of the given segment so that it can later be compared
     * by {@link #compare(int, int)}.
     *
     * <p>The merge sort visits the documents of each segment in increasing docID order, so {@code
     * docID} is non-decreasing for a given {@code readerIndex} between consecutive calls.
     *
     * @param readerIndex the index of the segment in the list passed to {@link
     *     #getComparableValues(List)}
     * @param docID the document whose value should be read
     */
    void setTopValue(int readerIndex, int docID) throws IOException;

    /**
     * Compares the values previously read by {@link #setTopValue(int, int)} for the two segments.
     * The returned value follows the same contract as {@link Comparator#compare(Object, Object)},
     * without applying the {@code reverse} flag (the caller is responsible for that).
     */
    int compare(int readerIndexA, int readerIndexB);

    /**
     * Adapts an array of {@link ComparableProvider} (one per segment) to {@link ComparableValues}
     */
    static ComparableValues fromComparableProviders(ComparableProvider[] providers) {
      final long[] values = new long[providers.length];
      return new ComparableValues() {
        @Override
        public void setTopValue(int readerIndex, int docID) throws IOException {
          values[readerIndex] = providers[readerIndex].getAsComparableLong(docID);
        }

        @Override
        public int compare(int readerIndexA, int readerIndexB) {
          return Long.compare(values[readerIndexA], values[readerIndexB]);
        }
      };
    }
  }

  /** A comparator of doc IDs, used for sorting documents within a segment */
  interface DocComparator {
    /**
     * Compare docID1 against docID2. The contract for the return value is the same as {@link
     * Comparator#compare(Object, Object)}.
     */
    int compare(int docID1, int docID2);
  }

  /**
   * Get an array of {@link ComparableProvider}, one per segment, for merge sorting documents in
   * different segments.
   *
   * <p>Implementations whose sort value is a {@code long} should implement this method; it is
   * exposed to the merger through the default {@link #getComparableValues(List)}. Implementations
   * that override {@link #getComparableValues(List)} directly need not implement this method.
   *
   * @param readers the readers to be merged
   */
  default ComparableProvider[] getComparableProviders(List<? extends LeafReader> readers)
      throws IOException {
    throw new UnsupportedOperationException(
        getClass().getName() + " does not implement getComparableProviders");
  }

  /**
   * Get a {@link ComparableValues}, used for merge sorting documents across the given segments.
   *
   * <p>The default implementation adapts {@link #getComparableProviders(List)} and is appropriate
   * for any sort whose value can be represented as a {@code long}.
   *
   * @param readers the readers to be merged
   */
  default ComparableValues getComparableValues(List<? extends LeafReader> readers)
      throws IOException {
    return ComparableValues.fromComparableProviders(getComparableProviders(readers));
  }

  /**
   * Get a comparator that determines the sort order of docs within a single Reader.
   *
   * <p>NB We cannot simply use the {@link FieldComparator} API because it requires docIDs to be
   * sent in-order. The default implementations allocate array[maxDoc] to hold native values for
   * comparison, but 1) they are transient (only alive while sorting this one segment) and 2) in the
   * typical index sorting case, they are only used to sort newly flushed segments, which will be
   * smaller than merged segments
   *
   * @param reader the Reader to sort
   * @param maxDoc the number of documents in the Reader
   */
  DocComparator getDocComparator(LeafReader reader, int maxDoc) throws IOException;

  /**
   * The SPI-registered name of a {@link SortFieldProvider} that will deserialize the parent
   * SortField
   */
  String getProviderName();

  /** Provide a NumericDocValues instance for a LeafReader */
  interface NumericDocValuesProvider {
    /** Returns the NumericDocValues instance for this LeafReader */
    NumericDocValues get(LeafReader reader) throws IOException;
  }

  /** Provide a SortedDocValues instance for a LeafReader */
  interface SortedDocValuesProvider {
    /** Returns the SortedDocValues instance for this LeafReader */
    SortedDocValues get(LeafReader reader) throws IOException;
  }

  /** Provide a BinaryDocValues instance for a LeafReader */
  @FunctionalInterface
  interface BinaryDocValuesProvider {
    /** Returns the BinaryDocValues instance for this LeafReader */
    BinaryDocValues get(LeafReader reader) throws IOException;
  }

  /** Sorts documents based on integer values from a NumericDocValues instance */
  final class IntSorter implements IndexSorter {

    private final Integer missingValue;
    private final int reverseMul;
    private final NumericDocValuesProvider valuesProvider;
    private final String providerName;

    /** Creates a new IntSorter */
    public IntSorter(
        String providerName,
        Integer missingValue,
        boolean reverse,
        NumericDocValuesProvider valuesProvider) {
      this.missingValue = missingValue;
      this.reverseMul = reverse ? -1 : 1;
      this.valuesProvider = valuesProvider;
      this.providerName = providerName;
    }

    @Override
    public ComparableProvider[] getComparableProviders(List<? extends LeafReader> readers)
        throws IOException {
      ComparableProvider[] providers = new ComparableProvider[readers.size()];
      final long missingValue;
      if (this.missingValue != null) {
        missingValue = this.missingValue;
      } else {
        missingValue = 0L;
      }

      for (int readerIndex = 0; readerIndex < readers.size(); readerIndex++) {
        final NumericDocValues values = valuesProvider.get(readers.get(readerIndex));

        providers[readerIndex] =
            docID -> {
              if (values.advanceExact(docID)) {
                return values.longValue();
              } else {
                return missingValue;
              }
            };
      }
      return providers;
    }

    @Override
    public DocComparator getDocComparator(LeafReader reader, int maxDoc) throws IOException {
      final NumericDocValues dvs = valuesProvider.get(reader);
      int[] values = new int[maxDoc];
      if (this.missingValue != null) {
        Arrays.fill(values, this.missingValue);
      }
      while (true) {
        int docID = dvs.nextDoc();
        if (docID == NO_MORE_DOCS) {
          break;
        }
        values[docID] = (int) dvs.longValue();
      }

      return (docID1, docID2) -> reverseMul * Integer.compare(values[docID1], values[docID2]);
    }

    @Override
    public String getProviderName() {
      return providerName;
    }
  }

  /** Sorts documents based on long values from a NumericDocValues instance */
  final class LongSorter implements IndexSorter {

    private final String providerName;
    private final Long missingValue;
    private final int reverseMul;
    private final NumericDocValuesProvider valuesProvider;

    /** Creates a new LongSorter */
    public LongSorter(
        String providerName,
        Long missingValue,
        boolean reverse,
        NumericDocValuesProvider valuesProvider) {
      this.providerName = providerName;
      this.missingValue = missingValue;
      this.reverseMul = reverse ? -1 : 1;
      this.valuesProvider = valuesProvider;
    }

    @Override
    public ComparableProvider[] getComparableProviders(List<? extends LeafReader> readers)
        throws IOException {
      ComparableProvider[] providers = new ComparableProvider[readers.size()];
      final long missingValue;
      if (this.missingValue != null) {
        missingValue = this.missingValue;
      } else {
        missingValue = 0L;
      }

      for (int readerIndex = 0; readerIndex < readers.size(); readerIndex++) {
        final NumericDocValues values = valuesProvider.get(readers.get(readerIndex));

        providers[readerIndex] =
            docID -> {
              if (values.advanceExact(docID)) {
                return values.longValue();
              } else {
                return missingValue;
              }
            };
      }
      return providers;
    }

    @Override
    public DocComparator getDocComparator(LeafReader reader, int maxDoc) throws IOException {
      final NumericDocValues dvs = valuesProvider.get(reader);
      long[] values = new long[maxDoc];
      if (this.missingValue != null) {
        Arrays.fill(values, this.missingValue);
      }
      while (true) {
        int docID = dvs.nextDoc();
        if (docID == NO_MORE_DOCS) {
          break;
        }
        values[docID] = dvs.longValue();
      }

      return (docID1, docID2) -> reverseMul * Long.compare(values[docID1], values[docID2]);
    }

    @Override
    public String getProviderName() {
      return providerName;
    }
  }

  /** Sorts documents based on float values from a NumericDocValues instance */
  final class FloatSorter implements IndexSorter {

    private final String providerName;
    private final Float missingValue;
    private final int reverseMul;
    private final NumericDocValuesProvider valuesProvider;

    /** Creates a new FloatSorter */
    public FloatSorter(
        String providerName,
        Float missingValue,
        boolean reverse,
        NumericDocValuesProvider valuesProvider) {
      this.providerName = providerName;
      this.missingValue = missingValue;
      this.reverseMul = reverse ? -1 : 1;
      this.valuesProvider = valuesProvider;
    }

    @Override
    public ComparableProvider[] getComparableProviders(List<? extends LeafReader> readers)
        throws IOException {
      ComparableProvider[] providers = new ComparableProvider[readers.size()];
      final int missValueBits = Float.floatToIntBits(missingValue != null ? missingValue : 0.0f);

      for (int readerIndex = 0; readerIndex < readers.size(); readerIndex++) {
        final NumericDocValues values = valuesProvider.get(readers.get(readerIndex));

        providers[readerIndex] =
            docID -> {
              final int valueBits =
                  values.advanceExact(docID) ? (int) values.longValue() : missValueBits;
              return NumericUtils.sortableFloatBits(valueBits);
            };
      }
      return providers;
    }

    @Override
    public DocComparator getDocComparator(LeafReader reader, int maxDoc) throws IOException {
      final NumericDocValues dvs = valuesProvider.get(reader);
      float[] values = new float[maxDoc];
      if (this.missingValue != null) {
        Arrays.fill(values, this.missingValue);
      }
      while (true) {
        int docID = dvs.nextDoc();
        if (docID == NO_MORE_DOCS) {
          break;
        }
        values[docID] = Float.intBitsToFloat((int) dvs.longValue());
      }

      return (docID1, docID2) -> reverseMul * Float.compare(values[docID1], values[docID2]);
    }

    @Override
    public String getProviderName() {
      return providerName;
    }
  }

  /** Sorts documents based on double values from a NumericDocValues instance */
  final class DoubleSorter implements IndexSorter {

    private final String providerName;
    private final Double missingValue;
    private final int reverseMul;
    private final NumericDocValuesProvider valuesProvider;

    /** Creates a new DoubleSorter */
    public DoubleSorter(
        String providerName,
        Double missingValue,
        boolean reverse,
        NumericDocValuesProvider valuesProvider) {
      this.providerName = providerName;
      this.missingValue = missingValue;
      this.reverseMul = reverse ? -1 : 1;
      this.valuesProvider = valuesProvider;
    }

    @Override
    public ComparableProvider[] getComparableProviders(List<? extends LeafReader> readers)
        throws IOException {
      ComparableProvider[] providers = new ComparableProvider[readers.size()];
      final long missingValueBits =
          Double.doubleToLongBits(missingValue != null ? missingValue : 0.0f);

      for (int readerIndex = 0; readerIndex < readers.size(); readerIndex++) {
        final NumericDocValues values = valuesProvider.get(readers.get(readerIndex));

        providers[readerIndex] =
            docID -> {
              final long valueBits =
                  values.advanceExact(docID) ? values.longValue() : missingValueBits;
              return NumericUtils.sortableDoubleBits(valueBits);
            };
      }
      return providers;
    }

    @Override
    public DocComparator getDocComparator(LeafReader reader, int maxDoc) throws IOException {
      final NumericDocValues dvs = valuesProvider.get(reader);
      double[] values = new double[maxDoc];
      if (missingValue != null) {
        Arrays.fill(values, missingValue);
      }
      while (true) {
        int docID = dvs.nextDoc();
        if (docID == NO_MORE_DOCS) {
          break;
        }
        values[docID] = Double.longBitsToDouble(dvs.longValue());
      }

      return (docID1, docID2) -> reverseMul * Double.compare(values[docID1], values[docID2]);
    }

    @Override
    public String getProviderName() {
      return providerName;
    }
  }

  /** Sorts documents based on terms from a SortedDocValues instance */
  final class StringSorter implements IndexSorter {

    private final String providerName;
    private final Object missingValue;
    private final int reverseMul;
    private final SortedDocValuesProvider valuesProvider;

    /** Creates a new StringSorter */
    public StringSorter(
        String providerName,
        Object missingValue,
        boolean reverse,
        SortedDocValuesProvider valuesProvider) {
      this.providerName = providerName;
      this.missingValue = missingValue;
      this.reverseMul = reverse ? -1 : 1;
      this.valuesProvider = valuesProvider;
    }

    @Override
    public ComparableProvider[] getComparableProviders(List<? extends LeafReader> readers)
        throws IOException {
      final ComparableProvider[] providers = new ComparableProvider[readers.size()];
      final SortedDocValues[] values = new SortedDocValues[readers.size()];
      for (int i = 0; i < readers.size(); i++) {
        final SortedDocValues sorted = valuesProvider.get(readers.get(i));
        values[i] = sorted;
      }
      OrdinalMap ordinalMap = OrdinalMap.build(null, values, PackedInts.DEFAULT);
      final int missingOrd;
      if (missingValue == SortField.STRING_LAST) {
        missingOrd = Integer.MAX_VALUE;
      } else {
        missingOrd = Integer.MIN_VALUE;
      }

      for (int readerIndex = 0; readerIndex < readers.size(); readerIndex++) {
        final SortedDocValues readerValues = values[readerIndex];
        final LongValues globalOrds = ordinalMap.getGlobalOrds(readerIndex);
        providers[readerIndex] =
            docID -> {
              if (readerValues.advanceExact(docID)) {
                // translate segment's ord to global ord space:
                return globalOrds.get(readerValues.ordValue());
              } else {
                return missingOrd;
              }
            };
      }
      return providers;
    }

    @Override
    public DocComparator getDocComparator(LeafReader reader, int maxDoc) throws IOException {
      final SortedDocValues sorted = valuesProvider.get(reader);
      final int missingOrd;
      if (missingValue == SortField.STRING_LAST) {
        missingOrd = Integer.MAX_VALUE;
      } else {
        missingOrd = Integer.MIN_VALUE;
      }

      final int[] ords = new int[maxDoc];
      Arrays.fill(ords, missingOrd);
      int docID;
      while ((docID = sorted.nextDoc()) != NO_MORE_DOCS) {
        ords[docID] = sorted.ordValue();
      }

      return (docID1, docID2) -> reverseMul * Integer.compare(ords[docID1], ords[docID2]);
    }

    @Override
    public String getProviderName() {
      return providerName;
    }
  }

  /**
   * Sorts documents by the bytes of a {@link BinaryDocValues} instance, using the given comparator.
   *
   * <p>Unlike {@link StringSorter} there is no global ordinal map: values are compared directly, so
   * the merge reads each (already sorted) segment strictly sequentially via {@link
   * BinaryDocValues#nextDoc()}, which stays linear and efficient even for block-compressed formats.
   */
  final class BinarySorter implements IndexSorter {

    private final String providerName;
    private final int reverseMul;

    /**
     * Compares two values. Either argument may be {@code null}, which represents a missing value;
     * the {@code reverse} flag is <em>not</em> applied by this comparator.
     */
    private final Comparator<BytesRef> comparator;

    private final BinaryDocValuesProvider valuesProvider;

    /** Creates a new BinarySorter */
    public BinarySorter(
        String providerName,
        boolean reverse,
        Comparator<BytesRef> comparator,
        BinaryDocValuesProvider valuesProvider) {
      this.providerName = providerName;
      this.reverseMul = reverse ? -1 : 1;
      this.comparator = comparator;
      this.valuesProvider = valuesProvider;
    }

    @Override
    public ComparableValues getComparableValues(List<? extends LeafReader> readers)
        throws IOException {
      final BinaryDocValues[] values = new BinaryDocValues[readers.size()];
      // The current sort key of each segment's head, or null if the head document has no value. We
      // keep the BytesRef without copying: it stays valid until this segment's cursor advances
      // again
      // (its next setTopValue), which is exactly the window during which the head is compared.
      final BytesRef[] heads = new BytesRef[readers.size()];
      for (int i = 0; i < readers.size(); i++) {
        values[i] = valuesProvider.get(readers.get(i));
      }
      final int[] lastDoc = new int[readers.size()];
      Arrays.fill(lastDoc, -1);
      return new ComparableValues() {
        @Override
        public void setTopValue(int readerIndex, int docID) throws IOException {
          // The merge sort visits each segment in non-decreasing docID order (consecutive documents
          // of the same block map to the same parent docID); we only ever scan forward, never seek,
          // so reads stay sequential even for block-compressed formats.
          assert docID >= lastDoc[readerIndex]
              : "out of order access: docID=" + docID + " < lastDoc=" + lastDoc[readerIndex];
          assert (lastDoc[readerIndex] = docID) >= 0;
          final BinaryDocValues v = values[readerIndex];
          int doc = v.docID();
          while (doc < docID) {
            doc = v.nextDoc();
          }
          heads[readerIndex] = doc == docID ? v.binaryValue() : null;
        }

        @Override
        public int compare(int readerIndexA, int readerIndexB) {
          return comparator.compare(heads[readerIndexA], heads[readerIndexB]);
        }
      };
    }

    @Override
    public DocComparator getDocComparator(LeafReader reader, int maxDoc) throws IOException {
      final BinaryDocValues values = valuesProvider.get(reader);
      // Materialize the values once for random access during the in-memory sort: concatenate them
      // into a single byte[] with a per-document offset array, tracking presence in a bitset.
      // Comparisons then view slices of that buffer in place, with no per-comparison copy.
      byte[] blob = new byte[Math.max(16, maxDoc)];
      final int[] offsets = new int[maxDoc + 1];
      final FixedBitSet present = new FixedBitSet(maxDoc);
      int pos = 0;
      int doc = values.nextDoc();
      for (int i = 0; i < maxDoc; i++) {
        offsets[i] = pos;
        if (doc == i) {
          final BytesRef v = values.binaryValue();
          blob = ArrayUtil.grow(blob, pos + v.length);
          System.arraycopy(v.bytes, v.offset, blob, pos, v.length);
          pos += v.length;
          present.set(i);
          doc = values.nextDoc();
        }
      }
      offsets[maxDoc] = pos;
      final byte[] data = blob;
      final BytesRef ref1 = new BytesRef(data);
      final BytesRef ref2 = new BytesRef(data);
      return (docID1, docID2) -> {
        BytesRef v1 = null;
        BytesRef v2 = null;
        if (present.get(docID1)) {
          ref1.offset = offsets[docID1];
          ref1.length = offsets[docID1 + 1] - offsets[docID1];
          v1 = ref1;
        }
        if (present.get(docID2)) {
          ref2.offset = offsets[docID2];
          ref2.length = offsets[docID2 + 1] - offsets[docID2];
          v2 = ref2;
        }
        return reverseMul * comparator.compare(v1, v2);
      };
    }

    @Override
    public String getProviderName() {
      return providerName;
    }
  }
}
