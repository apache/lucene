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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.PointsReader;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.TermVectorsReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.packed.PackedInts;

/**
 * An {@link org.apache.lucene.index.CodecReader} which supports sorting documents by a given {@link
 * Sort}. This can be used to re-sort and index after it's been created by wrapping all readers of
 * the index with this reader and adding it to a fresh IndexWriter via {@link
 * IndexWriter#addIndexes(CodecReader...)}. NOTE: This reader should only be used for merging.
 * Pulling fields from this reader might be very costly and memory intensive.
 *
 * @lucene.experimental
 */
public final class SortingCodecReader extends FilterCodecReader {

  private record SortingBits(Bits in, Sorter.DocMap docMap) implements Bits {

    @Override
    public boolean get(int index) {
      return in.get(docMap.newToOld(index));
    }

    @Override
    public int length() {
      return in.length();
    }
  }

  private static class SortingPointValues extends PointValues {

    private final PointValues in;
    private final Sorter.DocMap docMap;

    SortingPointValues(final PointValues in, Sorter.DocMap docMap) {
      this.in = Objects.requireNonNull(in);
      this.docMap = docMap;
    }

    @Override
    public PointTree getPointTree() throws IOException {
      return new SortingPointTree(in.getPointTree(), docMap);
    }

    @Override
    public byte[] getMinPackedValue() throws IOException {
      return in.getMinPackedValue();
    }

    @Override
    public byte[] getMaxPackedValue() throws IOException {
      return in.getMaxPackedValue();
    }

    @Override
    public int getNumDimensions() throws IOException {
      return in.getNumDimensions();
    }

    @Override
    public int getNumIndexDimensions() throws IOException {
      return in.getNumIndexDimensions();
    }

    @Override
    public int getBytesPerDimension() throws IOException {
      return in.getBytesPerDimension();
    }

    @Override
    public long size() {
      return in.size();
    }

    @Override
    public int getDocCount() {
      return in.getDocCount();
    }
  }

  private static class SortingPointTree implements PointValues.PointTree {

    private final PointValues.PointTree indexTree;
    private final Sorter.DocMap docMap;
    private final SortingIntersectVisitor sortingIntersectVisitor;

    SortingPointTree(PointValues.PointTree indexTree, Sorter.DocMap docMap) {
      this.indexTree = indexTree;
      this.docMap = docMap;
      this.sortingIntersectVisitor = new SortingIntersectVisitor(docMap);
    }

    @Override
    public PointValues.PointTree clone() {
      return new SortingPointTree(indexTree.clone(), docMap);
    }

    @Override
    public boolean moveToChild() throws IOException {
      return indexTree.moveToChild();
    }

    @Override
    public boolean moveToSibling() throws IOException {
      return indexTree.moveToSibling();
    }

    @Override
    public boolean moveToParent() throws IOException {
      return indexTree.moveToParent();
    }

    @Override
    public byte[] getMinPackedValue() {
      return indexTree.getMinPackedValue();
    }

    @Override
    public byte[] getMaxPackedValue() {
      return indexTree.getMaxPackedValue();
    }

    @Override
    public long size() {
      return indexTree.size();
    }

    @Override
    public void visitDocIDs(PointValues.IntersectVisitor visitor) throws IOException {
      sortingIntersectVisitor.setIntersectVisitor(visitor);
      indexTree.visitDocIDs(sortingIntersectVisitor);
    }

    @Override
    public void visitDocValues(PointValues.IntersectVisitor visitor) throws IOException {
      sortingIntersectVisitor.setIntersectVisitor(visitor);
      indexTree.visitDocValues(sortingIntersectVisitor);
    }
  }

  private static class SortingIntersectVisitor implements PointValues.IntersectVisitor {

    private final Sorter.DocMap docMap;

    private PointValues.IntersectVisitor visitor;

    SortingIntersectVisitor(Sorter.DocMap docMap) {
      this.docMap = docMap;
    }

    private void setIntersectVisitor(PointValues.IntersectVisitor visitor) {
      this.visitor = visitor;
    }

    @Override
    public void visit(int docID) throws IOException {
      visitor.visit(docMap.oldToNew(docID));
    }

    @Override
    public void visit(int docID, byte[] packedValue) throws IOException {
      visitor.visit(docMap.oldToNew(docID), packedValue);
    }

    @Override
    public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
      return visitor.compare(minPackedValue, maxPackedValue);
    }
  }

  /**
   * Factory for SortingValuesIterator. This enables us to create new iterators as needed without
   * recomputing the sorting mappings.
   */
  static class SortingIteratorSupplier implements Supplier<SortingValuesIterator> {
    private final FixedBitSet docBits;
    private final int[] docToOrd;
    private final int size;

    SortingIteratorSupplier(FixedBitSet docBits, int[] docToOrd, int size) {
      this.docBits = docBits;
      this.docToOrd = docToOrd;
      this.size = size;
    }

    @Override
    public SortingValuesIterator get() {
      return new SortingValuesIterator(docBits, docToOrd, size);
    }

    public int size() {
      return size;
    }
  }

  /**
   * Creates a factory for SortingValuesIterator. Does the work of computing the (new docId to old
   * ordinal) mapping, and caches the result, enabling it to create new iterators cheaply.
   *
   * @param values the values over which to iterate
   * @param docMap the mapping from "old" docIds to "new" (sorted) docIds.
   */
  public static SortingIteratorSupplier iteratorSupplier(
      KnnVectorValues values, Sorter.DocMap docMap) throws IOException {

    final int[] docToOrd = new int[docMap.size()];
    final FixedBitSet docBits = new FixedBitSet(docMap.size());
    int count = 0;
    // Note: docToOrd will contain zero for docids that have no vector. This is OK though
    // because the iterator cannot be positioned on such docs
    KnnVectorValues.DocIndexIterator iter = values.iterator();
    for (int doc = iter.nextDoc(); doc != NO_MORE_DOCS; doc = iter.nextDoc()) {
      int newDocId = docMap.oldToNew(doc);
      if (newDocId != -1) {
        docToOrd[newDocId] = iter.index();
        docBits.set(newDocId);
        ++count;
      }
    }
    return new SortingIteratorSupplier(docBits, docToOrd, count);
  }

  /**
   * Iterator over KnnVectorValues accepting a mapping to differently-sorted docs. Consequently
   * index() may skip around, not increasing monotonically as iteration proceeds.
   */
  public static class SortingValuesIterator extends KnnVectorValues.DocIndexIterator {
    private final FixedBitSet docBits;
    private final DocIdSetIterator docsWithValues;
    private final int[] docToOrd;

    int doc = -1;

    SortingValuesIterator(FixedBitSet docBits, int[] docToOrd, int size) {
      this.docBits = docBits;
      this.docToOrd = docToOrd;
      docsWithValues = new BitSetIterator(docBits, size);
    }

    @Override
    public int docID() {
      return doc;
    }

    @Override
    public int index() {
      assert docBits.get(doc);
      return docToOrd[doc];
    }

    @Override
    public int nextDoc() throws IOException {
      if (doc != NO_MORE_DOCS) {
        doc = docsWithValues.nextDoc();
      }
      return doc;
    }

    @Override
    public long cost() {
      return docBits.cardinality();
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }
  }

  /** Sorting FloatVectorValues that maps ordinals using the provided sortMap */
  private static class SortingFloatVectorValues extends FloatVectorValues {
    final FloatVectorValues delegate;
    final SortingIteratorSupplier iteratorSupplier;

    SortingFloatVectorValues(FloatVectorValues delegate, Sorter.DocMap sortMap) throws IOException {
      this.delegate = delegate;
      assert delegate != null;
      // SortingValuesIterator consumes the iterator and records the docs and ord mapping
      iteratorSupplier = iteratorSupplier(delegate, sortMap);
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
      // ords are interpreted in the delegate's ord-space.
      return delegate.vectorValue(ord);
    }

    @Override
    public int dimension() {
      return delegate.dimension();
    }

    @Override
    public int size() {
      return iteratorSupplier.size();
    }

    @Override
    public FloatVectorValues copy() {
      throw new UnsupportedOperationException();
    }

    @Override
    public DocIndexIterator iterator() {
      return iteratorSupplier.get();
    }
  }

  private static class SortingByteVectorValues extends ByteVectorValues {
    final ByteVectorValues delegate;
    final SortingIteratorSupplier iteratorSupplier;

    SortingByteVectorValues(ByteVectorValues delegate, Sorter.DocMap sortMap) throws IOException {
      this.delegate = delegate;
      // SortingValuesIterator consumes the iterator and records the docs and ord mapping
      iteratorSupplier = iteratorSupplier(delegate, sortMap);
    }

    @Override
    public byte[] vectorValue(int ord) throws IOException {
      return delegate.vectorValue(ord);
    }

    @Override
    public DocIndexIterator iterator() {
      return iteratorSupplier.get();
    }

    @Override
    public int dimension() {
      return delegate.dimension();
    }

    @Override
    public int size() {
      return iteratorSupplier.size();
    }

    @Override
    public ByteVectorValues copy() {
      throw new UnsupportedOperationException();
    }
  }

  /**
   * Return a sorted view of <code>reader</code> according to the order defined by <code>sort</code>
   * . If the reader is already sorted, this method might return the reader as-is.
   */
  public static CodecReader wrap(CodecReader reader, Sort sort) throws IOException {
    return wrap(reader, new Sorter(sort).sort(reader), sort);
  }

  /**
   * Expert: same as {@link #wrap(org.apache.lucene.index.CodecReader, Sort)} but operates directly
   * on a {@link Sorter.DocMap}.
   */
  public static CodecReader wrap(CodecReader reader, Sorter.DocMap docMap, Sort sort) {
    LeafMetaData metaData = reader.getMetaData();
    LeafMetaData newMetaData =
        new LeafMetaData(
            metaData.createdVersionMajor(), metaData.minVersion(), sort, metaData.hasBlocks());
    if (docMap == null) {
      // the reader is already sorted
      return new FilterCodecReader(reader) {
        @Override
        public CacheHelper getCoreCacheHelper() {
          return null;
        }

        @Override
        public CacheHelper getReaderCacheHelper() {
          return null;
        }

        @Override
        public LeafMetaData getMetaData() {
          return newMetaData;
        }

        @Override
        public String toString() {
          return "SortingCodecReader(" + in + ")";
        }
      };
    }
    if (reader.maxDoc() != docMap.size()) {
      throw new IllegalArgumentException(
          "reader.maxDoc() should be equal to docMap.size(), got "
              + reader.maxDoc()
              + " != "
              + docMap.size());
    }
    assert Sorter.isConsistent(docMap);
    return new SortingCodecReader(reader, docMap, newMetaData);
  }

  final Sorter.DocMap docMap; // pkg-protected to avoid synthetic accessor methods
  final LeafMetaData metaData;

  private SortingCodecReader(
      final CodecReader in, final Sorter.DocMap docMap, LeafMetaData metaData) {
    super(in);
    this.docMap = docMap;
    this.metaData = metaData;
  }

  @Override
  public FieldsProducer getPostingsReader() {
    FieldsProducer postingsReader = in.getPostingsReader();
    if (postingsReader == null) {
      return null;
    }
    return new FieldsProducer() {
      @Override
      public void close() throws IOException {
        postingsReader.close();
      }

      @Override
      public void checkIntegrity() throws IOException {
        postingsReader.checkIntegrity();
      }

      @Override
      public Iterator<String> iterator() {
        return postingsReader.iterator();
      }

      @Override
      public Terms terms(String field) throws IOException {
        Terms terms = postingsReader.terms(field);
        return terms == null
            ? null
            : new FreqProxTermsWriter.SortingTerms(
                terms, in.getFieldInfos().fieldInfo(field).getIndexOptions(), docMap);
      }

      @Override
      public int size() {
        return postingsReader.size();
      }
    };
  }

  @Override
  public StoredFieldsReader getFieldsReader() {
    StoredFieldsReader delegate = in.getFieldsReader();
    if (delegate == null) {
      return null;
    }
    return newStoredFieldsReader(delegate);
  }

  private StoredFieldsReader newStoredFieldsReader(StoredFieldsReader delegate) {
    return new StoredFieldsReader() {
      @Override
      public void prefetch(int docID) throws IOException {
        delegate.prefetch(docMap.newToOld(docID));
      }

      @Override
      public void document(int docID, StoredFieldVisitor visitor) throws IOException {
        delegate.document(docMap.newToOld(docID), visitor);
      }

      @Override
      public StoredFieldsReader clone() {
        return newStoredFieldsReader(delegate.clone());
      }

      @Override
      public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }
    };
  }

  @Override
  public Bits getLiveDocs() {
    final Bits inLiveDocs = in.getLiveDocs();
    if (inLiveDocs == null) {
      return null;
    } else {
      return new SortingBits(inLiveDocs, docMap);
    }
  }

  @Override
  public PointsReader getPointsReader() {
    final PointsReader delegate = in.getPointsReader();
    if (delegate == null) {
      return null;
    }
    return new PointsReader() {
      @Override
      public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
      }

      @Override
      public PointValues getValues(String field) throws IOException {
        var values = delegate.getValues(field);
        if (values == null) {
          return null;
        }
        return new SortingPointValues(delegate.getValues(field), docMap);
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }
    };
  }

  @Override
  public KnnVectorsReader getVectorReader() {
    KnnVectorsReader delegate = in.getVectorReader();
    if (delegate == null) {
      return null;
    }
    return new KnnVectorsReader() {
      @Override
      public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
      }

      @Override
      public FloatVectorValues getFloatVectorValues(String field) throws IOException {
        return new SortingFloatVectorValues(delegate.getFloatVectorValues(field), docMap);
      }

      @Override
      public ByteVectorValues getByteVectorValues(String field) throws IOException {
        return new SortingByteVectorValues(delegate.getByteVectorValues(field), docMap);
      }

      @Override
      public void search(String field, float[] target, KnnCollector knnCollector, Bits acceptDocs) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void search(String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs) {
        throw new UnsupportedOperationException();
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }
    };
  }

  @Override
  public NormsProducer getNormsReader() {
    final NormsProducer delegate = in.getNormsReader();
    if (delegate == null) {
      return null;
    }
    return new NormsProducer() {
      @Override
      public NumericDocValues getNorms(FieldInfo field) throws IOException {
        return new NumericDocValuesWriter.SortingNumericDocValues(
            getOrCreateNorms(field.name, () -> getNumericDocValues(delegate.getNorms(field))));
      }

      @Override
      public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }
    };
  }

  @Override
  public DocValuesProducer getDocValuesReader() {
    final DocValuesProducer delegate = in.getDocValuesReader();
    if (delegate == null) {
      return null;
    }
    return new DocValuesProducer() {
      @Override
      public NumericDocValues getNumeric(FieldInfo field) throws IOException {
        return new NumericDocValuesWriter.SortingNumericDocValues(
            getOrCreateDV(field.name, () -> getNumericDocValues(delegate.getNumeric(field))));
      }

      @Override
      public BinaryDocValues getBinary(FieldInfo field) throws IOException {
        return new BinaryDocValuesWriter.SortingBinaryDocValues(
            getOrCreateDV(
                field.name,
                () ->
                    new BinaryDocValuesWriter.BinaryDVs(
                        maxDoc(), docMap, delegate.getBinary(field))));
      }

      @Override
      public SortedDocValues getSorted(FieldInfo field) throws IOException {
        SortedDocValues oldDocValues = delegate.getSorted(field);
        return new SortedDocValuesWriter.SortingSortedDocValues(
            oldDocValues,
            getOrCreateDV(
                field.name,
                () -> {
                  int[] ords = new int[maxDoc()];
                  Arrays.fill(ords, -1);
                  int docID;
                  while ((docID = oldDocValues.nextDoc()) != NO_MORE_DOCS) {
                    int newDocID = docMap.oldToNew(docID);
                    ords[newDocID] = oldDocValues.ordValue();
                  }
                  return ords;
                }));
      }

      @Override
      public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
        final SortedNumericDocValues oldDocValues = delegate.getSortedNumeric(field);
        return new SortedNumericDocValuesWriter.SortingSortedNumericDocValues(
            oldDocValues,
            getOrCreateDV(
                field.name,
                () ->
                    new SortedNumericDocValuesWriter.LongValues(
                        maxDoc(), docMap, oldDocValues, PackedInts.FAST)));
      }

      @Override
      public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
        SortedSetDocValues oldDocValues = delegate.getSortedSet(field);
        return new SortedSetDocValuesWriter.SortingSortedSetDocValues(
            oldDocValues,
            getOrCreateDV(
                field.name,
                () ->
                    new SortedSetDocValuesWriter.DocOrds(
                        maxDoc(),
                        docMap,
                        oldDocValues,
                        PackedInts.FAST,
                        SortedSetDocValuesWriter.DocOrds.START_BITS_PER_VALUE)));
      }

      @Override
      public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }

      @Override
      public DocValuesSkipper getSkipper(FieldInfo field) throws IOException {
        // We can hardly return information about min/max values if doc IDs have been reordered.
        return null;
      }
    };
  }

  private NumericDocValuesWriter.NumericDVs getNumericDocValues(NumericDocValues oldNumerics)
      throws IOException {
    FixedBitSet docsWithField = new FixedBitSet(maxDoc());
    long[] values = new long[maxDoc()];
    int docID;
    while ((docID = oldNumerics.nextDoc()) != NO_MORE_DOCS) {
      int newDocID = docMap.oldToNew(docID);
      docsWithField.set(newDocID);
      values[newDocID] = oldNumerics.longValue();
    }
    return new NumericDocValuesWriter.NumericDVs(values, docsWithField);
  }

  @Override
  public TermVectorsReader getTermVectorsReader() {
    return newTermVectorsReader(in.getTermVectorsReader());
  }

  private TermVectorsReader newTermVectorsReader(TermVectorsReader delegate) {
    if (delegate == null) {
      return null;
    }
    return new TermVectorsReader() {
      @Override
      public void prefetch(int doc) throws IOException {
        delegate.prefetch(docMap.newToOld(doc));
      }

      @Override
      public Fields get(int doc) throws IOException {
        return delegate.get(docMap.newToOld(doc));
      }

      @Override
      public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
      }

      @Override
      public TermVectorsReader clone() {
        return newTermVectorsReader(delegate.clone());
      }

      @Override
      public void close() throws IOException {
        delegate.close();
      }
    };
  }

  @Override
  public String toString() {
    return "SortingCodecReader(" + in + ")";
  }

  // no caching on sorted views
  @Override
  public CacheHelper getCoreCacheHelper() {
    return null;
  }

  @Override
  public CacheHelper getReaderCacheHelper() {
    return null;
  }

  @Override
  public LeafMetaData getMetaData() {
    return metaData;
  }

  // we try to cache the last used DV or Norms instance since during merge
  // this instance is used more than once. We could in addition to this single instance
  // also cache the fields that are used for sorting since we do the work twice for these fields
  private String cachedField;
  private Object cachedObject;
  private boolean cacheIsNorms;

  private <T> T getOrCreateNorms(String field, IOSupplier<T> supplier) throws IOException {
    return getOrCreate(field, true, supplier);
  }

  @SuppressWarnings("unchecked")
  private synchronized <T> T getOrCreate(String field, boolean norms, IOSupplier<T> supplier)
      throws IOException {
    if ((field.equals(cachedField) && cacheIsNorms == norms) == false) {
      assert assertCreatedOnlyOnce(field, norms);
      cachedObject = supplier.get();
      cachedField = field;
      cacheIsNorms = norms;
    }
    assert cachedObject != null;
    return (T) cachedObject;
  }

  private final Map<String, Integer> cacheStats = new HashMap<>(); // only with assertions enabled

  private boolean assertCreatedOnlyOnce(String field, boolean norms) {
    assert Thread.holdsLock(this);
    // this is mainly there to make sure we change anything in the way we merge we realize it early
    int timesCached = cacheStats.compute(field + "N:" + norms, (_, i) -> i == null ? 1 : i + 1);
    if (timesCached > 1) {
      assert norms == false : "[" + field + "] norms must not be cached twice";
      boolean isSortField = false;
      // For things that aren't sort fields, it's possible for sort to be null here
      // In the event that we accidentally cache twice, its better not to throw an NPE
      if (metaData.sort() != null) {
        for (SortField sf : metaData.sort().getSort()) {
          if (field.equals(sf.getField())) {
            isSortField = true;
            break;
          }
        }
      }
      assert timesCached == 2
          : "["
              + field
              + "] must not be cached more than twice but was cached: "
              + timesCached
              + " times isSortField: "
              + isSortField;
      assert isSortField
          : "only sort fields should be cached twice but [" + field + "] is not a sort field";
    }
    return true;
  }

  private <T> T getOrCreateDV(String field, IOSupplier<T> supplier) throws IOException {
    return getOrCreate(field, false, supplier);
  }
}
