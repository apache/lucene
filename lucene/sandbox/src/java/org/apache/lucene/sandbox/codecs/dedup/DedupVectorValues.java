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
package org.apache.lucene.sandbox.codecs.dedup;

import static org.apache.lucene.index.VectorEncoding.BYTE;
import static org.apache.lucene.index.VectorEncoding.FLOAT16;
import static org.apache.lucene.index.VectorEncoding.FLOAT32;
import static org.apache.lucene.sandbox.codecs.dedup.DedupUtil.FIELD_ORD_TO_GROUP_ORD_BITS_PER_VALUE;
import static org.apache.lucene.sandbox.codecs.dedup.DedupUtil.SCRATCH_INITIAL_SIZE;
import static org.apache.lucene.search.VectorScorer.Bulk.fromRandomScorerDense;
import static org.apache.lucene.search.VectorScorer.Bulk.fromRandomScorerSparse;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene95.OffHeapByteVectorValues;
import org.apache.lucene.codecs.lucene95.OffHeapFloat16VectorValues;
import org.apache.lucene.codecs.lucene95.OffHeapFloatVectorValues;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.packed.DirectReader;

/**
 * Vector values that share a single copy of each distinct vector across the documents and fields
 * that reference it.
 *
 * <p>Every instance is backed by two views: the {@code fieldView} maps ordinals to docs and drives
 * iteration (one entry per document), while the {@code groupView} holds the de-duplicated vectors
 * (one entry per distinct vector). {@code fieldOrdToGroupOrd} translates a document ordinal in the
 * field into its group ordinal.
 */
sealed interface DedupVectorValues
    permits DedupVectorValues.ByteImpl,
        DedupVectorValues.FloatImpl,
        DedupVectorValues.Float16Impl,
        DedupScalarQuantizedVectorValues.FieldValues,
        DedupScalarQuantizedVectorValues.RawAndQuantizedValues {

  /** The dense view over distinct vectors, indexed by group ordinal. */
  KnnVectorValues getGroupView();

  /** Maps a per-document ordinal to its group ordinal in {@link #getGroupView()}. */
  FieldOrdToGroupOrd getFieldOrdToGroupOrd();

  /**
   * Maps a field's per-document ordinal to the ordinal of its (shared) vector within the group.
   * Backed on-heap while writing and off-heap while reading.
   */
  sealed interface FieldOrdToGroupOrd {
    int get(int ord);

    FieldOrdToGroupOrd copy() throws IOException;
  }

  static ByteImpl loadDedupBytes(
      FlatVectorsScorer vectorsScorer,
      VectorSimilarityFunction function,
      OrdToDocDISIReaderConfiguration configuration,
      int dimension,
      int groupNumVectors,
      IndexInput vectorData,
      long vectorDataOffset,
      long vectorDataSize,
      long fieldOrdToGroupOrdOffset,
      long fieldOrdToGroupOrdSize)
      throws IOException {

    final OffHeapByteVectorValues fieldView =
        OffHeapByteVectorValues.load(
            function, vectorsScorer, configuration, BYTE, dimension, 0, 0, vectorData);

    final OffHeapByteVectorValues groupView =
        new OffHeapByteVectorValues.DenseOffHeapVectorValues(
            dimension,
            groupNumVectors,
            vectorData.slice("group-slice", vectorDataOffset, vectorDataSize),
            fieldView.getVectorByteLength(),
            vectorsScorer,
            function);

    final FieldOrdToGroupOrd fieldOrdToGroupOrd =
        new FieldOrdToGroupOrdOffHeap(vectorData, fieldOrdToGroupOrdOffset, fieldOrdToGroupOrdSize);

    return new ByteImpl(vectorsScorer, function, fieldView, groupView, fieldOrdToGroupOrd);
  }

  /** {@link DedupVectorValues} over byte vectors. */
  final class ByteImpl extends ByteVectorValues implements DedupVectorValues {
    private final FlatVectorsScorer vectorsScorer;
    private final VectorSimilarityFunction function;
    private final ByteVectorValues fieldView;
    private final ByteVectorValues groupView;
    private final FieldOrdToGroupOrd fieldOrdToGroupOrd;
    private int[] scratch;

    ByteImpl(
        FlatVectorsScorer vectorsScorer,
        VectorSimilarityFunction function,
        ByteVectorValues fieldView,
        ByteVectorValues groupView,
        FieldOrdToGroupOrd fieldOrdToGroupOrd) {
      this.vectorsScorer = vectorsScorer;
      this.function = function;
      this.fieldView = fieldView;
      this.groupView = groupView;
      this.fieldOrdToGroupOrd = fieldOrdToGroupOrd;
      this.scratch = new int[SCRATCH_INITIAL_SIZE];
    }

    @Override
    public ByteVectorValues getGroupView() {
      return groupView;
    }

    @Override
    public FieldOrdToGroupOrd getFieldOrdToGroupOrd() {
      return fieldOrdToGroupOrd;
    }

    @Override
    public int ordToDoc(int ord) {
      return fieldView.ordToDoc(ord);
    }

    @Override
    public void prefetch(int[] ordsToPrefetch, int numOrds) throws IOException {
      if (scratch.length < ordsToPrefetch.length) { // grow if needed
        scratch = ArrayUtil.grow(scratch, ordsToPrefetch.length);
      }
      for (int i = 0; i < numOrds; i++) {
        scratch[i] = fieldOrdToGroupOrd.get(ordsToPrefetch[i]);
      }
      groupView.prefetch(scratch, numOrds);
    }

    @Override
    public byte[] vectorValue(int ord) throws IOException {
      return groupView.vectorValue(fieldOrdToGroupOrd.get(ord));
    }

    @Override
    public int dimension() {
      return fieldView.dimension();
    }

    @Override
    public int size() {
      return fieldView.size();
    }

    @Override
    public ByteImpl copy() throws IOException {
      return new ByteImpl(
          vectorsScorer, function, fieldView.copy(), groupView.copy(), fieldOrdToGroupOrd.copy());
    }

    @Override
    public DocIndexIterator iterator() {
      return fieldView.iterator();
    }

    @Override
    public VectorScorer scorer(byte[] target) throws IOException {
      if (size() == 0) {
        return null;
      }
      ByteImpl copy = copy();
      DocIndexIterator indexIterator = copy.iterator();
      RandomVectorScorer vectorScorer = vectorsScorer.getRandomVectorScorer(function, copy, target);
      boolean isDense = copy.fieldView instanceof OffHeapByteVectorValues.DenseOffHeapVectorValues;
      return new DedupVectorScorer(indexIterator, vectorScorer, isDense);
    }
  }

  static FloatImpl loadDedupFloats(
      FlatVectorsScorer vectorsScorer,
      VectorSimilarityFunction function,
      OrdToDocDISIReaderConfiguration configuration,
      int dimension,
      int groupNumVectors,
      IndexInput vectorData,
      long vectorDataOffset,
      long vectorDataSize,
      long fieldOrdToGroupOrdOffset,
      long fieldOrdToGroupOrdSize)
      throws IOException {

    final OffHeapFloatVectorValues fieldView =
        OffHeapFloatVectorValues.load(
            function, vectorsScorer, configuration, FLOAT32, dimension, 0, 0, vectorData);

    final OffHeapFloatVectorValues groupView =
        new OffHeapFloatVectorValues.DenseOffHeapVectorValues(
            dimension,
            groupNumVectors,
            vectorData.slice("group-slice", vectorDataOffset, vectorDataSize),
            fieldView.getVectorByteLength(),
            vectorsScorer,
            function);

    final FieldOrdToGroupOrd fieldOrdToGroupOrd =
        new FieldOrdToGroupOrdOffHeap(vectorData, fieldOrdToGroupOrdOffset, fieldOrdToGroupOrdSize);

    return new FloatImpl(vectorsScorer, function, fieldView, groupView, fieldOrdToGroupOrd);
  }

  /** {@link DedupVectorValues} over float vectors. */
  final class FloatImpl extends FloatVectorValues implements DedupVectorValues {
    private final FlatVectorsScorer vectorsScorer;
    private final VectorSimilarityFunction function;
    private final FloatVectorValues fieldView;
    private final FloatVectorValues groupView;
    private final FieldOrdToGroupOrd fieldOrdToGroupOrd;
    private int[] scratch;

    FloatImpl(
        FlatVectorsScorer vectorsScorer,
        VectorSimilarityFunction function,
        FloatVectorValues fieldView,
        FloatVectorValues groupView,
        FieldOrdToGroupOrd fieldOrdToGroupOrd) {
      this.vectorsScorer = vectorsScorer;
      this.function = function;
      this.fieldView = fieldView;
      this.groupView = groupView;
      this.fieldOrdToGroupOrd = fieldOrdToGroupOrd;
      this.scratch = new int[SCRATCH_INITIAL_SIZE];
    }

    @Override
    public FloatVectorValues getGroupView() {
      return groupView;
    }

    @Override
    public FieldOrdToGroupOrd getFieldOrdToGroupOrd() {
      return fieldOrdToGroupOrd;
    }

    @Override
    public int ordToDoc(int ord) {
      return fieldView.ordToDoc(ord);
    }

    @Override
    public void prefetch(int[] ordsToPrefetch, int numOrds) throws IOException {
      if (scratch.length < ordsToPrefetch.length) { // grow if needed
        scratch = ArrayUtil.grow(scratch, ordsToPrefetch.length);
      }
      for (int i = 0; i < numOrds; i++) {
        scratch[i] = fieldOrdToGroupOrd.get(ordsToPrefetch[i]);
      }
      groupView.prefetch(scratch, numOrds);
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
      return groupView.vectorValue(fieldOrdToGroupOrd.get(ord));
    }

    @Override
    public int dimension() {
      return fieldView.dimension();
    }

    @Override
    public int size() {
      return fieldView.size();
    }

    @Override
    public FloatImpl copy() throws IOException {
      return new FloatImpl(
          vectorsScorer, function, fieldView.copy(), groupView.copy(), fieldOrdToGroupOrd.copy());
    }

    @Override
    public DocIndexIterator iterator() {
      return fieldView.iterator();
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      if (size() == 0) {
        return null;
      }
      FloatImpl copy = copy();
      DocIndexIterator indexIterator = copy.iterator();
      RandomVectorScorer vectorScorer = vectorsScorer.getRandomVectorScorer(function, copy, target);
      boolean isDense = copy.fieldView instanceof OffHeapFloatVectorValues.DenseOffHeapVectorValues;
      return new DedupVectorScorer(indexIterator, vectorScorer, isDense);
    }
  }

  static Float16Impl loadDedupFloat16s(
      FlatVectorsScorer vectorsScorer,
      VectorSimilarityFunction function,
      OrdToDocDISIReaderConfiguration configuration,
      int dimension,
      int groupNumVectors,
      IndexInput vectorData,
      long vectorDataOffset,
      long vectorDataSize,
      long fieldOrdToGroupOrdOffset,
      long fieldOrdToGroupOrdSize)
      throws IOException {

    final OffHeapFloat16VectorValues fieldView =
        OffHeapFloat16VectorValues.load(
            function, vectorsScorer, configuration, FLOAT16, dimension, 0, 0, vectorData);

    final OffHeapFloat16VectorValues groupView =
        new OffHeapFloat16VectorValues.DenseOffHeapVectorValues(
            dimension,
            groupNumVectors,
            vectorData.slice("group-slice", vectorDataOffset, vectorDataSize),
            fieldView.getVectorByteLength(),
            vectorsScorer,
            function);

    final FieldOrdToGroupOrd fieldOrdToGroupOrd =
        new FieldOrdToGroupOrdOffHeap(vectorData, fieldOrdToGroupOrdOffset, fieldOrdToGroupOrdSize);

    return new Float16Impl(vectorsScorer, function, fieldView, groupView, fieldOrdToGroupOrd);
  }

  /** {@link DedupVectorValues} over float16 vectors. */
  final class Float16Impl extends Float16VectorValues implements DedupVectorValues {
    private final FlatVectorsScorer vectorsScorer;
    private final VectorSimilarityFunction function;
    private final Float16VectorValues fieldView;
    private final Float16VectorValues groupView;
    private final FieldOrdToGroupOrd fieldOrdToGroupOrd;
    private int[] scratch;

    Float16Impl(
        FlatVectorsScorer vectorsScorer,
        VectorSimilarityFunction function,
        Float16VectorValues fieldView,
        Float16VectorValues groupView,
        FieldOrdToGroupOrd fieldOrdToGroupOrd) {
      this.vectorsScorer = vectorsScorer;
      this.function = function;
      this.fieldView = fieldView;
      this.groupView = groupView;
      this.fieldOrdToGroupOrd = fieldOrdToGroupOrd;
      this.scratch = new int[SCRATCH_INITIAL_SIZE];
    }

    @Override
    public Float16VectorValues getGroupView() {
      return groupView;
    }

    @Override
    public FieldOrdToGroupOrd getFieldOrdToGroupOrd() {
      return fieldOrdToGroupOrd;
    }

    @Override
    public int ordToDoc(int ord) {
      return fieldView.ordToDoc(ord);
    }

    @Override
    public void prefetch(int[] ordsToPrefetch, int numOrds) throws IOException {
      if (scratch.length < ordsToPrefetch.length) { // grow if needed
        scratch = ArrayUtil.grow(scratch, ordsToPrefetch.length);
      }
      for (int i = 0; i < numOrds; i++) {
        scratch[i] = fieldOrdToGroupOrd.get(ordsToPrefetch[i]);
      }
      groupView.prefetch(scratch, numOrds);
    }

    @Override
    public short[] vectorValue(int ord) throws IOException {
      return groupView.vectorValue(fieldOrdToGroupOrd.get(ord));
    }

    @Override
    public int dimension() {
      return fieldView.dimension();
    }

    @Override
    public int size() {
      return fieldView.size();
    }

    @Override
    public Float16Impl copy() throws IOException {
      return new Float16Impl(
          vectorsScorer, function, fieldView.copy(), groupView.copy(), fieldOrdToGroupOrd.copy());
    }

    @Override
    public DocIndexIterator iterator() {
      return fieldView.iterator();
    }

    @Override
    public VectorScorer scorer(short[] target) throws IOException {
      if (size() == 0) {
        return null;
      }
      Float16Impl copy = copy();
      DocIndexIterator indexIterator = copy.iterator();
      RandomVectorScorer vectorScorer = vectorsScorer.getRandomVectorScorer(function, copy, target);
      boolean isDense =
          copy.fieldView instanceof OffHeapFloat16VectorValues.DenseOffHeapVectorValues;
      return new DedupVectorScorer(indexIterator, vectorScorer, isDense);
    }
  }

  record DedupVectorScorer(
      KnnVectorValues.DocIndexIterator indexIterator,
      RandomVectorScorer vectorScorer,
      boolean isDense)
      implements VectorScorer {

    @Override
    public float score() throws IOException {
      return vectorScorer.score(indexIterator.index());
    }

    @Override
    public DocIdSetIterator iterator() {
      return indexIterator;
    }

    @Override
    public Bulk bulk(DocIdSetIterator matchingDocs) {
      if (isDense) {
        return fromRandomScorerDense(vectorScorer, indexIterator, matchingDocs);
      } else {
        return fromRandomScorerSparse(vectorScorer, indexIterator, matchingDocs);
      }
    }
  }

  /** On-heap map used during a flush, backed directly by the buffered ordinals. */
  record FieldOrdToGroupOrdArrayList(IntArrayList fieldOrdToGroupOrd)
      implements FieldOrdToGroupOrd {

    @Override
    public int get(int ord) {
      return fieldOrdToGroupOrd.get(ord);
    }

    @Override
    public FieldOrdToGroupOrd copy() {
      throw new UnsupportedOperationException("not meant for copying");
    }
  }

  /** On-heap map used during a sorted flush, indirecting through a new-to-old ordinal map. */
  record FieldOrdToGroupOrdMappedArrayList(int[] map, IntArrayList fieldOrdToGroupOrd)
      implements FieldOrdToGroupOrd {

    @Override
    public int get(int ord) {
      return fieldOrdToGroupOrd.get(map[ord]);
    }

    @Override
    public FieldOrdToGroupOrd copy() {
      throw new UnsupportedOperationException("not meant for copying");
    }
  }

  /** Off-heap map used while reading, backed by a {@link DirectReader}. */
  record FieldOrdToGroupOrdOffHeap(
      IndexInput vectorData,
      long fieldOrdToGroupOrdOffset,
      long fieldOrdToGroupOrdSize,
      LongValues values)
      implements FieldOrdToGroupOrd {

    FieldOrdToGroupOrdOffHeap(
        IndexInput vectorData, long fieldOrdToGroupOrdOffset, long fieldOrdToGroupOrdSize)
        throws IOException {
      RandomAccessInput slice =
          vectorData.randomAccessSlice(fieldOrdToGroupOrdOffset, fieldOrdToGroupOrdSize);
      LongValues values = DirectReader.getInstance(slice, FIELD_ORD_TO_GROUP_ORD_BITS_PER_VALUE);
      this(vectorData, fieldOrdToGroupOrdOffset, fieldOrdToGroupOrdSize, values);
    }

    @Override
    public int get(int v) {
      return (int) values.get(v);
    }

    @Override
    public FieldOrdToGroupOrd copy() throws IOException {
      return new FieldOrdToGroupOrdOffHeap(
          vectorData.clone(), fieldOrdToGroupOrdOffset, fieldOrdToGroupOrdSize);
    }
  }
}
