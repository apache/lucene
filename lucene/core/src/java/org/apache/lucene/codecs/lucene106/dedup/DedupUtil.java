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
package org.apache.lucene.codecs.lucene106.dedup;

import static org.apache.lucene.index.VectorEncoding.BYTE;
import static org.apache.lucene.index.VectorEncoding.FLOAT16;
import static org.apache.lucene.index.VectorEncoding.FLOAT32;
import static org.apache.lucene.util.StringHelper.GOOD_FAST_HASH_SEED;
import static org.apache.lucene.util.StringHelper.murmurhash3_x64_128;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene95.OffHeapByteVectorValues;
import org.apache.lucene.codecs.lucene95.OffHeapFloat16VectorValues;
import org.apache.lucene.codecs.lucene95.OffHeapFloatVectorValues;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.LongValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.packed.DirectReader;
import org.apache.lucene.util.packed.DirectWriter;

/**
 * Shared helpers for the de-duplicating flat format: reading / writing field and group metadata,
 * vector hashing and alignment, and the {@link DedupVectorValues} views used on the read path.
 *
 * @lucene.experimental
 */
final class DedupUtil {

  private static final int ORD_TO_DOC_DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

  private static final int END_MARKER = -1;

  private static final int FIELD_ORD_TO_GROUP_ORD_ALIGN_BYTES = 4;

  // TODO: This is the number of bits used to write each group ordinal in the index-backed per-field
  //  FieldOrdToGroupOrd mapping. Evaluate using fewer bits to reduce index size, at the expense of
  //  costlier lookups.
  private static final int FIELD_ORD_TO_GROUP_ORD_BITS_PER_VALUE = 32;

  /** Initial allocation size for internal re-used int[] scratch buffers. */
  static final int SCRATCH_INITIAL_SIZE = 16;

  /** Key used to group vectors (dimension + encoding). */
  record GroupKey(int dimension, VectorEncoding encoding) {
    GroupKey(FieldInfo fieldInfo) {
      this(fieldInfo.getVectorDimension(), fieldInfo.getVectorEncoding());
    }
  }

  /**
   * Vector values that share a single copy of each distinct vector across the documents and fields
   * that reference it.
   *
   * <p>Every instance is backed by two views: the {@code fieldView} maps ordinals to docs and
   * drives iteration (one entry per document), while the {@code groupView} holds the de-duplicated
   * vectors (one entry per distinct vector). {@code fieldOrdToGroupOrd} translates a document
   * ordinal in the field into its group ordinal.
   */
  sealed interface DedupVectorValues {
    /** The dense view over distinct vectors, indexed by group ordinal. */
    KnnVectorValues getGroupView();

    /** Maps a per-document ordinal to its group ordinal in {@link #getGroupView()}. */
    FieldOrdToGroupOrd getFieldOrdToGroupOrd();
  }

  /**
   * Maps a field's per-document ordinal to the ordinal of its (shared) vector within the group.
   * Backed on-heap while writing and off-heap while reading.
   */
  sealed interface FieldOrdToGroupOrd {
    int get(int ord);

    FieldOrdToGroupOrd copy() throws IOException;
  }

  record GroupInfo(
      int groupOrd,
      int dimension,
      VectorEncoding encoding,
      int groupSize,
      long vectorDataOffset,
      long vectorDataSize) {}

  static void writeGroupInfo(IndexOutput meta, GroupInfo groupInfo) throws IOException {
    meta.writeInt(groupInfo.groupOrd);
    meta.writeInt(groupInfo.dimension);
    meta.writeInt(groupInfo.encoding.ordinal());
    meta.writeInt(groupInfo.groupSize);
    meta.writeLong(groupInfo.vectorDataOffset);
    meta.writeLong(groupInfo.vectorDataSize);
  }

  static void writeEndOfGroups(IndexOutput meta) throws IOException {
    meta.writeInt(END_MARKER);
  }

  static GroupInfo readGroupInfo(IndexInput meta) throws IOException {
    int groupOrd = meta.readInt();
    if (groupOrd == END_MARKER) {
      return null;
    }

    int dimension = meta.readInt();
    VectorEncoding encoding = VectorEncoding.values()[meta.readInt()];
    int groupSize = meta.readInt();
    long vectorDataOffset = meta.readLong();
    long vectorDataSize = meta.readLong();

    return new GroupInfo(
        groupOrd, dimension, encoding, groupSize, vectorDataOffset, vectorDataSize);
  }

  record WriteFieldInfo(
      int fieldNumber,
      VectorSimilarityFunction function,
      int dimension,
      VectorEncoding encoding,
      int groupOrd,
      int vectorCount,
      int maxDoc,
      DocsWithFieldSet docs,
      FieldOrdToGroupOrd fieldOrdToGroupOrd) {}

  static void writeFieldInfo(IndexOutput meta, IndexOutput vectorData, WriteFieldInfo fieldInfo)
      throws IOException {

    meta.writeInt(fieldInfo.fieldNumber);
    meta.writeInt(fieldInfo.function.ordinal());
    meta.writeInt(fieldInfo.dimension);
    meta.writeInt(fieldInfo.encoding.ordinal());
    meta.writeInt(fieldInfo.groupOrd);
    meta.writeInt(fieldInfo.vectorCount);

    // write ordToDoc
    OrdToDocDISIReaderConfiguration.writeStoredMeta(
        ORD_TO_DOC_DIRECT_MONOTONIC_BLOCK_SHIFT,
        meta,
        vectorData,
        fieldInfo.vectorCount,
        fieldInfo.maxDoc,
        fieldInfo.docs);

    // write fieldOrdToGroupOrd
    long fieldOrdToGroupOrdOffset = vectorData.alignFilePointer(FIELD_ORD_TO_GROUP_ORD_ALIGN_BYTES);
    DirectWriter writer =
        DirectWriter.getInstance(
            vectorData, fieldInfo.vectorCount, FIELD_ORD_TO_GROUP_ORD_BITS_PER_VALUE);
    for (int i = 0; i < fieldInfo.vectorCount; i++) {
      writer.add(fieldInfo.fieldOrdToGroupOrd.get(i));
    }
    writer.finish();
    long fieldOrdToGroupOrdSize = vectorData.getFilePointer() - fieldOrdToGroupOrdOffset;

    meta.writeLong(fieldOrdToGroupOrdOffset);
    meta.writeLong(fieldOrdToGroupOrdSize);
  }

  static void writeEndOfFields(IndexOutput meta) throws IOException {
    meta.writeInt(END_MARKER);
  }

  record ReadFieldInfo(
      int fieldNumber,
      VectorSimilarityFunction function,
      int dimension,
      VectorEncoding encoding,
      int groupOrd,
      int vectorCount,
      OrdToDocDISIReaderConfiguration ordToDoc,
      long fieldOrdToGroupOrdOffset,
      long fieldOrdToGroupOrdSize) {}

  static ReadFieldInfo readFieldInfo(IndexInput meta) throws IOException {

    int fieldNumber = meta.readInt();
    if (fieldNumber == END_MARKER) {
      return null;
    }

    VectorSimilarityFunction function = VectorSimilarityFunction.values()[meta.readInt()];
    int dimension = meta.readInt();
    VectorEncoding encoding = VectorEncoding.values()[meta.readInt()];
    int groupOrd = meta.readInt();
    int vectorCount = meta.readInt();
    OrdToDocDISIReaderConfiguration ordToDoc =
        OrdToDocDISIReaderConfiguration.fromStoredMeta(meta, vectorCount);
    long fieldOrdToGroupOrdOffset = meta.readLong();
    long fieldOrdToGroupOrdSize = meta.readLong();

    return new ReadFieldInfo(
        fieldNumber,
        function,
        dimension,
        encoding,
        groupOrd,
        vectorCount,
        ordToDoc,
        fieldOrdToGroupOrdOffset,
        fieldOrdToGroupOrdSize);
  }

  static long hashBytes(byte[] bytes) {
    return murmurhash3_x64_128(bytes, 0, bytes.length, GOOD_FAST_HASH_SEED)[0];
  }

  static long alignBytes(IndexOutput output, VectorEncoding encoding) throws IOException {
    int alignBytes =
        switch (encoding) {
          case BYTE -> 4;
          case FLOAT32, FLOAT16 -> 64;
        };
    return output.alignFilePointer(alignBytes);
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
      return new FieldOrdToGroupOrdArrayList(fieldOrdToGroupOrd);
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
      return new FieldOrdToGroupOrdMappedArrayList(map, fieldOrdToGroupOrd);
    }
  }

  /** Off-heap map used while reading, backed by a {@link DirectReader}. */
  static final class FieldOrdToGroupOrdOffHeap implements FieldOrdToGroupOrd {
    private final IndexInput vectorData;
    private final long fieldOrdToGroupOrdOffset;
    private final long fieldOrdToGroupOrdSize;
    private final LongValues values;

    FieldOrdToGroupOrdOffHeap(
        IndexInput vectorData, long fieldOrdToGroupOrdOffset, long fieldOrdToGroupOrdSize)
        throws IOException {
      this.vectorData = vectorData;
      this.fieldOrdToGroupOrdOffset = fieldOrdToGroupOrdOffset;
      this.fieldOrdToGroupOrdSize = fieldOrdToGroupOrdSize;

      RandomAccessInput slice =
          vectorData.randomAccessSlice(fieldOrdToGroupOrdOffset, fieldOrdToGroupOrdSize);
      this.values = DirectReader.getInstance(slice, FIELD_ORD_TO_GROUP_ORD_BITS_PER_VALUE);
    }

    @Override
    public int get(int v) {
      return (int) values.get(v);
    }

    @Override
    public FieldOrdToGroupOrd copy() throws IOException {
      return new FieldOrdToGroupOrdOffHeap(
          vectorData, fieldOrdToGroupOrdOffset, fieldOrdToGroupOrdSize);
    }
  }

  static ByteVectorValues loadDedupBytes(
      FlatVectorsScorer vectorsScorer,
      VectorSimilarityFunction function,
      OrdToDocDISIReaderConfiguration configuration,
      int dimension,
      int groupSize,
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
            groupSize,
            vectorData.slice("group-slice", vectorDataOffset, vectorDataSize),
            fieldView.getVectorByteLength(),
            vectorsScorer,
            function);

    final FieldOrdToGroupOrd fieldOrdToGroupOrd =
        new FieldOrdToGroupOrdOffHeap(vectorData, fieldOrdToGroupOrdOffset, fieldOrdToGroupOrdSize);

    return new ByteImpl(vectorsScorer, function, fieldView, groupView, fieldOrdToGroupOrd);
  }

  /** {@link DedupVectorValues} over byte vectors. */
  private static final class ByteImpl extends ByteVectorValues implements DedupVectorValues {
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
      DocIndexIterator iterator = copy.iterator();
      RandomVectorScorer vectorScorer = vectorsScorer.getRandomVectorScorer(function, copy, target);
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return vectorScorer.score(iterator.index());
        }

        @Override
        public DocIdSetIterator iterator() {
          return iterator;
        }

        @Override
        public Bulk bulk(DocIdSetIterator matchingDocs) {
          return Bulk.fromRandomScorerDense(vectorScorer, iterator, matchingDocs);
        }
      };
    }
  }

  static FloatVectorValues loadDedupFloats(
      FlatVectorsScorer vectorsScorer,
      VectorSimilarityFunction function,
      OrdToDocDISIReaderConfiguration configuration,
      int dimension,
      int groupSize,
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
            groupSize,
            vectorData.slice("group-slice", vectorDataOffset, vectorDataSize),
            fieldView.getVectorByteLength(),
            vectorsScorer,
            function);

    final FieldOrdToGroupOrd fieldOrdToGroupOrd =
        new FieldOrdToGroupOrdOffHeap(vectorData, fieldOrdToGroupOrdOffset, fieldOrdToGroupOrdSize);

    return new FloatImpl(vectorsScorer, function, fieldView, groupView, fieldOrdToGroupOrd);
  }

  /** {@link DedupVectorValues} over float vectors. */
  private static final class FloatImpl extends FloatVectorValues implements DedupVectorValues {
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
      DocIndexIterator iterator = copy.iterator();
      RandomVectorScorer vectorScorer = vectorsScorer.getRandomVectorScorer(function, copy, target);
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return vectorScorer.score(iterator.index());
        }

        @Override
        public DocIdSetIterator iterator() {
          return iterator;
        }

        @Override
        public Bulk bulk(DocIdSetIterator matchingDocs) {
          return Bulk.fromRandomScorerDense(vectorScorer, iterator, matchingDocs);
        }
      };
    }
  }

  static Float16VectorValues loadDedupFloat16s(
      FlatVectorsScorer vectorsScorer,
      VectorSimilarityFunction function,
      OrdToDocDISIReaderConfiguration configuration,
      int dimension,
      int groupSize,
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
            groupSize,
            vectorData.slice("group-slice", vectorDataOffset, vectorDataSize),
            fieldView.getVectorByteLength(),
            vectorsScorer,
            function);

    final FieldOrdToGroupOrd fieldOrdToGroupOrd =
        new FieldOrdToGroupOrdOffHeap(vectorData, fieldOrdToGroupOrdOffset, fieldOrdToGroupOrdSize);

    return new Float16Impl(vectorsScorer, function, fieldView, groupView, fieldOrdToGroupOrd);
  }

  /** {@link DedupVectorValues} over float16 vectors. */
  private static final class Float16Impl extends Float16VectorValues implements DedupVectorValues {
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
      DocIndexIterator iterator = copy.iterator();
      RandomVectorScorer vectorScorer = vectorsScorer.getRandomVectorScorer(function, copy, target);
      return new VectorScorer() {
        @Override
        public float score() throws IOException {
          return vectorScorer.score(iterator.index());
        }

        @Override
        public DocIdSetIterator iterator() {
          return iterator;
        }

        @Override
        public Bulk bulk(DocIdSetIterator matchingDocs) {
          return Bulk.fromRandomScorerDense(vectorScorer, iterator, matchingDocs);
        }
      };
    }
  }
}
