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
import static org.apache.lucene.index.VectorEncoding.FLOAT32;
import static org.apache.lucene.util.StringHelper.GOOD_FAST_HASH_SEED;
import static org.apache.lucene.util.StringHelper.murmurhash3_x64_128;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene95.OffHeapByteVectorValues;
import org.apache.lucene.codecs.lucene95.OffHeapFloatVectorValues;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
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

  private static final int DIRECT_MONOTONIC_BLOCK_SHIFT = 16;

  private static final int END_MARKER = -1;

  private static final int ORD_TO_VEC_ALIGN_BYTES = 4;

  // TODO: Evaluate using fewer bits.
  private static final int ORD_TO_VEC_BITS_PER_VALUE = 32;

  static final int ORD_UNKNOWN = -1;

  static final int SCRATCH_SIZE = 16;

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
   * <p>Every instance is backed by two views: the {@code delegate} maps ordinals to docs and drives
   * iteration (one entry per document), while the {@code groupView} holds the de-duplicated vectors
   * (one entry per distinct vector). {@code ordToVecOrd} translates a document ordinal into its
   * group ordinal.
   */
  sealed interface DedupVectorValues {
    /** The dense view over distinct vectors, indexed by group ordinal. */
    KnnVectorValues getGroupView();

    /** Maps a per-document ordinal to its group ordinal in {@link #getGroupView()}. */
    OrdToVecOrd getOrdToVecOrd();
  }

  /**
   * Maps a field's per-document ordinal to the ordinal of its (shared) vector within the group.
   * Backed on-heap while writing and off-heap while reading.
   */
  sealed interface OrdToVecOrd {
    int get(int ord);

    OrdToVecOrd copy() throws IOException;
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
      OrdToVecOrd ordToVecOrd) {}

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
        DIRECT_MONOTONIC_BLOCK_SHIFT,
        meta,
        vectorData,
        fieldInfo.vectorCount,
        fieldInfo.maxDoc,
        fieldInfo.docs);

    // write ordToVec
    long ordToVecOffset = vectorData.alignFilePointer(ORD_TO_VEC_ALIGN_BYTES);
    DirectWriter writer =
        DirectWriter.getInstance(vectorData, fieldInfo.vectorCount, ORD_TO_VEC_BITS_PER_VALUE);
    for (int i = 0; i < fieldInfo.vectorCount; i++) {
      writer.add(fieldInfo.ordToVecOrd.get(i));
    }
    writer.finish();
    long ordToVecSize = vectorData.getFilePointer() - ordToVecOffset;

    meta.writeLong(ordToVecOffset);
    meta.writeLong(ordToVecSize);
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
      long ordToVecOffset,
      long ordToVecSize) {}

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
    long ordToVecOffset = meta.readLong();
    long ordToVecSize = meta.readLong();

    return new ReadFieldInfo(
        fieldNumber,
        function,
        dimension,
        encoding,
        groupOrd,
        vectorCount,
        ordToDoc,
        ordToVecOffset,
        ordToVecSize);
  }

  static long hashBytes(byte[] bytes) {
    return murmurhash3_x64_128(bytes, 0, bytes.length, GOOD_FAST_HASH_SEED)[0];
  }

  static long alignBytes(IndexOutput output, VectorEncoding encoding) throws IOException {
    int alignBytes =
        switch (encoding) {
          case BYTE -> 4;
          case FLOAT32 -> 64;
        };
    return output.alignFilePointer(alignBytes);
  }

  /** On-heap map used during a flush, backed directly by the buffered ordinals. */
  record OrdToVecOrdArrayList(IntArrayList ordToVecOrd) implements OrdToVecOrd {
    @Override
    public int get(int ord) {
      return ordToVecOrd.get(ord);
    }

    @Override
    public OrdToVecOrd copy() {
      return new OrdToVecOrdArrayList(ordToVecOrd);
    }
  }

  /** On-heap map used during a sorted flush, indirecting through a new-to-old ordinal map. */
  record OrdToVecOrdMappedArrayList(int[] map, IntArrayList ordToVecOrd) implements OrdToVecOrd {
    @Override
    public int get(int ord) {
      return ordToVecOrd.get(map[ord]);
    }

    @Override
    public OrdToVecOrd copy() {
      return new OrdToVecOrdMappedArrayList(map, ordToVecOrd);
    }
  }

  /** Off-heap map used while reading, backed by a {@link DirectReader}. */
  static final class OrdToVecOrdOffHeap implements OrdToVecOrd {
    private final IndexInput vectorData;
    private final long ordToVecOffset;
    private final long ordToVecSize;
    private final LongValues values;

    OrdToVecOrdOffHeap(IndexInput vectorData, long ordToVecOffset, long ordToVecSize)
        throws IOException {
      this.vectorData = vectorData;
      this.ordToVecOffset = ordToVecOffset;
      this.ordToVecSize = ordToVecSize;

      RandomAccessInput slice = vectorData.randomAccessSlice(ordToVecOffset, ordToVecSize);
      this.values = DirectReader.getInstance(slice, ORD_TO_VEC_BITS_PER_VALUE);
    }

    @Override
    public int get(int v) {
      return (int) values.get(v);
    }

    @Override
    public OrdToVecOrd copy() throws IOException {
      return new OrdToVecOrdOffHeap(vectorData, ordToVecOffset, ordToVecSize);
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
      long ordToVecOffset,
      long ordToVecSize)
      throws IOException {

    final OffHeapByteVectorValues delegate =
        OffHeapByteVectorValues.load(
            function, vectorsScorer, configuration, BYTE, dimension, 0, 0, vectorData);

    final OffHeapByteVectorValues groupView =
        new OffHeapByteVectorValues.DenseOffHeapVectorValues(
            dimension,
            groupSize,
            vectorData.slice("group-slice", vectorDataOffset, vectorDataSize),
            delegate.getVectorByteLength(),
            vectorsScorer,
            function);

    final OrdToVecOrd ordToVecOrd =
        new OrdToVecOrdOffHeap(vectorData, ordToVecOffset, ordToVecSize);

    return new ByteImpl(vectorsScorer, function, delegate, groupView, ordToVecOrd);
  }

  /** {@link DedupVectorValues} over byte vectors. */
  private static final class ByteImpl extends ByteVectorValues implements DedupVectorValues {
    private final FlatVectorsScorer vectorsScorer;
    private final VectorSimilarityFunction function;
    private final ByteVectorValues delegate;
    private final ByteVectorValues groupView;
    private final OrdToVecOrd ordToVecOrd;
    private int[] scratch;

    ByteImpl(
        FlatVectorsScorer vectorsScorer,
        VectorSimilarityFunction function,
        ByteVectorValues delegate,
        ByteVectorValues groupView,
        OrdToVecOrd ordToVecOrd) {
      this.vectorsScorer = vectorsScorer;
      this.function = function;
      this.delegate = delegate;
      this.groupView = groupView;
      this.ordToVecOrd = ordToVecOrd;
      this.scratch = new int[SCRATCH_SIZE];
    }

    @Override
    public ByteVectorValues getGroupView() {
      return groupView;
    }

    @Override
    public OrdToVecOrd getOrdToVecOrd() {
      return ordToVecOrd;
    }

    @Override
    public int ordToDoc(int ord) {
      return delegate.ordToDoc(ord);
    }

    @Override
    public void prefetch(int[] ordsToPrefetch, int numOrds) throws IOException {
      if (scratch.length < ordsToPrefetch.length) { // grow if needed
        scratch = ArrayUtil.grow(scratch, ordsToPrefetch.length);
      }
      for (int i = 0; i < numOrds; i++) {
        scratch[i] = ordToVecOrd.get(ordsToPrefetch[i]);
      }
      groupView.prefetch(scratch, numOrds);
    }

    @Override
    public byte[] vectorValue(int ord) throws IOException {
      return groupView.vectorValue(ordToVecOrd.get(ord));
    }

    @Override
    public int dimension() {
      return delegate.dimension();
    }

    @Override
    public int size() {
      return delegate.size();
    }

    @Override
    public ByteImpl copy() throws IOException {
      return new ByteImpl(
          vectorsScorer, function, delegate.copy(), groupView.copy(), ordToVecOrd.copy());
    }

    @Override
    public DocIndexIterator iterator() {
      return delegate.iterator();
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
      long ordToVecOffset,
      long ordToVecSize)
      throws IOException {

    final OffHeapFloatVectorValues delegate =
        OffHeapFloatVectorValues.load(
            function, vectorsScorer, configuration, FLOAT32, dimension, 0, 0, vectorData);

    final OffHeapFloatVectorValues groupView =
        new OffHeapFloatVectorValues.DenseOffHeapVectorValues(
            dimension,
            groupSize,
            vectorData.slice("group-slice", vectorDataOffset, vectorDataSize),
            delegate.getVectorByteLength(),
            vectorsScorer,
            function);

    final OrdToVecOrd ordToVecOrd =
        new OrdToVecOrdOffHeap(vectorData, ordToVecOffset, ordToVecSize);

    return new FloatImpl(vectorsScorer, function, delegate, groupView, ordToVecOrd);
  }

  /** {@link DedupVectorValues} over float vectors. */
  private static final class FloatImpl extends FloatVectorValues implements DedupVectorValues {
    private final FlatVectorsScorer vectorsScorer;
    private final VectorSimilarityFunction function;
    private final FloatVectorValues delegate;
    private final FloatVectorValues groupView;
    private final OrdToVecOrd ordToVecOrd;
    private int[] scratch;

    FloatImpl(
        FlatVectorsScorer vectorsScorer,
        VectorSimilarityFunction function,
        FloatVectorValues delegate,
        FloatVectorValues groupView,
        OrdToVecOrd ordToVecOrd) {
      this.vectorsScorer = vectorsScorer;
      this.function = function;
      this.delegate = delegate;
      this.groupView = groupView;
      this.ordToVecOrd = ordToVecOrd;
      this.scratch = new int[SCRATCH_SIZE];
    }

    @Override
    public FloatVectorValues getGroupView() {
      return groupView;
    }

    @Override
    public OrdToVecOrd getOrdToVecOrd() {
      return ordToVecOrd;
    }

    @Override
    public int ordToDoc(int ord) {
      return delegate.ordToDoc(ord);
    }

    @Override
    public void prefetch(int[] ordsToPrefetch, int numOrds) throws IOException {
      if (scratch.length < ordsToPrefetch.length) { // grow if needed
        scratch = ArrayUtil.grow(scratch, ordsToPrefetch.length);
      }
      for (int i = 0; i < numOrds; i++) {
        scratch[i] = ordToVecOrd.get(ordsToPrefetch[i]);
      }
      groupView.prefetch(scratch, numOrds);
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
      return groupView.vectorValue(ordToVecOrd.get(ord));
    }

    @Override
    public int dimension() {
      return delegate.dimension();
    }

    @Override
    public int size() {
      return delegate.size();
    }

    @Override
    public FloatImpl copy() throws IOException {
      return new FloatImpl(
          vectorsScorer, function, delegate.copy(), groupView.copy(), ordToVecOrd.copy());
    }

    @Override
    public DocIndexIterator iterator() {
      return delegate.iterator();
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
}
