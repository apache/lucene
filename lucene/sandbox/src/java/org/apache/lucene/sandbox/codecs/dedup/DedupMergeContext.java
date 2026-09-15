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

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.apache.lucene.sandbox.codecs.dedup.DedupUtil.alignBytes;
import static org.apache.lucene.sandbox.codecs.dedup.DedupUtil.hashBytes;
import static org.apache.lucene.sandbox.codecs.dedup.DedupUtil.writeEndMarker;
import static org.apache.lucene.sandbox.codecs.dedup.DedupUtil.writeFieldInfo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.nio.ShortBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.ObjectCursor;
import org.apache.lucene.sandbox.codecs.dedup.DedupUtil.GroupInfo;
import org.apache.lucene.sandbox.codecs.dedup.DedupUtil.GroupKey;
import org.apache.lucene.sandbox.codecs.dedup.DedupVectorValues.FieldOrdToGroupOrd;
import org.apache.lucene.sandbox.codecs.dedup.DedupVectorValues.FieldOrdToGroupOrdArrayList;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.IOSupplier;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Merges de-duplicated flat vectors from several segments. Fields sharing a {@link
 * DedupUtil.GroupKey} are merged into one group: their vectors are streamed in merged doc order
 * through a {@link DedupGroup}, so a vector is written the first time it is seen and later
 * occurrences (within or across fields) reuse that group ordinal. Vectors originating from a dedup
 * source are compared by ordinal to avoid reading them back.
 *
 * @lucene.experimental
 */
final class DedupMergeContext implements Accountable {
  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(DedupMergeContext.class);
  private final List<FieldData> fieldDataList;

  DedupMergeContext() {
    this.fieldDataList = new ArrayList<>();
  }

  @Override
  public long ramBytesUsed() {
    return SHALLOW_SIZE + fieldDataList.size() * FieldData.SHALLOW_SIZE;
  }

  void addField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    fieldDataList.add(
        new FieldData(
            fieldInfo,
            new GroupKey(fieldInfo),
            new DocsWithFieldSet(),
            new IntArrayList(),
            getVectorMerger(fieldInfo, mergeState),
            mergeState.segmentInfo.maxDoc()));
  }

  /**
   * Merges each group's distinct vectors, followed by per-field metadata. When {@code quantizer} is
   * non-null, a quantized copy of each applicable group is also written and its block location
   * appended to the group metadata — copying records from source segments in this format, and
   * re-reading a distinct vector through its handle to quantize it otherwise.
   */
  void finish(
      IndexOutput meta,
      IndexOutput vectorData,
      IndexOutput quantizedVectorData,
      DedupQuantizer quantizer)
      throws IOException {

    // Evaluate compatible fields together for correct de-duplication
    Map<GroupKey, List<FieldData>> fieldGroups =
        fieldDataList.stream().collect(Collectors.groupingBy(FieldData::groupKey));

    Map<GroupKey, Integer> groupOrds = new HashMap<>();
    int groupOrd = 0;
    for (Map.Entry<GroupKey, List<FieldData>> entry : fieldGroups.entrySet()) {
      GroupKey groupKey = entry.getKey();
      int dimension = groupKey.dimension();
      VectorEncoding encoding = groupKey.encoding();

      long vectorDataOffset = alignBytes(vectorData, encoding);

      DedupMergeGroup<?, ?> mergeGroup =
          switch (encoding) {
            case BYTE -> new ByteGroup();
            case FLOAT32 -> new FloatGroup(dimension);
            case FLOAT16 -> new Float16Group(dimension);
          };

      for (FieldData fieldData : entry.getValue()) {
        mergeGroup.processField(fieldData, vectorData);
      }

      int groupNumVectors = mergeGroup.numVectors();
      long vectorDataSize = vectorData.getFilePointer() - vectorDataOffset;

      GroupInfo groupInfo =
          new GroupInfo(
              groupOrd, dimension, encoding, groupNumVectors, vectorDataOffset, vectorDataSize);
      groupInfo.write(meta);

      if (quantizer != null) {
        if (mergeGroup instanceof FloatGroup floatGroup) {
          Set<DedupQuantizer.Flavor> flavors = EnumSet.noneOf(DedupQuantizer.Flavor.class);
          for (FieldData fieldData : entry.getValue()) {
            flavors.add(
                DedupQuantizer.Flavor.of(fieldData.fieldInfo.getVectorSimilarityFunction()));
          }
          quantizer.writeGroup(
              meta,
              quantizedVectorData,
              encoding,
              dimension,
              groupNumVectors,
              flavors,
              ord -> floatGroup.get(ord).get(),
              floatGroup::preQuantized);
        } else {
          DedupQuantizer.writeEmptyGroup(meta);
        }
      }

      groupOrds.put(groupKey, groupOrd);
      groupOrd++;
    }

    writeEndMarker(meta);

    for (FieldData fieldData : fieldDataList) {
      writeFieldInfo(
          meta,
          vectorData,
          fieldData.fieldInfo.number,
          fieldData.fieldInfo.getVectorSimilarityFunction(),
          fieldData.fieldInfo.getVectorDimension(),
          fieldData.fieldInfo.getVectorEncoding(),
          groupOrds.get(fieldData.groupKey),
          fieldData.fieldOrdToGroupOrd.elementsCount,
          fieldData.maxDoc,
          fieldData.docsWithFieldSet,
          new FieldOrdToGroupOrdArrayList(fieldData.fieldOrdToGroupOrd));
    }

    writeEndMarker(meta);
  }

  abstract static sealed class DedupMergeGroup<T, U extends KnnVectorValues> extends DedupGroup<T> {
    abstract T vectorFrom(Sub<U> sub);

    void processField(FieldData fieldData, IndexOutput vectorData) throws IOException {
      @SuppressWarnings("unchecked")
      DocIDMerger<Sub<U>> merger = (DocIDMerger<Sub<U>>) fieldData.merger;

      // iterate merged docs one-by-one
      for (Sub<U> next = merger.next(); next != null; next = merger.next()) {
        T vector = vectorFrom(next);
        int groupNumVectors = numVectors();

        // add vector to group
        ObjectCursor<T> cursor = addUnique(vector);
        if (cursor.index == groupNumVectors) { // new addition
          // already on-heap, write immediately to avoid another IO read
          byte[] bytes = serialize(groupNumVectors);
          vectorData.writeBytes(bytes, bytes.length);
        }

        // record hit and ord in group
        fieldData.docsWithFieldSet.add(next.mappedDocID);
        fieldData.fieldOrdToGroupOrd.add(cursor.index);
      }
    }
  }

  private record ByteVector(ByteVectorValues values, int ord) implements IOSupplier<byte[]> {
    private static final long SHALLOW_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(ByteVector.class);

    @Override
    public byte[] get() throws IOException {
      return values.vectorValue(ord);
    }
  }

  private static final class ByteGroup extends DedupMergeGroup<ByteVector, ByteVectorValues> {
    private static final long SHALLOW_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(ByteGroup.class);

    @Override
    ByteVector vectorFrom(Sub<ByteVectorValues> sub) {
      return new ByteVector(sub.values, sub.iterator.index());
    }

    @Override
    public long hash(ByteVector vector) throws IOException {
      return hashBytes(vector.get());
    }

    @Override
    public boolean equals(ByteVector vector, ByteVector other) throws IOException {
      // Fast path: two docs from the same dedup source share a vector iff they map to the same
      // group ordinal, so we can compare ordinals without reading the vectors back.
      if (vector.values == other.values && vector.values instanceof DedupVectorValues dedup) {
        FieldOrdToGroupOrd fieldOrdToGroupOrd = dedup.getFieldOrdToGroupOrd();
        return fieldOrdToGroupOrd.get(vector.ord) == fieldOrdToGroupOrd.get(other.ord);
      }
      byte[] a = vector.get();
      if (vector.values == other.values) {
        a = a.clone(); // same reader reuses one buffer; copy before reading the other vector
      }
      return Arrays.equals(a, other.get());
    }

    @Override
    public ByteVector copy(ByteVector vectorValue) {
      return vectorValue;
    }

    @Override
    byte[] serialize(int ord) throws IOException {
      return get(ord).get();
    }

    @Override
    public long ramBytesUsed() {
      return SHALLOW_SIZE + super.ramBytesUsed() + numVectors() * ByteVector.SHALLOW_SIZE;
    }
  }

  private record FloatVector(FloatVectorValues values, int ord) implements IOSupplier<float[]> {
    private static final long SHALLOW_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(FloatVector.class);

    @Override
    public float[] get() throws IOException {
      return values.vectorValue(ord);
    }
  }

  private static final class FloatGroup extends DedupMergeGroup<FloatVector, FloatVectorValues> {
    private static final long SHALLOW_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(FloatGroup.class);

    private final byte[] bytes;
    private final FloatBuffer buffer;

    FloatGroup(int dimension) {
      int length = dimension * Float.BYTES;
      this.bytes = new byte[length];
      this.buffer = ByteBuffer.wrap(bytes).order(LITTLE_ENDIAN).asFloatBuffer();
    }

    @Override
    FloatVector vectorFrom(Sub<FloatVectorValues> sub) {
      return new FloatVector(sub.values, sub.iterator.index());
    }

    /**
     * The already-quantized record of the distinct vector at a group ordinal, when its source
     * segment is in this format (data-blind quantization is a pure function of the raw vector, so
     * the record can be copied on merge instead of re-quantizing), or {@code null}.
     */
    DedupQuantizer.PreQuantized preQuantized(int ord) {
      FloatVector handle = get(ord);
      if (handle.values()
          instanceof DedupScalarQuantizedVectorValues.RawAndQuantizedValues rawAndQuantized) {
        DedupScalarQuantizedVectorValues.FieldValues quantized =
            rawAndQuantized.getQuantizedValues();
        return new DedupQuantizer.PreQuantized(quantized, quantized.flavor(), handle.ord());
      }
      return null;
    }

    @Override
    public long hash(FloatVector vector) throws IOException {
      buffer.put(0, vector.get());
      return hashBytes(bytes);
    }

    @Override
    public boolean equals(FloatVector vector, FloatVector other) throws IOException {
      // Fast path: two docs from the same dedup source share a vector iff they map to the same
      // group ordinal, so we can compare ordinals without reading the vectors back.
      if (vector.values == other.values && vector.values instanceof DedupVectorValues dedup) {
        FieldOrdToGroupOrd fieldOrdToGroupOrd = dedup.getFieldOrdToGroupOrd();
        return fieldOrdToGroupOrd.get(vector.ord) == fieldOrdToGroupOrd.get(other.ord);
      }
      float[] a = vector.get();
      if (vector.values == other.values) {
        a = a.clone(); // same reader reuses one buffer; copy before reading the other vector
      }
      return Arrays.equals(a, other.get());
    }

    @Override
    public FloatVector copy(FloatVector vectorValue) {
      return vectorValue;
    }

    @Override
    byte[] serialize(int ord) throws IOException {
      buffer.put(0, get(ord).get());
      return bytes;
    }

    @Override
    public long ramBytesUsed() {
      return SHALLOW_SIZE + super.ramBytesUsed() + numVectors() * FloatVector.SHALLOW_SIZE;
    }
  }

  private record Float16Vector(Float16VectorValues values, int ord) implements IOSupplier<short[]> {
    private static final long SHALLOW_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(Float16Vector.class);

    @Override
    public short[] get() throws IOException {
      return values.vectorValue(ord);
    }
  }

  private static final class Float16Group
      extends DedupMergeGroup<Float16Vector, Float16VectorValues> {
    private static final long SHALLOW_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(Float16Group.class);

    private final byte[] bytes;
    private final ShortBuffer buffer;

    Float16Group(int dimension) {
      int length = dimension * Short.BYTES;
      this.bytes = new byte[length];
      this.buffer = ByteBuffer.wrap(bytes).order(LITTLE_ENDIAN).asShortBuffer();
    }

    @Override
    Float16Vector vectorFrom(Sub<Float16VectorValues> sub) {
      return new Float16Vector(sub.values, sub.iterator.index());
    }

    @Override
    public long hash(Float16Vector vector) throws IOException {
      buffer.put(0, vector.get());
      return hashBytes(bytes);
    }

    @Override
    public boolean equals(Float16Vector vector, Float16Vector other) throws IOException {
      // Fast path: two docs from the same dedup source share a vector iff they map to the same
      // group ordinal, so we can compare ordinals without reading the vectors back.
      if (vector.values == other.values && vector.values instanceof DedupVectorValues dedup) {
        FieldOrdToGroupOrd fieldOrdToGroupOrd = dedup.getFieldOrdToGroupOrd();
        return fieldOrdToGroupOrd.get(vector.ord) == fieldOrdToGroupOrd.get(other.ord);
      }
      short[] a = vector.get();
      if (vector.values == other.values) {
        a = a.clone(); // same reader reuses one buffer; copy before reading the other vector
      }
      return Arrays.equals(a, other.get());
    }

    @Override
    public Float16Vector copy(Float16Vector vectorValue) {
      return vectorValue;
    }

    @Override
    byte[] serialize(int ord) throws IOException {
      buffer.put(0, get(ord).get());
      return bytes;
    }

    @Override
    public long ramBytesUsed() {
      return SHALLOW_SIZE + super.ramBytesUsed() + numVectors() * Float16Vector.SHALLOW_SIZE;
    }
  }

  private record FieldData(
      FieldInfo fieldInfo,
      GroupKey groupKey,
      DocsWithFieldSet docsWithFieldSet,
      IntArrayList fieldOrdToGroupOrd,
      DocIDMerger<?> merger,
      int maxDoc) {

    static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(FieldData.class);
  }

  private static class Sub<T extends KnnVectorValues> extends DocIDMerger.Sub {
    private final T values;
    private final KnnVectorValues.DocIndexIterator iterator;

    Sub(MergeState.DocMap docMap, T values) {
      super(docMap);
      this.values = values;
      iterator = values.iterator();
    }

    @Override
    public int nextDoc() throws IOException {
      return iterator.nextDoc();
    }
  }

  private static DocIDMerger<Sub<? extends KnnVectorValues>> getVectorMerger(
      FieldInfo fieldInfo, MergeState mergeState) throws IOException {

    List<Sub<? extends KnnVectorValues>> subs = new ArrayList<>();
    for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {

      if (mergeState.knnVectorsReaders[i] == null
          || mergeState.fieldInfos[i].fieldInfo(fieldInfo.name) == null
          || mergeState.fieldInfos[i].fieldInfo(fieldInfo.name).hasVectorValues() == false) {
        continue;
      }

      KnnVectorValues vectorValues =
          switch (fieldInfo.getVectorEncoding()) {
            case BYTE -> mergeState.knnVectorsReaders[i].getByteVectorValues(fieldInfo.name);
            case FLOAT32 -> mergeState.knnVectorsReaders[i].getFloatVectorValues(fieldInfo.name);
            case FLOAT16 -> mergeState.knnVectorsReaders[i].getFloat16VectorValues(fieldInfo.name);
          };

      if (vectorValues == null) {
        continue;
      }

      subs.add(new Sub<>(mergeState.docMaps[i], vectorValues));
    }

    return DocIDMerger.of(subs, mergeState.needsIndexSort);
  }
}
