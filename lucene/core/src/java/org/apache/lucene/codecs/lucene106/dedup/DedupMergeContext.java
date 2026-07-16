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

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.apache.lucene.codecs.lucene106.dedup.DedupUtil.ORD_UNKNOWN;
import static org.apache.lucene.codecs.lucene106.dedup.DedupUtil.alignBytes;
import static org.apache.lucene.codecs.lucene106.dedup.DedupUtil.hashBytes;
import static org.apache.lucene.codecs.lucene106.dedup.DedupUtil.writeEndOfFields;
import static org.apache.lucene.codecs.lucene106.dedup.DedupUtil.writeEndOfGroups;
import static org.apache.lucene.codecs.lucene106.dedup.DedupUtil.writeFieldInfo;
import static org.apache.lucene.codecs.lucene106.dedup.DedupUtil.writeGroupInfo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.lucene.codecs.lucene106.dedup.DedupUtil.DedupVectorValues;
import org.apache.lucene.codecs.lucene106.dedup.DedupUtil.GroupInfo;
import org.apache.lucene.codecs.lucene106.dedup.DedupUtil.GroupKey;
import org.apache.lucene.codecs.lucene106.dedup.DedupUtil.OrdToVecOrd;
import org.apache.lucene.codecs.lucene106.dedup.DedupUtil.OrdToVecOrdArrayList;
import org.apache.lucene.codecs.lucene106.dedup.DedupUtil.WriteFieldInfo;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.internal.hppc.ObjectCursor;
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

  void finish(IndexOutput meta, IndexOutput vectorData) throws IOException {

    // Evaluate compatible fields together for correct de-duplication
    Map<GroupKey, List<FieldData>> fieldGroups =
        fieldDataList.stream().collect(Collectors.groupingBy(FieldData::groupKey));

    Map<GroupKey, Integer> groupOrds = new HashMap<>();
    int groupOrd = 0;
    for (Map.Entry<GroupKey, List<FieldData>> entry : fieldGroups.entrySet()) {
      GroupKey groupKey = entry.getKey();
      long vectorDataOffset = alignBytes(vectorData, groupKey.encoding());

      DedupMergeGroup<?, ?> mergeGroup =
          switch (groupKey.encoding()) {
            case BYTE -> new ByteGroup();
            case FLOAT32 -> new FloatGroup(groupKey.dimension());
          };

      for (FieldData fieldData : entry.getValue()) {
        mergeGroup.processField(fieldData, vectorData);
      }

      int dimension = groupKey.dimension();
      VectorEncoding encoding = groupKey.encoding();
      int groupSize = mergeGroup.size();
      long vectorDataSize = vectorData.getFilePointer() - vectorDataOffset;

      GroupInfo groupInfo =
          new GroupInfo(groupOrd, dimension, encoding, groupSize, vectorDataOffset, vectorDataSize);
      writeGroupInfo(meta, groupInfo);

      groupOrds.put(groupKey, groupOrd);
      groupOrd++;
    }

    writeEndOfGroups(meta);

    for (FieldData fieldData : fieldDataList) {
      WriteFieldInfo fieldInfo =
          new WriteFieldInfo(
              fieldData.fieldInfo.number,
              fieldData.fieldInfo.getVectorSimilarityFunction(),
              fieldData.fieldInfo.getVectorDimension(),
              fieldData.fieldInfo.getVectorEncoding(),
              groupOrds.get(fieldData.groupKey),
              fieldData.ordToVecOrd.elementsCount,
              fieldData.maxDoc,
              fieldData.docsWithFieldSet,
              new OrdToVecOrdArrayList(fieldData.ordToVecOrd));
      writeFieldInfo(meta, vectorData, fieldInfo);
    }

    writeEndOfFields(meta);
  }

  abstract static sealed class DedupMergeGroup<T, U extends KnnVectorValues> extends DedupGroup<T> {
    abstract T vectorFrom(Sub<U> sub);

    void processField(FieldData fieldData, IndexOutput vectorData) throws IOException {
      @SuppressWarnings("unchecked")
      DocIDMerger<Sub<U>> merger = (DocIDMerger<Sub<U>>) fieldData.merger;

      // iterate merged docs one-by-one
      for (Sub<U> next = merger.next(); next != null; next = merger.next()) {
        T vector = vectorFrom(next);
        int groupSize = size();

        // add vector to group
        ObjectCursor<T> cursor = super.addUnique(vector);
        if (cursor.index == groupSize) { // new addition
          // already on-heap, write immediately to avoid another IO read
          byte[] bytes = serialize(groupSize);
          vectorData.writeBytes(bytes, bytes.length);
        }

        // record hit and ord in group
        fieldData.docsWithFieldSet.add(next.mappedDocID);
        fieldData.ordToVecOrd.add(cursor.index);
      }
    }
  }

  record ByteVector(ByteVectorValues values, int ord) implements IOSupplier<byte[]> {
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
        OrdToVecOrd ordToVecOrd = dedup.getOrdToVecOrd();
        return ordToVecOrd.get(vector.ord) == ordToVecOrd.get(other.ord);
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
      return SHALLOW_SIZE + super.ramBytesUsed() + size() * ByteVector.SHALLOW_SIZE;
    }
  }

  record FloatVector(FloatVectorValues values, int ord) implements IOSupplier<float[]> {
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
    private int lastOrd;

    FloatGroup(int dimension) {
      int length = dimension * Float.BYTES;
      this.bytes = new byte[length];
      this.buffer = ByteBuffer.wrap(bytes).order(LITTLE_ENDIAN).asFloatBuffer();
      this.lastOrd = ORD_UNKNOWN;
    }

    @Override
    FloatVector vectorFrom(Sub<FloatVectorValues> sub) {
      return new FloatVector(sub.values, sub.iterator.index());
    }

    @Override
    public long hash(FloatVector vector) throws IOException {
      // the vector needs to be converted to bytes to use a utility hash function.
      // the existing buffer is used for this conversion, so lastOrd is reset too.
      buffer.put(0, vector.get());
      lastOrd = ORD_UNKNOWN;
      return hashBytes(bytes);
    }

    @Override
    public boolean equals(FloatVector vector, FloatVector other) throws IOException {
      // Fast path: two docs from the same dedup source share a vector iff they map to the same
      // group ordinal, so we can compare ordinals without reading the vectors back.
      if (vector.values == other.values && vector.values instanceof DedupVectorValues dedup) {
        OrdToVecOrd ordToVecOrd = dedup.getOrdToVecOrd();
        return ordToVecOrd.get(vector.ord) == ordToVecOrd.get(other.ord);
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
      if (ord != lastOrd) {
        buffer.put(0, get(ord).get());
        lastOrd = ord;
      }
      return bytes;
    }

    @Override
    public long ramBytesUsed() {
      return SHALLOW_SIZE + super.ramBytesUsed() + size() * FloatVector.SHALLOW_SIZE;
    }
  }

  private record FieldData(
      FieldInfo fieldInfo,
      GroupKey groupKey,
      DocsWithFieldSet docsWithFieldSet,
      IntArrayList ordToVecOrd,
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
          };

      if (vectorValues == null) {
        continue;
      }

      subs.add(new Sub<>(mergeState.docMaps[i], vectorValues));
    }

    return DocIDMerger.of(subs, mergeState.needsIndexSort);
  }
}
