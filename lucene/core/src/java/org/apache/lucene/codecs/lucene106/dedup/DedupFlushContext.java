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
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.lucene106.dedup.DedupUtil.GroupInfo;
import org.apache.lucene.codecs.lucene106.dedup.DedupUtil.GroupKey;
import org.apache.lucene.codecs.lucene106.dedup.DedupUtil.OrdToVecOrd;
import org.apache.lucene.codecs.lucene106.dedup.DedupUtil.OrdToVecOrdArrayList;
import org.apache.lucene.codecs.lucene106.dedup.DedupUtil.OrdToVecOrdMappedArrayList;
import org.apache.lucene.codecs.lucene106.dedup.DedupUtil.WriteFieldInfo;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * Buffers vectors added during a flush and de-duplicates them in memory. Fields sharing a {@link
 * DedupUtil.GroupKey} intern into the same {@link DedupGroup}; on {@link #flush} each group's
 * distinct vectors are written once, followed by per-field metadata mapping document ordinals to
 * group ordinals.
 *
 * @lucene.experimental
 */
final class DedupFlushContext implements Accountable {
  private final Map<GroupKey, DedupGroup<?>> groups;
  private final List<FieldData> fieldDataList;

  DedupFlushContext() {
    this.groups = new HashMap<>();
    this.fieldDataList = new ArrayList<>();
  }

  private static DedupGroup<?> getGroup(GroupKey groupKey) {
    return switch (groupKey.encoding()) {
      case BYTE -> new ByteGroup(groupKey.dimension());
      case FLOAT32 -> new FloatGroup(groupKey.dimension());
    };
  }

  @Override
  public long ramBytesUsed() {
    long total = 0;
    for (DedupGroup<?> group : groups.values()) {
      total += group.ramBytesUsed();
    }
    for (FieldData data : fieldDataList) {
      total += data.ramBytesUsed();
    }
    return total;
  }

  FlatFieldVectorsWriter<?> addField(FieldInfo fieldInfo) {
    GroupKey groupKey = new GroupKey(fieldInfo);
    DedupGroup<?> group = groups.computeIfAbsent(groupKey, DedupFlushContext::getGroup);
    DedupFlatFieldVectorsWriter<?> fieldVectorsWriter = new DedupFlatFieldVectorsWriter<>(group);

    fieldDataList.add(new FieldData(fieldInfo, groupKey, fieldVectorsWriter));
    return fieldVectorsWriter;
  }

  void flush(IndexOutput meta, IndexOutput vectorData, int maxDoc, Sorter.DocMap sortMap)
      throws IOException {

    Map<GroupKey, Integer> groupOrds = new HashMap<>();

    int groupOrd = 0;
    for (Map.Entry<GroupKey, DedupGroup<?>> entry : groups.entrySet()) {
      GroupKey groupKey = entry.getKey();
      DedupGroup<?> group = entry.getValue();

      int groupSize = group.size();
      long vectorDataOffset = alignBytes(vectorData, groupKey.encoding());

      // TODO: Write in sorted order for faster merge? (with sequential IO)
      for (int ord = 0; ord < groupSize; ord++) {
        byte[] bytes = group.serialize(ord);
        vectorData.writeBytes(bytes, bytes.length);
      }
      long vectorDataSize = vectorData.getFilePointer() - vectorDataOffset;

      int dimension = groupKey.dimension();
      VectorEncoding encoding = groupKey.encoding();

      GroupInfo groupInfo =
          new GroupInfo(groupOrd, dimension, encoding, groupSize, vectorDataOffset, vectorDataSize);
      writeGroupInfo(meta, groupInfo);

      groupOrds.put(groupKey, groupOrd);
      groupOrd++;
    }

    writeEndOfGroups(meta);

    for (FieldData fieldData : fieldDataList) {
      fieldData.fieldWriter.finish();

      IntArrayList ordToVecOrd = fieldData.fieldWriter.getOrdToVecOrd();
      int vectorCount = ordToVecOrd.elementsCount;

      DocsWithFieldSet docs;
      OrdToVecOrd ordToVecFinal;
      if (sortMap == null) {
        docs = fieldData.fieldWriter.getDocsWithFieldSet();
        ordToVecFinal = new OrdToVecOrdArrayList(ordToVecOrd);
      } else {
        DocsWithFieldSet oldDocs = fieldData.fieldWriter.getDocsWithFieldSet();
        docs = new DocsWithFieldSet();
        int[] new2OldOrd = new int[vectorCount];
        KnnVectorsWriter.mapOldOrdToNewOrd(oldDocs, sortMap, null, new2OldOrd, docs);
        ordToVecFinal = new OrdToVecOrdMappedArrayList(new2OldOrd, ordToVecOrd);
      }

      WriteFieldInfo fieldInfo =
          new WriteFieldInfo(
              fieldData.fieldInfo.number,
              fieldData.fieldInfo.getVectorSimilarityFunction(),
              fieldData.fieldInfo.getVectorDimension(),
              fieldData.fieldInfo.getVectorEncoding(),
              groupOrds.get(fieldData.groupKey),
              vectorCount,
              maxDoc,
              docs,
              ordToVecFinal);
      writeFieldInfo(meta, vectorData, fieldInfo);
    }

    writeEndOfFields(meta);
  }

  static final class ByteGroup extends DedupGroup<byte[]> {
    private static final long SHALLOW_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(ByteGroup.class);
    private final long ramBytesPerVector;

    ByteGroup(int dimension) {
      ramBytesPerVector =
          RamUsageEstimator.NUM_BYTES_OBJECT_REF
              + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
              + dimension;
    }

    @Override
    public long hash(byte[] vector) {
      return hashBytes(vector);
    }

    @Override
    public boolean equals(byte[] vector, byte[] other) {
      return Arrays.equals(vector, other);
    }

    @Override
    public byte[] copy(byte[] vectorValue) {
      return vectorValue.clone();
    }

    @Override
    byte[] serialize(int ord) {
      return get(ord);
    }

    @Override
    public long ramBytesUsed() {
      return SHALLOW_SIZE + super.ramBytesUsed() + size() * ramBytesPerVector;
    }
  }

  static final class FloatGroup extends DedupGroup<float[]> {
    private static final long SHALLOW_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(FloatGroup.class);

    private final long ramBytesPerVector;
    private final byte[] bytes;
    private final FloatBuffer buffer;
    private int lastOrd;

    FloatGroup(int dimension) {
      int length = dimension * Float.BYTES;
      this.ramBytesPerVector =
          RamUsageEstimator.NUM_BYTES_OBJECT_REF
              + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
              + length;
      this.bytes = new byte[length];
      this.buffer = ByteBuffer.wrap(bytes).order(LITTLE_ENDIAN).asFloatBuffer();
      this.lastOrd = ORD_UNKNOWN;
    }

    @Override
    public long hash(float[] vector) {
      // the vector needs to be converted to bytes to use a utility hash function.
      // the existing buffer is used for this conversion, so lastOrd is reset too.
      buffer.put(0, vector);
      lastOrd = ORD_UNKNOWN;
      return hashBytes(bytes);
    }

    @Override
    public boolean equals(float[] vector, float[] other) {
      return Arrays.equals(vector, other);
    }

    @Override
    public float[] copy(float[] vectorValue) {
      return vectorValue.clone();
    }

    @Override
    byte[] serialize(int ord) {
      if (ord != lastOrd) {
        buffer.put(0, get(ord));
        lastOrd = ord;
      }
      return bytes;
    }

    @Override
    public long ramBytesUsed() {
      return SHALLOW_SIZE + super.ramBytesUsed() + size() * ramBytesPerVector;
    }
  }

  private record FieldData(
      FieldInfo fieldInfo, GroupKey groupKey, DedupFlatFieldVectorsWriter<?> fieldWriter)
      implements Accountable {

    @Override
    public long ramBytesUsed() {
      return fieldWriter.ramBytesUsed();
    }
  }
}
