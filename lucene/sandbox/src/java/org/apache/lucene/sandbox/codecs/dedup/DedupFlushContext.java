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
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.internal.hppc.IntArrayList;
import org.apache.lucene.sandbox.codecs.dedup.DedupUtil.GroupInfo;
import org.apache.lucene.sandbox.codecs.dedup.DedupUtil.GroupKey;
import org.apache.lucene.sandbox.codecs.dedup.DedupVectorValues.FieldOrdToGroupOrd;
import org.apache.lucene.sandbox.codecs.dedup.DedupVectorValues.FieldOrdToGroupOrdArrayList;
import org.apache.lucene.sandbox.codecs.dedup.DedupVectorValues.FieldOrdToGroupOrdMappedArrayList;
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
    int dimension = groupKey.dimension();
    return switch (groupKey.encoding()) {
      case BYTE -> new ByteGroup(dimension);
      case FLOAT32 -> new FloatGroup(dimension);
      case FLOAT16 -> new Float16Group(dimension);
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

  /**
   * Writes each group's distinct vectors once, followed by per-field metadata. When {@code
   * quantizer} is non-null, a quantized copy of each applicable group is also written and its block
   * location appended to the group metadata.
   */
  void flush(
      IndexOutput meta,
      IndexOutput vectorData,
      IndexOutput quantizedVectorData,
      int maxDoc,
      Sorter.DocMap sortMap,
      DedupQuantizer quantizer)
      throws IOException {

    Map<GroupKey, Integer> groupOrds = new HashMap<>();

    // quantization flavors referencing each group, derived from its fields' similarity functions
    Map<GroupKey, Set<DedupQuantizer.Flavor>> groupFlavors = new HashMap<>();
    if (quantizer != null) {
      for (FieldData fieldData : fieldDataList) {
        if (fieldData.fieldInfo.getVectorEncoding() == VectorEncoding.FLOAT32) {
          groupFlavors
              .computeIfAbsent(fieldData.groupKey, _ -> EnumSet.noneOf(DedupQuantizer.Flavor.class))
              .add(DedupQuantizer.Flavor.of(fieldData.fieldInfo.getVectorSimilarityFunction()));
        }
      }
    }

    int groupOrd = 0;
    for (Map.Entry<GroupKey, DedupGroup<?>> entry : groups.entrySet()) {
      GroupKey groupKey = entry.getKey();
      int dimension = groupKey.dimension();
      VectorEncoding encoding = groupKey.encoding();

      DedupGroup<?> group = entry.getValue();
      int groupNumVectors = group.numVectors();
      long vectorDataOffset = alignBytes(vectorData, encoding);

      // TODO: Write in sorted order for faster merge? (with sequential IO)
      for (int ord = 0; ord < groupNumVectors; ord++) {
        byte[] bytes = group.serialize(ord);
        vectorData.writeBytes(bytes, bytes.length);
      }
      long vectorDataSize = vectorData.getFilePointer() - vectorDataOffset;

      GroupInfo groupInfo =
          new GroupInfo(
              groupOrd, dimension, encoding, groupNumVectors, vectorDataOffset, vectorDataSize);
      groupInfo.write(meta);

      if (quantizer != null) {
        if (group instanceof FloatGroup floatGroup) {
          quantizer.writeGroup(
              meta,
              quantizedVectorData,
              encoding,
              dimension,
              groupNumVectors,
              groupFlavors.get(groupKey),
              floatGroup::get,
              null);
        } else {
          DedupQuantizer.writeEmptyGroup(meta);
        }
      }

      groupOrds.put(groupKey, groupOrd);
      groupOrd++;
    }

    writeEndMarker(meta);

    for (FieldData fieldData : fieldDataList) {
      fieldData.fieldWriter.finish();

      IntArrayList fieldOrdToGroupOrd = fieldData.fieldWriter.getFieldOrdToGroupOrd();
      int vectorCount = fieldOrdToGroupOrd.elementsCount;

      DocsWithFieldSet docs;
      FieldOrdToGroupOrd fieldOrdToGroupOrdFinal;
      if (sortMap == null) {
        docs = fieldData.fieldWriter.getDocsWithFieldSet();
        fieldOrdToGroupOrdFinal = new FieldOrdToGroupOrdArrayList(fieldOrdToGroupOrd);
      } else {
        DocsWithFieldSet oldDocs = fieldData.fieldWriter.getDocsWithFieldSet();
        docs = new DocsWithFieldSet();
        int[] new2OldOrd = new int[vectorCount];
        KnnVectorsWriter.mapOldOrdToNewOrd(oldDocs, sortMap, null, new2OldOrd, docs);
        fieldOrdToGroupOrdFinal =
            new FieldOrdToGroupOrdMappedArrayList(new2OldOrd, fieldOrdToGroupOrd);
      }

      writeFieldInfo(
          meta,
          vectorData,
          fieldData.fieldInfo.number,
          fieldData.fieldInfo.getVectorSimilarityFunction(),
          fieldData.fieldInfo.getVectorDimension(),
          fieldData.fieldInfo.getVectorEncoding(),
          groupOrds.get(fieldData.groupKey),
          vectorCount,
          maxDoc,
          docs,
          fieldOrdToGroupOrdFinal);
    }

    writeEndMarker(meta);
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
      return SHALLOW_SIZE + super.ramBytesUsed() + numVectors() * ramBytesPerVector;
    }
  }

  static final class FloatGroup extends DedupGroup<float[]> {
    private static final long SHALLOW_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(FloatGroup.class);

    private final long ramBytesPerVector;
    private final byte[] bytes;
    private final FloatBuffer buffer;

    FloatGroup(int dimension) {
      int length = dimension * Float.BYTES;
      this.ramBytesPerVector =
          RamUsageEstimator.NUM_BYTES_OBJECT_REF
              + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
              + length;
      this.bytes = new byte[length];
      this.buffer = ByteBuffer.wrap(bytes).order(LITTLE_ENDIAN).asFloatBuffer();
    }

    @Override
    public long hash(float[] vector) {
      buffer.put(0, vector);
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
      buffer.put(0, get(ord));
      return bytes;
    }

    @Override
    public long ramBytesUsed() {
      return SHALLOW_SIZE + super.ramBytesUsed() + numVectors() * ramBytesPerVector;
    }
  }

  static final class Float16Group extends DedupGroup<short[]> {
    private static final long SHALLOW_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(Float16Group.class);

    private final long ramBytesPerVector;
    private final byte[] bytes;
    private final ShortBuffer buffer;

    Float16Group(int dimension) {
      int length = dimension * Short.BYTES;
      this.ramBytesPerVector =
          RamUsageEstimator.NUM_BYTES_OBJECT_REF
              + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
              + length;
      this.bytes = new byte[length];
      this.buffer = ByteBuffer.wrap(bytes).order(LITTLE_ENDIAN).asShortBuffer();
    }

    @Override
    public long hash(short[] vector) {
      buffer.put(0, vector);
      return hashBytes(bytes);
    }

    @Override
    public boolean equals(short[] vector, short[] other) {
      return Arrays.equals(vector, other);
    }

    @Override
    public short[] copy(short[] vectorValue) {
      return vectorValue.clone();
    }

    @Override
    byte[] serialize(int ord) {
      buffer.put(0, get(ord));
      return bytes;
    }

    @Override
    public long ramBytesUsed() {
      return SHALLOW_SIZE + super.ramBytesUsed() + numVectors() * ramBytesPerVector;
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
