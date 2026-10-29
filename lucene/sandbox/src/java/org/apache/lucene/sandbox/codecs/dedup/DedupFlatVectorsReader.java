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
import static org.apache.lucene.sandbox.codecs.dedup.DedupUtil.inputFor;
import static org.apache.lucene.sandbox.codecs.dedup.DedupUtil.mergeContext;
import static org.apache.lucene.sandbox.codecs.dedup.DedupVectorValues.loadDedupBytes;
import static org.apache.lucene.sandbox.codecs.dedup.DedupVectorValues.loadDedupFloat16s;
import static org.apache.lucene.sandbox.codecs.dedup.DedupVectorValues.loadDedupFloats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.Float16VectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.sandbox.codecs.dedup.DedupUtil.GroupInfo;
import org.apache.lucene.sandbox.codecs.dedup.DedupUtil.ReadFieldInfo;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

/**
 * Reads de-duplicated flat vectors written by {@link DedupFlatVectorsWriter}. Each field exposes a
 * view backed by its group's shared vectors and a {@code fieldOrdToGroupOrd} translation map.
 *
 * @lucene.experimental
 */
final class DedupFlatVectorsReader extends FlatVectorsReader {
  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(DedupFlatVectorsReader.class);

  private final FlatVectorsScorer vectorsScorer;
  private final Map<String, FieldEntry> fields;
  private final IndexInput vectorData;
  private final String vectorDataExtension;
  private final Directory directory;
  private final String vectorDataFN;
  private final IOContext dataContext;
  // the reader this one was created from
  private final DedupFlatVectorsReader original;
  // opened lazily for merges, closed with this reader, guarded by this
  private IndexInput mergeVectorData;
  // guarded by this
  private boolean closed;

  DedupFlatVectorsReader(
      SegmentReadState state,
      FlatVectorsScorer vectorsScorer,
      String metaCodecName,
      String metaExtension,
      String vectorDataCodecName,
      String vectorDataExtension,
      int versionStart,
      int versionCurrent)
      throws IOException {

    this.vectorsScorer = vectorsScorer;
    this.fields = new HashMap<>();
    this.vectorDataExtension = vectorDataExtension;
    this.directory = state.directory;
    this.vectorDataFN =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, vectorDataExtension);
    // how these are read is up to whoever wraps this format
    this.dataContext = state.context.union(FileTypeHint.DATA, FileDataHint.KNN_VECTORS);
    this.original = this;

    String metaFileName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, metaExtension);

    int versionMeta;
    try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName)) {
      Throwable priorE = null;
      try {
        versionMeta =
            CodecUtil.checkIndexHeader(
                meta,
                metaCodecName,
                versionStart,
                versionCurrent,
                state.segmentInfo.getId(),
                state.segmentSuffix);
        readMetaBody(meta, state.fieldInfos);
      } catch (Throwable e) {
        priorE = e;
        throw e;
      } finally {
        CodecUtil.checkFooter(meta, priorE);
      }
      this.vectorData =
          openDataInput(
              state,
              versionMeta,
              vectorDataCodecName,
              vectorDataFN,
              dataContext,
              versionStart,
              versionCurrent);
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, this);
      throw t;
    }
  }

  /** Reads the same fields as {@code reader}, through {@code vectorData}. */
  private DedupFlatVectorsReader(DedupFlatVectorsReader reader, IndexInput vectorData) {
    this.vectorsScorer = reader.vectorsScorer;
    this.fields = reader.fields;
    this.vectorData = vectorData;
    this.vectorDataExtension = reader.vectorDataExtension;
    this.directory = reader.directory;
    this.vectorDataFN = reader.vectorDataFN;
    this.dataContext = reader.dataContext;
    this.original = reader.original;
  }

  private void readMetaBody(ChecksumIndexInput meta, FieldInfos fieldInfos) throws IOException {
    List<GroupInfo> groupInfos = new ArrayList<>();
    while (true) {
      GroupInfo groupInfo = GroupInfo.readFromMeta(meta);
      if (groupInfo == null) {
        break;
      }
      groupInfos.add(groupInfo);
    }

    while (true) {
      ReadFieldInfo fieldInfo = ReadFieldInfo.read(meta);
      if (fieldInfo == null) {
        break;
      }

      FieldInfo info = fieldInfos.fieldInfo(fieldInfo.fieldNumber());
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldInfo.fieldNumber(), meta);
      } else if (fieldInfo.function() != info.getVectorSimilarityFunction()) {
        throw new CorruptIndexException(
            "Inconsistent vector function: indexed="
                + fieldInfo.function()
                + ", actual="
                + info.getVectorSimilarityFunction(),
            meta);
      } else if (fieldInfo.dimension() != info.getVectorDimension()) {
        throw new CorruptIndexException(
            "Inconsistent vector dimension: indexed="
                + fieldInfo.dimension()
                + ", actual="
                + info.getVectorDimension(),
            meta);
      } else if (fieldInfo.encoding() != info.getVectorEncoding()) {
        throw new CorruptIndexException(
            "Inconsistent vector encoding: indexed="
                + fieldInfo.encoding()
                + ", actual="
                + info.getVectorEncoding(),
            meta);
      }

      if (fieldInfo.groupOrd() < 0 || fieldInfo.groupOrd() >= groupInfos.size()) {
        throw new CorruptIndexException(
            "Invalid groupId=" + fieldInfo.groupOrd() + ", numGroups=" + groupInfos.size(), meta);
      }

      GroupInfo groupInfo = groupInfos.get(fieldInfo.groupOrd());
      if (fieldInfo.dimension() != groupInfo.dimension()) {
        throw new CorruptIndexException(
            "Vector dimension mismatch: field="
                + fieldInfo.dimension()
                + ", group="
                + groupInfo.dimension(),
            meta);
      } else if (fieldInfo.encoding() != groupInfo.encoding()) {
        throw new CorruptIndexException(
            "Vector encoding mismatch: field="
                + fieldInfo.encoding()
                + ", group="
                + groupInfo.encoding(),
            meta);
      }

      fields.put(info.name, new FieldEntry(fieldInfo, groupInfo));
    }
  }

  private static IndexInput openDataInput(
      SegmentReadState state,
      int versionMeta,
      String vectorDataCodecName,
      String fileName,
      IOContext context,
      int versionStart,
      int versionCurrent)
      throws IOException {

    IndexInput in = null;
    boolean success = false;
    try {
      in = state.directory.openInput(fileName, context);
      int versionVectorData =
          CodecUtil.checkIndexHeader(
              in,
              vectorDataCodecName,
              versionStart,
              versionCurrent,
              state.segmentInfo.getId(),
              state.segmentSuffix);
      if (versionMeta != versionVectorData) {
        throw new CorruptIndexException(
            "Format versions mismatch: meta="
                + versionMeta
                + ", "
                + vectorDataCodecName
                + "="
                + versionVectorData,
            in);
      }
      CodecUtil.retrieveChecksum(in);
      success = true;
      return in;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(in);
      }
    }
  }

  @Override
  public FlatVectorsScorer getFlatVectorScorer(String field) {
    return vectorsScorer;
  }

  // package-private for testing
  FieldEntry getEntry(String field, VectorEncoding expected) {
    FieldEntry entry = fields.get(field);
    if (entry == null) {
      throw new IllegalArgumentException("field=" + field + " not found");
    } else if (entry.fieldInfo.encoding() != expected) {
      throw new IllegalArgumentException("field=" + field + " not indexed as " + expected);
    }
    return entry;
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
    FieldEntry entry = getEntry(field, FLOAT32);
    FloatVectorValues vectorValues = getFloatVectorValues(entry);
    return vectorsScorer.getRandomVectorScorer(entry.fieldInfo.function(), vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
    FieldEntry entry = getEntry(field, BYTE);
    ByteVectorValues vectorValues = getByteVectorValues(entry);
    return vectorsScorer.getRandomVectorScorer(entry.fieldInfo.function(), vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, short[] target) throws IOException {
    FieldEntry entry = getEntry(field, FLOAT16);
    Float16VectorValues vectorValues = getFloat16VectorValues(entry);
    return vectorsScorer.getRandomVectorScorer(entry.fieldInfo.function(), vectorValues, target);
  }

  @Override
  public void checkIntegrity(MergePolicy.OneMerge merge) throws IOException {
    CodecUtil.checksumEntireFile(vectorData, merge);
  }

  private FloatVectorValues getFloatVectorValues(FieldEntry entry) throws IOException {
    return loadDedupFloats(
        vectorsScorer,
        entry.fieldInfo.function(),
        entry.fieldInfo.ordToDoc(),
        entry.fieldInfo.dimension(),
        entry.groupInfo.groupNumVectors(),
        vectorData,
        entry.groupInfo.vectorDataOffset(),
        entry.groupInfo.vectorDataSize(),
        entry.fieldInfo.fieldOrdToGroupOrdOffset(),
        entry.fieldInfo.fieldOrdToGroupOrdSize(),
        entry.fieldInfo.fieldOrdToGroupOrdBitsPerValue());
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    return getFloatVectorValues(getEntry(field, FLOAT32));
  }

  private ByteVectorValues getByteVectorValues(FieldEntry entry) throws IOException {
    return loadDedupBytes(
        vectorsScorer,
        entry.fieldInfo.function(),
        entry.fieldInfo.ordToDoc(),
        entry.fieldInfo.dimension(),
        entry.groupInfo.groupNumVectors(),
        vectorData,
        entry.groupInfo.vectorDataOffset(),
        entry.groupInfo.vectorDataSize(),
        entry.fieldInfo.fieldOrdToGroupOrdOffset(),
        entry.fieldInfo.fieldOrdToGroupOrdSize(),
        entry.fieldInfo.fieldOrdToGroupOrdBitsPerValue());
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    return getByteVectorValues(getEntry(field, BYTE));
  }

  private Float16VectorValues getFloat16VectorValues(FieldEntry entry) throws IOException {
    return loadDedupFloat16s(
        vectorsScorer,
        entry.fieldInfo.function(),
        entry.fieldInfo.ordToDoc(),
        entry.fieldInfo.dimension(),
        entry.groupInfo.groupNumVectors(),
        vectorData,
        entry.groupInfo.vectorDataOffset(),
        entry.groupInfo.vectorDataSize(),
        entry.fieldInfo.fieldOrdToGroupOrdOffset(),
        entry.fieldInfo.fieldOrdToGroupOrdSize(),
        entry.fieldInfo.fieldOrdToGroupOrdBitsPerValue());
  }

  @Override
  public Float16VectorValues getFloat16VectorValues(String field) throws IOException {
    return getFloat16VectorValues(getEntry(field, FLOAT16));
  }

  @Override
  public FlatVectorsReader getMergeInstance() throws IOException {
    // vectors are stored in doc order and merged front to back, so read them sequentially
    IndexInput data = original.mergeVectorData();
    return data == vectorData ? this : new DedupFlatVectorsReader(this, data.clone());
  }

  /** Returns the input for merges, opening the data file again if needed. */
  private synchronized IndexInput mergeVectorData() throws IOException {
    assert original == this;
    if (closed) {
      throw new AlreadyClosedException("this reader is closed");
    }
    if (mergeVectorData == null) {
      mergeVectorData = inputFor(mergeContext(dataContext), vectorData, directory, vectorDataFN);
    }
    return mergeVectorData;
  }

  @Override
  public void close() throws IOException {
    // the inputs can be the same
    Set<IndexInput> toClose = new LinkedHashSet<>();
    synchronized (this) {
      if (closed) {
        return;
      }
      closed = true;
      toClose.add(vectorData);
      toClose.add(mergeVectorData);
      mergeVectorData = null;
    }
    IOUtils.close(toClose);
  }

  @Override
  public long ramBytesUsed() {
    return SHALLOW_SIZE + fields.size() * FieldEntry.SHALLOW_SIZE;
  }

  @Override
  public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
    FieldEntry entry = fields.get(fieldInfo.name);
    if (entry == null) {
      return Map.of();
    }
    // TODO: This is an over-estimation.
    return Map.of(
        vectorDataExtension,
        entry.fieldInfo.fieldOrdToGroupOrdSize() + entry.groupInfo.vectorDataSize());
  }

  @Override
  public int getVectorCount(FieldInfo fieldInfo) {
    FieldEntry entry = fields.get(fieldInfo.name);
    if (entry == null) {
      throw new IllegalArgumentException("field=\"" + fieldInfo.name + "\" not found");
    }
    return entry.fieldInfo().vectorCount();
  }

  // package-private for testing
  record FieldEntry(ReadFieldInfo fieldInfo, GroupInfo groupInfo) {
    private static final long SHALLOW_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class)
            + RamUsageEstimator.shallowSizeOfInstance(ReadFieldInfo.class)
            + RamUsageEstimator.shallowSizeOfInstance(GroupInfo.class);
  }
}
