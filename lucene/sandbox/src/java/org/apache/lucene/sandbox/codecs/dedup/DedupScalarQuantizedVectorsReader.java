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
import static org.apache.lucene.sandbox.codecs.dedup.DedupVectorValues.loadDedupBytes;
import static org.apache.lucene.sandbox.codecs.dedup.DedupVectorValues.loadDedupFloat16s;
import static org.apache.lucene.sandbox.codecs.dedup.DedupVectorValues.loadDedupFloats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.sandbox.codecs.dedup.DedupQuantizer.QuantizedBlock;
import org.apache.lucene.sandbox.codecs.dedup.DedupScalarQuantizedVectorValues.FieldValues;
import org.apache.lucene.sandbox.codecs.dedup.DedupScalarQuantizedVectorValues.RawAndQuantizedValues;
import org.apache.lucene.sandbox.codecs.dedup.DedupUtil.GroupInfo;
import org.apache.lucene.sandbox.codecs.dedup.DedupUtil.ReadFieldInfo;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;
import org.apache.lucene.util.quantization.QuantizedVectorsReader;
import org.apache.lucene.util.quantization.ScalarQuantizer;

/**
 * Reads de-duplicated flat vectors written by {@link DedupScalarQuantizedVectorsFormat}. Each
 * FLOAT32 field exposes a full-precision view backed by the raw de-duplicated vectors, scored
 * against a data-blind quantized view sharing the same {@code fieldOrdToGroupOrd} translation map.
 * BYTE and FLOAT16 fields are stored and read raw only.
 *
 * @lucene.experimental
 */
final class DedupScalarQuantizedVectorsReader extends FlatVectorsReader
    implements QuantizedVectorsReader {
  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(DedupScalarQuantizedVectorsReader.class);

  private final DedupScalarQuantizedVectorsScorer vectorsScorer;
  private final Map<String, FieldEntry> fields;
  private final IndexInput vectorData;
  private final IndexInput quantizedVectorData;
  private final String vectorDataExtension;
  private final String quantizedVectorDataExtension;

  DedupScalarQuantizedVectorsReader(
      SegmentReadState state,
      DedupScalarQuantizedVectorsScorer vectorsScorer,
      String metaCodecName,
      String metaExtension,
      String vectorDataCodecName,
      String vectorDataExtension,
      String quantizedVectorDataCodecName,
      String quantizedVectorDataExtension,
      int versionStart,
      int versionCurrent)
      throws IOException {

    this.vectorsScorer = vectorsScorer;
    this.fields = new HashMap<>();
    this.vectorDataExtension = vectorDataExtension;
    this.quantizedVectorDataExtension = quantizedVectorDataExtension;

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
    }

    this.vectorData =
        openDataInput(
            state,
            versionMeta,
            vectorDataCodecName,
            vectorDataExtension,
            versionStart,
            versionCurrent);

    this.quantizedVectorData =
        openDataInput(
            state,
            versionMeta,
            quantizedVectorDataCodecName,
            quantizedVectorDataExtension,
            versionStart,
            versionCurrent);
  }

  private void readMetaBody(ChecksumIndexInput meta, FieldInfos fieldInfos) throws IOException {
    List<QuantizedGroupInfo> groupInfos = new ArrayList<>();
    while (true) {
      GroupInfo groupInfo = GroupInfo.readFromMeta(meta);
      if (groupInfo == null) {
        break;
      }
      Map<DedupQuantizer.Flavor, QuantizedBlock> quantizedBlocks = DedupQuantizer.readGroup(meta);
      validateQuantizedBlocks(meta, groupInfo, quantizedBlocks);
      groupInfos.add(new QuantizedGroupInfo(groupInfo, quantizedBlocks));
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

      QuantizedGroupInfo quantizedGroupInfo = groupInfos.get(fieldInfo.groupOrd());
      GroupInfo groupInfo = quantizedGroupInfo.groupInfo();
      QuantizedBlock quantizedBlock = null;
      if (fieldInfo.encoding() == FLOAT32) {
        DedupQuantizer.Flavor flavor = DedupQuantizer.Flavor.of(fieldInfo.function());
        quantizedBlock = quantizedGroupInfo.quantizedBlocks().get(flavor);
        if (quantizedBlock == null) {
          throw new CorruptIndexException(
              "Missing quantized block for flavor=" + flavor + ", field=" + info.name, meta);
        }
      }
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

      fields.put(info.name, new FieldEntry(fieldInfo, groupInfo, quantizedBlock));
    }
  }

  private static void validateQuantizedBlocks(
      ChecksumIndexInput meta,
      GroupInfo groupInfo,
      Map<DedupQuantizer.Flavor, QuantizedBlock> quantizedBlocks)
      throws IOException {
    if (groupInfo.encoding() != FLOAT32) {
      if (quantizedBlocks.isEmpty() == false) {
        throw new CorruptIndexException(
            "Unexpected quantized data for encoding=" + groupInfo.encoding(), meta);
      }
      return;
    }
    for (Map.Entry<DedupQuantizer.Flavor, QuantizedBlock> entry : quantizedBlocks.entrySet()) {
      long expectedSize =
          (long) groupInfo.groupNumVectors()
              * (entry.getValue().encoding().getDocPackedLength(groupInfo.dimension())
                  + DedupScalarQuantizedVectorValues.CORRECTIVE_BYTES);
      if (entry.getValue().quantizedDataSize() != expectedSize) {
        throw new CorruptIndexException(
            "Quantized data size mismatch: flavor="
                + entry.getKey()
                + ", expected="
                + expectedSize
                + ", actual="
                + entry.getValue().quantizedDataSize()
                + ", numVectors="
                + groupInfo.groupNumVectors()
                + ", dimension="
                + groupInfo.dimension(),
            meta);
      }
    }
  }

  private static IndexInput openDataInput(
      SegmentReadState state,
      int versionMeta,
      String vectorDataCodecName,
      String vectorDataExtension,
      int versionStart,
      int versionCurrent)
      throws IOException {

    String fileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, vectorDataExtension);

    IOContext.FileOpenHint[] hints = {
      FileTypeHint.DATA, FileDataHint.KNN_VECTORS, DataAccessHint.RANDOM
    };
    IOContext context = state.context.withHints(hints);

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
    } else if (entry.fieldInfo().encoding() != expected) {
      throw new IllegalArgumentException("field=" + field + " not indexed as " + expected);
    }
    return entry;
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
    FieldEntry entry = getEntry(field, FLOAT32);
    FieldValues quantizedValues = getQuantizedVectorValues(entry);
    return vectorsScorer.getRandomVectorScorer(
        entry.fieldInfo().function(), quantizedValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
    FieldEntry entry = getEntry(field, BYTE);
    ByteVectorValues vectorValues = getByteVectorValues(entry);
    return vectorsScorer.getRandomVectorScorer(entry.fieldInfo().function(), vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, short[] target) throws IOException {
    FieldEntry entry = getEntry(field, FLOAT16);
    Float16VectorValues vectorValues = getFloat16VectorValues(entry);
    return vectorsScorer.getRandomVectorScorer(entry.fieldInfo().function(), vectorValues, target);
  }

  @Override
  public void checkIntegrity(MergePolicy.OneMerge merge) throws IOException {
    CodecUtil.checksumEntireFile(vectorData, merge);
    CodecUtil.checksumEntireFile(quantizedVectorData, merge);
  }

  private DedupVectorValues.FloatImpl getRawFloatVectorValues(FieldEntry entry) throws IOException {
    return loadDedupFloats(
        vectorsScorer,
        entry.fieldInfo().function(),
        entry.fieldInfo().ordToDoc(),
        entry.fieldInfo().dimension(),
        entry.groupInfo().groupNumVectors(),
        vectorData,
        entry.groupInfo().vectorDataOffset(),
        entry.groupInfo().vectorDataSize(),
        entry.fieldInfo().fieldOrdToGroupOrdOffset(),
        entry.fieldInfo().fieldOrdToGroupOrdSize());
  }

  private FieldValues getQuantizedVectorValues(FieldEntry entry) throws IOException {
    return DedupScalarQuantizedVectorValues.loadQuantized(
        vectorsScorer,
        entry.fieldInfo().function(),
        entry.quantizedBlock().encoding(),
        entry.fieldInfo().ordToDoc(),
        entry.fieldInfo().dimension(),
        entry.groupInfo().groupNumVectors(),
        vectorData,
        quantizedVectorData,
        entry.quantizedBlock().quantizedDataOffset(),
        entry.quantizedBlock().quantizedDataSize(),
        entry.fieldInfo().fieldOrdToGroupOrdOffset(),
        entry.fieldInfo().fieldOrdToGroupOrdSize());
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    FieldEntry entry = getEntry(field, FLOAT32);
    return new RawAndQuantizedValues(
        getRawFloatVectorValues(entry), getQuantizedVectorValues(entry));
  }

  private ByteVectorValues getByteVectorValues(FieldEntry entry) throws IOException {
    return loadDedupBytes(
        vectorsScorer,
        entry.fieldInfo().function(),
        entry.fieldInfo().ordToDoc(),
        entry.fieldInfo().dimension(),
        entry.groupInfo().groupNumVectors(),
        vectorData,
        entry.groupInfo().vectorDataOffset(),
        entry.groupInfo().vectorDataSize(),
        entry.fieldInfo().fieldOrdToGroupOrdOffset(),
        entry.fieldInfo().fieldOrdToGroupOrdSize());
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    return getByteVectorValues(getEntry(field, BYTE));
  }

  private Float16VectorValues getFloat16VectorValues(FieldEntry entry) throws IOException {
    return loadDedupFloat16s(
        vectorsScorer,
        entry.fieldInfo().function(),
        entry.fieldInfo().ordToDoc(),
        entry.fieldInfo().dimension(),
        entry.groupInfo().groupNumVectors(),
        vectorData,
        entry.groupInfo().vectorDataOffset(),
        entry.groupInfo().vectorDataSize(),
        entry.fieldInfo().fieldOrdToGroupOrdOffset(),
        entry.fieldInfo().fieldOrdToGroupOrdSize());
  }

  @Override
  public Float16VectorValues getFloat16VectorValues(String field) throws IOException {
    return getFloat16VectorValues(getEntry(field, FLOAT16));
  }

  @Override
  public QuantizedByteVectorValues getQuantizedVectorValues(String field) throws IOException {
    FieldEntry entry = getEntry(field, FLOAT32);
    return getQuantizedVectorValues(entry);
  }

  @Override
  public ScalarQuantizer getQuantizationState(String fieldName) {
    return null;
  }

  @Override
  public CloseableRandomVectorScorerSupplier getRandomVectorScorerSupplierForMerge(
      FieldInfo fieldInfo, SegmentWriteState segmentWriteState) throws IOException {
    FieldEntry entry = fields.get(fieldInfo.name);
    if (entry == null || entry.fieldInfo().encoding() != FLOAT32) {
      // BYTE and FLOAT16 fields are stored raw only
      return null;
    }
    FieldValues quantizedValues = getQuantizedVectorValues(entry);
    ScalarEncoding encoding = entry.quantizedBlock().encoding();
    if (encoding.isAsymmetric() == false) {
      RandomVectorScorerSupplier supplier =
          vectorsScorer.getRandomVectorScorerSupplier(
              fieldInfo.getVectorSimilarityFunction(), quantizedValues);
      return CloseableRandomVectorScorerSupplier.create(supplier, quantizedValues.size(), () -> {});
    }

    // Asymmetric encodings compare query-encoded vectors against the stored doc-encoded vectors.
    // Write a query-encoded record per distinct vector of the group into a temporary file; both
    // sides then resolve through the shared fieldOrdToGroupOrd translation.
    DedupVectorValues.FloatImpl rawValues = getRawFloatVectorValues(entry);
    FloatVectorValues groupView = rawValues.getGroupView();
    int dimension = entry.fieldInfo().dimension();
    int groupNumVectors = entry.groupInfo().groupNumVectors();
    DedupQuantizer.Flavor flavor =
        DedupQuantizer.Flavor.of(fieldInfo.getVectorSimilarityFunction());

    String tempQueryVectorsName = null;
    try (IndexOutput tempQueryVectors =
        segmentWriteState.directory.createTempOutput(
            segmentWriteState.segmentInfo.name, "dedup_queries", segmentWriteState.context)) {
      tempQueryVectorsName = tempQueryVectors.getName();

      OptimizedScalarQuantizer quantizer = flavor.quantizer();
      float[] zeroCentroid = new float[dimension];
      float[] vector = new float[dimension];
      byte[] scratch = new byte[encoding.getDiscreteDimensions(dimension)];
      byte[] toQuery = new byte[encoding.getQueryPackedLength(dimension)];
      for (int groupOrd = 0; groupOrd < groupNumVectors; groupOrd++) {
        // copy: normalization and quantization mutate the input, which is a shared buffer
        System.arraycopy(groupView.vectorValue(groupOrd), 0, vector, 0, dimension);
        if (flavor.normalized()) {
          VectorUtil.l2normalize(vector);
        }
        OptimizedScalarQuantizer.QuantizationResult corrections =
            quantizer.scalarQuantize(vector, scratch, encoding.getQueryBits(), zeroCentroid);
        OptimizedScalarQuantizer.transposeHalfByte(scratch, toQuery);
        tempQueryVectors.writeBytes(toQuery, toQuery.length);
        tempQueryVectors.writeInt(Float.floatToIntBits(corrections.lowerInterval()));
        tempQueryVectors.writeInt(Float.floatToIntBits(corrections.upperInterval()));
        tempQueryVectors.writeInt(Float.floatToIntBits(corrections.additionalCorrection()));
        tempQueryVectors.writeInt(corrections.quantizedComponentSum());
      }
      CodecUtil.writeFooter(tempQueryVectors);
    } catch (Throwable t) {
      if (tempQueryVectorsName != null) {
        IOUtils.deleteFilesSuppressingExceptions(
            t, segmentWriteState.directory, tempQueryVectorsName);
      }
      throw t;
    }

    IndexInput queryVectorsInput =
        segmentWriteState.directory.openInput(tempQueryVectorsName, segmentWriteState.context);
    final String finalTempQueryVectorsName = tempQueryVectorsName;
    try {
      QuantizedByteVectorValues queryValues =
          DedupScalarQuantizedVectorValues.groupValues(
              true,
              vectorsScorer,
              fieldInfo.getVectorSimilarityFunction(),
              flavor,
              encoding,
              dimension,
              groupNumVectors,
              queryVectorsInput);
      RandomVectorScorerSupplier supplier =
          DedupScalarQuantizedVectorsScorer.asymmetricMergeSupplier(
              fieldInfo.getVectorSimilarityFunction(), queryValues, quantizedValues);
      return CloseableRandomVectorScorerSupplier.create(
          supplier,
          quantizedValues.size(),
          () -> {
            IOUtils.close(queryVectorsInput);
            IOUtils.deleteFilesIgnoringExceptions(
                segmentWriteState.directory, finalTempQueryVectorsName);
          });
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, queryVectorsInput);
      IOUtils.deleteFilesSuppressingExceptions(
          t, segmentWriteState.directory, finalTempQueryVectorsName);
      throw t;
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(vectorData, quantizedVectorData);
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
    long vectorDataSize =
        entry.fieldInfo().fieldOrdToGroupOrdSize() + entry.groupInfo().vectorDataSize();

    if (entry.quantizedBlock() == null) {
      return Map.of(vectorDataExtension, vectorDataSize);
    }

    return Map.of(
        vectorDataExtension,
        vectorDataSize,
        quantizedVectorDataExtension,
        entry.quantizedBlock().quantizedDataSize());
  }

  // package-private for testing
  record FieldEntry(ReadFieldInfo fieldInfo, GroupInfo groupInfo, QuantizedBlock quantizedBlock) {
    private static final long SHALLOW_SIZE =
        RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class)
            + RamUsageEstimator.shallowSizeOfInstance(ReadFieldInfo.class)
            + RamUsageEstimator.shallowSizeOfInstance(GroupInfo.class)
            + RamUsageEstimator.shallowSizeOfInstance(QuantizedBlock.class);
  }

  private record QuantizedGroupInfo(
      GroupInfo groupInfo, Map<DedupQuantizer.Flavor, QuantizedBlock> quantizedBlocks) {}
}
