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
package org.apache.lucene.codecs.lucene104;

import static org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat.VECTOR_DATA_EXTENSION;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readSimilarityFunction;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readVectorEncoding;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.DataAccessHint;
import org.apache.lucene.store.FileDataHint;
import org.apache.lucene.store.FileTypeHint;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.apache.lucene.util.quantization.QuantizedVectorsReader;
import org.apache.lucene.util.quantization.ScalarQuantizer;

/** Reader for scalar quantized vectors in the Lucene 10.4 format. */
public class Lucene104ScalarQuantizedVectorsReader extends FlatVectorsReader
    implements QuantizedVectorsReader {

  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(Lucene104ScalarQuantizedVectorsReader.class);

  private final Map<String, FieldEntry> fields = new HashMap<>();
  private final IndexInput quantizedVectorData;
  private final FlatVectorsReader rawVectorsReader;
  private final Lucene104ScalarQuantizedVectorScorer vectorScorer;
  public static final int EXHAUSTIVE_BULK_SCORE_ORDS = 64;

  /** Sole constructor */
  public Lucene104ScalarQuantizedVectorsReader(
      SegmentReadState state,
      FlatVectorsReader rawVectorsReader,
      Lucene104ScalarQuantizedVectorScorer vectorsScorer)
      throws IOException {
    super(vectorsScorer);
    this.vectorScorer = vectorsScorer;
    this.rawVectorsReader = rawVectorsReader;
    int versionMeta = -1;
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene104ScalarQuantizedVectorsFormat.META_EXTENSION);
    try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName)) {
      Throwable priorE = null;
      try {
        versionMeta =
            CodecUtil.checkIndexHeader(
                meta,
                Lucene104ScalarQuantizedVectorsFormat.META_CODEC_NAME,
                Lucene104ScalarQuantizedVectorsFormat.VERSION_START,
                Lucene104ScalarQuantizedVectorsFormat.VERSION_CURRENT,
                state.segmentInfo.getId(),
                state.segmentSuffix);
        readFields(meta, state.fieldInfos);
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(meta, priorE);
      }
      quantizedVectorData =
          openDataInput(
              state,
              versionMeta,
              VECTOR_DATA_EXTENSION,
              Lucene104ScalarQuantizedVectorsFormat.VECTOR_DATA_CODEC_NAME,
              // Quantized vectors are accessed randomly from their node ID stored in the HNSW
              // graph.
              state.context.withHints(
                  FileTypeHint.DATA, FileDataHint.KNN_VECTORS, DataAccessHint.RANDOM));
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, this);
      throw t;
    }
  }

  private void readFields(ChecksumIndexInput meta, FieldInfos infos) throws IOException {
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
      FieldInfo info = infos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      }
      FieldEntry fieldEntry = readField(meta, info);
      validateFieldEntry(info, fieldEntry);
      fields.put(info.name, fieldEntry);
    }
  }

  static void validateFieldEntry(FieldInfo info, FieldEntry fieldEntry) {
    int dimension = info.getVectorDimension();
    if (dimension != fieldEntry.dimension) {
      throw new IllegalStateException(
          "Inconsistent vector dimension for field=\""
              + info.name
              + "\"; "
              + dimension
              + " != "
              + fieldEntry.dimension);
    }

    long numQuantizedVectorBytes =
        Math.multiplyExact(
            (fieldEntry.scalarEncoding.getDocPackedLength(dimension)
                + (Float.BYTES * 3)
                + Integer.BYTES),
            (long) fieldEntry.size);
    if (numQuantizedVectorBytes != fieldEntry.vectorDataLength) {
      throw new IllegalStateException(
          "vector data length "
              + fieldEntry.vectorDataLength
              + " not matching size = "
              + fieldEntry.size
              + " * (dims="
              + dimension
              + " + 16"
              + ") = "
              + numQuantizedVectorBytes);
    }
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
    FieldEntry fi = fields.get(field);
    if (fi == null) {
      return null;
    }
    return vectorScorer.getRandomVectorScorer(
        fi.similarityFunction,
        OffHeapScalarQuantizedVectorValues.load(
            fi.ordToDocDISIReaderConfiguration,
            fi.dimension,
            fi.size,
            new OptimizedScalarQuantizer(fi.similarityFunction),
            fi.scalarEncoding,
            fi.similarityFunction,
            vectorScorer,
            fi.centroid,
            fi.centroidDP,
            fi.vectorDataOffset,
            fi.vectorDataLength,
            quantizedVectorData),
        target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
    return rawVectorsReader.getRandomVectorScorer(field, target);
  }

  @Override
  public void checkIntegrity() throws IOException {
    rawVectorsReader.checkIntegrity();
    CodecUtil.checksumEntireFile(quantizedVectorData);
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    FieldEntry fi = fields.get(field);
    if (fi == null) {
      return null;
    }
    if (fi.vectorEncoding != VectorEncoding.FLOAT32) {
      throw new IllegalArgumentException(
          "field=\""
              + field
              + "\" is encoded as: "
              + fi.vectorEncoding
              + " expected: "
              + VectorEncoding.FLOAT32);
    }

    FloatVectorValues rawFloatVectorValues = rawVectorsReader.getFloatVectorValues(field);

    if (rawFloatVectorValues.size() == 0) {
      return OffHeapScalarQuantizedFloatVectorValues.load(
          fi.ordToDocDISIReaderConfiguration,
          fi.dimension,
          fi.size,
          fi.scalarEncoding,
          fi.similarityFunction,
          vectorScorer,
          fi.centroid,
          fi.vectorDataOffset,
          fi.vectorDataLength,
          quantizedVectorData);
    }

    OffHeapScalarQuantizedVectorValues sqvv =
        OffHeapScalarQuantizedVectorValues.load(
            fi.ordToDocDISIReaderConfiguration,
            fi.dimension,
            fi.size,
            new OptimizedScalarQuantizer(fi.similarityFunction),
            fi.scalarEncoding,
            fi.similarityFunction,
            vectorScorer,
            fi.centroid,
            fi.centroidDP,
            fi.vectorDataOffset,
            fi.vectorDataLength,
            quantizedVectorData);
    return new ScalarQuantizedVectorValues(rawFloatVectorValues, sqvv);
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    return rawVectorsReader.getByteVectorValues(field);
  }

  @Override
  public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
      throws IOException {
    rawVectorsReader.search(field, target, knnCollector, acceptDocs);
  }

  @Override
  public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
      throws IOException {
    if (knnCollector.k() == 0) return;
    final RandomVectorScorer scorer = getRandomVectorScorer(field, target);
    if (scorer == null) return;
    Bits acceptedOrds = scorer.getAcceptOrds(acceptDocs.bits());
    // if k is larger than the number of vectors we expect to visit in an HNSW search,
    // we can just iterate over all vectors and collect them.
    int[] ords = new int[EXHAUSTIVE_BULK_SCORE_ORDS];
    float[] scores = new float[EXHAUSTIVE_BULK_SCORE_ORDS];
    int numOrds = 0;
    int numVectors = scorer.maxOrd();
    for (int i = 0; i < numVectors; i++) {
      if (acceptedOrds == null || acceptedOrds.get(i)) {
        if (knnCollector.earlyTerminated()) {
          break;
        }
        ords[numOrds++] = i;
        if (numOrds == ords.length) {
          knnCollector.incVisitedCount(numOrds);
          if (scorer.bulkScore(ords, scores, numOrds) > knnCollector.minCompetitiveSimilarity()) {
            for (int j = 0; j < numOrds; j++) {
              knnCollector.collect(scorer.ordToDoc(ords[j]), scores[j]);
            }
          }
          numOrds = 0;
        }
      }
    }

    if (numOrds > 0) {
      knnCollector.incVisitedCount(numOrds);
      if (scorer.bulkScore(ords, scores, numOrds) > knnCollector.minCompetitiveSimilarity()) {
        for (int j = 0; j < numOrds; j++) {
          knnCollector.collect(scorer.ordToDoc(ords[j]), scores[j]);
        }
      }
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(quantizedVectorData, rawVectorsReader);
  }

  @Override
  public long ramBytesUsed() {
    long size = SHALLOW_SIZE;
    size +=
        RamUsageEstimator.sizeOfMap(
            fields, RamUsageEstimator.shallowSizeOfInstance(FieldEntry.class));
    size += rawVectorsReader.ramBytesUsed();
    return size;
  }

  @Override
  public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
    Objects.requireNonNull(fieldInfo);
    var raw = rawVectorsReader.getOffHeapByteSize(fieldInfo);
    var fieldEntry = fields.get(fieldInfo.name);
    if (fieldEntry == null) {
      assert fieldInfo.getVectorEncoding() == VectorEncoding.BYTE;
      return raw;
    }
    var quant = Map.of(VECTOR_DATA_EXTENSION, fieldEntry.vectorDataLength());
    return KnnVectorsReader.mergeOffHeapByteSizeMaps(raw, quant);
  }

  public float[] getCentroid(String field) {
    FieldEntry fieldEntry = fields.get(field);
    if (fieldEntry != null) {
      return fieldEntry.centroid;
    }
    return null;
  }

  private static IndexInput openDataInput(
      SegmentReadState state,
      int versionMeta,
      String fileExtension,
      String codecName,
      IOContext context)
      throws IOException {
    String fileName =
        IndexFileNames.segmentFileName(state.segmentInfo.name, state.segmentSuffix, fileExtension);
    IndexInput in = state.directory.openInput(fileName, context);
    try {
      int versionVectorData =
          CodecUtil.checkIndexHeader(
              in,
              codecName,
              Lucene104ScalarQuantizedVectorsFormat.VERSION_START,
              Lucene104ScalarQuantizedVectorsFormat.VERSION_CURRENT,
              state.segmentInfo.getId(),
              state.segmentSuffix);
      if (versionMeta != versionVectorData) {
        throw new CorruptIndexException(
            "Format versions mismatch: meta="
                + versionMeta
                + ", "
                + codecName
                + "="
                + versionVectorData,
            in);
      }
      CodecUtil.retrieveChecksum(in);
      return in;
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, in);
      throw t;
    }
  }

  private FieldEntry readField(IndexInput input, FieldInfo info) throws IOException {
    VectorEncoding vectorEncoding = readVectorEncoding(input);
    VectorSimilarityFunction similarityFunction = readSimilarityFunction(input);
    if (similarityFunction != info.getVectorSimilarityFunction()) {
      throw new IllegalStateException(
          "Inconsistent vector similarity function for field=\""
              + info.name
              + "\"; "
              + similarityFunction
              + " != "
              + info.getVectorSimilarityFunction());
    }
    return FieldEntry.create(input, vectorEncoding, info.getVectorSimilarityFunction());
  }

  @Override
  public org.apache.lucene.util.quantization.QuantizedByteVectorValues getQuantizedVectorValues(
      String field) throws IOException {
    FieldEntry fi = fields.get(field);
    if (fi == null) {
      return null;
    }
    if (fi.vectorEncoding != VectorEncoding.FLOAT32) {
      throw new IllegalArgumentException(
          "field=\""
              + field
              + "\" is encoded as: "
              + fi.vectorEncoding
              + " expected: "
              + VectorEncoding.FLOAT32);
    }
    var qv =
        OffHeapScalarQuantizedVectorValues.load(
            fi.ordToDocDISIReaderConfiguration,
            fi.dimension,
            fi.size,
            new OptimizedScalarQuantizer(fi.similarityFunction),
            fi.scalarEncoding,
            fi.similarityFunction,
            vectorScorer,
            fi.centroid,
            fi.centroidDP,
            fi.vectorDataOffset,
            fi.vectorDataLength,
            quantizedVectorData);
    return new org.apache.lucene.util.quantization.QuantizedByteVectorValues() {
      @Override
      public float getScoreCorrectionConstant(int ord) throws IOException {
        return 0;
      }

      @Override
      public byte[] vectorValue(int ord) throws IOException {
        return qv.vectorValue(ord);
      }

      @Override
      public int dimension() {
        return qv.dimension();
      }

      @Override
      public int size() {
        return qv.size();
      }
    };
  }

  @Override
  public ScalarQuantizer getQuantizationState(String fieldName) {
    return null;
  }

  private record FieldEntry(
      VectorSimilarityFunction similarityFunction,
      VectorEncoding vectorEncoding,
      int dimension,
      long vectorDataOffset,
      long vectorDataLength,
      int size,
      ScalarEncoding scalarEncoding,
      float[] centroid,
      float centroidDP,
      OrdToDocDISIReaderConfiguration ordToDocDISIReaderConfiguration) {

    static FieldEntry create(
        IndexInput input,
        VectorEncoding vectorEncoding,
        VectorSimilarityFunction similarityFunction)
        throws IOException {
      int dimension = input.readVInt();
      long vectorDataOffset = input.readVLong();
      long vectorDataLength = input.readVLong();
      int size = input.readVInt();
      final float[] centroid;
      float centroidDP = 0;
      ScalarEncoding scalarEncoding = ScalarEncoding.UNSIGNED_BYTE;
      if (size > 0) {
        int wireNumber = input.readVInt();
        scalarEncoding =
            ScalarEncoding.fromWireNumber(wireNumber)
                .orElseThrow(
                    () ->
                        new IllegalStateException(
                            "Could not get ScalarEncoding from wire number: " + wireNumber));
        centroid = new float[dimension];
        input.readFloats(centroid, 0, dimension);
        centroidDP = Float.intBitsToFloat(input.readInt());
      } else {
        centroid = null;
      }
      OrdToDocDISIReaderConfiguration conf =
          OrdToDocDISIReaderConfiguration.fromStoredMeta(input, size);
      return new FieldEntry(
          similarityFunction,
          vectorEncoding,
          dimension,
          vectorDataOffset,
          vectorDataLength,
          size,
          scalarEncoding,
          centroid,
          centroidDP,
          conf);
    }
  }

  /** Vector values holding row and quantized vector values */
  protected static final class ScalarQuantizedVectorValues extends FloatVectorValues {
    private final FloatVectorValues rawVectorValues;
    private final QuantizedByteVectorValues quantizedVectorValues;

    ScalarQuantizedVectorValues(
        FloatVectorValues rawVectorValues, QuantizedByteVectorValues quantizedVectorValues) {
      this.rawVectorValues = rawVectorValues;
      this.quantizedVectorValues = quantizedVectorValues;
    }

    @Override
    public int dimension() {
      return rawVectorValues.dimension();
    }

    @Override
    public int size() {
      return rawVectorValues.size();
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
      return rawVectorValues.vectorValue(ord);
    }

    @Override
    public ScalarQuantizedVectorValues copy() throws IOException {
      return new ScalarQuantizedVectorValues(rawVectorValues.copy(), quantizedVectorValues.copy());
    }

    @Override
    public Bits getAcceptOrds(Bits acceptDocs) {
      return rawVectorValues.getAcceptOrds(acceptDocs);
    }

    @Override
    public int ordToDoc(int ord) {
      return rawVectorValues.ordToDoc(ord);
    }

    @Override
    public DocIndexIterator iterator() {
      return rawVectorValues.iterator();
    }

    @Override
    public VectorScorer scorer(float[] query) throws IOException {
      return quantizedVectorValues.scorer(query);
    }

    @Override
    public VectorScorer rescorer(float[] target) throws IOException {
      return rawVectorValues.rescorer(target);
    }

    QuantizedByteVectorValues getQuantizedVectorValues() throws IOException {
      return quantizedVectorValues;
    }
  }
}
