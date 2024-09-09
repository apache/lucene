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
package org.apache.lucene.codecs.lucene912;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readSimilarityFunction;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readVectorEncoding;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
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
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.ReadAdvice;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.OrdinalTranslatedKnnCollector;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.quantization.BQVectorUtils;
import org.apache.lucene.util.quantization.BinaryQuantizer;

/**
 * Reads raw and binarized vectors from the index segments for KNN search.
 *
 * @lucene.experimental
 */
public class Lucene912BinaryQuantizedVectorsReader extends FlatVectorsReader {

  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(Lucene912BinaryQuantizedVectorsReader.class);

  private final Map<String, FieldEntry> fields = new HashMap<>();
  private final IndexInput quantizedVectorData;
  private final FlatVectorsReader rawVectorsReader;
  private final BinaryFlatVectorsScorer vectorScorer;

  public Lucene912BinaryQuantizedVectorsReader(
      SegmentReadState state,
      FlatVectorsReader rawVectorsReader,
      BinaryFlatVectorsScorer vectorsScorer)
      throws IOException {
    super(vectorsScorer);
    this.vectorScorer = vectorsScorer;
    this.rawVectorsReader = rawVectorsReader;
    int versionMeta = -1;
    String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name,
            state.segmentSuffix,
            Lucene912BinaryQuantizedVectorsFormat.META_EXTENSION);
    boolean success = false;
    try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName)) {
      Throwable priorE = null;
      try {
        versionMeta =
            CodecUtil.checkIndexHeader(
                meta,
                Lucene912BinaryQuantizedVectorsFormat.META_CODEC_NAME,
                Lucene912BinaryQuantizedVectorsFormat.VERSION_START,
                Lucene912BinaryQuantizedVectorsFormat.VERSION_CURRENT,
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
              Lucene912BinaryQuantizedVectorsFormat.VECTOR_DATA_EXTENSION,
              Lucene912BinaryQuantizedVectorsFormat.VECTOR_DATA_CODEC_NAME,
              // Quantized vectors are accessed randomly from their node ID stored in the HNSW
              // graph.
              state.context.withReadAdvice(ReadAdvice.RANDOM));
      success = true;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(this);
      }
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

    int binaryDims = BQVectorUtils.discretize(dimension, 64) / 8;
    int correctionsCount =
        fieldEntry.similarityFunction != VectorSimilarityFunction.EUCLIDEAN ? 3 : 2;
    long numQuantizedVectorBytes =
        Math.multiplyExact(binaryDims + (Float.BYTES * correctionsCount), fieldEntry.size);
    if (numQuantizedVectorBytes != fieldEntry.vectorDataLength) {
      throw new IllegalStateException(
          "Binarized vector data length "
              + fieldEntry.vectorDataLength
              + " not matching size = "
              + fieldEntry.size
              + " * (binaryBytes="
              + binaryDims
              + " + 8"
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
        OffHeapBinarizedVectorValues.load(
            fi.ordToDocDISIReaderConfiguration,
            fi.dimension,
            fi.size,
            new BinaryQuantizer(fi.descritizedDimension, fi.similarityFunction),
            fi.similarityFunction,
            vectorScorer,
            fi.centroids,
            fi.vectorDataOffset,
            fi.vectorDataLength,
            fi.clusterOffset,
            fi.clusterLength,
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
    OffHeapBinarizedVectorValues bvv =
        OffHeapBinarizedVectorValues.load(
            fi.ordToDocDISIReaderConfiguration,
            fi.dimension,
            fi.size,
            new BinaryQuantizer(fi.descritizedDimension, fi.similarityFunction),
            fi.similarityFunction,
            vectorScorer,
            fi.centroids,
            fi.vectorDataOffset,
            fi.vectorDataLength,
            fi.clusterOffset,
            fi.clusterLength,
            quantizedVectorData);
    return new BinarizedVectorValues(rawVectorsReader.getFloatVectorValues(field), bvv);
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    return rawVectorsReader.getByteVectorValues(field);
  }

  @Override
  public void search(String field, float[] target, KnnCollector knnCollector, Bits acceptDocs)
      throws IOException {
    if (knnCollector.k() == 0) return;
    final RandomVectorScorer scorer = getRandomVectorScorer(field, target);
    if (scorer == null) return;
    OrdinalTranslatedKnnCollector collector =
        new OrdinalTranslatedKnnCollector(knnCollector, scorer::ordToDoc);
    Bits acceptedOrds = scorer.getAcceptOrds(acceptDocs);
    for (int i = 0; i < scorer.maxOrd(); i++) {
      if (acceptedOrds == null || acceptedOrds.get(i)) {
        collector.collect(i, scorer.score(i));
        collector.incVisitedCount(1);
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

  public float[][] getCentroids(String field) {
    FieldEntry fieldEntry = fields.get(field);
    if (fieldEntry != null) {
      return fieldEntry.centroids;
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
    boolean success = false;
    try {
      int versionVectorData =
          CodecUtil.checkIndexHeader(
              in,
              codecName,
              Lucene912BinaryQuantizedVectorsFormat.VERSION_START,
              Lucene912BinaryQuantizedVectorsFormat.VERSION_CURRENT,
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
      success = true;
      return in;
    } finally {
      if (success == false) {
        IOUtils.closeWhileHandlingException(in);
      }
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

  private record FieldEntry(
      VectorSimilarityFunction similarityFunction,
      VectorEncoding vectorEncoding,
      int dimension,
      int descritizedDimension,
      long vectorDataOffset,
      long vectorDataLength,
      long clusterOffset,
      long clusterLength,
      int size,
      int numCentroids,
      float[][] centroids,
      float[] centroidDPs,
      OrdToDocDISIReaderConfiguration ordToDocDISIReaderConfiguration) {

    static FieldEntry create(
        IndexInput input,
        VectorEncoding vectorEncoding,
        VectorSimilarityFunction similarityFunction)
        throws IOException {
      int dimension = input.readVInt();
      long vectorDataOffset = input.readVLong();
      long vectorDataLength = input.readVLong();
      long clusterOffset = -1;
      long clusterLength = -1;
      int size = input.readVInt();
      final int numCentroids;
      final float[][] centroids;
      final float[] centroidDPs;
      if (size > 0) {
        numCentroids = input.readVInt();
        centroids = new float[numCentroids][dimension];
        centroidDPs = new float[numCentroids];
        for (int i = 0; i < numCentroids; i++) {
          float[] centroid = centroids[i];
          input.readFloats(centroid, 0, dimension);
          centroidDPs[i] = Float.intBitsToFloat(input.readInt());
        }
        if (numCentroids > 1) {
          clusterOffset = input.readVLong();
          clusterLength = input.readVLong();
          assert clusterOffset >= 0 && clusterLength > 0;
        }
      } else {
        numCentroids = 0;
        centroids = null;
        centroidDPs = null;
      }
      OrdToDocDISIReaderConfiguration conf =
          OrdToDocDISIReaderConfiguration.fromStoredMeta(input, size);
      return new FieldEntry(
          similarityFunction,
          vectorEncoding,
          dimension,
          BQVectorUtils.discretize(dimension, 64),
          vectorDataOffset,
          vectorDataLength,
          clusterOffset,
          clusterLength,
          size,
          numCentroids,
          centroids,
          centroidDPs,
          conf);
    }
  }

  /** Binarized vector values holding row and quantized vector values */
  protected static final class BinarizedVectorValues extends FloatVectorValues {
    private final FloatVectorValues rawVectorValues;
    private final OffHeapBinarizedVectorValues quantizedVectorValues;

    BinarizedVectorValues(
        FloatVectorValues rawVectorValues, OffHeapBinarizedVectorValues quantizedVectorValues) {
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
    public float[] vectorValue() throws IOException {
      return rawVectorValues.vectorValue();
    }

    @Override
    public int docID() {
      return rawVectorValues.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int rawDocId = rawVectorValues.nextDoc();
      int quantizedDocId = quantizedVectorValues.nextDoc();
      assert rawDocId == quantizedDocId;
      return quantizedDocId;
    }

    @Override
    public int advance(int target) throws IOException {
      int rawDocId = rawVectorValues.advance(target);
      int quantizedDocId = quantizedVectorValues.advance(target);
      assert rawDocId == quantizedDocId;
      return quantizedDocId;
    }

    @Override
    public VectorScorer scorer(float[] query) throws IOException {
      return quantizedVectorValues.scorer(query);
    }

    protected OffHeapBinarizedVectorValues getQuantizedVectorValues() throws IOException {
      return quantizedVectorValues;
    }
  }
}
