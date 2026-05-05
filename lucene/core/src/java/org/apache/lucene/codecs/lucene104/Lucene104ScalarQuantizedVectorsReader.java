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
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.quantization.OptimizedScalarQuantizer.transposeHalfByte;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
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
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.HadamardRotation;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;
import org.apache.lucene.util.quantization.QuantizedVectorsReader;
import org.apache.lucene.util.quantization.ScalarQuantizer;

/**
 * Reader for scalar quantized vectors in the Lucene 10.4 format.
 *
 * @lucene.experimental
 */
public class Lucene104ScalarQuantizedVectorsReader extends FlatVectorsReader
    implements QuantizedVectorsReader {

  private static final long SHALLOW_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(Lucene104ScalarQuantizedVectorsReader.class);

  private final Map<String, FieldEntry> fields = new HashMap<>();
  private final IndexInput quantizedVectorData;
  private final FlatVectorsReader rawVectorsReader;
  private final Lucene104ScalarQuantizedVectorScorer vectorScorer;
  /** Lazily built Hadamard rotations, keyed by field name. */
  private final Map<String, HadamardRotation> rotations = new ConcurrentHashMap<>();

  public static final int EXHAUSTIVE_BULK_SCORE_ORDS = 64;

  public Lucene104ScalarQuantizedVectorsReader(
      SegmentReadState state,
      FlatVectorsReader rawVectorsReader,
      Lucene104ScalarQuantizedVectorScorer vectorsScorer)
      throws IOException {
    // Quantized vectors are accessed randomly from their node ID stored in the HNSW
    // graph.
    this(state, rawVectorsReader, vectorsScorer, DataAccessHint.RANDOM);
  }

  public Lucene104ScalarQuantizedVectorsReader(
      SegmentReadState state,
      FlatVectorsReader rawVectorsReader,
      Lucene104ScalarQuantizedVectorScorer vectorsScorer,
      DataAccessHint accessHint)
      throws IOException {
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
        readFields(meta, versionMeta, state.fieldInfos);
      } catch (Throwable exception) {
        priorE = exception;
      } finally {
        CodecUtil.checkFooter(meta, priorE);
      }

      final IOContext.FileOpenHint[] hints =
          Stream.of(FileTypeHint.DATA, FileDataHint.KNN_VECTORS, accessHint)
              .filter(Objects::nonNull)
              .toArray(IOContext.FileOpenHint[]::new);
      quantizedVectorData =
          openDataInput(
              state,
              versionMeta,
              VECTOR_DATA_EXTENSION,
              Lucene104ScalarQuantizedVectorsFormat.VECTOR_DATA_CODEC_NAME,
              state.context.withHints(hints));
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, this);
      throw t;
    }
  }

  private void readFields(ChecksumIndexInput meta, int versionMeta, FieldInfos infos)
      throws IOException {
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
      FieldInfo info = infos.fieldInfo(fieldNumber);
      if (info == null) {
        throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
      }
      FieldEntry fieldEntry = readField(meta, versionMeta, info);
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
  public FlatVectorsScorer getFlatVectorScorer(String field) throws IOException {
    return vectorScorer;
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
    FieldEntry fi = fields.get(field);
    if (fi == null) {
      return null;
    }
    // If preconditioning is enabled on this field, rotate the query once up front. Because the
    // rotation is orthogonal, every downstream similarity computation between this rotated query
    // and the (rotated) stored vectors produces the same distance it would between the original
    // query and the original (un-rotated) stored vectors.
    float[] scoringTarget = target;
    HadamardRotation rotation = rotationOrNull(field, fi);
    if (rotation != null && target != null) {
      float[] rotated = new float[target.length];
      rotation.rotate(target, rotated);
      scoringTarget = rotated;
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
        scoringTarget);
  }

  /**
   * Returns the rotation instance for the given field, or {@code null} if preconditioning is
   * disabled on that field. Instances are lazily built and cached; they are immutable and
   * thread-safe.
   */
  HadamardRotation rotationOrNull(String field, FieldEntry fi) {
    if (fi.rotationSeed == Lucene104ScalarQuantizedVectorsFormat.ROTATION_DISABLED) {
      return null;
    }
    return rotations.computeIfAbsent(
        field, _ -> HadamardRotation.create(fi.dimension, fi.rotationSeed));
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

    // When preconditioning is enabled, the raw delegate holds rotated values. External callers
    // (rerank, CheckIndex, field-exists, etc.) expect to see the vectors they indexed, so we
    // inverse-rotate on the fly here. Merge callers go through {@link #getMergeInstance()} which
    // returns a lightweight view that skips the inverse rotation.
    HadamardRotation rotation = rotationOrNull(field, fi);
    if (rotation != null && rawFloatVectorValues != null) {
      rawFloatVectorValues = new InverseRotatedFloatVectorValues(rawFloatVectorValues, rotation);
    }

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
  public FlatVectorsReader getMergeInstance() throws IOException {
    // Expose the raw rotated stored vectors to the merge path so that the downstream merge
    // operates entirely in rotated space. External readers use {@code this} directly, which does
    // inverse-rotate stored vectors to honour the "what you indexed is what you read" contract.
    return new MergeReader();
  }

  /**
   * A trivial merge-only view that behaves exactly like the outer reader except that {@link
   * #getFloatVectorValues(String)} hands through the rotated-basis stored vectors without
   * inverse-rotating them.
   */
  private final class MergeReader extends FlatVectorsReader {

    MergeReader() {
      super(Lucene104ScalarQuantizedVectorsReader.this.vectorScorer);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, float[] target)
        throws IOException {
      return Lucene104ScalarQuantizedVectorsReader.this.getRandomVectorScorer(field, target);
    }

    @Override
    public RandomVectorScorer getRandomVectorScorer(String field, byte[] target)
        throws IOException {
      return Lucene104ScalarQuantizedVectorsReader.this.getRandomVectorScorer(field, target);
    }

    @Override
    public FloatVectorValues getFloatVectorValues(String field) throws IOException {
      // Hand through the raw rotated stored vectors without inverse-rotating.
      FieldEntry fi = fields.get(field);
      if (fi == null) {
        return null;
      }
      return rawVectorsReader.getFloatVectorValues(field);
    }

    @Override
    public ByteVectorValues getByteVectorValues(String field) throws IOException {
      return Lucene104ScalarQuantizedVectorsReader.this.getByteVectorValues(field);
    }

    @Override
    public void checkIntegrity() throws IOException {
      Lucene104ScalarQuantizedVectorsReader.this.checkIntegrity();
    }

    @Override
    public void close() {
      // no-op: the outer reader owns the resources.
    }

    @Override
    public long ramBytesUsed() {
      return Lucene104ScalarQuantizedVectorsReader.this.ramBytesUsed();
    }

    @Override
    public KnnVectorsReader unwrapReaderForField(String field) {
      return Lucene104ScalarQuantizedVectorsReader.this.unwrapReaderForField(field);
    }

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
      return Lucene104ScalarQuantizedVectorsReader.this.getOffHeapByteSize(fieldInfo);
    }
  }

  /**
   * A {@link FloatVectorValues} that inverse-rotates values returned by a backing (rotated)
   * delegate on access. Used by {@link #getFloatVectorValues(String)} so external callers see the
   * original vectors they indexed even when preconditioning is enabled.
   */
  private static final class InverseRotatedFloatVectorValues extends FloatVectorValues {

    private final FloatVectorValues delegate;
    private final HadamardRotation rotation;
    private final float[] out;
    private final float[] scratch;

    InverseRotatedFloatVectorValues(FloatVectorValues delegate, HadamardRotation rotation) {
      this.delegate = delegate;
      this.rotation = rotation;
      this.out = new float[rotation.dimension()];
      this.scratch = new float[rotation.dimension()];
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
      float[] rotated = delegate.vectorValue(ord);
      rotation.inverseRotate(rotated, out, scratch);
      return out;
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
    public FloatVectorValues copy() throws IOException {
      return new InverseRotatedFloatVectorValues(delegate.copy(), rotation);
    }

    @Override
    public KnnVectorValues.DocIndexIterator iterator() {
      return delegate.iterator();
    }

    @Override
    public int ordToDoc(int ord) {
      return delegate.ordToDoc(ord);
    }

    @Override
    public VectorScorer scorer(float[] target) throws IOException {
      float[] rotated = new float[target.length];
      rotation.rotate(target, rotated);
      return delegate.scorer(rotated);
    }

    @Override
    public VectorScorer rescorer(float[] target) throws IOException {
      float[] rotated = new float[target.length];
      rotation.rotate(target, rotated);
      return delegate.rescorer(rotated);
    }
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

  private FieldEntry readField(IndexInput input, int versionMeta, FieldInfo info)
      throws IOException {
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
    return FieldEntry.create(
        input, versionMeta, vectorEncoding, info.getVectorSimilarityFunction());
  }

  @Override
  public QuantizedByteVectorValues getQuantizedVectorValues(String field) throws IOException {
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
    return OffHeapScalarQuantizedVectorValues.load(
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
  }

  @Override
  public ScalarQuantizer getQuantizationState(String fieldName) {
    return null;
  }

  @Override
  public CloseableRandomVectorScorerSupplier getRandomVectorScorerSupplierForMerge(
      FieldInfo fieldInfo, SegmentWriteState segmentWriteState) throws IOException {
    FieldEntry fi = fields.get(fieldInfo.name);
    if (fi == null) {
      return null;
    }
    QuantizedByteVectorValues vectorValues = getQuantizedVectorValues(fieldInfo.name);
    if (fi.scalarEncoding.isAsymmetric() == false) {
      RandomVectorScorerSupplier supplier =
          vectorScorer.getRandomVectorScorerSupplier(
              fieldInfo.getVectorSimilarityFunction(), vectorValues);
      return CloseableRandomVectorScorerSupplier.create(supplier, vectorValues.size(), () -> {});
    }
    FloatVectorValues floatVectorValues = getFloatVectorValues(fieldInfo.name);
    OptimizedScalarQuantizer quantizer =
        new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
    String tempScoreQuantizedVectorName = null;
    DocsWithFieldSet docsWithField;
    try (IndexOutput tempScoreQuantizedVector =
        segmentWriteState.directory.createTempOutput(
            segmentWriteState.segmentInfo.name, "queries", segmentWriteState.context)) {
      tempScoreQuantizedVectorName = tempScoreQuantizedVector.getName();
      docsWithField =
          writeBinarizedQueryData(
              vectorValues,
              fi.scalarEncoding,
              tempScoreQuantizedVector,
              floatVectorValues,
              quantizer);
      CodecUtil.writeFooter(tempScoreQuantizedVector);
    } catch (Throwable t) {
      if (tempScoreQuantizedVectorName != null) {
        IOUtils.deleteFilesSuppressingExceptions(
            t, segmentWriteState.directory, tempScoreQuantizedVectorName);
      }
      throw t;
    }
    IndexInput quantizedScoreDataInput =
        segmentWriteState.directory.openInput(
            tempScoreQuantizedVectorName, segmentWriteState.context);
    try {
      OffHeapScalarQuantizedVectorValues scoreVectorValues =
          new OffHeapScalarQuantizedVectorValues.DenseOffHeapVectorValues(
              true,
              fieldInfo.getVectorDimension(),
              docsWithField.cardinality(),
              vectorValues.getCentroid(),
              vectorValues.getCentroidDP(),
              quantizer,
              fi.scalarEncoding,
              fieldInfo.getVectorSimilarityFunction(),
              vectorScorer,
              quantizedScoreDataInput);
      RandomVectorScorerSupplier scorerSupplier =
          vectorScorer.getRandomVectorScorerSupplier(
              fieldInfo.getVectorSimilarityFunction(), scoreVectorValues, vectorValues);
      final String finalTempScoreQuantizedVectorName = tempScoreQuantizedVectorName;
      return CloseableRandomVectorScorerSupplier.create(
          scorerSupplier,
          vectorValues.size(),
          () -> {
            IOUtils.close(quantizedScoreDataInput);
            IOUtils.deleteFilesIgnoringExceptions(
                segmentWriteState.directory, finalTempScoreQuantizedVectorName);
          });
    } catch (Throwable t) {
      IOUtils.closeWhileSuppressingExceptions(t, quantizedScoreDataInput);
      throw t;
    }
  }

  static DocsWithFieldSet writeBinarizedQueryData(
      QuantizedByteVectorValues quantizedByteVectorValues,
      ScalarEncoding encoding,
      IndexOutput binarizedQueryData,
      FloatVectorValues floatVectorValues,
      OptimizedScalarQuantizer binaryQuantizer)
      throws IOException {
    if (encoding.isAsymmetric() == false) {
      throw new IllegalArgumentException("encoding and queryEncoding must be different");
    }
    DocsWithFieldSet docsWithField = new DocsWithFieldSet();
    int discretizedDims = encoding.getDiscreteDimensions(floatVectorValues.dimension());
    byte[] quantizationScratch = new byte[discretizedDims];
    byte[] toQuery = new byte[encoding.getQueryPackedLength(discretizedDims)];
    KnnVectorValues.DocIndexIterator iterator = floatVectorValues.iterator();
    for (int docV = iterator.nextDoc(); docV != NO_MORE_DOCS; docV = iterator.nextDoc()) {
      // write index vector
      OptimizedScalarQuantizer.QuantizationResult r =
          binaryQuantizer.scalarQuantize(
              floatVectorValues.vectorValue(iterator.index()),
              quantizationScratch,
              encoding.getQueryBits(),
              quantizedByteVectorValues.getCentroid());
      docsWithField.add(docV);
      // pack and store the 4bit query vector
      transposeHalfByte(quantizationScratch, toQuery);
      binarizedQueryData.writeBytes(toQuery, toQuery.length);
      binarizedQueryData.writeInt(Float.floatToIntBits(r.lowerInterval()));
      binarizedQueryData.writeInt(Float.floatToIntBits(r.upperInterval()));
      binarizedQueryData.writeInt(Float.floatToIntBits(r.additionalCorrection()));
      binarizedQueryData.writeInt(r.quantizedComponentSum());
    }
    return docsWithField;
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
      long rotationSeed,
      OrdToDocDISIReaderConfiguration ordToDocDISIReaderConfiguration) {

    static FieldEntry create(
        IndexInput input,
        int versionMeta,
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
      long rotationSeed =
          versionMeta >= Lucene104ScalarQuantizedVectorsFormat.VERSION_PRECONDITIONED
              ? input.readLong()
              : Lucene104ScalarQuantizedVectorsFormat.ROTATION_DISABLED;
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
          rotationSeed,
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
