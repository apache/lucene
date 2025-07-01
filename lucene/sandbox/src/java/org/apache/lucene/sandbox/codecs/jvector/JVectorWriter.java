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

package org.apache.lucene.sandbox.codecs.jvector;

import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readVectorEncoding;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OnDiskSequentialGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.feature.Feature;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.disk.feature.InlineVectors;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatFieldVectorsWriter;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.hnsw.CloseableRandomVectorScorerSupplier;

/**
 * Writes vector data using the JVector format. This class is responsible for serializing vectors
 * and building index structures such as graphs or quantization data, during the indexing process.
 */
public class JVectorWriter extends KnnVectorsWriter {

  private static final long SHALLOW_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(JVectorWriter.class);
  private static final FlatVectorsFormat FLAT_VECTORS_FORMAT =
      new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());
  private final List<FieldWriter<?>> fields = new ArrayList<>();

  private final IndexOutput meta;
  private final IndexOutput vectorIndex;
  private final FlatVectorsWriter flatVectorWriter;
  private final String indexDataFileName;
  private final String baseDataFileName;
  private final SegmentWriteState segmentWriteState;
  private final int maxConn;
  private final int beamWidth;
  private final float degreeOverflow;
  private final float alpha;
  private final Function<Integer, Integer>
      numberOfSubspacesPerVectorSupplier; // Number of subspaces used per vector for PQ quantization
  // as a function of the original dimension
  private final int
      minimumBatchSizeForQuantization; // Threshold for the vector count above which we will trigger
  // PQ quantization
  private final boolean mergeOnDisk;

  private boolean finished = false;

  public JVectorWriter(
      SegmentWriteState segmentWriteState,
      int maxConn,
      int beamWidth,
      float degreeOverflow,
      float alpha,
      Function<Integer, Integer> numberOfSubspacesPerVectorSupplier,
      int minimumBatchSizeForQuantization,
      boolean mergeOnDisk)
      throws IOException {
    this.segmentWriteState = segmentWriteState;
    this.maxConn = maxConn;
    this.beamWidth = beamWidth;
    this.degreeOverflow = degreeOverflow;
    this.alpha = alpha;
    this.numberOfSubspacesPerVectorSupplier = numberOfSubspacesPerVectorSupplier;
    this.minimumBatchSizeForQuantization = minimumBatchSizeForQuantization;
    this.mergeOnDisk = mergeOnDisk;
    this.flatVectorWriter = FLAT_VECTORS_FORMAT.fieldsWriter(segmentWriteState);
    String metaFileName =
        IndexFileNames.segmentFileName(
            segmentWriteState.segmentInfo.name,
            segmentWriteState.segmentSuffix,
            JVectorFormat.META_EXTENSION);

    this.indexDataFileName =
        IndexFileNames.segmentFileName(
            segmentWriteState.segmentInfo.name,
            segmentWriteState.segmentSuffix,
            JVectorFormat.VECTOR_INDEX_EXTENSION);
    this.baseDataFileName =
        segmentWriteState.segmentInfo.name + "_" + segmentWriteState.segmentSuffix;

    boolean success = false;
    try {
      meta = segmentWriteState.directory.createOutput(metaFileName, segmentWriteState.context);
      vectorIndex =
          segmentWriteState.directory.createOutput(indexDataFileName, segmentWriteState.context);
      CodecUtil.writeIndexHeader(
          meta,
          JVectorFormat.META_CODEC_NAME,
          JVectorFormat.VERSION_CURRENT,
          segmentWriteState.segmentInfo.getId(),
          segmentWriteState.segmentSuffix);

      CodecUtil.writeIndexHeader(
          vectorIndex,
          JVectorFormat.VECTOR_INDEX_CODEC_NAME,
          JVectorFormat.VERSION_CURRENT,
          segmentWriteState.segmentInfo.getId(),
          segmentWriteState.segmentSuffix);

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    if (fieldInfo.getVectorEncoding() == VectorEncoding.BYTE) {
      final String errorMessage =
          "byte[] vectors are not supported in JVector. "
              + "Instead you should only use float vectors and leverage product quantization during indexing."
              + "This can provides much greater savings in storage and memory";
      throw new UnsupportedOperationException(errorMessage);
    }
    final FlatFieldVectorsWriter<?> flatFieldVectorsWriter = flatVectorWriter.addField(fieldInfo);
    FieldWriter<?> newField =
        new FieldWriter<>(fieldInfo, segmentWriteState.segmentInfo.name, flatFieldVectorsWriter);

    fields.add(newField);
    return newField;
  }

  @SuppressWarnings("unchecked")
  public KnnFieldVectorsWriter<?> addMergeField(
      FieldInfo fieldInfo, FloatVectorValues mergeFloatVector, RandomAccessVectorValues ravv)
      throws UnsupportedOperationException {
    if (fieldInfo.getVectorEncoding() == VectorEncoding.BYTE) {
      final String errorMessage =
          "byte[] vectors are not supported in JVector. "
              + "Instead you should only use float vectors and leverage product quantization during indexing."
              + "This can provides much greater savings in storage and memory";
      throw new UnsupportedOperationException(errorMessage);
    }
    return new FieldWriter<>(fieldInfo, segmentWriteState.segmentInfo.name, mergeFloatVector, ravv);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    CloseableRandomVectorScorerSupplier scorerSupplier =
        flatVectorWriter.mergeOneFieldToIndex(fieldInfo, mergeState);
    try {
      switch (fieldInfo.getVectorEncoding()) {
        case BYTE:
          var byteWriter = (FieldWriter<byte[]>) addField(fieldInfo);
          ByteVectorValues mergedBytes =
              MergedVectorValues.mergeByteVectorValues(fieldInfo, mergeState);
          var iterator = mergedBytes.iterator();
          for (int doc = iterator.nextDoc();
              doc != DocIdSetIterator.NO_MORE_DOCS;
              doc = iterator.nextDoc()) {
            byteWriter.addValue(doc, mergedBytes.vectorValue(doc));
          }
          writeField(byteWriter);
          break;
        case FLOAT32:
          final FieldWriter<float[]> floatVectorFieldWriter;
          FloatVectorValues mergeFloatVector =
              MergedVectorValues.mergeFloatVectorValues(fieldInfo, mergeState);
          if (mergeOnDisk) {
            final var ravv =
                new RandomAccessMergedFloatVectorValues(fieldInfo, mergeState, scorerSupplier);
            floatVectorFieldWriter =
                (FieldWriter<float[]>) addMergeField(fieldInfo, mergeFloatVector, ravv);
          } else {
            floatVectorFieldWriter = (FieldWriter<float[]>) addField(fieldInfo);
            var itr = mergeFloatVector.iterator();
            for (int doc = itr.nextDoc();
                doc != DocIdSetIterator.NO_MORE_DOCS;
                doc = itr.nextDoc()) {
              floatVectorFieldWriter.addValue(doc, mergeFloatVector.vectorValue(doc));
            }
          }
          writeField(floatVectorFieldWriter);
          break;
      }
    } finally {
      IOUtils.close(scorerSupplier);
    }
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    flatVectorWriter.flush(maxDoc, sortMap);
    for (FieldWriter<?> field : fields) {
      if (sortMap == null) {
        writeField(field);
      } else {
        throw new UnsupportedOperationException("Not implemented yet");
      }
    }
  }

  private void writeField(FieldWriter<?> fieldData) throws IOException {
    OnHeapGraphIndex graph = fieldData.getGraph();
    final var vectorIndexFieldMetadata = writeGraph(graph, fieldData);
    meta.writeInt(fieldData.fieldInfo.number);
    vectorIndexFieldMetadata.toOutput(meta);
  }

  private VectorIndexFieldMetadata writeGraph(OnHeapGraphIndex graph, FieldWriter<?> fieldData)
      throws IOException {
    final String vectorIndexFieldFileName =
        baseDataFileName
            + "_"
            + fieldData.fieldInfo.name
            + "."
            + JVectorFormat.VECTOR_INDEX_EXTENSION;

    final int fieldNumber = fieldData.fieldInfo.number;
    final VectorEncoding vectorEncoding = fieldData.fieldInfo.getVectorEncoding();
    final VectorSimilarityFunction vectorSimilarityFunction =
        fieldData.fieldInfo.getVectorSimilarityFunction();
    final int vectorDimension = fieldData.fieldInfo.getVectorDimension();
    final long vectorIndexOffset;
    final long vectorIndexLength;
    final long pqCodebooksAndVectorsOffset;
    final long pqCodebooksAndVectorsLength;

    try (IndexOutput indexOutput =
            segmentWriteState.directory.createOutput(
                vectorIndexFieldFileName, segmentWriteState.context);
        final var jVectorIndexWriter = new JVectorIndexWriter(indexOutput); ) {
      CodecUtil.writeIndexHeader(
          indexOutput,
          JVectorFormat.VECTOR_INDEX_CODEC_NAME,
          JVectorFormat.VERSION_CURRENT,
          segmentWriteState.segmentInfo.getId(),
          segmentWriteState.segmentSuffix);
      final long startOffset = indexOutput.getFilePointer();
      vectorIndexOffset = startOffset;

      try (var writer =
          new OnDiskSequentialGraphIndexWriter.Builder(graph, jVectorIndexWriter)
              .with(new InlineVectors(fieldData.randomAccessVectorValues.dimension()))
              .build()) {
        var suppliers =
            Feature.singleStateFactory(
                FeatureId.INLINE_VECTORS,
                nodeId ->
                    new InlineVectors.State(fieldData.randomAccessVectorValues.getVector(nodeId)));

        writer.write(suppliers);
        long endGraphOffset = jVectorIndexWriter.position();
        vectorIndexLength = endGraphOffset - startOffset;

        if (fieldData.randomAccessVectorValues.size() >= minimumBatchSizeForQuantization) {
          writePQCodebooksAndVectors(jVectorIndexWriter, fieldData);
          pqCodebooksAndVectorsLength = jVectorIndexWriter.position() - endGraphOffset;
          pqCodebooksAndVectorsOffset = endGraphOffset;
        } else {
          pqCodebooksAndVectorsOffset = 0;
          pqCodebooksAndVectorsLength = 0;
        }
        CodecUtil.writeFooter(indexOutput);
      }
      return new VectorIndexFieldMetadata(
          fieldNumber,
          vectorEncoding,
          vectorSimilarityFunction,
          vectorDimension,
          vectorIndexOffset,
          vectorIndexLength,
          pqCodebooksAndVectorsOffset,
          pqCodebooksAndVectorsLength);
    }
  }

  /**
   * Writes the product quantization (PQ) codebooks and encoded vectors to a DataOutput stream. This
   * method compresses the original vector data using product quantization and encodes all vector
   * values into a smaller, compressed form for storage or transfer.
   *
   * @param out The DataOutput stream where the compressed PQ codebooks and encoded vectors will be
   *     written.
   * @param fieldData The field writer object providing access to the vector data to be compressed.
   * @throws IOException If an I/O error occurs during writing.
   */
  private void writePQCodebooksAndVectors(DataOutput out, FieldWriter<?> fieldData)
      throws IOException {
    final var M =
        numberOfSubspacesPerVectorSupplier.apply(fieldData.randomAccessVectorValues.dimension());
    final var numberOfClustersPerSubspace =
        Math.min(256, fieldData.randomAccessVectorValues.size()); // number of centroids per
    // subspace
    ProductQuantization pq =
        ProductQuantization.compute(
            fieldData.randomAccessVectorValues,
            M, // number of subspaces
            numberOfClustersPerSubspace, // number of centroids per subspace
            fieldData.fieldInfo.getVectorSimilarityFunction()
                == VectorSimilarityFunction.EUCLIDEAN); // center the dataset
    var pqv = pq.encodeAll(fieldData.randomAccessVectorValues);
    // write the compressed vectors to disk
    pqv.write(out);
  }

  /**
   * Metadata associated with a single field's vector index. Includes information such as offsets,
   * lengths, encoding types, and other field-specific indexing data required during read and write
   * phases.
   */
  public static class VectorIndexFieldMetadata {
    int fieldNumber;
    VectorEncoding vectorEncoding;
    VectorSimilarityFunction vectorSimilarityFunction;
    int vectorDimension;
    long vectorIndexOffset;
    long vectorIndexLength;
    long pqCodebooksAndVectorsOffset;
    long pqCodebooksAndVectorsLength;

    public void toOutput(IndexOutput out) throws IOException {
      out.writeInt(fieldNumber);
      out.writeInt(vectorEncoding.ordinal());
      out.writeInt(JVectorReader.VectorSimilarityMapper.distFuncToOrd(vectorSimilarityFunction));
      out.writeVInt(vectorDimension);
      out.writeVLong(vectorIndexOffset);
      out.writeVLong(vectorIndexLength);
      out.writeVLong(pqCodebooksAndVectorsOffset);
      out.writeVLong(pqCodebooksAndVectorsLength);
    }

    public VectorIndexFieldMetadata(
        int fieldNumber,
        VectorEncoding vectorEncoding,
        VectorSimilarityFunction vectorSimilarityFunction,
        int vectorDimension,
        long vectorIndexOffset,
        long vectorIndexLength,
        long pqCodebooksAndVectorsOffset,
        long pqCodebooksAndVectorsLength) {
      this.fieldNumber = fieldNumber;
      this.vectorEncoding = vectorEncoding;
      this.vectorSimilarityFunction = vectorSimilarityFunction;
      this.vectorDimension = vectorDimension;
      this.vectorIndexOffset = vectorIndexOffset;
      this.vectorIndexLength = vectorIndexLength;
      this.pqCodebooksAndVectorsOffset = pqCodebooksAndVectorsOffset;
      this.pqCodebooksAndVectorsLength = pqCodebooksAndVectorsLength;
    }

    public VectorIndexFieldMetadata(IndexInput in) throws IOException {
      this.fieldNumber = in.readInt();
      this.vectorEncoding = readVectorEncoding(in); // This reads a byte
      this.vectorSimilarityFunction =
          JVectorReader.VectorSimilarityMapper.ordToLuceneDistFunc(in.readInt());
      this.vectorDimension = in.readVInt();
      this.vectorIndexOffset = in.readVLong();
      this.vectorIndexLength = in.readVLong();
      this.pqCodebooksAndVectorsOffset = in.readVLong();
      this.pqCodebooksAndVectorsLength = in.readVLong();
    }

    public int getFieldNumber() {
      return fieldNumber;
    }

    public VectorEncoding getVectorEncoding() {
      return vectorEncoding;
    }

    public VectorSimilarityFunction getVectorSimilarityFunction() {
      return vectorSimilarityFunction;
    }

    public int getVectorDimension() {
      return vectorDimension;
    }

    public long getVectorIndexOffset() {
      return vectorIndexOffset;
    }

    public long getVectorIndexLength() {
      return vectorIndexLength;
    }

    public long getPqCodebooksAndVectorsOffset() {
      return pqCodebooksAndVectorsOffset;
    }

    public long getPqCodebooksAndVectorsLength() {
      return pqCodebooksAndVectorsLength;
    }
  }

  @Override
  public void finish() throws IOException {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;

    if (meta != null) {
      meta.writeInt(-1);
      CodecUtil.writeFooter(meta);
    }

    if (vectorIndex != null) {
      CodecUtil.writeFooter(vectorIndex);
    }

    flatVectorWriter.finish();
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(meta, vectorIndex, flatVectorWriter);
  }

  @Override
  public long ramBytesUsed() {
    long total = SHALLOW_RAM_BYTES_USED;
    for (FieldWriter<?> field : fields) {
      total += field.ramBytesUsed();
    }
    return total;
  }

  class FieldWriter<T> extends KnnFieldVectorsWriter<T> {
    private final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(FieldWriter.class);
    private final FieldInfo fieldInfo;
    private int lastDocID = -1;
    private final GraphIndexBuilder graphIndexBuilder;
    private final RandomAccessVectorValues randomAccessVectorValues;
    private final FloatVectorValues mergedFloatVector;
    private final FlatFieldVectorsWriter<T> flatFieldVectorsWriter;
    private final BuildScoreProvider buildScoreProvider;

    FieldWriter(
        FieldInfo fieldInfo,
        String segmentName,
        FloatVectorValues mergedFloatVector,
        RandomAccessVectorValues ravv) {
      this.flatFieldVectorsWriter = null;
      this.randomAccessVectorValues = ravv;
      this.mergedFloatVector = mergedFloatVector;
      this.fieldInfo = fieldInfo;
      this.buildScoreProvider =
          BuildScoreProvider.randomAccessScoreProvider(
              randomAccessVectorValues, getVectorSimilarityFunction(fieldInfo));
      this.graphIndexBuilder =
          new GraphIndexBuilder(
              buildScoreProvider,
              fieldInfo.getVectorDimension(),
              maxConn,
              beamWidth,
              degreeOverflow,
              alpha,
              true);
    }

    FieldWriter(
        FieldInfo fieldInfo, String segmentName, FlatFieldVectorsWriter<T> flatFieldVectorsWriter) {
      this.flatFieldVectorsWriter = flatFieldVectorsWriter;
      this.randomAccessVectorValues =
          new RandomAccessVectorValuesOverFlatFields(flatFieldVectorsWriter, fieldInfo);
      this.mergedFloatVector = null;
      this.fieldInfo = fieldInfo;
      this.buildScoreProvider =
          BuildScoreProvider.randomAccessScoreProvider(
              randomAccessVectorValues, getVectorSimilarityFunction(fieldInfo));
      this.graphIndexBuilder =
          new GraphIndexBuilder(
              buildScoreProvider,
              randomAccessVectorValues.dimension(),
              maxConn,
              beamWidth,
              degreeOverflow,
              alpha,
              true);
    }

    @Override
    public void addValue(int docID, T vectorValue) throws IOException {
      if (docID <= lastDocID) {
        throw new IllegalArgumentException(
            "VectorValuesField \""
                + fieldInfo.name
                + "\" appears more than once in this document (only one value is allowed per field)");
      }
      if (vectorValue instanceof float[]) {
        flatFieldVectorsWriter.addValue(docID, vectorValue);
      } else {
        throw new IllegalArgumentException("Unsupported vector type: " + vectorValue.getClass());
      }

      lastDocID = docID;
    }

    @Override
    public T copyValue(T vectorValue) {
      throw new UnsupportedOperationException("copyValue not supported");
    }

    @Override
    public long ramBytesUsed() {
      return SHALLOW_SIZE + flatFieldVectorsWriter.ramBytesUsed();
    }

    io.github.jbellis.jvector.vector.VectorSimilarityFunction getVectorSimilarityFunction(
        FieldInfo fieldInfo) {
      switch (fieldInfo.getVectorSimilarityFunction()) {
        case EUCLIDEAN:
          return io.github.jbellis.jvector.vector.VectorSimilarityFunction.EUCLIDEAN;
        case COSINE:
          return io.github.jbellis.jvector.vector.VectorSimilarityFunction.COSINE;
        case DOT_PRODUCT:
          return io.github.jbellis.jvector.vector.VectorSimilarityFunction.DOT_PRODUCT;
        // $CASES-OMITTED$
        default:
          throw new IllegalArgumentException(
              "Unsupported similarity function: " + fieldInfo.getVectorSimilarityFunction());
      }
    }

    /**
     * Builds and returns the {@link OnHeapGraphIndex} for the current field by adding all vector
     * entries to the graph index builder. If a merged vector view is available, it uses the
     * associated iterator to add only the live documents. Otherwise, it adds all vectors in the
     * {@code randomAccessVectorValues}. After populating the graph, it performs any necessary
     * cleanup and returns the final in-memory graph index.
     *
     * @return the constructed {@link OnHeapGraphIndex}
     * @throws IOException if reading vector data fails
     */
    public OnHeapGraphIndex getGraph() throws IOException {

      if (mergedFloatVector != null) {
        var itr = mergedFloatVector.iterator();
        for (int doc = itr.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = itr.nextDoc()) {
          graphIndexBuilder.addGraphNode(doc, randomAccessVectorValues.getVector(doc));
        }
      } else {
        for (int i = 0; i < randomAccessVectorValues.size(); i++) {
          graphIndexBuilder.addGraphNode(i, randomAccessVectorValues.getVector(i));
        }
      }

      graphIndexBuilder.cleanup();
      return graphIndexBuilder.getGraph();
    }
  }

  static class RandomAccessMergedFloatVectorValues implements RandomAccessVectorValues {
    private static final int READER_ID = 0;
    private static final int READER_ORD = 1;

    private final VectorTypeSupport VECTOR_TYPE_SUPPORT =
        VectorizationProvider.getInstance().getVectorTypeSupport();

    private final KnnVectorsReader[] readers;
    private final FloatVectorValues[] perReaderFloatVectorValues;

    private final int totalDocsCount;
    private final int size;

    private final int[][] ordMapping;

    private final int dimension;

    private String fieldName;

    public RandomAccessMergedFloatVectorValues(
        FieldInfo fieldInfo,
        MergeState mergeState,
        CloseableRandomVectorScorerSupplier scorerSupplier)
        throws IOException {
      this.fieldName = fieldInfo.name;
      this.totalDocsCount = Math.toIntExact(Arrays.stream(mergeState.maxDocs).asLongStream().sum());

      int totalVectorsCount = 0;
      int dimension = 0;

      List<KnnVectorsReader> allReaders = new ArrayList<>();

      for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
        FieldInfos fieldInfos = mergeState.fieldInfos[i];
        if (KnnVectorsWriter.MergedVectorValues.hasVectorValues(fieldInfos, fieldName)) {
          KnnVectorsReader reader = mergeState.knnVectorsReaders[i];
          if (reader != null) {
            FloatVectorValues values = reader.getFloatVectorValues(fieldName);
            if (values != null) {
              allReaders.add(reader);
              totalVectorsCount += values.size();
              dimension = Math.max(dimension, values.dimension());
            }
          }
        }
      }

      assert (totalVectorsCount <= totalDocsCount)
          : "Total number of vectors exceeds the total number of documents";
      assert (dimension > 0) : "No vectors found for field " + fieldName;

      this.size = totalVectorsCount;
      this.readers = new KnnVectorsReader[allReaders.size()];
      for (int i = 0; i < readers.length; i++) {
        readers[i] = allReaders.get(i);
      }
      this.perReaderFloatVectorValues = new FloatVectorValues[readers.length];
      this.dimension = dimension;

      this.ordMapping = new int[totalDocsCount][2];

      int documentsIterated = 0;

      MergeState.DocMap[] docMaps = mergeState.docMaps;

      for (int readerIdx = 0; readerIdx < readers.length; readerIdx++) {
        final FloatVectorValues values = readers[readerIdx].getFloatVectorValues(fieldName);
        perReaderFloatVectorValues[readerIdx] = values;

        KnnVectorValues.DocIndexIterator it = values.iterator();
        for (int docId = it.nextDoc();
            docId != DocIdSetIterator.NO_MORE_DOCS;
            docId = it.nextDoc()) {
          if (docMaps[readerIdx].get(docId) != -1) {
            final int globalOrd = docMaps[readerIdx].get(docId);
            ordMapping[globalOrd][READER_ID] = readerIdx;
            ordMapping[globalOrd][READER_ORD] = docId;
          }

          documentsIterated++;
        }
      }

      if (documentsIterated < totalVectorsCount) {
        throw new IllegalStateException(
            "More documents were expected than what was found in the readers. "
                + " Expected at least number of total Vectors: "
                + totalVectorsCount
                + " but found only "
                + documentsIterated
                + " documents");
      }
    }

    @Override
    public int size() {
      return size;
    }

    @Override
    public int dimension() {
      return dimension;
    }

    @Override
    public VectorFloat<?> getVector(int ord) {
      if (ord < 0 || ord >= totalDocsCount) {
        throw new IllegalArgumentException("Ordinal out of bounds " + ord);
      }

      try {
        final int readerIdx = ordMapping[ord][READER_ID];
        final int readerOrd = ordMapping[ord][READER_ORD];

        synchronized (this) {
          final FloatVectorValues values = perReaderFloatVectorValues[readerIdx];
          final float[] vector = values.vectorValue(readerOrd);
          final float[] copy = new float[vector.length];
          System.arraycopy(vector, 0, copy, 0, vector.length);
          return VECTOR_TYPE_SUPPORT.createFloatVector(copy);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public boolean isValueShared() {
      return false;
    }

    @Override
    public RandomAccessVectorValues copy() {
      throw new UnsupportedOperationException("Copy not supported");
    }
  }

  static class RandomAccessVectorValuesOverFlatFields implements RandomAccessVectorValues {
    private final VectorTypeSupport VECTOR_TYPE_SUPPORT =
        VectorizationProvider.getInstance().getVectorTypeSupport();

    private final FlatFieldVectorsWriter<?> flatFieldVectorsWriter;
    private final int dimension;

    RandomAccessVectorValuesOverFlatFields(
        FlatFieldVectorsWriter<?> flatFieldVectorsWriter, FieldInfo fieldInfo) {
      this.flatFieldVectorsWriter = flatFieldVectorsWriter;
      this.dimension = fieldInfo.getVectorDimension();
    }

    @Override
    public int size() {
      return flatFieldVectorsWriter.getVectors().size();
    }

    @Override
    public int dimension() {
      return dimension;
    }

    @Override
    public VectorFloat<?> getVector(int nodeId) {
      final float[] vector = (float[]) flatFieldVectorsWriter.getVectors().get(nodeId);
      return VECTOR_TYPE_SUPPORT.createFloatVector(vector);
    }

    @Override
    public boolean isValueShared() {
      return false;
    }

    @Override
    public RandomAccessVectorValues copy() {
      throw new UnsupportedOperationException("Copy not supported");
    }
  }
}
