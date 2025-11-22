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
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OnDiskSequentialGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.OrdinalMapper;
import io.github.jbellis.jvector.graph.disk.feature.Feature;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.disk.feature.InlineVectors;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.IntUnaryOperator;
import java.util.stream.IntStream;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnFieldVectorsWriter;
import org.apache.lucene.codecs.KnnVectorsWriter;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.DocsWithFieldSet;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.Sorter;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.RamUsageEstimator;

/**
 * JVectorWriter is responsible for writing vector data into index segments using the JVector
 * library.
 *
 * <h2>Persisting the JVector Graph Index</h2>
 *
 * <p>Flushing data into disk segments occurs in two scenarios:
 *
 * <ol>
 *   <li>When the segment is being flushed to disk (e.g., when a new segment is created) via {@link
 *       #flush(int, Sorter.DocMap)}
 *   <li>When the segment is a result of a merge (e.g., when multiple segments are merged into one)
 *       via {@link #mergeOneField(FieldInfo, MergeState)}
 * </ol>
 *
 * <h2>jVector Graph Ordinal to Lucene Document ID Mapping</h2>
 *
 * <p>JVector keeps its own ordinals to identify its nodes. Those ordinals can be different from the
 * Lucene document IDs. Document IDs in Lucene can change after a merge operation. Therefore, we
 * need to maintain a mapping between JVector ordinals and Lucene document IDs that can hold across
 * merges.
 *
 * <p>Document IDs in Lucene are mapped across merges and sorts using the {@link
 * org.apache.lucene.index.MergeState.DocMap} for merges and {@link
 * org.apache.lucene.index.Sorter.DocMap} for flush/sorts. For jVector however, we don't want to
 * modify the ordinals in the jVector graph, and therefore we need to maintain a mapping between the
 * jVector ordinals and the new Lucene document IDs. This is achieved by keeping checkpoints of the
 * {@link GraphNodeIdToDocMap} class in the index metadata and allowing us to update the mapping as
 * needed across merges by constructing a new mapping from the previous mapping and the {@link
 * org.apache.lucene.index.MergeState.DocMap} provided in the {@link MergeState}.
 */
public class JVectorWriter extends KnnVectorsWriter {
  private static final VectorTypeSupport VECTOR_TYPE_SUPPORT =
      VectorizationProvider.getInstance().getVectorTypeSupport();
  private static final long SHALLOW_RAM_BYTES_USED =
      RamUsageEstimator.shallowSizeOfInstance(JVectorWriter.class);

  private final List<FieldWriter> fields = new ArrayList<>();

  private final IndexOutput meta;
  private final IndexOutput vectorIndex;
  private final String indexDataFileName;
  private final String baseDataFileName;
  private final SegmentWriteState segmentWriteState;
  private final int maxConn;
  private final int beamWidth;
  private final float degreeOverflow;
  private final float alpha;
  /// Number of subspaces used per vector in PQ quantization as a function of the original dimension
  private final IntUnaryOperator numberOfSubspacesPerVectorSupplier;
  private final int
      minimumBatchSizeForQuantization; // Threshold for the vector count above which we will trigger
  // PQ quantization
  private final boolean hierarchyEnabled;

  private boolean finished = false;

  public JVectorWriter(
      SegmentWriteState segmentWriteState,
      int maxConn,
      int beamWidth,
      float degreeOverflow,
      float alpha,
      IntUnaryOperator numberOfSubspacesPerVectorSupplier,
      int minimumBatchSizeForQuantization,
      boolean hierarchyEnabled)
      throws IOException {
    this.segmentWriteState = segmentWriteState;
    this.maxConn = maxConn;
    this.beamWidth = beamWidth;
    this.degreeOverflow = degreeOverflow;
    this.alpha = alpha;
    this.numberOfSubspacesPerVectorSupplier = numberOfSubspacesPerVectorSupplier;
    this.minimumBatchSizeForQuantization = minimumBatchSizeForQuantization;
    this.hierarchyEnabled = hierarchyEnabled;
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

  @Override
  public KnnFieldVectorsWriter<?> addField(FieldInfo fieldInfo) throws IOException {
    if (fieldInfo.getVectorEncoding() == VectorEncoding.BYTE) {
      final String errorMessage =
          "byte[] vectors are not supported in JVector. "
              + "Instead you should only use float vectors and leverage product quantization during indexing."
              + "This can provides much greater savings in storage and memory";
      throw new UnsupportedOperationException(errorMessage);
    }
    FieldWriter newField = new FieldWriter(fieldInfo);

    fields.add(newField);
    return newField;
  }

  @Override
  public void mergeOneField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    try {
      switch (fieldInfo.getVectorEncoding()) {
        case BYTE:
          throw new UnsupportedEncodingException("Byte vectors are not supported in JVector.");
        case FLOAT32:
          mergeAndWriteField(fieldInfo, mergeState);
          break;
      }
    } catch (Exception e) {
      throw e;
    }
  }

  @Override
  public void flush(int maxDoc, Sorter.DocMap sortMap) throws IOException {
    for (FieldWriter field : fields) {
      final DocsWithFieldSet newDocIds;
      final OrdinalMapper ordinalMapper;
      if (sortMap != null) {
        assert field.docIds.cardinality() <= sortMap.size();
        final int size = field.docIds.cardinality();
        final int[] oldToNew = new int[size];
        final int[] newToOld = new int[size];
        newDocIds = new DocsWithFieldSet();
        KnnVectorsWriter.mapOldOrdToNewOrd(field.docIds, sortMap, oldToNew, newToOld, newDocIds);
        ordinalMapper = new ArrayOrdinalMapper(size - 1, oldToNew, newToOld);
      } else {
        newDocIds = field.docIds;
        ordinalMapper = null;
      }
      final RandomAccessVectorValues randomAccessVectorValues = field.toRandomAccessVectorValues();
      final BuildScoreProvider buildScoreProvider;
      final PQVectors pqVectors;
      final FieldInfo fieldInfo = field.fieldInfo;
      if (randomAccessVectorValues.size() >= minimumBatchSizeForQuantization) {
        pqVectors = getPQVectors(randomAccessVectorValues, fieldInfo);
        buildScoreProvider =
            BuildScoreProvider.pqBuildScoreProvider(
                JVectorFormat.toJVectorSimilarity(fieldInfo.getVectorSimilarityFunction()),
                pqVectors);
      } else {
        // Not enough vectors for quantization; use full precision vectors instead
        pqVectors = null;
        buildScoreProvider =
            BuildScoreProvider.randomAccessScoreProvider(
                randomAccessVectorValues,
                JVectorFormat.toJVectorSimilarity(fieldInfo.getVectorSimilarityFunction()));
      }

      final GraphNodeIdToDocMap graphNodeIdToDocMap = new GraphNodeIdToDocMap(newDocIds);
      OnHeapGraphIndex graph =
          getGraph(
              buildScoreProvider,
              randomAccessVectorValues,
              fieldInfo,
              segmentWriteState.segmentInfo.name,
              Runnable::run);
      writeField(
          field.fieldInfo,
          randomAccessVectorValues,
          pqVectors,
          ordinalMapper,
          graphNodeIdToDocMap,
          graph);
    }
  }

  private record ArrayOrdinalMapper(int maxOrdinal, int[] oldToNew, int[] newToOld)
      implements OrdinalMapper {
    @Override
    public int maxOrdinal() {
      return maxOrdinal;
    }

    @Override
    public int oldToNew(int oldOrdinal) {
      return oldToNew[oldOrdinal];
    }

    @Override
    public int newToOld(int newOrdinal) {
      return newToOld[newOrdinal];
    }
  }

  private void writeField(
      FieldInfo fieldInfo,
      RandomAccessVectorValues randomAccessVectorValues,
      PQVectors pqVectors,
      OrdinalMapper ordinalMapper,
      GraphNodeIdToDocMap graphNodeIdToDocMap,
      OnHeapGraphIndex graph)
      throws IOException {
    final var vectorIndexFieldMetadata =
        writeGraph(
            graph,
            randomAccessVectorValues,
            fieldInfo,
            pqVectors,
            ordinalMapper,
            graphNodeIdToDocMap);
    meta.writeInt(fieldInfo.number);
    vectorIndexFieldMetadata.toOutput(meta);
  }

  /**
   * Writes the graph and PQ codebooks and compressed vectors to the vector index file
   *
   * @param graph graph
   * @param randomAccessVectorValues random access vector values
   * @param fieldInfo field info
   * @return Tuple of start offset and length of the graph
   * @throws IOException IOException
   */
  private VectorIndexFieldMetadata writeGraph(
      OnHeapGraphIndex graph,
      RandomAccessVectorValues randomAccessVectorValues,
      FieldInfo fieldInfo,
      PQVectors pqVectors,
      OrdinalMapper ordinalMapper,
      GraphNodeIdToDocMap graphNodeIdToDocMap)
      throws IOException {
    // field data file, which contains the graph
    final String vectorIndexFieldFileName =
        baseDataFileName + "_" + fieldInfo.name + "." + JVectorFormat.VECTOR_INDEX_EXTENSION;

    try (IndexOutput indexOutput =
            segmentWriteState.directory.createOutput(
                vectorIndexFieldFileName, segmentWriteState.context);
        final var jVectorIndexWriter = new JVectorIndexWriter(indexOutput)) {
      // Header for the field data file
      CodecUtil.writeIndexHeader(
          indexOutput,
          JVectorFormat.VECTOR_INDEX_CODEC_NAME,
          JVectorFormat.VERSION_CURRENT,
          segmentWriteState.segmentInfo.getId(),
          segmentWriteState.segmentSuffix);
      final long startOffset = indexOutput.getFilePointer();
      final var writerBuilder =
          new OnDiskSequentialGraphIndexWriter.Builder(graph, jVectorIndexWriter)
              .with(new InlineVectors(randomAccessVectorValues.dimension()));
      if (ordinalMapper != null) {
        writerBuilder.withMapper(ordinalMapper);
      }
      try (var writer = writerBuilder.build()) {
        var suppliers =
            Feature.singleStateFactory(
                FeatureId.INLINE_VECTORS,
                nodeId -> new InlineVectors.State(randomAccessVectorValues.getVector(nodeId)));
        writer.write(suppliers);
        final long endGraphOffset = jVectorIndexWriter.position();

        // If PQ is enabled and we have enough vectors, write the PQ codebooks and compressed
        // vectors
        final long pqOffset;
        final long pqLength;
        if (pqVectors != null) {
          pqOffset = endGraphOffset;
          // write the compressed vectors and codebooks to disk
          pqVectors.write(jVectorIndexWriter);
          pqLength = jVectorIndexWriter.position() - endGraphOffset;
        } else {
          pqOffset = 0;
          pqLength = 0;
        }
        CodecUtil.writeFooter(indexOutput);

        return new VectorIndexFieldMetadata(
            fieldInfo.number,
            fieldInfo.getVectorEncoding(),
            JVectorFormat.toJVectorSimilarity(fieldInfo.getVectorSimilarityFunction()),
            randomAccessVectorValues.dimension(),
            startOffset,
            endGraphOffset - startOffset,
            pqOffset,
            pqLength,
            degreeOverflow,
            graphNodeIdToDocMap);
      }
    }
  }

  private PQVectors getPQVectors(
      RandomAccessVectorValues randomAccessVectorValues, FieldInfo fieldInfo) throws IOException {
    final boolean globallyCenter =
        switch (fieldInfo.getVectorSimilarityFunction()) {
          case EUCLIDEAN -> true;
          case COSINE, DOT_PRODUCT, MAXIMUM_INNER_PRODUCT -> false;
        };
    final int M =
        numberOfSubspacesPerVectorSupplier.applyAsInt(randomAccessVectorValues.dimension());
    final var numberOfClustersPerSubspace =
        Math.min(256, randomAccessVectorValues.size()); // number of centroids per
    // subspace

    ProductQuantization pq =
        ProductQuantization.compute(
            randomAccessVectorValues, M, numberOfClustersPerSubspace, globallyCenter);

    return (PQVectors) pq.encodeAll(randomAccessVectorValues);
  }

  /// Metadata about the index to be persisted on disk
  public static class VectorIndexFieldMetadata {
    final int fieldNumber;
    final VectorEncoding vectorEncoding;
    final VectorSimilarityFunction vectorSimilarityFunction;
    final int vectorDimension;
    final long vectorIndexOffset;
    final long vectorIndexLength;
    final long pqCodebooksAndVectorsOffset;
    final long pqCodebooksAndVectorsLength;
    final float degreeOverflow; // important when leveraging cache
    final GraphNodeIdToDocMap graphNodeIdToDocMap;

    public VectorIndexFieldMetadata(
        int fieldNumber,
        VectorEncoding vectorEncoding,
        VectorSimilarityFunction vectorSimilarityFunction,
        int vectorDimension,
        long vectorIndexOffset,
        long vectorIndexLength,
        long pqCodebooksAndVectorsOffset,
        long pqCodebooksAndVectorsLength,
        float degreeOverflow,
        GraphNodeIdToDocMap graphNodeIdToDocMap) {
      this.fieldNumber = fieldNumber;
      this.vectorEncoding = vectorEncoding;
      this.vectorSimilarityFunction = vectorSimilarityFunction;
      this.vectorDimension = vectorDimension;
      this.vectorIndexOffset = vectorIndexOffset;
      this.vectorIndexLength = vectorIndexLength;
      this.pqCodebooksAndVectorsOffset = pqCodebooksAndVectorsOffset;
      this.pqCodebooksAndVectorsLength = pqCodebooksAndVectorsLength;
      this.degreeOverflow = degreeOverflow;
      this.graphNodeIdToDocMap = graphNodeIdToDocMap;
    }

    public void toOutput(IndexOutput out) throws IOException {
      out.writeInt(fieldNumber);
      out.writeInt(vectorEncoding.ordinal());
      out.writeInt(vectorSimilarityFunction.ordinal());
      out.writeVInt(vectorDimension);
      out.writeVLong(vectorIndexOffset);
      out.writeVLong(vectorIndexLength);
      out.writeVLong(pqCodebooksAndVectorsOffset);
      out.writeVLong(pqCodebooksAndVectorsLength);
      out.writeInt(Float.floatToIntBits(degreeOverflow));
      graphNodeIdToDocMap.toOutput(out);
    }

    public VectorIndexFieldMetadata(IndexInput in) throws IOException {
      this.fieldNumber = in.readInt();
      this.vectorEncoding = readVectorEncoding(in);
      this.vectorSimilarityFunction = VectorSimilarityFunction.values()[in.readInt()];
      this.vectorDimension = in.readVInt();
      this.vectorIndexOffset = in.readVLong();
      this.vectorIndexLength = in.readVLong();
      this.pqCodebooksAndVectorsOffset = in.readVLong();
      this.pqCodebooksAndVectorsLength = in.readVLong();
      this.degreeOverflow = Float.intBitsToFloat(in.readInt());
      this.graphNodeIdToDocMap = new GraphNodeIdToDocMap(in);
    }
  }

  @Override
  public void finish() throws IOException {
    if (finished) {
      throw new IllegalStateException("already finished");
    }
    finished = true;

    if (meta != null) {
      // write end of fields marker
      meta.writeInt(-1);
      CodecUtil.writeFooter(meta);
    }

    if (vectorIndex != null) {
      CodecUtil.writeFooter(vectorIndex);
    }
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(meta, vectorIndex);
  }

  @Override
  public long ramBytesUsed() {
    long total = SHALLOW_RAM_BYTES_USED;
    for (FieldWriter field : fields) {
      // the field tracks the delegate field usage
      total += field.ramBytesUsed();
    }
    return total;
  }

  /**
   * The FieldWriter class is responsible for writing vector field data into index segments. It
   * provides functionality to process vector values as those being added, manage memory usage, and
   * build HNSW graph indexing structures for efficient retrieval during search queries.
   */
  static class FieldWriter extends KnnFieldVectorsWriter<float[]> {
    private final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(FieldWriter.class);
    private final FieldInfo fieldInfo;
    // The ordering of docIds matches the ordering of vectors, the index in this list corresponds to
    // the jVector ordinal
    private final List<VectorFloat<?>> vectors = new ArrayList<>();
    private DocsWithFieldSet docIds;

    FieldWriter(FieldInfo fieldInfo) {
      /** For creating a new field from a flat field vectors writer. */
      this.fieldInfo = fieldInfo;
      this.docIds = new DocsWithFieldSet();
    }

    @Override
    public void addValue(int docID, float[] vectorValue) throws IOException {
      if (docID < docIds.cardinality()) {
        throw new IllegalArgumentException(
            "VectorValuesField \""
                + fieldInfo.name
                + "\" appears more than once in this document (only one value is allowed per field)");
      }
      docIds.add(docID);
      vectors.add(VECTOR_TYPE_SUPPORT.createFloatVector(copyValue(vectorValue)));
    }

    @Override
    public float[] copyValue(float[] vectorValue) {
      return vectorValue.clone();
    }

    public RandomAccessVectorValues toRandomAccessVectorValues() {
      return new ListRandomAccessVectorValues(vectors, fieldInfo.getVectorDimension());
    }

    @Override
    public long ramBytesUsed() {
      return SHALLOW_SIZE
          + (long) vectors.size() * fieldInfo.getVectorDimension() * Float.BYTES
          + docIds.ramBytesUsed();
    }
  }

  private void mergeAndWriteField(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
    assert fieldInfo.hasVectorValues();
    final int dimension = fieldInfo.getVectorDimension();
    final int mergeCount = mergeState.knnVectorsReaders.length;

    // Collect the sub-readers into a list to make a DocIdMerger
    final List<SubFloatVectors> subs = new ArrayList<>(mergeCount);
    final FloatVectorValues[] vectors = new FloatVectorValues[mergeCount];
    for (int i = 0; i < mergeCount; ++i) {
      if (false == MergedVectorValues.hasVectorValues(mergeState.fieldInfos[i], fieldInfo.name)) {
        continue;
      }
      final var reader = mergeState.knnVectorsReaders[i];
      if (reader == null) {
        continue;
      }
      final var values = reader.getFloatVectorValues(fieldInfo.name);
      if (values == null || values.size() == 0) {
        continue;
      }

      assert values.dimension() == dimension;
      subs.add(new SubFloatVectors(mergeState.docMaps[i], i, values));
      vectors[i] = values;
    }

    // These arrays may be larger than strictly necessary if there are deleted docs/missing fields
    final int totalMaxDocs = Arrays.stream(mergeState.maxDocs).reduce(0, Math::addExact);
    final int[] liveDocCounts = new int[mergeCount];
    final DocsWithFieldSet docIds = new DocsWithFieldSet();
    final int[] ordToReaderIndex = new int[totalMaxDocs];
    final int[] ordToReaderOrd = new int[totalMaxDocs];

    // Construct ordinal mappings for the new graph
    int ord = 0;
    final var docIdMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);
    for (var sub = docIdMerger.next(); sub != null; sub = docIdMerger.next()) {
      final int readerIndex = sub.readerIndex;
      liveDocCounts[readerIndex] += 1;
      docIds.add(sub.mappedDocID);
      ordToReaderIndex[ord] = sub.readerIndex;
      ordToReaderOrd[ord] = sub.index();
      ord += 1;
    }

    final int totalLiveDocsCount = ord;
    if (totalLiveDocsCount == 0) {
      // Avoid writing an empty graph
      return;
    }

    // Make a RandomAccessVectorValues instance using the new graph ordinals
    final var ravv =
        new RandomAccessMergedFloatVectorValues(
            totalLiveDocsCount,
            dimension,
            vectors,
            i -> ordToReaderIndex[i],
            i -> ordToReaderOrd[i]);

    // Find the largest quantized reader to re-use its PQ codebook, if possible
    int largestQuantizedReaderIndex = 0;
    ProductQuantization pq = null;
    for (int i = 0; i < liveDocCounts.length; ++i) {
      if (liveDocCounts[i] > liveDocCounts[largestQuantizedReaderIndex]) {
        if (mergeState.knnVectorsReaders[i] instanceof JVectorReader jVectorReader) {
          final var maybeNewPq = jVectorReader.getProductQuantizationForField(fieldInfo.name);
          if (maybeNewPq.isPresent()) {
            largestQuantizedReaderIndex = i;
            pq = maybeNewPq.get();
          }
        }
      }
    }

    // Perform PQ if applicable
    final PQVectors pqVectors;
    if (pq != null) {
      // Refine the leadingCompressor with the remaining vectors in the merge
      ProductQuantization newPq = pq;
      for (int i = 0; i < mergeCount; i++) {
        if (i == largestQuantizedReaderIndex || vectors[i] == null) {
          // Skip the reader associated with the re-used PQ codebook
          continue;
        }
        final FloatVectorValues values = vectors[i];
        final RandomAccessVectorValues randomAccessVectorValues =
            new RandomAccessVectorValuesOverVectorValues(values);
        newPq = newPq.refine(randomAccessVectorValues);
      }
      pqVectors = (PQVectors) newPq.encodeAll(ravv);
    } else if (ravv.size() >= minimumBatchSizeForQuantization) {
      // No pre-existing codebooks, check if we have enough vectors to trigger quantization
      pqVectors = getPQVectors(ravv, fieldInfo);
    } else {
      pqVectors = null;
    }

    final BuildScoreProvider buildScoreProvider;
    final var similarityFunction =
        JVectorFormat.toJVectorSimilarity(fieldInfo.getVectorSimilarityFunction());
    if (pqVectors != null) {
      // Re-use PQ codebooks to build a new graph from scratch
      buildScoreProvider = BuildScoreProvider.pqBuildScoreProvider(similarityFunction, pqVectors);
      // Pre-init the diversity provider here to avoid doing it lazily (as it could block the SIMD
      // threads)
      buildScoreProvider.diversityProviderFor(0);
    } else {
      buildScoreProvider = BuildScoreProvider.randomAccessScoreProvider(ravv, similarityFunction);
    }
    final var graphNodeIdToDocMap = new GraphNodeIdToDocMap(docIds);
    final var graph =
        getGraph(
            buildScoreProvider,
            ravv,
            fieldInfo,
            segmentWriteState.segmentInfo.name,
            mergeState.intraMergeTaskExecutor);
    writeField(fieldInfo, ravv, pqVectors, null, graphNodeIdToDocMap, graph);
  }

  private static final class SubFloatVectors extends DocIDMerger.Sub {
    final int readerIndex;
    final KnnVectorValues.DocIndexIterator iterator;
    int docId = -1;

    SubFloatVectors(MergeState.DocMap docMap, int readerIndex, FloatVectorValues values) {
      super(docMap);
      this.readerIndex = readerIndex;
      this.iterator = values.iterator();
    }

    @Override
    public int nextDoc() throws IOException {
      docId = iterator.nextDoc();
      return docId;
    }

    public int index() {
      return iterator.index();
    }
  }

  private static final class RandomAccessMergedFloatVectorValues
      implements RandomAccessVectorValues {
    private final int size;
    private final int dimension;
    private final FloatVectorValues[] vectors;
    private final IntUnaryOperator ordToReader;
    private final IntUnaryOperator ordToReaderOrd;

    public RandomAccessMergedFloatVectorValues(
        int size,
        int dimension,
        FloatVectorValues[] values,
        IntUnaryOperator ordToReader,
        IntUnaryOperator ordToReaderOrd) {
      this.size = size;
      this.dimension = dimension;
      this.vectors = values;
      this.ordToReader = ordToReader;
      this.ordToReaderOrd = ordToReaderOrd;
    }

    @Override
    public RandomAccessMergedFloatVectorValues copy() {
      final FloatVectorValues[] newVectors = new FloatVectorValues[vectors.length];
      for (int i = 0; i < newVectors.length; ++i) {
        if (vectors[i] != null) {
          try {
            newVectors[i] = vectors[i].copy();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
      }
      return new RandomAccessMergedFloatVectorValues(
          size, dimension, newVectors, ordToReader, ordToReaderOrd);
    }

    @Override
    public int dimension() {
      return dimension;
    }

    @Override
    public VectorFloat<?> getVector(int nodeId) {
      final var vector = VECTOR_TYPE_SUPPORT.createFloatVector(dimension);
      getVectorInto(nodeId, vector, 0);
      return vector;
    }

    @Override
    public void getVectorInto(int node, VectorFloat<?> destinationVector, int offset) {
      final FloatVectorValues values = vectors[ordToReader.applyAsInt(node)];
      final int ord = ordToReaderOrd.applyAsInt(node);

      if (values instanceof JVectorFloatVectorValues jVectorValues) {
        jVectorValues.getVectorInto(ord, destinationVector, offset);
      }

      final float[] srcVector;
      try {
        srcVector = values.vectorValue(ord);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      for (int i = 0; i < srcVector.length; ++i) {
        destinationVector.set(i + offset, srcVector[i]);
      }
    }

    @Override
    public boolean isValueShared() {
      // force thread-local copies
      return true;
    }

    @Override
    public int size() {
      return size;
    }
  }

  /**
   * This method will return the graph index for the field
   *
   * @return OnHeapGraphIndex
   */
  public OnHeapGraphIndex getGraph(
      BuildScoreProvider buildScoreProvider,
      RandomAccessVectorValues randomAccessVectorValues,
      FieldInfo fieldInfo,
      String segmentName,
      Executor executor) {
    assert randomAccessVectorValues.size() > 0 : "Cannot build empty graph";
    final GraphIndexBuilder graphIndexBuilder =
        new GraphIndexBuilder(
            buildScoreProvider,
            fieldInfo.getVectorDimension(),
            maxConn,
            beamWidth,
            degreeOverflow,
            alpha,
            hierarchyEnabled);

    /*
     * We cannot always use randomAccessVectorValues for the graph building
     * because it's size will not always correspond to the document count.
     * To have the right mapping from docId to vector ordinal we need to use the mergedFloatVector.
     * This is the case when we are merging segments and we might have more documents than vectors.
     */
    final OnHeapGraphIndex graphIndex;
    final var vv = randomAccessVectorValues.threadLocalSupplier();

    // parallel graph construction from the merge documents Ids
    final int size = randomAccessVectorValues.size();
    IntStream.range(0, size)
        .mapToObj(
            ord ->
                CompletableFuture.runAsync(
                    () -> graphIndexBuilder.addGraphNode(ord, vv.get().getVector(ord)), executor))
        .reduce((a, b) -> a.runAfterBoth(b, () -> {}))
        .ifPresent(CompletableFuture::join);
    graphIndexBuilder.cleanup();
    graphIndex = (OnHeapGraphIndex) graphIndexBuilder.getGraph();

    return graphIndex;
  }

  static class RandomAccessVectorValuesOverVectorValues implements RandomAccessVectorValues {
    private final FloatVectorValues values;

    public RandomAccessVectorValuesOverVectorValues(FloatVectorValues values) {
      this.values = values;
    }

    @Override
    public int size() {
      return values.size();
    }

    @Override
    public int dimension() {
      return values.dimension();
    }

    @Override
    public VectorFloat<?> getVector(int nodeId) {
      try {
        final float[] vector = values.vectorValue(nodeId);
        return VECTOR_TYPE_SUPPORT.createFloatVector(ArrayUtil.copyArray(vector));
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public boolean isValueShared() {
      // Access to float values is not thread safe
      return true;
    }

    @Override
    public RandomAccessVectorValues copy() {
      try {
        return new RandomAccessVectorValuesOverVectorValues(values.copy());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
