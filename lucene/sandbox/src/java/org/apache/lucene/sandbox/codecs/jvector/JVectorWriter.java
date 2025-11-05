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

import static io.github.jbellis.jvector.quantization.KMeansPlusPlusClusterer.UNWEIGHTED;
import static org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsReader.readVectorEncoding;
import static org.apache.lucene.sandbox.codecs.jvector.JVectorFormat.SIMD_POOL_FLUSH;
import static org.apache.lucene.sandbox.codecs.jvector.JVectorFormat.SIMD_POOL_MERGE;

import io.github.jbellis.jvector.graph.GraphIndexBuilder;
import io.github.jbellis.jvector.graph.ListRandomAccessVectorValues;
import io.github.jbellis.jvector.graph.OnHeapGraphIndex;
import io.github.jbellis.jvector.graph.RandomAccessVectorValues;
import io.github.jbellis.jvector.graph.disk.OnDiskSequentialGraphIndexWriter;
import io.github.jbellis.jvector.graph.disk.feature.Feature;
import io.github.jbellis.jvector.graph.disk.feature.FeatureId;
import io.github.jbellis.jvector.graph.disk.feature.InlineVectors;
import io.github.jbellis.jvector.graph.similarity.BuildScoreProvider;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
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
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
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
 * org.apache.lucene.index.MergeState.DocMap} provided in the {@link MergeState}. And across sorts
 * with {@link FieldWriter#applySort(Sorter.DocMap)} during flushes.
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
      if (sortMap != null) {
        field.applySort(sortMap);
      }
      final RandomAccessVectorValues randomAccessVectorValues = field.toRandomAccessVectorValues();
      final BuildScoreProvider buildScoreProvider;
      final PQVectors pqVectors;
      final FieldInfo fieldInfo = field.fieldInfo;
      if (randomAccessVectorValues.size() >= minimumBatchSizeForQuantization) {
        pqVectors = getPQVectors(randomAccessVectorValues, fieldInfo);
        buildScoreProvider =
            BuildScoreProvider.pqBuildScoreProvider(
                getVectorSimilarityFunction(fieldInfo), pqVectors);
      } else {
        // Not enough vectors for quantization; use full precision vectors instead
        pqVectors = null;
        buildScoreProvider =
            BuildScoreProvider.randomAccessScoreProvider(
                randomAccessVectorValues, getVectorSimilarityFunction(fieldInfo));
      }

      final GraphNodeIdToDocMap graphNodeIdToDocMap = field.createGraphNodeIdToDocMap();
      OnHeapGraphIndex graph =
          getGraph(
              buildScoreProvider,
              randomAccessVectorValues,
              fieldInfo,
              segmentWriteState.segmentInfo.name,
              SIMD_POOL_FLUSH);
      writeField(field.fieldInfo, randomAccessVectorValues, pqVectors, graphNodeIdToDocMap, graph);
    }
  }

  private void writeField(
      FieldInfo fieldInfo,
      RandomAccessVectorValues randomAccessVectorValues,
      PQVectors pqVectors,
      GraphNodeIdToDocMap graphNodeIdToDocMap,
      OnHeapGraphIndex graph)
      throws IOException {
    final var vectorIndexFieldMetadata =
        writeGraph(graph, randomAccessVectorValues, fieldInfo, pqVectors, graphNodeIdToDocMap);
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
      if (graph.size() == 0) {
        CodecUtil.writeFooter(indexOutput);
        return new VectorIndexFieldMetadata(
            fieldInfo.number,
            fieldInfo.getVectorEncoding(),
            fieldInfo.getVectorSimilarityFunction(),
            randomAccessVectorValues.dimension(),
            0,
            0,
            0,
            0,
            degreeOverflow,
            graphNodeIdToDocMap);
      }
      try (var writer =
          new OnDiskSequentialGraphIndexWriter.Builder(graph, jVectorIndexWriter)
              .with(new InlineVectors(randomAccessVectorValues.dimension()))
              .build()) {
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
            fieldInfo.getVectorSimilarityFunction(),
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
    final VectorSimilarityFunction vectorSimilarityFunction =
        fieldInfo.getVectorSimilarityFunction();
    final int M =
        numberOfSubspacesPerVectorSupplier.applyAsInt(randomAccessVectorValues.dimension());
    final var numberOfClustersPerSubspace =
        Math.min(256, randomAccessVectorValues.size()); // number of centroids per
    // subspace
    ProductQuantization pq =
        ProductQuantization.compute(
            randomAccessVectorValues,
            M, // number of subspaces
            numberOfClustersPerSubspace, // number of centroids per subspace
            vectorSimilarityFunction == VectorSimilarityFunction.EUCLIDEAN, // center the dataset
            UNWEIGHTED,
            SIMD_POOL_MERGE,
            ForkJoinPool.commonPool());

    return pq.encodeAll(randomAccessVectorValues, SIMD_POOL_MERGE);
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
      out.writeInt(JVectorReader.VectorSimilarityMapper.distFuncToOrd(vectorSimilarityFunction));
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
      this.vectorSimilarityFunction =
          JVectorReader.VectorSimilarityMapper.ordToLuceneDistFunc(in.readInt());
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

    public void applySort(Sorter.DocMap sortMap) throws IOException {
      // Ensure that all existing docs can be sorted
      final int[] oldToNewOrd = new int[vectors.size()];
      final DocsWithFieldSet oldDocIds = docIds;
      docIds = new DocsWithFieldSet();
      mapOldOrdToNewOrd(oldDocIds, sortMap, oldToNewOrd, null, docIds);

      // Swap vectors into their new ordinals
      for (int oldOrd = 0; oldOrd < vectors.size(); ++oldOrd) {
        final int newOrd = oldToNewOrd[oldOrd];
        if (oldOrd == newOrd) {
          continue;
        }

        // Swap the element at oldOrd into its position at newOrd and update the index mapping
        Collections.swap(vectors, oldOrd, newOrd);
        oldToNewOrd[oldOrd] = oldToNewOrd[newOrd];
        oldToNewOrd[newOrd] = newOrd;

        // The element at oldOrd may be displaced and need to be swapped again
        if (oldToNewOrd[oldOrd] != oldOrd) {
          oldOrd -= 1;
        }
      }
    }

    public RandomAccessVectorValues toRandomAccessVectorValues() {
      return new ListRandomAccessVectorValues(vectors, fieldInfo.getVectorDimension());
    }

    public GraphNodeIdToDocMap createGraphNodeIdToDocMap() {
      return new GraphNodeIdToDocMap(docIds);
    }

    @Override
    public long ramBytesUsed() {
      return SHALLOW_SIZE
          + (long) vectors.size() * fieldInfo.getVectorDimension() * Float.BYTES
          + docIds.ramBytesUsed();
    }
  }

  static io.github.jbellis.jvector.vector.VectorSimilarityFunction getVectorSimilarityFunction(
      FieldInfo fieldInfo) {
    return switch (fieldInfo.getVectorSimilarityFunction()) {
      case EUCLIDEAN -> io.github.jbellis.jvector.vector.VectorSimilarityFunction.EUCLIDEAN;
      case COSINE -> io.github.jbellis.jvector.vector.VectorSimilarityFunction.COSINE;
      case DOT_PRODUCT -> io.github.jbellis.jvector.vector.VectorSimilarityFunction.DOT_PRODUCT;
      // $CASES-OMITTED$
      default ->
          throw new IllegalArgumentException(
              "Unsupported similarity function: " + fieldInfo.getVectorSimilarityFunction());
    };
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

    // Make a RandomAccessVectorValues instance using the new graph ordinals
    final int totalLiveDocsCount = ord;
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
      pqVectors = newPq.encodeAll(ravv, SIMD_POOL_MERGE);
    } else if (ravv.size() >= minimumBatchSizeForQuantization) {
      // No pre-existing codebooks, check if we have enough vectors to trigger quantization
      pqVectors = getPQVectors(ravv, fieldInfo);
    } else {
      pqVectors = null;
    }

    final BuildScoreProvider buildScoreProvider;
    final var similarityFunction = getVectorSimilarityFunction(fieldInfo);
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
            SIMD_POOL_MERGE);
    writeField(fieldInfo, ravv, pqVectors, graphNodeIdToDocMap, graph);
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
      ForkJoinPool SIMD_POOL) {
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
    var vv = randomAccessVectorValues.threadLocalSupplier();

    // parallel graph construction from the merge documents Ids
    final int size = randomAccessVectorValues.size();
    SIMD_POOL
        .submit(
            () ->
                IntStream.range(0, size)
                    .parallel()
                    .forEach(ord -> graphIndexBuilder.addGraphNode(ord, vv.get().getVector(ord))))
        .join();
    graphIndexBuilder.cleanup();
    graphIndex = (OnHeapGraphIndex) graphIndexBuilder.getGraph();

    return graphIndex;
  }

  static class RandomAccessVectorValuesOverVectorValues implements RandomAccessVectorValues {
    private final VectorTypeSupport VECTOR_TYPE_SUPPORT =
        VectorizationProvider.getInstance().getVectorTypeSupport();
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
        // Access to float values is not thread safe
        synchronized (this) {
          final float[] vector = values.vectorValue(nodeId);
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
}
