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

import io.github.jbellis.jvector.disk.RandomAccessReader;
import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.similarity.DefaultSearchScoreProvider;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.graph.similarity.SearchScoreProvider;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.*;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.*;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;
import org.opensearch.knn.common.KNNConstants;

public class JVectorReader extends KnnVectorsReader {
  private static final VectorTypeSupport VECTOR_TYPE_SUPPORT =
      VectorizationProvider.getInstance().getVectorTypeSupport();

  private final FieldInfos fieldInfos;
  private final String baseDataFileName;
  // Maps field name to field entries
  private final Map<String, FieldEntry> fieldEntryMap = new HashMap<>(1);
  private final Directory directory;
  private final SegmentReadState state;

  public JVectorReader(SegmentReadState state) throws IOException {
    this.state = state;
    this.fieldInfos = state.fieldInfos;
    this.baseDataFileName = state.segmentInfo.name + "_" + state.segmentSuffix;
    final String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, JVectorFormat.META_EXTENSION);
    this.directory = state.directory;
    boolean success = false;
    try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName)) {
      CodecUtil.checkIndexHeader(
          meta,
          JVectorFormat.META_CODEC_NAME,
          JVectorFormat.VERSION_START,
          JVectorFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      readFields(meta);
      CodecUtil.checkFooter(meta);

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @Override
  public void checkIntegrity() throws IOException {
    for (FieldEntry fieldEntry : fieldEntryMap.values()) {
      // Verify the vector index file
      try (var indexInput =
          state.directory.openInput(fieldEntry.vectorIndexFieldDataFileName, IOContext.READONCE)) {
        CodecUtil.checksumEntireFile(indexInput);
      }

      // Verify the neighbors score cache file
      try (var indexInput =
          state.directory.openInput(
              fieldEntry.neighborsScoreCacheIndexFieldFileName, IOContext.READONCE)) {
        CodecUtil.checksumEntireFile(indexInput);
      }
    }
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    final FieldEntry fieldEntry = fieldEntryMap.get(field);
    return new JVectorFloatVectorValues(
        fieldEntry.index, fieldEntry.similarityFunction, fieldEntry.graphNodeIdToDocMap);
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    /** Byte vector values are not supported in jVector library. Instead use PQ. */
    return null;
  }

  public Optional<ProductQuantization> getProductQuantizationForField(String field)
      throws IOException {
    final FieldEntry fieldEntry = fieldEntryMap.get(field);
    if (fieldEntry.pqVectors == null) {
      return Optional.empty();
    }

    return Optional.of(fieldEntry.pqVectors.getCompressor());
  }

  public RandomAccessReader getNeighborsScoreCacheForField(String field) throws IOException {
    final FieldEntry fieldEntry = fieldEntryMap.get(field);
    return fieldEntry.neighborsScoreCacheIndexReaderSupplier.get();
  }

  public OnDiskGraphIndex getOnDiskGraphIndex(String field) throws IOException {
    return fieldEntryMap.get(field).index;
  }

  @Override
  public void search(String field, float[] target, KnnCollector knnCollector, Bits acceptDocs)
      throws IOException {
    final OnDiskGraphIndex index = fieldEntryMap.get(field).index;
    final JVectorKnnCollector jvectorKnnCollector;
    if (knnCollector instanceof JVectorKnnCollector) {
      jvectorKnnCollector = (JVectorKnnCollector) knnCollector;
    } else {
      // KnnCollector must be of type JVectorKnnCollector, for now we will re-wrap it but this is
      // not ideal
      jvectorKnnCollector =
          new JVectorKnnCollector(
              knnCollector,
              KNNConstants.DEFAULT_QUERY_SIMILARITY_THRESHOLD.floatValue(),
              KNNConstants.DEFAULT_QUERY_RERANK_FLOOR.floatValue(),
              KNNConstants.DEFAULT_OVER_QUERY_FACTOR,
              KNNConstants.DEFAULT_QUERY_USE_PRUNING);
    }

    // search for a random vector using a GraphSearcher and SearchScoreProvider
    VectorFloat<?> q = VECTOR_TYPE_SUPPORT.createFloatVector(target);
    final SearchScoreProvider ssp;

    try (var view = index.getView()) {
      if (fieldEntryMap.get(field).pqVectors
          != null) { // Quantized, use the precomputed score function
        final PQVectors pqVectors = fieldEntryMap.get(field).pqVectors;
        // SearchScoreProvider that does a first pass with the loaded-in-memory PQVectors,
        // then reranks with the exact vectors that are stored on disk in the index
        ScoreFunction.ApproximateScoreFunction asf =
            pqVectors.precomputedScoreFunctionFor(q, fieldEntryMap.get(field).similarityFunction);
        ScoreFunction.ExactScoreFunction reranker =
            view.rerankerFor(q, fieldEntryMap.get(field).similarityFunction);
        ssp = new DefaultSearchScoreProvider(asf, reranker);
      } else { // Not quantized, used typical searcher
        ssp =
            DefaultSearchScoreProvider.exact(q, fieldEntryMap.get(field).similarityFunction, view);
      }
      final GraphNodeIdToDocMap jvectorLuceneDocMap = fieldEntryMap.get(field).graphNodeIdToDocMap;
      // Convert the acceptDocs bitmap from Lucene to jVector ordinal bitmap filter
      // Logic works as follows: if acceptDocs is null, we accept all ordinals. Otherwise, we check
      // if the jVector ordinal has a
      // corresponding Lucene doc ID accepted by acceptDocs filter.
      io.github.jbellis.jvector.util.Bits compatibleBits =
          ord -> acceptDocs == null || acceptDocs.get(jvectorLuceneDocMap.getLuceneDocId(ord));

      try (var graphSearcher = new GraphSearcher(index)) {
        final var searchResults =
            graphSearcher.search(
                ssp,
                jvectorKnnCollector.k(),
                jvectorKnnCollector.k() * jvectorKnnCollector.getOverQueryFactor(),
                jvectorKnnCollector.getThreshold(),
                jvectorKnnCollector.getRerankFloor(),
                compatibleBits);
        for (SearchResult.NodeScore ns : searchResults.getNodes()) {
          jvectorKnnCollector.collect(jvectorLuceneDocMap.getLuceneDocId(ns.node), ns.score);
        }
      }
    }
  }

  @Override
  public void search(String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs)
      throws IOException {
    // TODO: implement this
    throw new UnsupportedOperationException("Byte vector search is not supported yet with jVector");
  }

  @Override
  public void close() throws IOException {
    for (FieldEntry fieldEntry : fieldEntryMap.values()) {
      IOUtils.close(fieldEntry);
    }
    fieldEntryMap.clear();
  }

  private void readFields(ChecksumIndexInput meta) throws IOException {
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
      final FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldNumber); // read field number
      JVectorWriter.VectorIndexFieldMetadata vectorIndexFieldMetadata =
          new JVectorWriter.VectorIndexFieldMetadata(meta);
      assert fieldInfo.number == vectorIndexFieldMetadata.getFieldNumber();
      fieldEntryMap.put(fieldInfo.name, new FieldEntry(fieldInfo, vectorIndexFieldMetadata));
    }
  }

  class FieldEntry implements Closeable {
    private final FieldInfo fieldInfo;
    private final VectorEncoding vectorEncoding;
    private final VectorSimilarityFunction similarityFunction;
    private final int dimension;
    private final long vectorIndexOffset;
    private final long vectorIndexLength;
    private final long pqCodebooksAndVectorsLength;
    private final long pqCodebooksAndVectorsOffset;
    private final String vectorIndexFieldDataFileName;
    private final String neighborsScoreCacheIndexFieldFileName;
    private final GraphNodeIdToDocMap graphNodeIdToDocMap;
    private final ReaderSupplier indexReaderSupplier;
    private final ReaderSupplier pqCodebooksReaderSupplier;
    private final ReaderSupplier neighborsScoreCacheIndexReaderSupplier;
    private final OnDiskGraphIndex index;
    private final PQVectors pqVectors; // The product quantized vectors with their codebooks

    public FieldEntry(
        FieldInfo fieldInfo, JVectorWriter.VectorIndexFieldMetadata vectorIndexFieldMetadata)
        throws IOException {
      this.similarityFunction =
          VectorSimilarityMapper.ordToDistFunc(
              vectorIndexFieldMetadata.getVectorSimilarityFunction().ordinal());
      this.vectorEncoding = vectorIndexFieldMetadata.getVectorEncoding();
      this.vectorIndexOffset = vectorIndexFieldMetadata.getVectorIndexOffset();
      this.vectorIndexLength = vectorIndexFieldMetadata.getVectorIndexLength();
      this.pqCodebooksAndVectorsLength = vectorIndexFieldMetadata.getPqCodebooksAndVectorsLength();
      this.pqCodebooksAndVectorsOffset = vectorIndexFieldMetadata.getPqCodebooksAndVectorsOffset();
      this.dimension = vectorIndexFieldMetadata.getVectorDimension();
      this.graphNodeIdToDocMap = vectorIndexFieldMetadata.getGraphNodeIdToDocMap();

      this.vectorIndexFieldDataFileName =
          baseDataFileName + "_" + fieldInfo.name + "." + JVectorFormat.VECTOR_INDEX_EXTENSION;
      this.neighborsScoreCacheIndexFieldFileName =
          baseDataFileName
              + "_"
              + fieldInfo.name
              + "."
              + JVectorFormat.NEIGHBORS_SCORE_CACHE_EXTENSION;

      // For the slice we would like to include the Lucene header, unfortunately, we have to do this
      // because jVector use global
      // offsets instead of local offsets
      final long sliceLength =
          vectorIndexLength
              + CodecUtil.indexHeaderLength(
                  JVectorFormat.VECTOR_INDEX_CODEC_NAME, state.segmentSuffix);
      // Load the graph index
      this.indexReaderSupplier =
          new JVectorRandomAccessReader.Supplier(
              directory.openInput(vectorIndexFieldDataFileName, state.context), 0, sliceLength);
      this.index = OnDiskGraphIndex.load(indexReaderSupplier, vectorIndexOffset);

      // If quantized load the compressed product quantized vectors with their codebooks
      if (pqCodebooksAndVectorsLength > 0) {
        assert pqCodebooksAndVectorsOffset > 0;
        if (pqCodebooksAndVectorsOffset < vectorIndexOffset) {
          throw new IllegalArgumentException(
              "pqCodebooksAndVectorsOffset must be greater than vectorIndexOffset");
        }
        this.pqCodebooksReaderSupplier =
            new JVectorRandomAccessReader.Supplier(
                directory.openInput(vectorIndexFieldDataFileName, IOContext.READONCE),
                pqCodebooksAndVectorsOffset,
                pqCodebooksAndVectorsLength);
        try (final var randomAccessReader = pqCodebooksReaderSupplier.get()) {
          this.pqVectors = PQVectors.load(randomAccessReader);
        }
      } else {
        this.pqCodebooksReaderSupplier = null;
        this.pqVectors = null;
      }

      final IndexInput indexInput =
          directory.openInput(neighborsScoreCacheIndexFieldFileName, state.context);
      CodecUtil.readIndexHeader(indexInput);

      this.neighborsScoreCacheIndexReaderSupplier =
          new JVectorRandomAccessReader.Supplier(indexInput);
    }

    @Override
    public void close() throws IOException {
      if (indexReaderSupplier != null) {
        IOUtils.close(indexReaderSupplier::close);
      }
      if (pqCodebooksReaderSupplier != null) {
        IOUtils.close(pqCodebooksReaderSupplier::close);
      }
      if (neighborsScoreCacheIndexReaderSupplier != null) {
        IOUtils.close(neighborsScoreCacheIndexReaderSupplier::close);
      }
    }
  }

  /** Utility class to map between Lucene and jVector similarity functions and metadata ordinals. */
  public static class VectorSimilarityMapper {
    /**
     * List of vector similarity functions supported by <a
     * href="https://github.com/jbellis/jvector">jVector library</a> The similarity functions orders
     * matter in this list because it is later used to resolve the similarity function by ordinal.
     */
    public static final List<VectorSimilarityFunction> JVECTOR_SUPPORTED_SIMILARITY_FUNCTIONS =
        List.of(
            VectorSimilarityFunction.EUCLIDEAN,
            VectorSimilarityFunction.DOT_PRODUCT,
            VectorSimilarityFunction.COSINE);

    public static final Map<
            org.apache.lucene.index.VectorSimilarityFunction, VectorSimilarityFunction>
        LUCENE_TO_JVECTOR_MAP =
            Map.of(
                org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN,
                VectorSimilarityFunction.EUCLIDEAN,
                org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT,
                VectorSimilarityFunction.DOT_PRODUCT,
                org.apache.lucene.index.VectorSimilarityFunction.COSINE,
                VectorSimilarityFunction.COSINE);

    public static int distFuncToOrd(org.apache.lucene.index.VectorSimilarityFunction func) {
      if (LUCENE_TO_JVECTOR_MAP.containsKey(func)) {
        return JVECTOR_SUPPORTED_SIMILARITY_FUNCTIONS.indexOf(LUCENE_TO_JVECTOR_MAP.get(func));
      }

      throw new IllegalArgumentException("invalid distance function: " + func);
    }

    public static VectorSimilarityFunction ordToDistFunc(int ord) {
      return JVECTOR_SUPPORTED_SIMILARITY_FUNCTIONS.get(ord);
    }

    public static org.apache.lucene.index.VectorSimilarityFunction ordToLuceneDistFunc(int ord) {
      if (ord < 0 || ord >= JVECTOR_SUPPORTED_SIMILARITY_FUNCTIONS.size()) {
        throw new IllegalArgumentException("Invalid ordinal: " + ord);
      }
      VectorSimilarityFunction jvectorFunc = JVECTOR_SUPPORTED_SIMILARITY_FUNCTIONS.get(ord);
      for (Map.Entry<org.apache.lucene.index.VectorSimilarityFunction, VectorSimilarityFunction>
          entry : LUCENE_TO_JVECTOR_MAP.entrySet()) {
        if (entry.getValue().equals(jvectorFunc)) {
          return entry.getKey();
        }
      }
      throw new IllegalStateException(
          "No matching Lucene VectorSimilarityFunction found for ordinal: " + ord);
    }
  }
}
