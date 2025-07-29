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

import io.github.jbellis.jvector.disk.ReaderSupplier;
import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.similarity.DefaultSearchScoreProvider;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction;
import io.github.jbellis.jvector.graph.similarity.SearchScoreProvider;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorScorerUtil;
import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsFormat;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.IOUtils;

/**
 * A KnnVectorsReader implementation for the JVector codec that supports reading and searching
 * on-disk graph-based vector indices and optional product quantized vectors. Loads per-field vector
 * metadata and exposes float vector values, similarity search, and integrity checking. Uses
 * GraphSearcher with optional reranking for approximate or exact search. Falls back to Lucene's
 * FlatVectorsReader during merge operations.
 */
public class JVectorReader extends KnnVectorsReader {
  private static final VectorTypeSupport VECTOR_TYPE_SUPPORT =
      VectorizationProvider.getInstance().getVectorTypeSupport();
  private static final FlatVectorsFormat FLAT_VECTORS_FORMAT =
      new Lucene99FlatVectorsFormat(FlatVectorScorerUtil.getLucene99FlatVectorsScorer());

  // TODO: Expose these values with JVectorFormat constructor args
  public static final Double DEFAULT_QUERY_SIMILARITY_THRESHOLD = 0.0;
  public static final Double DEFAULT_QUERY_RERANK_FLOOR = 0.0;
  public static final Boolean DEFAULT_QUERY_USE_PRUNING = false;
  public static final int DEFAULT_OVER_QUERY_FACTOR = 3;

  private final FieldInfos fieldInfos;
  private final String baseDataFileName;
  private final Map<String, FieldEntry> fieldEntryMap = new HashMap<>(1);
  private final Directory directory;
  private final SegmentReadState state;
  private final FlatVectorsReader flatVectorsReader;
  private final boolean mergeOnDisk;

  public JVectorReader(SegmentReadState state, boolean mergeOnDisk) throws IOException {
    this.state = state;
    this.mergeOnDisk = mergeOnDisk;
    this.flatVectorsReader = FLAT_VECTORS_FORMAT.fieldsReader(state);
    this.fieldInfos = state.fieldInfos;
    this.baseDataFileName = state.segmentInfo.name + "_" + state.segmentSuffix;
    String metaFileName =
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
      try (var indexInput =
          state.directory.openInput(fieldEntry.vectorIndexFieldDataFileName, state.context)) {
        CodecUtil.checksumEntireFile(indexInput);
      }
    }
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    if (mergeOnDisk) {
      return flatVectorsReader.getFloatVectorValues(field);
    }
    final FieldEntry fieldEntry = fieldEntryMap.get(field);
    return new JVectorFloatVectorValues(fieldEntry.index, fieldEntry.similarityFunction);
  }

  @Override
  public ByteVectorValues getByteVectorValues(String field) throws IOException {
    return null;
  }

  @Override
  public void search(String field, float[] target, KnnCollector knnCollector, Bits acceptDocs)
      throws IOException {
    final OnDiskGraphIndex index = fieldEntryMap.get(field).index;
    final JVectorKnnCollector jvectorKnnCollector;
    if (knnCollector instanceof JVectorKnnCollector) {
      jvectorKnnCollector = (JVectorKnnCollector) knnCollector;
    } else {
      jvectorKnnCollector =
          new JVectorKnnCollector(
              knnCollector,
              DEFAULT_QUERY_SIMILARITY_THRESHOLD.floatValue(),
              DEFAULT_QUERY_RERANK_FLOOR.floatValue(),
              DEFAULT_OVER_QUERY_FACTOR,
              DEFAULT_QUERY_USE_PRUNING);
    }

    VectorFloat<?> query = VECTOR_TYPE_SUPPORT.createFloatVector(target);
    final SearchScoreProvider ssp;

    try (var view = index.getView()) {
      if (fieldEntryMap.get(field).pqVectors
          != null) { // Quantized, use the precomputed score function
        final PQVectors pqVectors = fieldEntryMap.get(field).pqVectors;
        // SearchScoreProvider that does a first pass with the loaded-in-memory PQVectors,
        // then reranks with the exact vectors that are stored on disk in the index
        ScoreFunction.ApproximateScoreFunction asf =
            pqVectors.precomputedScoreFunctionFor(
                query, fieldEntryMap.get(field).similarityFunction);
        ScoreFunction.ExactScoreFunction reranker =
            view.rerankerFor(query, fieldEntryMap.get(field).similarityFunction);
        ssp = new DefaultSearchScoreProvider(asf, reranker);
      } else { // Not quantized, used typical searcher
        ssp =
            DefaultSearchScoreProvider.exact(
                query, fieldEntryMap.get(field).similarityFunction, view);
      }
      // Acceptdocs document bits filtering
      io.github.jbellis.jvector.util.Bits compatibleBits =
          doc -> acceptDocs == null || acceptDocs.get(doc);
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
          jvectorKnnCollector.collect(ns.node, ns.score);
        }
      }
    }
  }

  @Override
  public void search(String field, byte[] target, KnnCollector knnCollector, Bits acceptDocs)
      throws IOException {
    // TODO
  }

  @Override
  public void close() throws IOException {
    IOUtils.close(flatVectorsReader);
    for (FieldEntry fieldEntry : fieldEntryMap.values()) {
      IOUtils.close(fieldEntry);
    }
  }

  private void readFields(ChecksumIndexInput meta) throws IOException {
    for (int fieldNumber = meta.readInt(); fieldNumber != -1; fieldNumber = meta.readInt()) {
      final FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldNumber);
      JVectorWriter.VectorIndexFieldMetadata vectorIndexFieldMetadata =
          new JVectorWriter.VectorIndexFieldMetadata(meta);
      assert fieldInfo.number == vectorIndexFieldMetadata.getFieldNumber();
      fieldEntryMap.put(fieldInfo.name, new FieldEntry(fieldInfo, vectorIndexFieldMetadata));
    }
  }

  class FieldEntry implements Closeable {
    private final VectorSimilarityFunction similarityFunction;
    private final long vectorIndexOffset;
    private final long vectorIndexLength;
    private final long pqCodebooksAndVectorsLength;
    private final long pqCodebooksAndVectorsOffset;
    private final String vectorIndexFieldDataFileName;
    private final ReaderSupplier indexReaderSupplier;
    private final ReaderSupplier pqCodebooksReaderSupplier;
    private final OnDiskGraphIndex index;
    private final PQVectors pqVectors; // The product quantized vectors with their codebooks

    public FieldEntry(
        FieldInfo fieldInfo, JVectorWriter.VectorIndexFieldMetadata vectorIndexFieldMetadata)
        throws IOException {
      this.similarityFunction =
          VectorSimilarityMapper.ordToDistFunc(
              vectorIndexFieldMetadata.getVectorSimilarityFunction().ordinal());
      this.vectorIndexOffset = vectorIndexFieldMetadata.getVectorIndexOffset();
      this.vectorIndexLength = vectorIndexFieldMetadata.getVectorIndexLength();
      this.pqCodebooksAndVectorsLength = vectorIndexFieldMetadata.getPqCodebooksAndVectorsLength();
      this.pqCodebooksAndVectorsOffset = vectorIndexFieldMetadata.getPqCodebooksAndVectorsOffset();

      this.vectorIndexFieldDataFileName =
          baseDataFileName + "_" + fieldInfo.name + "." + JVectorFormat.VECTOR_INDEX_EXTENSION;

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
                directory.openInput(vectorIndexFieldDataFileName, state.context),
                pqCodebooksAndVectorsOffset,
                pqCodebooksAndVectorsLength);

        try (final var randomAccessReader = pqCodebooksReaderSupplier.get()) {
          this.pqVectors = PQVectors.load(randomAccessReader);
        }
      } else {
        this.pqCodebooksReaderSupplier = null;
        this.pqVectors = null;
      }
    }

    @Override
    public void close() throws IOException {
      if (indexReaderSupplier != null) {
        IOUtils.close(indexReaderSupplier::close);
      }
      if (pqCodebooksReaderSupplier != null) {
        IOUtils.close(pqCodebooksReaderSupplier::close);
      }
    }
  }

  /**
   * This is a list of vector similarity functions that are currently supported by this version of
   * the JVector Lucene codec: Euclidean, Dot Product/Angular, Cosine Note: If benchmarking with
   * luceneutil, these are selectable through knnPerfTest.py
   */
  public static class VectorSimilarityMapper {

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

      throw new IllegalArgumentException("Invalid distance function: " + func);
    }

    public static VectorSimilarityFunction ordToDistFunc(int ord) {
      return JVECTOR_SUPPORTED_SIMILARITY_FUNCTIONS.get(ord);
    }

    public static org.apache.lucene.index.VectorSimilarityFunction ordToLuceneDistFunc(int ord) {
      if (ord < 0 || ord >= JVECTOR_SUPPORTED_SIMILARITY_FUNCTIONS.size()) {
        throw new IllegalArgumentException("Invalid ord: " + ord);
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
