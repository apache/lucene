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
import io.github.jbellis.jvector.graph.similarity.SearchScoreProvider;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.*;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.search.knn.KnnSearchStrategy;
import org.apache.lucene.store.*;
import org.apache.lucene.util.IOUtils;

/// Implements KnnVectorsReader over an on-disk JVector index serialized using {@link JVectorWriter}
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
    }
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    final FieldEntry fieldEntry = fieldEntryMap.get(field);
    if (fieldEntry == null || fieldEntry.index == null) {
      return new FloatVectorValues() {
        @Override
        public float[] vectorValue(int ord) throws IOException {
          throw new IndexOutOfBoundsException();
        }

        @Override
        public FloatVectorValues copy() throws IOException {
          return this;
        }

        @Override
        public int dimension() {
          return fieldEntry.vectorDimension;
        }

        @Override
        public int size() {
          return 0;
        }

        @Override
        public VectorScorer scorer(float[] target) throws IOException {
          return null;
        }
      };
    }

    return new JVectorFloatVectorValues(
        fieldEntry.index,
        fieldEntry.pqVectors,
        fieldEntry.similarityFunction,
        fieldEntry.graphNodeIdToDocMap);
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

  public boolean hasIndex(String field) {
    final var fieldEntry = fieldEntryMap.get(field);
    return fieldEntry != null && fieldEntry.index != null;
  }

  @Override
  public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
      throws IOException {
    final var fieldEntry = fieldEntryMap.get(field);
    final OnDiskGraphIndex index = fieldEntry.index;
    if (index == null) {
      // Skip search when the graph is empty
      return;
    }

    final JVectorSearchStrategy searchStrategy;
    if (knnCollector.getSearchStrategy() instanceof JVectorSearchStrategy strategy) {
      searchStrategy = strategy;
    } else if (knnCollector.getSearchStrategy() instanceof KnnSearchStrategy.Seeded seeded
        && seeded.originalStrategy() instanceof JVectorSearchStrategy strategy) {
      searchStrategy = strategy;
    } else searchStrategy = JVectorSearchStrategy.DEFAULT;

    // search for a random vector using a GraphSearcher and SearchScoreProvider
    VectorFloat<?> q = VECTOR_TYPE_SUPPORT.createFloatVector(target);
    final SearchScoreProvider ssp;

    try (var view = index.getView()) {
      if (fieldEntry.pqVectors != null) { // Quantized, use the precomputed score function
        final PQVectors pqVectors = fieldEntry.pqVectors;
        // SearchScoreProvider that does a first pass with the loaded-in-memory PQVectors,
        // then reranks with the exact vectors that are stored on disk in the index
        final var asf = pqVectors.precomputedScoreFunctionFor(q, fieldEntry.similarityFunction);
        final var reranker = view.rerankerFor(q, fieldEntry.similarityFunction);
        ssp = new DefaultSearchScoreProvider(asf, reranker);
      } else { // Not quantized, used typical searcher
        ssp = DefaultSearchScoreProvider.exact(q, fieldEntry.similarityFunction, view);
      }
      final GraphNodeIdToDocMap jvectorLuceneDocMap = fieldEntry.graphNodeIdToDocMap;
      // Convert the acceptDocs bitmap from Lucene to jVector ordinal bitmap filter
      // Logic works as follows: if acceptDocs is null, we accept all ordinals. Otherwise, we check
      // if the jVector ordinal has a
      // corresponding Lucene doc ID accepted by acceptDocs filter.

      Bits compatibleBits = Bits.ALL;
      if (acceptDocs != null) {
        final var luceneBits = acceptDocs.bits();
        if (luceneBits != null) {
          compatibleBits = ord -> luceneBits.get(jvectorLuceneDocMap.getLuceneDocId(ord));
        }
      }

      try (var graphSearcher = new GraphSearcher(index)) {
        final var searchResults =
            graphSearcher.search(
                ssp,
                knnCollector.k(),
                knnCollector.k() * searchStrategy.overQueryFactor,
                searchStrategy.threshold,
                searchStrategy.rerankFloor,
                compatibleBits);
        for (SearchResult.NodeScore ns : searchResults.getNodes()) {
          knnCollector.collect(jvectorLuceneDocMap.getLuceneDocId(ns.node), ns.score);
        }
        // JVector does not seem to count the entry-point as visited
        knnCollector.incVisitedCount(1 + searchResults.getVisitedCount());
      }
    }
  }

  @Override
  public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
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
      assert fieldInfo.number == vectorIndexFieldMetadata.fieldNumber;
      fieldEntryMap.put(fieldInfo.name, new FieldEntry(fieldInfo, vectorIndexFieldMetadata));
    }
  }

  class FieldEntry implements Closeable {
    private final VectorSimilarityFunction similarityFunction;
    private final int vectorDimension;
    private final long vectorIndexOffset;
    private final long vectorIndexLength;
    private final long pqCodebooksAndVectorsLength;
    private final long pqCodebooksAndVectorsOffset;
    private final String vectorIndexFieldDataFileName;
    private final GraphNodeIdToDocMap graphNodeIdToDocMap;
    private final ReaderSupplier indexReaderSupplier;
    private final ReaderSupplier pqCodebooksReaderSupplier;
    private final OnDiskGraphIndex index;
    private final PQVectors pqVectors; // The product quantized vectors with their codebooks

    public FieldEntry(
        FieldInfo fieldInfo, JVectorWriter.VectorIndexFieldMetadata vectorIndexFieldMetadata)
        throws IOException {
      this.similarityFunction = vectorIndexFieldMetadata.vectorSimilarityFunction;
      this.vectorDimension = vectorIndexFieldMetadata.vectorDimension;
      this.vectorIndexOffset = vectorIndexFieldMetadata.vectorIndexOffset;
      this.vectorIndexLength = vectorIndexFieldMetadata.vectorIndexLength;
      this.pqCodebooksAndVectorsLength = vectorIndexFieldMetadata.pqCodebooksAndVectorsLength;
      this.pqCodebooksAndVectorsOffset = vectorIndexFieldMetadata.pqCodebooksAndVectorsOffset;
      this.graphNodeIdToDocMap = vectorIndexFieldMetadata.graphNodeIdToDocMap;

      this.vectorIndexFieldDataFileName =
          baseDataFileName + "_" + fieldInfo.name + "." + JVectorFormat.VECTOR_INDEX_EXTENSION;

      assert vectorIndexLength > 0 : "Read empty JVector graph";
      // For the slice we would like to include the Lucene header, unfortunately, we have to do
      // this because jVector use global offsets instead of local offsets
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
}
