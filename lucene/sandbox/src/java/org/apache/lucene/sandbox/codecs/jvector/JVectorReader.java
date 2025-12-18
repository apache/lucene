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

import io.github.jbellis.jvector.graph.GraphSearcher;
import io.github.jbellis.jvector.graph.SearchResult;
import io.github.jbellis.jvector.graph.disk.OnDiskGraphIndex;
import io.github.jbellis.jvector.graph.similarity.DefaultSearchScoreProvider;
import io.github.jbellis.jvector.graph.similarity.ScoreFunction.ExactScoreFunction;
import io.github.jbellis.jvector.graph.similarity.SearchScoreProvider;
import io.github.jbellis.jvector.quantization.PQVectors;
import io.github.jbellis.jvector.quantization.ProductQuantization;
import io.github.jbellis.jvector.util.Bits;
import io.github.jbellis.jvector.util.ExplicitThreadLocal;
import io.github.jbellis.jvector.vector.VectorSimilarityFunction;
import io.github.jbellis.jvector.vector.VectorizationProvider;
import io.github.jbellis.jvector.vector.types.VectorFloat;
import io.github.jbellis.jvector.vector.types.VectorTypeSupport;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

  private final IndexInput data;
  // Maps field name to field entries
  private final Map<String, FieldEntry> fieldEntryMap;

  public JVectorReader(SegmentReadState state) throws IOException {
    final List<JVectorWriter.VectorIndexFieldMetadata> fieldMetaList = new ArrayList<>();
    final String metaFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, JVectorFormat.META_EXTENSION);
    try (ChecksumIndexInput meta = state.directory.openChecksumInput(metaFileName)) {
      Throwable priorE = null;
      try {
        CodecUtil.checkIndexHeader(
            meta,
            JVectorFormat.META_CODEC_NAME,
            JVectorFormat.VERSION_START,
            JVectorFormat.VERSION_CURRENT,
            state.segmentInfo.getId(),
            state.segmentSuffix);

        JVectorWriter.VectorIndexFieldMetadata fieldMeta;
        while ((fieldMeta = parseNextField(meta, state.fieldInfos)) != null) {
          fieldMetaList.add(fieldMeta);
        }
      } catch (Throwable t) {
        priorE = t;
      } finally {
        CodecUtil.checkFooter(meta, priorE);
      }

      final String dataFileName =
          IndexFileNames.segmentFileName(
              state.segmentInfo.name, state.segmentSuffix, JVectorFormat.VECTOR_INDEX_EXTENSION);
      this.data =
          state.directory.openInput(
              dataFileName, state.context.withHints(FileTypeHint.DATA, DataAccessHint.RANDOM));

      CodecUtil.checkHeader(
          data,
          JVectorFormat.VECTOR_INDEX_CODEC_NAME,
          JVectorFormat.VERSION_START,
          JVectorFormat.VERSION_CURRENT);
      CodecUtil.retrieveChecksum(data);

      this.fieldEntryMap = new HashMap<>(fieldMetaList.size());
      for (var fieldMeta : fieldMetaList) {
        final FieldInfo fieldInfo = state.fieldInfos.fieldInfo(fieldMeta.fieldNumber());
        if (fieldEntryMap.containsKey(fieldInfo.name)) {
          throw new CorruptIndexException("Duplicate field: " + fieldInfo.name, meta);
        }
        fieldEntryMap.put(fieldInfo.name, new FieldEntry(data, fieldMeta));
      }
    }
  }

  @Override
  public void checkIntegrity() throws IOException {
    CodecUtil.checksumEntireFile(data);
  }

  @Override
  public FloatVectorValues getFloatVectorValues(String field) throws IOException {
    final FieldEntry fieldEntry = fieldEntryMap.get(field);
    if (fieldEntry == null) {
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
          return 0;
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

  @Override
  public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs)
      throws IOException {
    final var fieldEntry = fieldEntryMap.get(field);
    assert fieldEntry != null;

    final JVectorSearchStrategy searchStrategy;
    if (knnCollector.getSearchStrategy() instanceof JVectorSearchStrategy strategy) {
      searchStrategy = strategy;
    } else if (knnCollector.getSearchStrategy() instanceof KnnSearchStrategy.Seeded seeded
        && seeded.originalStrategy() instanceof JVectorSearchStrategy strategy) {
      searchStrategy = strategy;
    } else searchStrategy = JVectorSearchStrategy.DEFAULT;

    // search for a random vector using a GraphSearcher and SearchScoreProvider
    VectorFloat<?> q = VECTOR_TYPE_SUPPORT.createFloatVector(target);

    // Get a thread-local GraphSearcher
    final var searcher = fieldEntry.searchers.get();
    final OnDiskGraphIndex.View view = (OnDiskGraphIndex.View) searcher.getView();

    final SearchScoreProvider ssp;
    if (fieldEntry.pqVectors != null) { // Quantized, use the precomputed score function
      final PQVectors pqVectors = fieldEntry.pqVectors;
      // SearchScoreProvider that does a first pass with the loaded-in-memory PQVectors,
      // then reranks with the exact vectors that are stored on disk in the index
      final var asf = pqVectors.precomputedScoreFunctionFor(q, fieldEntry.similarityFunction);
      final ExactScoreFunction reranker;
      if (searchStrategy.overQueryFactor > 1) {
        reranker = view.rerankerFor(q, fieldEntry.similarityFunction);
      } else {
        reranker = null;
      }
      ssp = new DefaultSearchScoreProvider(asf, reranker);
    } else { // Not quantized, used typical searcher
      ssp = DefaultSearchScoreProvider.exact(q, fieldEntry.similarityFunction, view);
    }

    // Convert the acceptDocs bitmap from Lucene to jVector ordinal bitmap filter
    // Logic works as follows: if acceptDocs is null, we accept all ordinals. Otherwise, we check
    // if the jVector ordinal has a
    // corresponding Lucene doc ID accepted by acceptDocs filter.

    Bits compatibleBits = Bits.ALL;
    if (acceptDocs != null) {
      final var luceneBits = acceptDocs.bits();
      if (luceneBits != null) {
        compatibleBits = ord -> luceneBits.get(fieldEntry.graphNodeIdToDocMap.getLuceneDocId(ord));
      }
    }

    final var searchResults =
        searcher.search(
            ssp,
            knnCollector.k(),
            knnCollector.k() * searchStrategy.overQueryFactor,
            searchStrategy.threshold,
            searchStrategy.rerankFloor,
            compatibleBits);
    for (SearchResult.NodeScore ns : searchResults.getNodes()) {
      knnCollector.collect(fieldEntry.graphNodeIdToDocMap.getLuceneDocId(ns.node), ns.score);
    }
    // JVector does not seem to count the entry-point as visited
    knnCollector.incVisitedCount(1 + searchResults.getVisitedCount());
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
    IOUtils.close(data);
  }

  public OnDiskGraphIndex getIndex(final String field) {
    final var fieldEntry = fieldEntryMap.get(field);
    if (fieldEntry == null) {
      return null;
    }

    return fieldEntry.index;
  }

  public GraphNodeIdToDocMap getDocIdMap(final String field) {
    final var fieldEntry = fieldEntryMap.get(field);
    if (fieldEntry == null) {
      return null;
    }

    return fieldEntry.graphNodeIdToDocMap;
  }

  private static JVectorWriter.VectorIndexFieldMetadata parseNextField(
      IndexInput meta, FieldInfos fieldInfos) throws IOException {
    final int fieldNumber = meta.readInt();
    if (fieldNumber == -1) {
      return null;
    }

    final FieldInfo fieldInfo = fieldInfos.fieldInfo(fieldNumber);
    if (fieldInfo == null) {
      throw new CorruptIndexException("Invalid field number: " + fieldNumber, meta);
    }

    return JVectorWriter.VectorIndexFieldMetadata.read(meta);
  }

  class FieldEntry implements Closeable {
    private final VectorSimilarityFunction similarityFunction;
    private final GraphNodeIdToDocMap graphNodeIdToDocMap;
    private final OnDiskGraphIndex index;
    private final ExplicitThreadLocal<GraphSearcher> searchers;
    private final PQVectors pqVectors; // The product quantized vectors with their codebooks

    public FieldEntry(
        IndexInput data, JVectorWriter.VectorIndexFieldMetadata vectorIndexFieldMetadata)
        throws IOException {
      this.similarityFunction = vectorIndexFieldMetadata.vectorSimilarityFunction();
      this.graphNodeIdToDocMap = vectorIndexFieldMetadata.graphNodeIdToDocMap();

      final long graphOffset = vectorIndexFieldMetadata.vectorIndexOffset();
      final long graphLength = vectorIndexFieldMetadata.vectorIndexLength();
      assert graphLength > 0 : "Read empty JVector graph";
      // Load the graph index from cloned slices of data (no need to close)
      final var indexReaderSupplier =
          new JVectorRandomAccessReader.Supplier(data.slice("graph", graphOffset, graphLength));
      this.index = OnDiskGraphIndex.load(indexReaderSupplier);
      this.searchers = ExplicitThreadLocal.withInitial(() -> new GraphSearcher(index));

      // If quantized load the compressed product quantized vectors with their codebooks
      final long pqOffset = vectorIndexFieldMetadata.pqCodebooksAndVectorsOffset();
      final long pqLength = vectorIndexFieldMetadata.pqCodebooksAndVectorsLength();
      if (pqLength > 0) {
        assert pqOffset > 0;
        if (pqOffset < graphOffset) {
          throw new IllegalArgumentException("pqOffset must be greater than vectorIndexOffset");
        }
        final var pqSlice = data.slice("pq", pqOffset, pqLength);
        try (final var randomAccessReader = new JVectorRandomAccessReader(pqSlice)) {
          this.pqVectors = PQVectors.load(randomAccessReader);
        }
      } else {
        this.pqVectors = null;
      }
    }

    @Override
    public void close() throws IOException {
      try {
        searchers.close();
      } catch (Exception e) {
        throw new IOException("Fail to close searchers", e);
      }
      index.close();
    }
  }
}
