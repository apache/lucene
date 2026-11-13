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
package org.apache.lucene.internal.vectorization;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorScorer;
import org.apache.lucene.codecs.lucene104.OffHeapScalarQuantizedVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer.QuantizationResult;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues.ScalarEncoding;

/**
 * A {@link Lucene104ScalarQuantizedVectorScorer} that scores vectors in place in the {@link
 * MemorySegment} that holds them, several at a time in {@link RandomVectorScorer#bulkScore} so that
 * their cache misses overlap, instead of copying each on heap first. Vectors that are not memory
 * mapped in one segment are scored by the superclass.
 */
final class Lucene104MemorySegmentScalarQuantizedVectorScorer
    extends Lucene104ScalarQuantizedVectorScorer {

  static final Lucene104MemorySegmentScalarQuantizedVectorScorer INSTANCE =
      new Lucene104MemorySegmentScalarQuantizedVectorScorer(
          Lucene99MemorySegmentFlatVectorsScorer.INSTANCE);

  private final FlatVectorsScorer nonQuantizedDelegate;

  private Lucene104MemorySegmentScalarQuantizedVectorScorer(
      FlatVectorsScorer nonQuantizedDelegate) {
    super(nonQuantizedDelegate);
    this.nonQuantizedDelegate = nonQuantizedDelegate;
  }

  @Override
  public String toString() {
    return "Lucene104MemorySegmentScalarQuantizedVectorScorer(nonQuantizedDelegate="
        + nonQuantizedDelegate
        + ")";
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues)
      throws IOException {
    if (vectorValues instanceof QuantizedByteVectorValues qv
        && qv.getScalarEncoding().isAsymmetric() == false) {
      QuantizedVectorField quantizedVectorField = QuantizedVectorField.of(qv, similarityFunction);
      if (quantizedVectorField != null) {
        return new OffHeapScorerSupplier(quantizedVectorField, qv, qv);
      }
    }
    return super.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction,
      QuantizedByteVectorValues scoringVectors,
      QuantizedByteVectorValues targetVectors)
      throws IOException {
    if (targetVectors.getScalarEncoding().isAsymmetric()) {
      QuantizedVectorField quantizedVectorField =
          QuantizedVectorField.of(targetVectors, similarityFunction);
      if (quantizedVectorField != null) {
        return new OffHeapScorerSupplier(quantizedVectorField, targetVectors, scoringVectors);
      }
    }
    return super.getRandomVectorScorerSupplier(similarityFunction, scoringVectors, targetVectors);
  }

  @Override
  protected RandomVectorScorer getVectorScorer(
      VectorSimilarityFunction similarityFunction,
      QuantizedByteVectorValues qv,
      byte[] quantizedQuery,
      QuantizationResult queryCorrections)
      throws IOException {
    QuantizedVectorField quantizedVectorField = QuantizedVectorField.of(qv, similarityFunction);
    if (quantizedVectorField != null) {
      return ScorerBase.create(quantizedVectorField, qv, quantizedQuery, queryCorrections);
    }
    return super.getVectorScorer(similarityFunction, qv, quantizedQuery, queryCorrections);
  }

  /**
   * Represent quantized vectors in a single field. {@code dataSeg} contains the vectors laid out
   * serially as entries, each entry consists of quantized bytes followed by corrective terms.
   */
  private record QuantizedVectorField(
      MemorySegment dataSeg,
      VectorSimilarityFunction similarity,
      ScalarEncoding encoding,
      int size,
      int dimension,
      float centroidDp,
      int packedLength,
      int stride) {

    static final int CORRECTIONS_BYTES = 3 * Float.BYTES + Integer.BYTES;
    static final ValueLayout.OfFloat LE_FLOAT =
        ValueLayout.JAVA_FLOAT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
    static final ValueLayout.OfInt LE_INT =
        ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    static QuantizedVectorField of(
        QuantizedByteVectorValues values, VectorSimilarityFunction similarity) throws IOException {
      if (values instanceof OffHeapScalarQuantizedVectorValues == false) {
        return null;
      }
      IndexInput slice = values.getSlice();
      MemorySegment segment = null;
      if (slice != null
          && FilterIndexInput.unwrapOnlyTest(slice) instanceof MemorySegmentAccessInput input) {
        segment = input.segmentSliceOrNull(0, input.length());
      }
      if (segment == null) {
        return null;
      }
      int packedLength = values.getVectorByteLength();
      int stride = packedLength + CORRECTIONS_BYTES;
      if (segment.byteSize() < (long) stride * values.size()) {
        throw new IllegalArgumentException("input length is less than expected vector data");
      }
      return new QuantizedVectorField(
          segment,
          similarity,
          values.getScalarEncoding(),
          values.size(),
          values.dimension(),
          values.getCentroidDP(),
          packedLength,
          stride);
    }

    /**
     * The length of a quantized query: the discretized dimensions for 8 and 7 bit vectors, twice
     * the packed length (one nibble per byte) for 4 bit vectors, and the transposed 4 bit planes
     * for the 1 and 2 bit vectors.
     */
    int queryLength() {
      return encoding.isAsymmetric()
          ? encoding.getQueryPackedLength(dimension)
          : encoding.getDiscreteDimensions(dimension);
    }

    void checkQuery(byte[] query) {
      if (query.length != queryLength()) {
        throw new IllegalArgumentException(
            "query length " + query.length + " != " + queryLength() + " for " + encoding);
      }
    }

    void checkOrdinal(int ord) {
      if (ord < 0 || ord >= size) {
        throw new IllegalArgumentException("illegal ordinal: " + ord);
      }
    }

    long offset(int ord) {
      checkOrdinal(ord);
      return (long) ord * stride;
    }
  }

  /**
   * Scores one quantized query, held on heap, against {@link QuantizedVectorField}: four vectors
   * per {@link #bulkScore} pass in place, and {@link #score} like the superclass, reading the
   * vector on heap. There is one subclass per kernel, chosen once per scorer, so that no encoding
   * is looked up while scoring.
   */
  private abstract static class ScorerBase extends RandomVectorScorer.AbstractRandomVectorScorer {
    final QuantizedVectorField quantizedVectorField;
    final MemorySegment dataSeg;
    final int packedLength;

    /** The values {@link #score} reads from; not shared with another scorer. */
    private final QuantizedByteVectorValues values;

    private final int[] dotProducts = new int[4];
    byte[] query;
    private QuantizationResult queryCorrections;

    ScorerBase(
        QuantizedVectorField quantizedVectorField,
        QuantizedByteVectorValues values,
        byte[] query,
        QuantizationResult queryCorrections) {
      super(values);
      this.quantizedVectorField = quantizedVectorField;
      this.dataSeg = quantizedVectorField.dataSeg();
      this.packedLength = quantizedVectorField.packedLength();
      this.values = values;
      setQuery(query, queryCorrections);
    }

    static ScorerBase create(
        QuantizedVectorField quantizedVectorField,
        QuantizedByteVectorValues values,
        byte[] query,
        QuantizationResult queryCorrections) {
      return switch (quantizedVectorField.encoding()) {
        // SEVEN_BIT values are in [0, 127], where signed and unsigned products agree
        case UNSIGNED_BYTE, SEVEN_BIT ->
            new Uint8Scorer(quantizedVectorField, values, query, queryCorrections);
        case PACKED_NIBBLE ->
            new NibbleScorer(quantizedVectorField, values, query, queryCorrections);
        case SINGLE_BIT_QUERY_NIBBLE ->
            new BitScorer(quantizedVectorField, values, query, queryCorrections);
        case DIBIT_QUERY_NIBBLE ->
            new DibitScorer(quantizedVectorField, values, query, queryCorrections);
      };
    }

    /** Scores the given query from now on. */
    void setQuery(byte[] query, QuantizationResult queryCorrections) {
      quantizedVectorField.checkQuery(query);
      this.query = query;
      this.queryCorrections = queryCorrections;
    }

    /** The dot products of the query with the four vectors at the offsets, into {@code results}. */
    abstract void dotProducts(long o1, long o2, long o3, long o4, int[] results);

    @Override
    public float score(int node) throws IOException {
      quantizedVectorField.checkOrdinal(node);
      return quantizedScore(
          query, queryCorrections, values, node, quantizedVectorField.similarity());
    }

    @Override
    public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
      float max = Float.NEGATIVE_INFINITY;
      int i = 0;
      for (; i + 4 <= numNodes; i += 4) {
        long o1 = quantizedVectorField.offset(nodes[i]);
        long o2 = quantizedVectorField.offset(nodes[i + 1]);
        long o3 = quantizedVectorField.offset(nodes[i + 2]);
        long o4 = quantizedVectorField.offset(nodes[i + 3]);
        dotProducts(o1, o2, o3, o4, dotProducts);
        scores[i] = score(dotProducts[0], o1);
        max = Math.max(max, scores[i]);
        scores[i + 1] = score(dotProducts[1], o2);
        max = Math.max(max, scores[i + 1]);
        scores[i + 2] = score(dotProducts[2], o3);
        max = Math.max(max, scores[i + 2]);
        scores[i + 3] = score(dotProducts[3], o4);
        max = Math.max(max, scores[i + 3]);
      }
      // the remaining nodes, one at a time
      for (; i < numNodes; i++) {
        scores[i] = score(nodes[i]);
        max = Math.max(max, scores[i]);
      }
      return max;
    }

    /** Scores the dot product with the vector at {@code offset}, with its corrective terms. */
    private float score(int dotProduct, long offset) {
      long at = offset + packedLength;
      return convertToQuantizedScore(
          dotProduct,
          queryCorrections,
          dataSeg.get(QuantizedVectorField.LE_FLOAT, at),
          dataSeg.get(QuantizedVectorField.LE_FLOAT, at + Float.BYTES),
          dataSeg.get(QuantizedVectorField.LE_FLOAT, at + 2 * Float.BYTES),
          dataSeg.get(QuantizedVectorField.LE_INT, at + 3 * Float.BYTES),
          quantizedVectorField.encoding(),
          quantizedVectorField.dimension(),
          quantizedVectorField.centroidDp(),
          quantizedVectorField.similarity());
    }
  }

  private static final class Uint8Scorer extends ScorerBase {
    Uint8Scorer(
        QuantizedVectorField quantizedVectorField,
        QuantizedByteVectorValues values,
        byte[] query,
        QuantizationResult queryCorrections) {
      super(quantizedVectorField, values, query, queryCorrections);
    }

    @Override
    void dotProducts(long o1, long o2, long o3, long o4, int[] results) {
      MemorySegmentQuantizedVectorOps.uint8DotProductBulk(
          query, dataSeg, o1, o2, o3, o4, packedLength, results);
    }
  }

  private static final class NibbleScorer extends ScorerBase {
    NibbleScorer(
        QuantizedVectorField quantizedVectorField,
        QuantizedByteVectorValues values,
        byte[] query,
        QuantizationResult queryCorrections) {
      super(quantizedVectorField, values, query, queryCorrections);
    }

    @Override
    void dotProducts(long o1, long o2, long o3, long o4, int[] results) {
      MemorySegmentQuantizedVectorOps.int4DotProductSinglePackedBulk(
          query, dataSeg, o1, o2, o3, o4, packedLength, results);
    }
  }

  private static final class BitScorer extends ScorerBase {
    BitScorer(
        QuantizedVectorField quantizedVectorField,
        QuantizedByteVectorValues values,
        byte[] query,
        QuantizationResult queryCorrections) {
      super(quantizedVectorField, values, query, queryCorrections);
    }

    @Override
    void dotProducts(long o1, long o2, long o3, long o4, int[] results) {
      MemorySegmentQuantizedVectorOps.int4BitDotProductBulk(
          query, dataSeg, o1, o2, o3, o4, packedLength, results);
    }
  }

  /** A 2 bit vector is two bit planes of half the packed length each. */
  private static final class DibitScorer extends ScorerBase {
    DibitScorer(
        QuantizedVectorField quantizedVectorField,
        QuantizedByteVectorValues values,
        byte[] query,
        QuantizationResult queryCorrections) {
      super(quantizedVectorField, values, query, queryCorrections);
    }

    @Override
    void dotProducts(long o1, long o2, long o3, long o4, int[] results) {
      MemorySegmentQuantizedVectorOps.int4DibitDotProductBulk(
          query, dataSeg, o1, o2, o3, o4, packedLength >> 1, results);
    }
  }

  /**
   * Scores vectors of {@code queryVectors} against {@link QuantizedVectorField}. Both are the same
   * vectors for symmetric encodings, where a {@link ScalarEncoding#PACKED_NIBBLE} vector is
   * unpacked into the query layout; for asymmetric encodings the query vectors already have the
   * query layout.
   */
  private static final class OrdinalScorer
      extends UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer {
    private final QuantizedVectorField quantizedVectorField;
    private final QuantizedByteVectorValues values;
    private final QuantizedByteVectorValues queryVectors;
    private final byte[] unpacked;
    private ScorerBase scorer;

    /** Both values are this scorer's own copies. */
    OrdinalScorer(
        QuantizedVectorField quantizedVectorField,
        QuantizedByteVectorValues values,
        QuantizedByteVectorValues queryVectors) {
      super(values);
      this.quantizedVectorField = quantizedVectorField;
      this.values = values;
      this.queryVectors = queryVectors;
      this.unpacked =
          quantizedVectorField.encoding() == ScalarEncoding.PACKED_NIBBLE
              ? new byte[quantizedVectorField.queryLength()]
              : null;
    }

    @Override
    public void setScoringOrdinal(int node) throws IOException {
      byte[] query = queryVectors.vectorValue(node);
      if (unpacked != null) {
        OffHeapScalarQuantizedVectorValues.unpackNibbles(query, unpacked);
        query = unpacked;
      }
      QuantizationResult corrections = queryVectors.getCorrectiveTerms(node);
      if (scorer == null) {
        scorer = ScorerBase.create(quantizedVectorField, values, query, corrections);
      } else {
        scorer.setQuery(query, corrections);
      }
    }

    @Override
    public float score(int node) throws IOException {
      return scorer().score(node);
    }

    @Override
    public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
      return scorer().bulkScore(nodes, scores, numNodes);
    }

    private ScorerBase scorer() {
      if (scorer == null) {
        throw new IllegalStateException("setScoringOrdinal was not called");
      }
      return scorer;
    }
  }

  private record OffHeapScorerSupplier(
      QuantizedVectorField quantizedVectorField,
      QuantizedByteVectorValues values,
      QuantizedByteVectorValues queryVectors)
      implements RandomVectorScorerSupplier {

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      return new OrdinalScorer(quantizedVectorField, values.copy(), queryVectors.copy());
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new OffHeapScorerSupplier(quantizedVectorField, values.copy(), queryVectors.copy());
    }
  }
}
