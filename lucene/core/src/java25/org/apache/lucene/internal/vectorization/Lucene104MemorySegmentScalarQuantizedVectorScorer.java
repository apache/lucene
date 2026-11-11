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
      Records records = Records.of(qv, similarityFunction);
      if (records != null) {
        return new OffHeapScorerSupplier(records, qv, qv);
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
      Records records = Records.of(targetVectors, similarityFunction);
      if (records != null) {
        return new OffHeapScorerSupplier(records, targetVectors, scoringVectors);
      }
    }
    return super.getRandomVectorScorerSupplier(similarityFunction, scoringVectors, targetVectors);
  }

  @Override
  protected RandomVectorScorer getQuantizedQueryScorer(
      VectorSimilarityFunction similarityFunction,
      QuantizedByteVectorValues qv,
      byte[] quantizedQuery,
      QuantizationResult queryCorrections)
      throws IOException {
    Records records = Records.of(qv, similarityFunction);
    if (records != null) {
      return QueryScorer.create(records, qv, quantizedQuery, queryCorrections);
    }
    return super.getQuantizedQueryScorer(similarityFunction, qv, quantizedQuery, queryCorrections);
  }

  /**
   * The records of one field in a segment: each is the packed vector followed by its corrective
   * terms, three floats and an int. Holds values only, so one instance serves every scorer of a
   * supplier and its copies.
   *
   * <p>The segment is that of the whole slice, like in {@link
   * Lucene99MemorySegmentFlatVectorsScorer}. Scoring after the input is closed fails on the
   * segment, not with an {@link org.apache.lucene.store.AlreadyClosedException}.
   */
  private record Records(
      MemorySegment segment,
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

    /**
     * Returns the records of {@code values}, or null if they cannot be scored this way: when they
     * are not off heap, or when they span more than one mapped chunk.
     */
    static Records of(QuantizedByteVectorValues values, VectorSimilarityFunction similarity)
        throws IOException {
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
      return new Records(
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
   * Scores one quantized query, held on heap, against {@link Records}: four vectors per {@link
   * #bulkScore} pass in place, and {@link #score} like the superclass, reading the vector on heap.
   * There is one subclass per kernel, chosen once per scorer, so that no encoding is looked up
   * while scoring.
   */
  private abstract static class QueryScorer extends RandomVectorScorer.AbstractRandomVectorScorer {
    final Records records;
    final MemorySegment segment;
    final int packedLength;

    /** The values {@link #score} reads from; not shared with another scorer. */
    private final QuantizedByteVectorValues values;

    private final int[] dotProducts = new int[4];
    byte[] query;
    private QuantizationResult queryCorrections;

    QueryScorer(
        Records records,
        QuantizedByteVectorValues values,
        byte[] query,
        QuantizationResult queryCorrections) {
      super(values);
      this.records = records;
      this.segment = records.segment();
      this.packedLength = records.packedLength();
      this.values = values;
      setQuery(query, queryCorrections);
    }

    static QueryScorer create(
        Records records,
        QuantizedByteVectorValues values,
        byte[] query,
        QuantizationResult queryCorrections) {
      return switch (records.encoding()) {
        // SEVEN_BIT values are in [0, 127], where signed and unsigned products agree
        case UNSIGNED_BYTE, SEVEN_BIT ->
            new Uint8QueryScorer(records, values, query, queryCorrections);
        case PACKED_NIBBLE -> new NibbleQueryScorer(records, values, query, queryCorrections);
        case SINGLE_BIT_QUERY_NIBBLE ->
            new BitQueryScorer(records, values, query, queryCorrections);
        case DIBIT_QUERY_NIBBLE -> new DibitQueryScorer(records, values, query, queryCorrections);
      };
    }

    /** Scores the given query from now on. */
    void setQuery(byte[] query, QuantizationResult queryCorrections) {
      records.checkQuery(query);
      this.query = query;
      this.queryCorrections = queryCorrections;
    }

    /** The dot products of the query with the four vectors at the offsets, into {@code results}. */
    abstract void dotProducts(long o1, long o2, long o3, long o4, int[] results);

    @Override
    public float score(int node) throws IOException {
      records.checkOrdinal(node);
      return quantizedScore(query, queryCorrections, values, node, records.similarity());
    }

    @Override
    public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
      float max = Float.NEGATIVE_INFINITY;
      int i = 0;
      for (; i + 4 <= numNodes; i += 4) {
        long o1 = records.offset(nodes[i]);
        long o2 = records.offset(nodes[i + 1]);
        long o3 = records.offset(nodes[i + 2]);
        long o4 = records.offset(nodes[i + 3]);
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
      return quantizedScore(
          dotProduct,
          queryCorrections,
          segment.get(Records.LE_FLOAT, at),
          segment.get(Records.LE_FLOAT, at + Float.BYTES),
          segment.get(Records.LE_FLOAT, at + 2 * Float.BYTES),
          segment.get(Records.LE_INT, at + 3 * Float.BYTES),
          records.encoding(),
          records.dimension(),
          records.centroidDp(),
          records.similarity());
    }
  }

  private static final class Uint8QueryScorer extends QueryScorer {
    Uint8QueryScorer(
        Records records,
        QuantizedByteVectorValues values,
        byte[] query,
        QuantizationResult queryCorrections) {
      super(records, values, query, queryCorrections);
    }

    @Override
    void dotProducts(long o1, long o2, long o3, long o4, int[] results) {
      MemorySegmentQuantizedVectorOps.uint8DotProductBulk(
          query, segment, o1, o2, o3, o4, packedLength, results);
    }
  }

  private static final class NibbleQueryScorer extends QueryScorer {
    NibbleQueryScorer(
        Records records,
        QuantizedByteVectorValues values,
        byte[] query,
        QuantizationResult queryCorrections) {
      super(records, values, query, queryCorrections);
    }

    @Override
    void dotProducts(long o1, long o2, long o3, long o4, int[] results) {
      MemorySegmentQuantizedVectorOps.int4DotProductSinglePackedBulk(
          query, segment, o1, o2, o3, o4, packedLength, results);
    }
  }

  private static final class BitQueryScorer extends QueryScorer {
    BitQueryScorer(
        Records records,
        QuantizedByteVectorValues values,
        byte[] query,
        QuantizationResult queryCorrections) {
      super(records, values, query, queryCorrections);
    }

    @Override
    void dotProducts(long o1, long o2, long o3, long o4, int[] results) {
      MemorySegmentQuantizedVectorOps.int4BitDotProductBulk(
          query, segment, o1, o2, o3, o4, packedLength, results);
    }
  }

  /** A 2 bit vector is two bit planes of half the packed length each. */
  private static final class DibitQueryScorer extends QueryScorer {
    DibitQueryScorer(
        Records records,
        QuantizedByteVectorValues values,
        byte[] query,
        QuantizationResult queryCorrections) {
      super(records, values, query, queryCorrections);
    }

    @Override
    void dotProducts(long o1, long o2, long o3, long o4, int[] results) {
      MemorySegmentQuantizedVectorOps.int4DibitDotProductBulk(
          query, segment, o1, o2, o3, o4, packedLength >> 1, results);
    }
  }

  /**
   * Scores vectors of {@code queryVectors} against {@link Records}. Both are the same vectors for
   * symmetric encodings, where a {@link ScalarEncoding#PACKED_NIBBLE} vector is unpacked into the
   * query layout; for asymmetric encodings the query vectors already have the query layout.
   */
  private static final class OrdinalScorer
      extends UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer {
    private final Records records;
    private final QuantizedByteVectorValues values;
    private final QuantizedByteVectorValues queryVectors;
    private final byte[] unpacked;
    private QueryScorer scorer;

    /** Both values are this scorer's own copies. */
    OrdinalScorer(
        Records records, QuantizedByteVectorValues values, QuantizedByteVectorValues queryVectors) {
      super(values);
      this.records = records;
      this.values = values;
      this.queryVectors = queryVectors;
      this.unpacked =
          records.encoding() == ScalarEncoding.PACKED_NIBBLE
              ? new byte[records.queryLength()]
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
        scorer = QueryScorer.create(records, values, query, corrections);
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

    private QueryScorer scorer() {
      if (scorer == null) {
        throw new IllegalStateException("setScoringOrdinal was not called");
      }
      return scorer;
    }
  }

  /**
   * A supplier of {@link OrdinalScorer}s. {@code values} are the records as {@link
   * QuantizedByteVectorValues}, for {@link RandomVectorScorer#score}; a {@link #copy} gets its own
   * copies of both values, and each scorer gets its own copies of those.
   */
  private record OffHeapScorerSupplier(
      Records records, QuantizedByteVectorValues values, QuantizedByteVectorValues queryVectors)
      implements RandomVectorScorerSupplier {

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      return new OrdinalScorer(records, values.copy(), queryVectors.copy());
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new OffHeapScorerSupplier(records, values.copy(), queryVectors.copy());
    }
  }
}
