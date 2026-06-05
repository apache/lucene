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

import static java.lang.foreign.ValueLayout.JAVA_INT_UNALIGNED;
import static org.apache.lucene.codecs.hnsw.ScalarQuantizedVectorScorer.quantizeQuery;

import java.io.IOException;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorScorer;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.FloatToFloatFunction;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.LegacyQuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizer;

/**
 * MemorySegment-based optimized scorer for Lucene99 scalar quantized vectors. Uses the Panama
 * Vector API for bulk off-heap scoring when the underlying index input supports direct
 * MemorySegment access.
 *
 * <p>Bulk scoring activation: The bulk path requires the vector data to be accessible as a
 * contiguous MemorySegment via {@link MemorySegmentAccessInput#segmentSliceOrNull(long, long)}. If
 * the index spans multiple segments (e.g., files larger than the platform's mmap limit), the slice
 * may be unavailable and bulk scoring falls back to the default single-vector loop. This fallback
 * is silent and correct.
 */
class Lucene99MemorySegmentScalarQuantizedVectorScorer implements FlatVectorsScorer {
  static final Lucene99MemorySegmentScalarQuantizedVectorScorer INSTANCE =
      new Lucene99MemorySegmentScalarQuantizedVectorScorer();

  private static final FlatVectorsScorer DELEGATE =
      new Lucene99ScalarQuantizedVectorScorer(DefaultFlatVectorScorer.INSTANCE);

  private Lucene99MemorySegmentScalarQuantizedVectorScorer() {}

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues)
      throws IOException {
    if (vectorValues instanceof LegacyQuantizedByteVectorValues quantized
        && quantized.getSlice() instanceof MemorySegmentAccessInput input) {
      return new RandomVectorScorerSupplierImpl(similarityFunction, quantized, input);
    }
    return DELEGATE.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, float[] target)
      throws IOException {
    if (vectorValues instanceof LegacyQuantizedByteVectorValues quantized
        && quantized.getSlice() instanceof MemorySegmentAccessInput input) {
      if (Constants.NATIVE_DOT_PRODUCT_ENABLED) {
        return new NativeRandomVectorScorerImpl(similarityFunction, quantized, input, target);
      }
      return new RandomVectorScorerImpl(similarityFunction, quantized, input, target);
    }
    return DELEGATE.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, byte[] target)
      throws IOException {
    return DELEGATE.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public String toString() {
    return "Lucene99MemorySegmentScalarQuantizedVectorScorer()";
  }

  private abstract static class RandomVectorScorerBase
      extends RandomVectorScorer.AbstractRandomVectorScorer {

    private final ScalarQuantizer quantizer;
    private final float constMultiplier;
    private final MemorySegmentAccessInput input;
    final int vectorByteSize;
    final int nodeSize;
    private final Scorer scorer;
    private final FloatToFloatFunction scaler;
    private final boolean isUint8;
    final boolean isEuclidean;
    final MemorySegment dataSeg;
    private byte[] scratch;
    private final int[] scratchIntScores = new int[4];

    RandomVectorScorerBase(
        VectorSimilarityFunction similarityFunction,
        LegacyQuantizedByteVectorValues values,
        MemorySegmentAccessInput input) {
      super(values);

      this.quantizer = values.getScalarQuantizer();
      this.constMultiplier = this.quantizer.getConstantMultiplier();
      this.input = input;
      this.vectorByteSize = values.getVectorByteLength();
      this.nodeSize = this.vectorByteSize + Float.BYTES;

      boolean bitsLe4 = this.quantizer.getBits() <= 4;
      this.isUint8 = !bitsLe4;
      this.isEuclidean = similarityFunction == VectorSimilarityFunction.EUCLIDEAN;

      this.scorer =
          switch (similarityFunction) {
            case EUCLIDEAN -> {
              if (bitsLe4) {
                if (this.vectorByteSize != values.dimension()) {
                  yield this::compressedInt4Euclidean;
                }
                yield this::int4Euclidean;
              }
              yield this::euclidean;
            }
            case DOT_PRODUCT, COSINE, MAXIMUM_INNER_PRODUCT -> {
              if (bitsLe4) {
                if (this.vectorByteSize != values.dimension()) {
                  yield this::compressedInt4DotProduct;
                }
                yield this::int4DotProduct;
              }
              yield this::dotProduct;
            }
          };

      this.scaler =
          switch (similarityFunction) {
            case EUCLIDEAN -> VectorUtil::normalizeDistanceToUnitInterval;
            case DOT_PRODUCT, COSINE -> VectorUtil::normalizeToUnitInterval;
            case MAXIMUM_INNER_PRODUCT -> VectorUtil::scaleMaxInnerProductScore;
          };

      // Cache the MemorySegment for bulk operations; fall back to null if unavailable
      MemorySegment seg = null;
      try {
        seg = input.segmentSliceOrNull(0L, input.length());
      } catch (IOException e) {
        // ignore, bulkScore will fall back to default loop
      }
      this.dataSeg = seg;

      checkInvariants();
    }

    final void checkInvariants() {
      if (input.length() < (long) nodeSize * maxOrd()) {
        throw new IllegalArgumentException("input length is less than expected vector data");
      }
    }

    final void checkOrdinal(int ord) {
      if (ord < 0 || ord >= maxOrd()) {
        throw new IllegalArgumentException("illegal ordinal: " + ord);
      }
    }

    ScalarQuantizer getQuantizer() {
      return quantizer;
    }

    private static final ValueLayout.OfInt INT_UNALIGNED_LE =
        JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

    @SuppressWarnings("restricted")
    Node getNode(int ord) throws IOException {
      checkOrdinal(ord);
      long byteOffset = (long) ord * nodeSize;
      MemorySegment node = input.segmentSliceOrNull(byteOffset, nodeSize);
      if (node == null) {
        if (scratch == null) {
          scratch = new byte[nodeSize];
        }
        input.readBytes(byteOffset, scratch, 0, nodeSize);
        node = MemorySegment.ofArray(scratch);
      }
      return new Node(
          node.reinterpret(vectorByteSize),
          Float.intBitsToFloat(node.get(INT_UNALIGNED_LE, vectorByteSize)));
    }

    float scoreBody(int ord, float queryOffset) throws IOException {
      checkOrdinal(ord);
      Node node = getNode(ord);
      return scaler.apply(scorer.score(node.vector) * constMultiplier + node.offset + queryOffset);
    }

    float bulkScoreBody(int[] nodes, float[] scores, int numNodes, float queryOffset)
        throws IOException {
      if (dataSeg == null || !isUint8) {
        // Fallback to default single-vector scoring loop for non-uint8 formats
        // (int4 bulk scoring is not implemented) or when MemorySegment is unavailable
        return super.bulkScore(nodes, scores, numNodes);
      }
      if (numNodes > nodes.length || numNodes > scores.length) {
        throw new IllegalArgumentException(
            "numNodes ("
                + numNodes
                + ") exceeds nodes.length ("
                + nodes.length
                + ") or scores.length ("
                + scores.length
                + ")");
      }
      int i = 0;
      float maxScore = Float.NEGATIVE_INFINITY;
      final int limit = numNodes & ~3;
      for (; i < limit; i += 4) {
        checkOrdinal(nodes[i]);
        checkOrdinal(nodes[i + 1]);
        checkOrdinal(nodes[i + 2]);
        checkOrdinal(nodes[i + 3]);
        long addr1 = (long) nodes[i] * nodeSize;
        long addr2 = (long) nodes[i + 1] * nodeSize;
        long addr3 = (long) nodes[i + 2] * nodeSize;
        long addr4 = (long) nodes[i + 3] * nodeSize;
        // Read offsets directly from dataSeg (avoids input.readInt overhead)
        float off1 = Float.intBitsToFloat(dataSeg.get(INT_UNALIGNED_LE, addr1 + vectorByteSize));
        float off2 = Float.intBitsToFloat(dataSeg.get(INT_UNALIGNED_LE, addr2 + vectorByteSize));
        float off3 = Float.intBitsToFloat(dataSeg.get(INT_UNALIGNED_LE, addr3 + vectorByteSize));
        float off4 = Float.intBitsToFloat(dataSeg.get(INT_UNALIGNED_LE, addr4 + vectorByteSize));
        bulkVectorOp(addr1, addr2, addr3, addr4, scratchIntScores);
        scores[i] = scaler.apply(scratchIntScores[0] * constMultiplier + off1 + queryOffset);
        maxScore = Math.max(maxScore, scores[i]);
        scores[i + 1] = scaler.apply(scratchIntScores[1] * constMultiplier + off2 + queryOffset);
        maxScore = Math.max(maxScore, scores[i + 1]);
        scores[i + 2] = scaler.apply(scratchIntScores[2] * constMultiplier + off3 + queryOffset);
        maxScore = Math.max(maxScore, scores[i + 2]);
        scores[i + 3] = scaler.apply(scratchIntScores[3] * constMultiplier + off4 + queryOffset);
        maxScore = Math.max(maxScore, scores[i + 3]);
      }
      int remaining = numNodes - i;
      if (remaining > 0) {
        // Tail handling: process 1-3 remaining nodes. Unused slots are padded by
        // aliasing to addr1, so bulkVectorOp computes a dummy score that is ignored.
        checkOrdinal(nodes[i]);
        long addr1 = (long) nodes[i] * nodeSize;
        long addr2 = addr1;
        long addr3 = addr1;
        if (remaining > 1) {
          checkOrdinal(nodes[i + 1]);
          addr2 = (long) nodes[i + 1] * nodeSize;
        }
        if (remaining > 2) {
          checkOrdinal(nodes[i + 2]);
          addr3 = (long) nodes[i + 2] * nodeSize;
        }
        // Read offsets directly from dataSeg
        float off1 = Float.intBitsToFloat(dataSeg.get(INT_UNALIGNED_LE, addr1 + vectorByteSize));
        float off2 = Float.intBitsToFloat(dataSeg.get(INT_UNALIGNED_LE, addr2 + vectorByteSize));
        float off3 = Float.intBitsToFloat(dataSeg.get(INT_UNALIGNED_LE, addr3 + vectorByteSize));
        bulkVectorOp(addr1, addr2, addr3, addr1, scratchIntScores);
        scores[i] = scaler.apply(scratchIntScores[0] * constMultiplier + off1 + queryOffset);
        maxScore = Math.max(maxScore, scores[i]);
        if (remaining > 1) {
          scores[i + 1] = scaler.apply(scratchIntScores[1] * constMultiplier + off2 + queryOffset);
          maxScore = Math.max(maxScore, scores[i + 1]);
        }
        if (remaining > 2) {
          scores[i + 2] = scaler.apply(scratchIntScores[2] * constMultiplier + off3 + queryOffset);
          maxScore = Math.max(maxScore, scores[i + 2]);
        }
      }
      return maxScore;
    }

    void bulkVectorOp(long d1, long d2, long d3, long d4, int[] scores) {
      MemorySegment s1 = dataSeg.asSlice(d1, vectorByteSize);
      MemorySegment s2 = dataSeg.asSlice(d2, vectorByteSize);
      MemorySegment s3 = dataSeg.asSlice(d3, vectorByteSize);
      MemorySegment s4 = dataSeg.asSlice(d4, vectorByteSize);
      if (isEuclidean) {
        scores[0] = euclidean(s1);
        scores[1] = euclidean(s2);
        scores[2] = euclidean(s3);
        scores[3] = euclidean(s4);
      } else {
        scores[0] = dotProduct(s1);
        scores[1] = dotProduct(s2);
        scores[2] = dotProduct(s3);
        scores[3] = dotProduct(s4);
      }
    }

    abstract int euclidean(MemorySegment doc);

    abstract int int4Euclidean(MemorySegment doc);

    abstract int compressedInt4Euclidean(MemorySegment doc);

    abstract int dotProduct(MemorySegment doc);

    abstract int int4DotProduct(MemorySegment doc);

    abstract int compressedInt4DotProduct(MemorySegment doc);

    record Node(MemorySegment vector, float offset) {}

    @FunctionalInterface
    private interface Scorer {
      int score(MemorySegment doc) throws IOException;
    }
  }

  private static class RandomVectorScorerImpl extends RandomVectorScorerBase {
    protected final byte[] targetBytes;
    private final float queryOffset;

    RandomVectorScorerImpl(
        VectorSimilarityFunction similarityFunction,
        LegacyQuantizedByteVectorValues values,
        MemorySegmentAccessInput input,
        float[] target) {
      super(similarityFunction, values, input);
      this.targetBytes = new byte[target.length];
      this.queryOffset = quantizeQuery(target, targetBytes, similarityFunction, getQuantizer());
    }

    @Override
    public float score(int node) throws IOException {
      return scoreBody(node, queryOffset);
    }

    @Override
    public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
      return bulkScoreBody(nodes, scores, numNodes, queryOffset);
    }

    @Override
    int euclidean(MemorySegment doc) {
      return PanamaVectorUtilSupport.uint8SquareDistance(targetBytes, doc);
    }

    @Override
    int int4Euclidean(MemorySegment doc) {
      return PanamaVectorUtilSupport.int4SquareDistance(targetBytes, doc);
    }

    @Override
    int compressedInt4Euclidean(MemorySegment doc) {
      return PanamaVectorUtilSupport.int4SquareDistanceSinglePacked(targetBytes, doc);
    }

    @Override
    int dotProduct(MemorySegment doc) {
      return PanamaVectorUtilSupport.uint8DotProduct(targetBytes, doc);
    }

    @Override
    int int4DotProduct(MemorySegment doc) {
      return PanamaVectorUtilSupport.int4DotProduct(targetBytes, doc);
    }

    @Override
    int compressedInt4DotProduct(MemorySegment doc) {
      return PanamaVectorUtilSupport.int4DotProductSinglePacked(targetBytes, doc);
    }
  }

  /**
   * Native implementation of RandomVectorScorer. Uses JNI for single-vector ops but inherits bulk
   * scoring from the parent (which uses Panama Vector API). Native bulk kernels are not yet
   * implemented.
   */
  private static class NativeRandomVectorScorerImpl extends RandomVectorScorerImpl {
    NativeRandomVectorScorerImpl(
        VectorSimilarityFunction similarityFunction,
        LegacyQuantizedByteVectorValues values,
        MemorySegmentAccessInput input,
        float[] target) {
      super(similarityFunction, values, input, target);
    }

    @Override
    int euclidean(MemorySegment doc) {
      return NativeVectorUtilSupport.uint8SquareDistance(targetBytes, doc);
    }

    @Override
    int int4Euclidean(MemorySegment doc) {
      return NativeVectorUtilSupport.int4SquareDistance(targetBytes, doc);
    }

    @Override
    int compressedInt4Euclidean(MemorySegment doc) {
      return NativeVectorUtilSupport.int4SquareDistanceSinglePacked(targetBytes, doc);
    }

    @Override
    int dotProduct(MemorySegment doc) {
      return NativeVectorUtilSupport.uint8DotProduct(targetBytes, doc);
    }

    @Override
    int int4DotProduct(MemorySegment doc) {
      return NativeVectorUtilSupport.int4DotProduct(targetBytes, doc);
    }

    @Override
    int compressedInt4DotProduct(MemorySegment doc) {
      return NativeVectorUtilSupport.int4DotProductSinglePacked(targetBytes, doc);
    }
  }

  private record RandomVectorScorerSupplierImpl(
      VectorSimilarityFunction similarityFunction,
      LegacyQuantizedByteVectorValues values,
      MemorySegmentAccessInput input)
      implements RandomVectorScorerSupplier {

    @Override
    public UpdateableRandomVectorScorer scorer() {
      if (Constants.NATIVE_DOT_PRODUCT_ENABLED) {
        return new NativeUpdateableRandomVectorScorerImpl(similarityFunction, values, input);
      }
      return new UpdateableRandomVectorScorerImpl(similarityFunction, values, input);
    }

    @Override
    public RandomVectorScorerSupplier copy() {
      return new RandomVectorScorerSupplierImpl(similarityFunction, values, input);
    }
  }

  private static class UpdateableRandomVectorScorerImpl extends RandomVectorScorerBase
      implements UpdateableRandomVectorScorer {
    protected MemorySegment query;
    private float queryOffset;

    UpdateableRandomVectorScorerImpl(
        VectorSimilarityFunction similarityFunction,
        LegacyQuantizedByteVectorValues values,
        MemorySegmentAccessInput input) {
      super(similarityFunction, values, input);
    }

    @Override
    public void setScoringOrdinal(int ord) throws IOException {
      checkOrdinal(ord);
      Node node = getNode(ord);
      query = node.vector;
      queryOffset = node.offset;
    }

    @Override
    public float score(int node) throws IOException {
      return scoreBody(node, queryOffset);
    }

    @Override
    public float bulkScore(int[] nodes, float[] scores, int numNodes) throws IOException {
      return bulkScoreBody(nodes, scores, numNodes, queryOffset);
    }

    @Override
    int euclidean(MemorySegment doc) {
      return PanamaVectorUtilSupport.uint8SquareDistance(query, doc);
    }

    @Override
    int int4Euclidean(MemorySegment doc) {
      return PanamaVectorUtilSupport.int4SquareDistance(query, doc);
    }

    @Override
    int compressedInt4Euclidean(MemorySegment doc) {
      return PanamaVectorUtilSupport.int4SquareDistanceBothPacked(query, doc);
    }

    @Override
    int dotProduct(MemorySegment doc) {
      return PanamaVectorUtilSupport.uint8DotProduct(query, doc);
    }

    @Override
    int int4DotProduct(MemorySegment doc) {
      return PanamaVectorUtilSupport.int4DotProduct(query, doc);
    }

    @Override
    int compressedInt4DotProduct(MemorySegment doc) {
      return PanamaVectorUtilSupport.int4DotProductBothPacked(query, doc);
    }
  }

  /**
   * Native implementation of UpdateableRandomVectorScorer. Uses JNI for single-vector ops but
   * inherits bulk scoring from the parent (which uses Panama Vector API). Native bulk kernels are
   * not yet implemented.
   */
  private static class NativeUpdateableRandomVectorScorerImpl
      extends UpdateableRandomVectorScorerImpl {

    NativeUpdateableRandomVectorScorerImpl(
        VectorSimilarityFunction similarityFunction,
        LegacyQuantizedByteVectorValues values,
        MemorySegmentAccessInput input) {
      super(similarityFunction, values, input);
    }

    @Override
    int euclidean(MemorySegment doc) {
      return NativeVectorUtilSupport.uint8SquareDistance(query, doc);
    }

    @Override
    int int4Euclidean(MemorySegment doc) {
      return NativeVectorUtilSupport.int4SquareDistance(query, doc);
    }

    @Override
    int compressedInt4Euclidean(MemorySegment doc) {
      return NativeVectorUtilSupport.int4SquareDistanceBothPacked(query, doc);
    }

    @Override
    int dotProduct(MemorySegment doc) {
      return NativeVectorUtilSupport.uint8DotProduct(query, doc);
    }

    @Override
    int int4DotProduct(MemorySegment doc) {
      return NativeVectorUtilSupport.int4DotProduct(query, doc);
    }

    @Override
    int compressedInt4DotProduct(MemorySegment doc) {
      return NativeVectorUtilSupport.int4DotProductBothPacked(query, doc);
    }
  }
}
