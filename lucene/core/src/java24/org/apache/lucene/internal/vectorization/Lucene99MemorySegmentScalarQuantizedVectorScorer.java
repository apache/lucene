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
import org.apache.lucene.util.FloatToFloatFunction;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizer;

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
    if (vectorValues instanceof QuantizedByteVectorValues quantized
        && quantized.getSlice() instanceof MemorySegmentAccessInput input) {
      return new RandomVectorScorerSupplierImpl(similarityFunction, quantized, input);
    }
    return DELEGATE.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, float[] target)
      throws IOException {
    if (vectorValues instanceof QuantizedByteVectorValues quantized
        && quantized.getSlice() instanceof MemorySegmentAccessInput input) {
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
    private final int vectorByteSize;
    private final int nodeSize;
    private final Scorer scorer;
    private final FloatToFloatFunction scaler;
    private byte[] scratch;

    RandomVectorScorerBase(
        VectorSimilarityFunction similarityFunction,
        QuantizedByteVectorValues values,
        MemorySegmentAccessInput input) {
      super(values);

      this.quantizer = values.getScalarQuantizer();
      this.constMultiplier = this.quantizer.getConstantMultiplier();
      this.input = input;
      this.vectorByteSize = values.getVectorByteLength();
      this.nodeSize = this.vectorByteSize + Float.BYTES;

      this.scorer =
          switch (similarityFunction) {
            case EUCLIDEAN -> {
              if (this.quantizer.getBits() <= 4) {
                if (this.vectorByteSize != values.dimension()) {
                  yield this::compressedInt4Euclidean;
                }
                yield this::int4Euclidean;
              }
              yield this::euclidean;
            }
            case DOT_PRODUCT, COSINE, MAXIMUM_INNER_PRODUCT -> {
              if (this.quantizer.getBits() <= 4) {
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
    private final byte[] targetBytes;
    private final float queryOffset;

    RandomVectorScorerImpl(
        VectorSimilarityFunction similarityFunction,
        QuantizedByteVectorValues values,
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

  private record RandomVectorScorerSupplierImpl(
      VectorSimilarityFunction similarityFunction,
      QuantizedByteVectorValues values,
      MemorySegmentAccessInput input)
      implements RandomVectorScorerSupplier {

    @Override
    public UpdateableRandomVectorScorer scorer() {
      return new UpdateableRandomVectorScorerImpl(similarityFunction, values, input);
    }

    @Override
    public RandomVectorScorerSupplier copy() {
      return new RandomVectorScorerSupplierImpl(similarityFunction, values, input);
    }
  }

  private static class UpdateableRandomVectorScorerImpl extends RandomVectorScorerBase
      implements UpdateableRandomVectorScorer {
    private MemorySegment query;
    private float queryOffset;

    UpdateableRandomVectorScorerImpl(
        VectorSimilarityFunction similarityFunction,
        QuantizedByteVectorValues values,
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
}
