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

package org.apache.lucene.codecs.lucene99;

import static org.apache.lucene.codecs.hnsw.ScalarQuantizedVectorScorer.quantizeQuery;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizer;

/**
 * Optimized scalar quantized implementation of {@link FlatVectorsScorer} for quantized vectors
 * stored in the Lucene99 format.
 *
 * @lucene.experimental
 */
public class Lucene99ScalarQuantizedVectorScorer implements FlatVectorsScorer {

  private final FlatVectorsScorer nonQuantizedDelegate;

  public Lucene99ScalarQuantizedVectorScorer(FlatVectorsScorer flatVectorsScorer) {
    nonQuantizedDelegate = flatVectorsScorer;
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues)
      throws IOException {
    if (vectorValues instanceof QuantizedByteVectorValues quantizedByteVectorValues) {
      return new ScalarQuantizedRandomVectorScorerSupplier(
          quantizedByteVectorValues, similarityFunction);
    }
    // It is possible to get to this branch during initial indexing and flush
    return nonQuantizedDelegate.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, float[] target)
      throws IOException {
    if (vectorValues instanceof QuantizedByteVectorValues quantizedByteVectorValues) {
      ScalarQuantizer scalarQuantizer = quantizedByteVectorValues.getScalarQuantizer();
      byte[] targetBytes = new byte[target.length];
      float offsetCorrection =
          quantizeQuery(target, targetBytes, similarityFunction, scalarQuantizer);
      return fromVectorSimilarity(
          targetBytes,
          offsetCorrection,
          similarityFunction,
          scalarQuantizer.getConstantMultiplier(),
          quantizedByteVectorValues);
    }
    // It is possible to get to this branch during initial indexing and flush
    return nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, byte[] target)
      throws IOException {
    return nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public String toString() {
    return "ScalarQuantizedVectorScorer(" + "nonQuantizedDelegate=" + nonQuantizedDelegate + ')';
  }

  static UpdateableRandomVectorScorer fromVectorSimilarity(
      byte[] targetBytes,
      float offsetCorrection,
      VectorSimilarityFunction sim,
      float constMultiplier,
      QuantizedByteVectorValues values) {
    checkDimensions(targetBytes.length, values.dimension());
    return switch (sim) {
      case EUCLIDEAN -> new Euclidean(values, constMultiplier, targetBytes);
      case COSINE, DOT_PRODUCT ->
          dotProductFactory(
              targetBytes,
              offsetCorrection,
              constMultiplier,
              values,
              f -> Math.max((1 + f) / 2, 0));
      case MAXIMUM_INNER_PRODUCT ->
          dotProductFactory(
              targetBytes,
              offsetCorrection,
              constMultiplier,
              values,
              VectorUtil::scaleMaxInnerProductScore);
    };
  }

  static void checkDimensions(int queryLen, int fieldLen) {
    if (queryLen != fieldLen) {
      throw new IllegalArgumentException(
          "vector query dimension: " + queryLen + " differs from field dimension: " + fieldLen);
    }
  }

  private static UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer
      dotProductFactory(
          byte[] targetBytes,
          float offsetCorrection,
          float constMultiplier,
          QuantizedByteVectorValues values,
          FloatToFloatFunction scoreAdjustmentFunction) {
    if (values.getScalarQuantizer().getBits() <= 4) {
      if (values.getVectorByteLength() != values.dimension() && values.getSlice() != null) {
        return new CompressedInt4DotProduct(
            values, constMultiplier, targetBytes, offsetCorrection, scoreAdjustmentFunction);
      }
      return new Int4DotProduct(
          values, constMultiplier, targetBytes, offsetCorrection, scoreAdjustmentFunction);
    }
    return new DotProduct(
        values, constMultiplier, targetBytes, offsetCorrection, scoreAdjustmentFunction);
  }

  private static class Euclidean
      extends UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer {
    private final float constMultiplier;
    private final byte[] targetBytes;
    private final QuantizedByteVectorValues values;

    private Euclidean(QuantizedByteVectorValues values, float constMultiplier, byte[] targetBytes) {
      super(values);
      this.values = values;
      this.constMultiplier = constMultiplier;
      this.targetBytes = targetBytes;
    }

    @Override
    public float score(int node) throws IOException {
      byte[] nodeVector = values.vectorValue(node);
      int squareDistance = VectorUtil.squareDistance(nodeVector, targetBytes);
      float adjustedDistance = squareDistance * constMultiplier;
      return 1 / (1f + adjustedDistance);
    }

    @Override
    public void setScoringOrdinal(int node) throws IOException {
      System.arraycopy(values.vectorValue(node), 0, targetBytes, 0, targetBytes.length);
    }
  }

  /** Calculates dot product on quantized vectors, applying the appropriate corrections */
  private static class DotProduct
      extends UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer {
    private final float constMultiplier;
    private final QuantizedByteVectorValues values;
    private final byte[] targetBytes;
    private float offsetCorrection;
    private final FloatToFloatFunction scoreAdjustmentFunction;

    public DotProduct(
        QuantizedByteVectorValues values,
        float constMultiplier,
        byte[] targetBytes,
        float offsetCorrection,
        FloatToFloatFunction scoreAdjustmentFunction) {
      super(values);
      this.constMultiplier = constMultiplier;
      this.values = values;
      this.targetBytes = targetBytes;
      this.offsetCorrection = offsetCorrection;
      this.scoreAdjustmentFunction = scoreAdjustmentFunction;
    }

    @Override
    public float score(int vectorOrdinal) throws IOException {
      byte[] storedVector = values.vectorValue(vectorOrdinal);
      float vectorOffset = values.getScoreCorrectionConstant(vectorOrdinal);
      int dotProduct = VectorUtil.dotProduct(storedVector, targetBytes);
      // For the current implementation of scalar quantization, all dotproducts should be >= 0;
      assert dotProduct >= 0;
      float adjustedDistance = dotProduct * constMultiplier + offsetCorrection + vectorOffset;
      return scoreAdjustmentFunction.apply(adjustedDistance);
    }

    @Override
    public void setScoringOrdinal(int node) throws IOException {
      System.arraycopy(values.vectorValue(node), 0, targetBytes, 0, targetBytes.length);
      offsetCorrection = values.getScoreCorrectionConstant(node);
    }
  }

  // TODO consider splitting this into two classes. right now the "query" vector is always
  // decompressed
  //    it could stay compressed if we had a compressed version of the target vector
  private static class CompressedInt4DotProduct
      extends UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer {
    private final float constMultiplier;
    private final QuantizedByteVectorValues values;
    private final byte[] compressedVector;
    private final byte[] targetBytes;
    private float offsetCorrection;
    private final FloatToFloatFunction scoreAdjustmentFunction;

    private CompressedInt4DotProduct(
        QuantizedByteVectorValues values,
        float constMultiplier,
        byte[] targetBytes,
        float offsetCorrection,
        FloatToFloatFunction scoreAdjustmentFunction) {
      super(values);
      this.constMultiplier = constMultiplier;
      this.values = values;
      this.compressedVector = new byte[values.getVectorByteLength()];
      this.targetBytes = targetBytes;
      this.offsetCorrection = offsetCorrection;
      this.scoreAdjustmentFunction = scoreAdjustmentFunction;
    }

    @Override
    public float score(int vectorOrdinal) throws IOException {
      // get compressed vector, in Lucene99, vector values are stored and have a single value for
      // offset correction
      values.getSlice().seek((long) vectorOrdinal * (values.getVectorByteLength() + Float.BYTES));
      values.getSlice().readBytes(compressedVector, 0, compressedVector.length);
      float vectorOffset = values.getScoreCorrectionConstant(vectorOrdinal);
      int dotProduct = VectorUtil.int4DotProductPacked(targetBytes, compressedVector);
      // For the current implementation of scalar quantization, all dotproducts should be >= 0;
      assert dotProduct >= 0;
      float adjustedDistance = dotProduct * constMultiplier + offsetCorrection + vectorOffset;
      return scoreAdjustmentFunction.apply(adjustedDistance);
    }

    @Override
    public void setScoringOrdinal(int node) throws IOException {
      System.arraycopy(values.vectorValue(node), 0, targetBytes, 0, targetBytes.length);
      offsetCorrection = values.getScoreCorrectionConstant(node);
    }
  }

  private static class Int4DotProduct
      extends UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer {
    private final float constMultiplier;
    private final QuantizedByteVectorValues values;
    private final byte[] targetBytes;
    private float offsetCorrection;
    private final FloatToFloatFunction scoreAdjustmentFunction;

    public Int4DotProduct(
        QuantizedByteVectorValues values,
        float constMultiplier,
        byte[] targetBytes,
        float offsetCorrection,
        FloatToFloatFunction scoreAdjustmentFunction) {
      super(values);
      this.constMultiplier = constMultiplier;
      this.values = values;
      this.targetBytes = targetBytes;
      this.offsetCorrection = offsetCorrection;
      this.scoreAdjustmentFunction = scoreAdjustmentFunction;
    }

    @Override
    public float score(int vectorOrdinal) throws IOException {
      byte[] storedVector = values.vectorValue(vectorOrdinal);
      float vectorOffset = values.getScoreCorrectionConstant(vectorOrdinal);
      int dotProduct = VectorUtil.int4DotProduct(storedVector, targetBytes);
      // For the current implementation of scalar quantization, all dotproducts should be >= 0;
      assert dotProduct >= 0;
      float adjustedDistance = dotProduct * constMultiplier + offsetCorrection + vectorOffset;
      return scoreAdjustmentFunction.apply(adjustedDistance);
    }

    @Override
    public void setScoringOrdinal(int node) throws IOException {
      System.arraycopy(values.vectorValue(node), 0, targetBytes, 0, targetBytes.length);
      offsetCorrection = values.getScoreCorrectionConstant(node);
    }
  }

  @FunctionalInterface
  private interface FloatToFloatFunction {
    float apply(float f);
  }

  private static final class ScalarQuantizedRandomVectorScorerSupplier
      implements RandomVectorScorerSupplier {

    private final VectorSimilarityFunction vectorSimilarityFunction;
    private final QuantizedByteVectorValues values;
    private final QuantizedByteVectorValues targetVectors;

    public ScalarQuantizedRandomVectorScorerSupplier(
        QuantizedByteVectorValues values, VectorSimilarityFunction vectorSimilarityFunction)
        throws IOException {
      this.values = values;
      this.targetVectors = values.copy();
      this.vectorSimilarityFunction = vectorSimilarityFunction;
    }

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      byte[] vectorValue = new byte[values.dimension()];
      float offsetCorrection = 0;
      return fromVectorSimilarity(
          vectorValue,
          offsetCorrection,
          vectorSimilarityFunction,
          values.getScalarQuantizer().getConstantMultiplier(),
          targetVectors);
    }

    @Override
    public ScalarQuantizedRandomVectorScorerSupplier copy() throws IOException {
      return new ScalarQuantizedRandomVectorScorerSupplier(values.copy(), vectorSimilarityFunction);
    }

    @Override
    public String toString() {
      return "ScalarQuantizedRandomVectorScorerSupplier(vectorSimilarityFunction="
          + vectorSimilarityFunction
          + ")";
    }
  }
}
