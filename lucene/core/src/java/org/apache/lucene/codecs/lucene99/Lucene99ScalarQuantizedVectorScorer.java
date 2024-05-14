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
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.RandomAccessQuantizedByteVectorValues;
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
      VectorSimilarityFunction similarityFunction, RandomAccessVectorValues vectorValues)
      throws IOException {
    if (vectorValues instanceof RandomAccessQuantizedByteVectorValues) {
      return new ScalarQuantizedRandomVectorScorerSupplier(
          (RandomAccessQuantizedByteVectorValues) vectorValues, similarityFunction);
    }
    // It is possible to get to this branch during initial indexing and flush
    return nonQuantizedDelegate.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction,
      RandomAccessVectorValues vectorValues,
      float[] target)
      throws IOException {
    if (vectorValues instanceof RandomAccessQuantizedByteVectorValues) {
      RandomAccessQuantizedByteVectorValues quantizedByteVectorValues =
          (RandomAccessQuantizedByteVectorValues) vectorValues;
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
      VectorSimilarityFunction similarityFunction,
      RandomAccessVectorValues vectorValues,
      byte[] target)
      throws IOException {
    return nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public String toString() {
    return "ScalarQuantizedVectorScorer(" + "nonQuantizedDelegate=" + nonQuantizedDelegate + ')';
  }

  static RandomVectorScorer fromVectorSimilarity(
      byte[] targetBytes,
      float offsetCorrection,
      VectorSimilarityFunction sim,
      float constMultiplier,
      RandomAccessQuantizedByteVectorValues values) {
    switch (sim) {
      case EUCLIDEAN:
        return new Euclidean(values, constMultiplier, targetBytes);
      case COSINE:
      case DOT_PRODUCT:
        return dotProductFactory(
            targetBytes, offsetCorrection, constMultiplier, values, f -> Math.max((1 + f) / 2, 0f));
      case MAXIMUM_INNER_PRODUCT:
        return dotProductFactory(
            targetBytes,
            offsetCorrection,
            constMultiplier,
            values,
            VectorUtil::scaleMaxInnerProductScore);
      default:
        throw new IllegalArgumentException("Unsupported similarity function: " + sim);
    }
  }

  private static RandomVectorScorer.AbstractRandomVectorScorer dotProductFactory(
      byte[] targetBytes,
      float offsetCorrection,
      float constMultiplier,
      RandomAccessQuantizedByteVectorValues values,
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

  private static class Euclidean extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final float constMultiplier;
    private final byte[] targetBytes;
    private final RandomAccessQuantizedByteVectorValues values;

    private Euclidean(
        RandomAccessQuantizedByteVectorValues values, float constMultiplier, byte[] targetBytes) {
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
  }

  /** Calculates dot product on quantized vectors, applying the appropriate corrections */
  private static class DotProduct extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final float constMultiplier;
    private final RandomAccessQuantizedByteVectorValues values;
    private final byte[] targetBytes;
    private final float offsetCorrection;
    private final FloatToFloatFunction scoreAdjustmentFunction;

    public DotProduct(
        RandomAccessQuantizedByteVectorValues values,
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
  }

  private static class CompressedInt4DotProduct
      extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final float constMultiplier;
    private final RandomAccessQuantizedByteVectorValues values;
    private final byte[] compressedVector;
    private final byte[] targetBytes;
    private final float offsetCorrection;
    private final FloatToFloatFunction scoreAdjustmentFunction;

    private CompressedInt4DotProduct(
        RandomAccessQuantizedByteVectorValues values,
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
  }

  private static class Int4DotProduct extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final float constMultiplier;
    private final RandomAccessQuantizedByteVectorValues values;
    private final byte[] targetBytes;
    private final float offsetCorrection;
    private final FloatToFloatFunction scoreAdjustmentFunction;

    public Int4DotProduct(
        RandomAccessQuantizedByteVectorValues values,
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
  }

  @FunctionalInterface
  private interface FloatToFloatFunction {
    float apply(float f);
  }

  private static final class ScalarQuantizedRandomVectorScorerSupplier
      implements RandomVectorScorerSupplier {

    private final VectorSimilarityFunction vectorSimilarityFunction;
    private final RandomAccessQuantizedByteVectorValues values;
    private final RandomAccessQuantizedByteVectorValues values1;
    private final RandomAccessQuantizedByteVectorValues values2;

    public ScalarQuantizedRandomVectorScorerSupplier(
        RandomAccessQuantizedByteVectorValues values,
        VectorSimilarityFunction vectorSimilarityFunction)
        throws IOException {
      this.values = values;
      this.values1 = values.copy();
      this.values2 = values.copy();
      this.vectorSimilarityFunction = vectorSimilarityFunction;
    }

    @Override
    public RandomVectorScorer scorer(int ord) throws IOException {
      byte[] vectorValue = values1.vectorValue(ord);
      float offsetCorrection = values1.getScoreCorrectionConstant(ord);
      return fromVectorSimilarity(
          vectorValue,
          offsetCorrection,
          vectorSimilarityFunction,
          values.getScalarQuantizer().getConstantMultiplier(),
          values2);
    }

    @Override
    public ScalarQuantizedRandomVectorScorerSupplier copy() throws IOException {
      return new ScalarQuantizedRandomVectorScorerSupplier(values.copy(), vectorSimilarityFunction);
    }
  }
}
