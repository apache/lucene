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

  static RandomVectorScorer fromVectorSimilarity(
      byte[] targetBytes,
      float offsetCorrection,
      VectorSimilarityFunction sim,
      float constMultiplier,
      QuantizedByteVectorValues values)
      throws IOException {
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

  private static RandomVectorScorer.AbstractRandomVectorScorer dotProductFactory(
      byte[] targetBytes,
      float offsetCorrection,
      float constMultiplier,
      QuantizedByteVectorValues vectorValues,
      FloatToFloatFunction scoreAdjustmentFunction)
      throws IOException {
    QuantizedByteVectorValues.QuantizedBytes vectors = vectorValues.vectors();
    if (vectorValues.getScalarQuantizer().getBits() <= 4) {
      if (vectorValues.getVectorByteLength() != vectorValues.dimension()
          && vectors.getSlice() != null) {
        return new CompressedInt4DotProduct(
            vectorValues, constMultiplier, targetBytes, offsetCorrection, scoreAdjustmentFunction);
      }
      return new Int4DotProduct(
          vectorValues, constMultiplier, targetBytes, offsetCorrection, scoreAdjustmentFunction);
    }
    return new DotProduct(
        vectorValues, constMultiplier, targetBytes, offsetCorrection, scoreAdjustmentFunction);
  }

  private static class Euclidean extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final float constMultiplier;
    private final byte[] targetBytes;
    private final QuantizedByteVectorValues.QuantizedBytes vectors;

    private Euclidean(
        QuantizedByteVectorValues vectorValues, float constMultiplier, byte[] targetBytes)
        throws IOException {
      super(vectorValues);
      vectors = vectorValues.vectors();
      this.constMultiplier = constMultiplier;
      this.targetBytes = targetBytes;
    }

    @Override
    public float score(int node) throws IOException {
      byte[] nodeVector = vectors.get(node);
      int squareDistance = VectorUtil.squareDistance(nodeVector, targetBytes);
      float adjustedDistance = squareDistance * constMultiplier;
      return 1 / (1f + adjustedDistance);
    }
  }

  /** Calculates dot product on quantized vectors, applying the appropriate corrections */
  private static class DotProduct extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final float constMultiplier;
    private final QuantizedByteVectorValues.QuantizedBytes vectors;
    private final byte[] targetBytes;
    private final float offsetCorrection;
    private final FloatToFloatFunction scoreAdjustmentFunction;

    public DotProduct(
        QuantizedByteVectorValues vectorValues,
        float constMultiplier,
        byte[] targetBytes,
        float offsetCorrection,
        FloatToFloatFunction scoreAdjustmentFunction)
        throws IOException {
      super(vectorValues);
      this.constMultiplier = constMultiplier;
      vectors = vectorValues.vectors();
      this.targetBytes = targetBytes;
      this.offsetCorrection = offsetCorrection;
      this.scoreAdjustmentFunction = scoreAdjustmentFunction;
    }

    @Override
    public float score(int vectorOrdinal) throws IOException {
      byte[] storedVector = vectors.get(vectorOrdinal);
      float vectorOffset = vectors.getScoreCorrectionConstant(vectorOrdinal);
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
    private final QuantizedByteVectorValues vectorValues;
    private final QuantizedByteVectorValues.QuantizedBytes vectors;
    private final byte[] compressedVector;
    private final byte[] targetBytes;
    private final float offsetCorrection;
    private final FloatToFloatFunction scoreAdjustmentFunction;

    private CompressedInt4DotProduct(
        QuantizedByteVectorValues vectorValues,
        float constMultiplier,
        byte[] targetBytes,
        float offsetCorrection,
        FloatToFloatFunction scoreAdjustmentFunction)
        throws IOException {
      super(vectorValues);
      this.constMultiplier = constMultiplier;
      this.vectorValues = vectorValues;
      vectors = vectorValues.vectors();
      this.compressedVector = new byte[vectorValues.getVectorByteLength()];
      this.targetBytes = targetBytes;
      this.offsetCorrection = offsetCorrection;
      this.scoreAdjustmentFunction = scoreAdjustmentFunction;
    }

    @Override
    public float score(int vectorOrdinal) throws IOException {
      // get compressed vector, in Lucene99, vector values are stored and have a single value for
      // offset correction
      vectors
          .getSlice()
          .seek((long) vectorOrdinal * (vectorValues.getVectorByteLength() + Float.BYTES));
      vectors.getSlice().readBytes(compressedVector, 0, compressedVector.length);
      float vectorOffset = vectors.getScoreCorrectionConstant(vectorOrdinal);
      int dotProduct = VectorUtil.int4DotProductPacked(targetBytes, compressedVector);
      // For the current implementation of scalar quantization, all dotproducts should be >= 0;
      assert dotProduct >= 0;
      float adjustedDistance = dotProduct * constMultiplier + offsetCorrection + vectorOffset;
      return scoreAdjustmentFunction.apply(adjustedDistance);
    }
  }

  private static class Int4DotProduct extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final float constMultiplier;
    private final QuantizedByteVectorValues.QuantizedBytes vectors;
    private final byte[] targetBytes;
    private final float offsetCorrection;
    private final FloatToFloatFunction scoreAdjustmentFunction;

    public Int4DotProduct(
        QuantizedByteVectorValues vectorValues,
        float constMultiplier,
        byte[] targetBytes,
        float offsetCorrection,
        FloatToFloatFunction scoreAdjustmentFunction)
        throws IOException {
      super(vectorValues);
      this.constMultiplier = constMultiplier;
      vectors = vectorValues.vectors();
      this.targetBytes = targetBytes;
      this.offsetCorrection = offsetCorrection;
      this.scoreAdjustmentFunction = scoreAdjustmentFunction;
    }

    @Override
    public float score(int vectorOrdinal) throws IOException {
      byte[] storedVector = vectors.get(vectorOrdinal);
      float vectorOffset = vectors.getScoreCorrectionConstant(vectorOrdinal);
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
    private final QuantizedByteVectorValues vectorValues;

    public ScalarQuantizedRandomVectorScorerSupplier(
        QuantizedByteVectorValues vectorValues, VectorSimilarityFunction vectorSimilarityFunction)
        throws IOException {
      this.vectorValues = vectorValues;
      this.vectorSimilarityFunction = vectorSimilarityFunction;
    }

    @Override
    public RandomVectorScorer scorer(int ord) throws IOException {
      QuantizedByteVectorValues.QuantizedBytes vectors = vectorValues.vectors();
      byte[] vectorValue = vectors.get(ord);
      float offsetCorrection = vectors.getScoreCorrectionConstant(ord);
      return fromVectorSimilarity(
          vectorValue,
          offsetCorrection,
          vectorSimilarityFunction,
          vectorValues.getScalarQuantizer().getConstantMultiplier(),
          vectorValues);
    }

    @Override
    public String toString() {
      return "ScalarQuantizedRandomVectorScorerSupplier(vectorSimilarityFunction="
          + vectorSimilarityFunction
          + ")";
    }
  }
}
