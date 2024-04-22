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
import static org.apache.lucene.util.VectorUtil.scaleMaxInnerProductScore;

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
    if (vectorValues instanceof RandomAccessQuantizedByteVectorValues quantizedByteVectorValues) {
      return new ScalarQuantizedRandomVectorScorerSupplier(
          quantizedByteVectorValues, similarityFunction);
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
    if (vectorValues instanceof RandomAccessQuantizedByteVectorValues quantizedByteVectorValues) {
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
    return switch (sim) {
      case EUCLIDEAN -> new Euclidean(values, constMultiplier, targetBytes);
      case COSINE, DOT_PRODUCT -> new DotProduct(
          values, constMultiplier, targetBytes, offsetCorrection);
      case MAXIMUM_INNER_PRODUCT -> new MaximumInnerProduct(
          values, constMultiplier, targetBytes, offsetCorrection);
    };
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
    private final byte[] compressedVector;
    private final byte[] targetBytes;
    private final float offsetCorrection;

    public DotProduct(
        RandomAccessQuantizedByteVectorValues values,
        float constMultiplier,
        byte[] targetBytes,
        float offsetCorrection) {
      super(values);
      this.constMultiplier = constMultiplier;
      this.values = values;
      if (values.getScalarQuantizer().getBits() <= 4
          && values.getVectorByteLength() != values.dimension()
          && values.getSlice() != null) {
        this.compressedVector = new byte[values.getVectorByteLength()];
      } else {
        this.compressedVector = null;
      }
      this.targetBytes = targetBytes;
      this.offsetCorrection = offsetCorrection;
    }

    @Override
    public float score(int vectorOrdinal) throws IOException {
      float adjustedDistance =
          optimizedDotProduct(
              targetBytes,
              offsetCorrection,
              vectorOrdinal,
              values,
              compressedVector,
              constMultiplier);
      return (1 + adjustedDistance) / 2;
    }
  }

  /** Calculates max inner product on quantized vectors, applying the appropriate corrections */
  private static class MaximumInnerProduct extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final float constMultiplier;
    private final RandomAccessQuantizedByteVectorValues values;
    private final byte[] compressedVector;
    private final byte[] targetBytes;
    private final float offsetCorrection;

    public MaximumInnerProduct(
        RandomAccessQuantizedByteVectorValues values,
        float constMultiplier,
        byte[] targetBytes,
        float offsetCorrection) {
      super(values);
      this.constMultiplier = constMultiplier;
      this.values = values;
      if (values.getScalarQuantizer().getBits() <= 4
          && values.getVectorByteLength() != values.dimension()
          && values.getSlice() != null) {
        this.compressedVector = new byte[values.getVectorByteLength()];
      } else {
        this.compressedVector = null;
      }
      this.targetBytes = targetBytes;
      this.offsetCorrection = offsetCorrection;
    }

    @Override
    public float score(int vectorOrdinal) throws IOException {
      float adjustedDistance =
          optimizedDotProduct(
              targetBytes,
              offsetCorrection,
              vectorOrdinal,
              values,
              compressedVector,
              constMultiplier);
      return scaleMaxInnerProductScore(adjustedDistance);
    }
  }

  private static float optimizedDotProduct(
      byte[] queryVector,
      float queryOffset,
      int vectorOrdinal,
      RandomAccessQuantizedByteVectorValues values,
      byte[] compressedVector,
      float constMultiplier)
      throws IOException {
    final int dotProduct;
    final float vectorOffset;
    if (values.getScalarQuantizer().getBits() <= 4) {
      // This means we can use our optimized path for 4-bit quantization comparisons
      if (values.getSlice() != null && compressedVector != null) {
        // get compressed vector, in Lucene99, vector values are stored and have a single value for
        // offset correction
        values.getSlice().seek((long) vectorOrdinal * (values.getVectorByteLength() + Float.BYTES));
        values.getSlice().readBytes(compressedVector, 0, compressedVector.length);
        vectorOffset = values.getScoreCorrectionConstant(vectorOrdinal);
        dotProduct = VectorUtil.int4DotProductPacked(queryVector, compressedVector);
      } else {
        // fall back to the standard path, which reads the vector value and inflates it if necessary
        byte[] storedVector = values.vectorValue(vectorOrdinal);
        vectorOffset = values.getScoreCorrectionConstant(vectorOrdinal);
        // For 4-bit quantization, we use a specialized dot product implementation
        dotProduct = VectorUtil.int4DotProduct(storedVector, queryVector);
      }
    } else {
      byte[] storedVector = values.vectorValue(vectorOrdinal);
      vectorOffset = values.getScoreCorrectionConstant(vectorOrdinal);
      dotProduct = VectorUtil.dotProduct(storedVector, queryVector);
    }
    return dotProduct * constMultiplier + queryOffset + vectorOffset;
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
