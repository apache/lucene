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

package org.apache.lucene.search;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import org.apache.lucene.document.LateInteractionField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;

/**
 * A {@link DoubleValuesSource} that scores documents using similarity between a multi-vector query,
 * and indexed document multi-vectors.
 *
 * <p>This is useful re-ranking query results using late interaction models, where documents and
 * queries are represented as multi-vectors of composing token vectors. Document vectors are indexed
 * using {@link org.apache.lucene.document.LateInteractionField}.
 *
 * @lucene.experimental
 */
public class LateInteractionFloatValuesSource extends DoubleValuesSource {

  private final String fieldName;
  private final float[][] queryVector;
  private final VectorSimilarityFunction vectorSimilarityFunction;
  private final MultiVectorSimilarity scoreFunction;

  public LateInteractionFloatValuesSource(String fieldName, float[][] queryVector) {
    this(fieldName, queryVector, VectorSimilarityFunction.COSINE, ScoreFunction.SUM_MAX_SIM);
  }

  public LateInteractionFloatValuesSource(
      String fieldName, float[][] queryVector, VectorSimilarityFunction vectorSimilarityFunction) {
    this(fieldName, queryVector, vectorSimilarityFunction, ScoreFunction.SUM_MAX_SIM);
  }

  public LateInteractionFloatValuesSource(
      String fieldName,
      float[][] queryVector,
      VectorSimilarityFunction vectorSimilarityFunction,
      MultiVectorSimilarity scoreFunction) {
    this.fieldName = Objects.requireNonNull(fieldName);
    this.queryVector = validateQueryVector(queryVector);
    this.vectorSimilarityFunction = Objects.requireNonNull(vectorSimilarityFunction);
    this.scoreFunction = Objects.requireNonNull(scoreFunction);
  }

  private float[][] validateQueryVector(float[][] queryVector) {
    if (queryVector == null || queryVector.length == 0) {
      throw new IllegalArgumentException("queryVector must not be null or empty");
    }
    if (queryVector[0] == null || queryVector[0].length == 0) {
      throw new IllegalArgumentException(
          "composing token vectors in provided query vector should not be null or empty");
    }
    for (int i = 1; i < queryVector.length; i++) {
      if (queryVector[i] == null || queryVector[i].length != queryVector[0].length) {
        throw new IllegalArgumentException(
            "all composing token vectors in provided query vector should have the same length");
      }
    }
    return queryVector;
  }

  @Override
  public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
    BinaryDocValues values = ctx.reader().getBinaryDocValues(fieldName);
    if (values == null) {
      return DoubleValues.EMPTY;
    }

    return new DoubleValues() {
      @Override
      public double doubleValue() throws IOException {
        return scoreFunction.compare(
            queryVector,
            LateInteractionField.decode(values.binaryValue()),
            vectorSimilarityFunction);
      }

      @Override
      public boolean advanceExact(int doc) throws IOException {
        return values.advanceExact(doc);
      }
    };
  }

  @Override
  public boolean needsScores() {
    return false;
  }

  @Override
  public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
    return this;
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        fieldName, Arrays.deepHashCode(queryVector), vectorSimilarityFunction, scoreFunction);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    LateInteractionFloatValuesSource other = (LateInteractionFloatValuesSource) obj;
    return Objects.equals(fieldName, other.fieldName)
        && vectorSimilarityFunction == other.vectorSimilarityFunction
        && scoreFunction == other.scoreFunction
        && Arrays.deepEquals(queryVector, other.queryVector);
  }

  @Override
  public String toString() {
    return "LateInteractionFloatValuesSource(fieldName="
        + fieldName
        + " similarityFunction="
        + vectorSimilarityFunction
        + " scoreFunction="
        + scoreFunction.getClass()
        + " queryVector="
        + Arrays.deepToString(queryVector)
        + ")";
  }

  @Override
  public boolean isCacheable(LeafReaderContext ctx) {
    return true;
  }

  /** Defines the function to compute similarity score between query and document multi-vectors */
  public enum ScoreFunction implements MultiVectorSimilarity {

    /**
     * Computes the sum of max similarity between query and document vectors.
     *
     * <p>For each query token vector, this takes the best raw similarity with any document token
     * vector: the dot product for {@link VectorSimilarityFunction#DOT_PRODUCT} and {@link
     * VectorSimilarityFunction#MAXIMUM_INNER_PRODUCT}, the cosine for {@link
     * VectorSimilarityFunction#COSINE}, and the negated squared distance for {@link
     * VectorSimilarityFunction#EUCLIDEAN}. These maxima are summed to {@code S}, and the score for
     * {@code n} query token vectors is {@code n * f(S / n)}, where {@code f} is the score scaling
     * that {@link VectorSimilarityFunction#compare(float[], float[])} applies to a single raw
     * similarity. The score is therefore non-negative and ranks documents in order of {@code S}.
     * Summing the per-token scaled scores instead would not, because the scaling is not affine for
     * {@link VectorSimilarityFunction#EUCLIDEAN} and {@link
     * VectorSimilarityFunction#MAXIMUM_INNER_PRODUCT}. For {@link VectorSimilarityFunction#COSINE}
     * and {@link VectorSimilarityFunction#DOT_PRODUCT} the two are equal, and with a single query
     * token vector the score equals the best per-token {@code compare} score.
     */
    SUM_MAX_SIM {
      @Override
      public float compare(
          float[][] queryVector,
          float[][] docVector,
          VectorSimilarityFunction vectorSimilarityFunction) {
        if (docVector.length == 0) {
          return Float.MIN_VALUE;
        }
        double sum = 0;
        for (float[] q : queryVector) {
          float maxSim = Float.NEGATIVE_INFINITY;
          for (float[] d : docVector) {
            if (q.length != d.length) {
              throw new IllegalArgumentException(
                  "Provided multi-vectors are incompatible. "
                      + "Their composing token vectors should have the same dimension, got "
                      + q.length
                      + " != "
                      + d.length);
            }
            maxSim = Float.max(maxSim, rawSimilarity(vectorSimilarityFunction, q, d));
          }
          sum += maxSim;
        }
        final int n = queryVector.length;
        return n * scaleRawSimilarity(vectorSimilarityFunction, (float) (sum / n));
      }
    };

    /** Raw similarity between two token vectors, where larger means more similar. */
    private static float rawSimilarity(
        VectorSimilarityFunction vectorSimilarityFunction, float[] q, float[] d) {
      return switch (vectorSimilarityFunction) {
        case EUCLIDEAN -> -VectorUtil.squareDistance(q, d);
        case DOT_PRODUCT, MAXIMUM_INNER_PRODUCT -> VectorUtil.dotProduct(q, d);
        case COSINE -> VectorUtil.cosine(q, d);
      };
    }

    /**
     * Applies the scaling of {@link VectorSimilarityFunction#compare(float[], float[])} to a raw
     * similarity produced by {@link #rawSimilarity}.
     */
    private static float scaleRawSimilarity(
        VectorSimilarityFunction vectorSimilarityFunction, float rawSimilarity) {
      return switch (vectorSimilarityFunction) {
        case EUCLIDEAN -> VectorUtil.normalizeDistanceToUnitInterval(-rawSimilarity);
        case DOT_PRODUCT, COSINE -> VectorUtil.normalizeToUnitInterval(rawSimilarity);
        case MAXIMUM_INNER_PRODUCT -> VectorUtil.scaleMaxInnerProductScore(rawSimilarity);
      };
    }
  }
}
