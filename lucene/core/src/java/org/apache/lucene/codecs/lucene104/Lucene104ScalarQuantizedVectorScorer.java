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
package org.apache.lucene.codecs.lucene104;

import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizedVectorSimilarity;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;

/** Vector scorer over OptimizedScalarQuantized vectors */
public class Lucene104ScalarQuantizedVectorScorer implements FlatVectorsScorer {
  private final FlatVectorsScorer nonQuantizedDelegate;

  public Lucene104ScalarQuantizedVectorScorer(FlatVectorsScorer nonQuantizedDelegate) {
    this.nonQuantizedDelegate = nonQuantizedDelegate;
  }

  static void checkDimensions(int queryLen, int fieldLen) {
    if (queryLen != fieldLen) {
      throw new IllegalArgumentException(
          "vector query dimension: " + queryLen + " differs from field dimension: " + fieldLen);
    }
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues)
      throws IOException {
    if (vectorValues instanceof QuantizedByteVectorValues qv) {
      return new ScalarQuantizedVectorScorerSupplier(qv, similarityFunction);
    }
    // It is possible to get to this branch during initial indexing and flush
    return nonQuantizedDelegate.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, float[] target)
      throws IOException {
    if (vectorValues instanceof QuantizedByteVectorValues qv) {
      checkDimensions(target.length, qv.dimension());
      OptimizedScalarQuantizer quantizer = qv.getQuantizer();
      byte[] targetQuantized =
          new byte
              [OptimizedScalarQuantizer.discretize(
                  target.length, qv.getScalarEncoding().getDimensionsPerByte())];
      // We make a copy as the quantization process mutates the input
      float[] copy = ArrayUtil.copyOfSubArray(target, 0, target.length);
      if (similarityFunction == COSINE) {
        VectorUtil.l2normalize(copy);
      }
      target = copy;
      var targetCorrectiveTerms =
          quantizer.scalarQuantize(
              target, targetQuantized, qv.getScalarEncoding().getBits(), qv.getCentroid());
      return new RandomVectorScorer.AbstractRandomVectorScorer(qv) {
        private final OptimizedScalarQuantizedVectorSimilarity similarity =
            new OptimizedScalarQuantizedVectorSimilarity(
                similarityFunction,
                qv.dimension(),
                qv.getCentroidDP(),
                qv.getScalarEncoding().getBits());

        @Override
        public float score(int node) throws IOException {
          return similarity.score(
              dotProduct(targetQuantized, qv, node),
              targetCorrectiveTerms,
              qv.getCorrectiveTerms(node));
        }
      };
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
    return "Lucene104ScalarQuantizedVectorScorer(nonQuantizedDelegate="
        + nonQuantizedDelegate
        + ")";
  }

  private static final class ScalarQuantizedVectorScorerSupplier
      implements RandomVectorScorerSupplier {
    private final QuantizedByteVectorValues targetValues;
    private final QuantizedByteVectorValues values;
    private final OptimizedScalarQuantizedVectorSimilarity similarity;

    public ScalarQuantizedVectorScorerSupplier(
        QuantizedByteVectorValues values, VectorSimilarityFunction similarity) throws IOException {
      this.targetValues = values.copy();
      this.values = values;
      this.similarity =
          new OptimizedScalarQuantizedVectorSimilarity(
              similarity,
              values.dimension(),
              values.getCentroidDP(),
              values.getScalarEncoding().getBits());
    }

    private ScalarQuantizedVectorScorerSupplier(
        QuantizedByteVectorValues values, OptimizedScalarQuantizedVectorSimilarity similarity)
        throws IOException {
      this.targetValues = values.copy();
      this.values = values;
      this.similarity = similarity;
    }

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(values) {
        private byte[] targetVector;
        private OptimizedScalarQuantizer.QuantizationResult targetCorrectiveTerms;

        @Override
        public float score(int node) throws IOException {
          return similarity.score(
              dotProduct(targetVector, values, node),
              targetCorrectiveTerms,
              values.getCorrectiveTerms(node));
        }

        @Override
        public void setScoringOrdinal(int node) throws IOException {
          var rawTargetVector = targetValues.vectorValue(node);
          switch (values.getScalarEncoding()) {
            case UNSIGNED_BYTE -> targetVector = rawTargetVector;
            case SEVEN_BIT -> targetVector = rawTargetVector;
            case PACKED_NIBBLE -> {
              if (targetVector == null) {
                targetVector = new byte[OptimizedScalarQuantizer.discretize(values.dimension(), 2)];
              }
              OffHeapScalarQuantizedVectorValues.unpackNibbles(rawTargetVector, targetVector);
            }
          }
          targetCorrectiveTerms = targetValues.getCorrectiveTerms(node);
        }
      };
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new ScalarQuantizedVectorScorerSupplier(values.copy(), similarity);
    }
  }

  private static float dotProduct(
      byte[] query, QuantizedByteVectorValues targetVectors, int targetOrd) throws IOException {
    var scalarEncoding = targetVectors.getScalarEncoding();
    byte[] doc = targetVectors.vectorValue(targetOrd);
    return switch (scalarEncoding) {
      case UNSIGNED_BYTE -> VectorUtil.uint8DotProduct(query, doc);
      case SEVEN_BIT -> VectorUtil.dotProduct(query, doc);
      case PACKED_NIBBLE -> VectorUtil.int4DotProductSinglePacked(query, doc);
    };
  }
}
