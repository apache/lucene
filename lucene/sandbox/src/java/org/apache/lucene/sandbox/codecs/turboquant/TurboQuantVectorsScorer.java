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
package org.apache.lucene.sandbox.codecs.turboquant;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;

/**
 * Scorer for TurboQuant quantized vectors. Rotates the query vector once, then computes distances
 * in the rotated space against quantized candidate vectors.
 *
 * <p>This is a naive (non-SIMD) implementation for correctness. Phase 3 replaces it with
 * LUT-based SIMD scoring.
 */
public class TurboQuantVectorsScorer implements FlatVectorsScorer {

  private final FlatVectorsScorer rawScorer;

  public TurboQuantVectorsScorer(FlatVectorsScorer rawScorer) {
    this.rawScorer = rawScorer;
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues)
      throws IOException {
    if (vectorValues instanceof OffHeapTurboQuantVectorValues quantizedValues) {
      return new TurboQuantScorerSupplier(similarityFunction, quantizedValues);
    }
    return rawScorer.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, float[] target)
      throws IOException {
    if (vectorValues instanceof OffHeapTurboQuantVectorValues quantizedValues) {
      return new TurboQuantQueryScorer(similarityFunction, quantizedValues, target);
    }
    return rawScorer.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, byte[] target)
      throws IOException {
    throw new UnsupportedOperationException("TurboQuant only supports float32 vectors");
  }

  @Override
  public String toString() {
    return "TurboQuantVectorsScorer(rawScorer=" + rawScorer + ")";
  }

  /** Scorer for a single query against quantized vectors. */
  private static class TurboQuantQueryScorer extends RandomVectorScorer.AbstractRandomVectorScorer {
    private final VectorSimilarityFunction similarityFunction;
    private final OffHeapTurboQuantVectorValues quantizedValues;
    private final float[] rotatedQuery;

    TurboQuantQueryScorer(
        VectorSimilarityFunction similarityFunction,
        OffHeapTurboQuantVectorValues quantizedValues,
        float[] target) {
      super(quantizedValues);
      this.similarityFunction = similarityFunction;
      this.quantizedValues = quantizedValues;

      // Rotate query once
      HadamardRotation rotation = quantizedValues.getRotation();
      int d = target.length;

      // Normalize for cosine similarity
      float[] normalized;
      if (similarityFunction == VectorSimilarityFunction.COSINE) {
        normalized = new float[d];
        float norm = 0;
        for (int i = 0; i < d; i++) norm += target[i] * target[i];
        norm = (float) Math.sqrt(norm);
        if (norm > 0) {
          for (int i = 0; i < d; i++) normalized[i] = target[i] / norm;
        }
      } else {
        normalized = target;
      }

      this.rotatedQuery = new float[d];
      rotation.rotate(normalized, rotatedQuery);
    }

    @Override
    public float score(int node) throws IOException {
      float[] centroids = quantizedValues.getCentroids();
      int d = quantizedValues.dimension();
      byte[] packedIndices = quantizedValues.vectorValue(node);
      int b = quantizedValues.getBitsPerCoordinate();
      float docNorm = quantizedValues.getNorm(node);

      return switch (similarityFunction) {
        case DOT_PRODUCT -> {
          float dot = TurboQuantScoringUtil.dotProduct(rotatedQuery, packedIndices, centroids, b, d);
          // DOT_PRODUCT expects unit vectors; dot already approximates true dot product
          yield Math.max((1 + dot) / 2, 0);
        }
        case MAXIMUM_INNER_PRODUCT -> {
          float dot = TurboQuantScoringUtil.dotProduct(rotatedQuery, packedIndices, centroids, b, d);
          // Reconstruct unnormalized dot product: query is already unnormalized, doc was normalized
          float rawDot = dot * docNorm;
          yield VectorUtil.scaleMaxInnerProductScore(rawDot);
        }
        case COSINE -> {
          float dot = TurboQuantScoringUtil.dotProduct(rotatedQuery, packedIndices, centroids, b, d);
          yield Math.max((1 + dot) / 2, 0);
        }
        case EUCLIDEAN -> {
          float dist =
              TurboQuantScoringUtil.squareDistance(
                  rotatedQuery, packedIndices, centroids, b, d, docNorm);
          yield 1 / (1 + dist);
        }
      };
    }
  }

  /** Supplier for graph-building scorers (doc-vs-doc scoring). */
  private static class TurboQuantScorerSupplier implements RandomVectorScorerSupplier {
    private final VectorSimilarityFunction similarityFunction;
    private final OffHeapTurboQuantVectorValues quantizedValues;

    TurboQuantScorerSupplier(
        VectorSimilarityFunction similarityFunction,
        OffHeapTurboQuantVectorValues quantizedValues) {
      this.similarityFunction = similarityFunction;
      this.quantizedValues = quantizedValues;
    }

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      OffHeapTurboQuantVectorValues copy = quantizedValues.copy();
      return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(copy) {
        private byte[] currentIndices;
        private float currentNorm;

        @Override
        public void setScoringOrdinal(int ord) throws IOException {
          currentIndices = copy.vectorValue(ord);
          currentNorm = copy.getNorm(ord);
        }

        @Override
        public float score(int node) throws IOException {
          float[] centroids = copy.getCentroids();
          int d = copy.dimension();
          int b = copy.getBitsPerCoordinate();
          byte[] nodePackedIndices = copy.vectorValue(node);
          float nodeNorm = copy.getNorm(node);

          byte[] nodeIndices = new byte[d];
          TurboQuantBitPacker.unpack(nodePackedIndices, b, d, nodeIndices);
          byte[] curIndices = new byte[d];
          TurboQuantBitPacker.unpack(currentIndices, b, d, curIndices);

          // Approximate distance using quantized centroids
          float dot = 0;
          for (int i = 0; i < d; i++) {
            dot += centroids[curIndices[i] & 0xFF] * centroids[nodeIndices[i] & 0xFF];
          }
          return switch (similarityFunction) {
            case DOT_PRODUCT ->
                Math.max((1 + dot) / 2, 0);
            case MAXIMUM_INNER_PRODUCT ->
                VectorUtil.scaleMaxInnerProductScore(dot * currentNorm * nodeNorm);
            case COSINE -> Math.max((1 + dot) / 2, 0);
            case EUCLIDEAN -> {
              float dist = 0;
              for (int i = 0; i < d; i++) {
                float a = centroids[curIndices[i] & 0xFF] * currentNorm;
                float bv = centroids[nodeIndices[i] & 0xFF] * nodeNorm;
                float diff = a - bv;
                dist += diff * diff;
              }
              yield 1 / (1 + dist);
            }
          };
        }

      };
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new TurboQuantScorerSupplier(
          similarityFunction, quantizedValues.copy());
    }
  }
}
