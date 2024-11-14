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

package org.apache.lucene.codecs.hnsw;

import java.io.IOException;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizedVectorSimilarity;
import org.apache.lucene.util.quantization.ScalarQuantizer;

/**
 * Default scalar quantized implementation of {@link FlatVectorsScorer}.
 *
 * @lucene.experimental
 */
public class ScalarQuantizedVectorScorer implements FlatVectorsScorer {

  public static float quantizeQuery(
      float[] query,
      byte[] quantizedQuery,
      VectorSimilarityFunction similarityFunction,
      ScalarQuantizer scalarQuantizer) {
    float[] processedQuery =
        switch (similarityFunction) {
          case EUCLIDEAN, DOT_PRODUCT, MAXIMUM_INNER_PRODUCT -> query;
          case COSINE -> {
            float[] queryCopy = ArrayUtil.copyArray(query);
            VectorUtil.l2normalize(queryCopy);
            yield queryCopy;
          }
        };
    return scalarQuantizer.quantize(processedQuery, quantizedQuery, similarityFunction);
  }

  private final FlatVectorsScorer nonQuantizedDelegate;

  public ScalarQuantizedVectorScorer(FlatVectorsScorer flatVectorsScorer) {
    nonQuantizedDelegate = flatVectorsScorer;
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues)
      throws IOException {
    if (vectorValues instanceof QuantizedByteVectorValues quantizedByteVectorValues) {
      return new ScalarQuantizedRandomVectorScorerSupplier(
          similarityFunction,
          quantizedByteVectorValues.getScalarQuantizer(),
          quantizedByteVectorValues);
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
      ScalarQuantizedVectorSimilarity scalarQuantizedVectorSimilarity =
          ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
              similarityFunction,
              scalarQuantizer.getConstantMultiplier(),
              scalarQuantizer.getBits());
      return new RandomVectorScorer.AbstractRandomVectorScorer(quantizedByteVectorValues) {
        @Override
        public float score(int node) throws IOException {
          byte[] nodeVector = quantizedByteVectorValues.vectorValue(node);
          float nodeOffset = quantizedByteVectorValues.getScoreCorrectionConstant(node);
          return scalarQuantizedVectorSimilarity.score(
              targetBytes, offsetCorrection, nodeVector, nodeOffset);
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
    return "ScalarQuantizedVectorScorer(" + "nonQuantizedDelegate=" + nonQuantizedDelegate + ')';
  }

  /**
   * Quantized vector scorer supplier
   *
   * @lucene.experimental
   */
  public static class ScalarQuantizedRandomVectorScorerSupplier
      implements RandomVectorScorerSupplier {

    private final QuantizedByteVectorValues values;
    private final ScalarQuantizedVectorSimilarity similarity;
    private final VectorSimilarityFunction vectorSimilarityFunction;

    public ScalarQuantizedRandomVectorScorerSupplier(
        VectorSimilarityFunction similarityFunction,
        ScalarQuantizer scalarQuantizer,
        QuantizedByteVectorValues values) {
      this.similarity =
          ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
              similarityFunction,
              scalarQuantizer.getConstantMultiplier(),
              scalarQuantizer.getBits());
      this.values = values;
      this.vectorSimilarityFunction = similarityFunction;
    }

    private ScalarQuantizedRandomVectorScorerSupplier(
        ScalarQuantizedVectorSimilarity similarity,
        VectorSimilarityFunction vectorSimilarityFunction,
        QuantizedByteVectorValues values) {
      this.similarity = similarity;
      this.values = values;
      this.vectorSimilarityFunction = vectorSimilarityFunction;
    }

    @Override
    public RandomVectorScorer scorer(int ord) throws IOException {
      final QuantizedByteVectorValues vectorsCopy = values.copy();
      final byte[] queryVector = values.vectorValue(ord);
      final float queryOffset = values.getScoreCorrectionConstant(ord);
      return new RandomVectorScorer.AbstractRandomVectorScorer(vectorsCopy) {
        @Override
        public float score(int node) throws IOException {
          byte[] nodeVector = vectorsCopy.vectorValue(node);
          float nodeOffset = vectorsCopy.getScoreCorrectionConstant(node);
          return similarity.score(queryVector, queryOffset, nodeVector, nodeOffset);
        }
      };
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new ScalarQuantizedRandomVectorScorerSupplier(
          similarity, vectorSimilarityFunction, values.copy());
    }

    @Override
    public String toString() {
      return "ScalarQuantizedRandomVectorScorerSupplier(vectorSimilarityFunction="
          + vectorSimilarityFunction
          + ")";
    }
  }
}
