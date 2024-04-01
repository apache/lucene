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
package org.apache.lucene.util.quantization;

import java.io.IOException;
import org.apache.lucene.codecs.VectorSimilarity;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;

/**
 * Quantized vector similarity
 *
 * @lucene.experimental
 */
public class ScalarQuantizedVectorSimilarity {

  public static float quantizeQuery(
      float[] query,
      byte[] quantizedQuery,
      VectorSimilarity similarity,
      ScalarQuantizer scalarQuantizer) {
    float[] processedQuery = query;
    if (similarity.requiresQuantizationNormalization()) {
      processedQuery = ArrayUtil.copyOfSubArray(query, 0, query.length);
      VectorUtil.l2normalize(processedQuery);
    }
    return scalarQuantizer.quantize(processedQuery, quantizedQuery, similarity);
  }

  private final VectorSimilarity configuredScorer;
  private final float multiplier;

  public ScalarQuantizedVectorSimilarity(VectorSimilarity configuredScorer, float multiplier) {
    // This is a hack to avoid adding a new method to VectorSimilarity
    // Cosine is an exception as all quantization comparisons are dot products assuming normalized
    // vectors
    if (configuredScorer.getName().equals(VectorSimilarity.CosineSimilarity.NAME)) {
      this.configuredScorer = VectorSimilarity.DotProductSimilarity.INSTANCE;
    } else {
      this.configuredScorer = configuredScorer;
    }
    this.multiplier = multiplier;
  }

  public VectorSimilarity.VectorComparator getByteVectorComparator(
      QuantizedByteVectorProvider quantizedByteVectorProvider) throws IOException {
    return new VectorSimilarity.VectorComparator() {
      private final QuantizedByteVectorProvider provider = quantizedByteVectorProvider.copy();
      private final VectorSimilarity.VectorComparator rawComparator =
          configuredScorer.getByteVectorComparator(quantizedByteVectorProvider);

      @Override
      public float compare(int vectorOrd1, int vectorOrd2) throws IOException {
        float comparisonResult = rawComparator.compare(vectorOrd1, vectorOrd2);
        provider.vectorValue(vectorOrd1);
        quantizedByteVectorProvider.vectorValue(vectorOrd2);
        float correction1 = provider.getScoreCorrectionConstant();
        float correction2 = quantizedByteVectorProvider.getScoreCorrectionConstant();
        return correction1 + correction2 + comparisonResult * multiplier;
      }

      @Override
      public float scaleScore(float comparisonResult) {
        return configuredScorer.scaleVectorScore(comparisonResult);
      }

      @Override
      public VectorSimilarity.VectorScorer asScorer(int vectorOrd) throws IOException {
        VectorSimilarity.VectorScorer scorer = rawComparator.asScorer(vectorOrd);
        provider.vectorValue(vectorOrd);
        float correction = provider.getScoreCorrectionConstant();

        return new VectorSimilarity.VectorScorer() {
          @Override
          public float compare(int vectorOrd) throws IOException {
            float rawScore = scorer.compare(vectorOrd);
            quantizedByteVectorProvider.vectorValue(vectorOrd);
            float correction2 = quantizedByteVectorProvider.getScoreCorrectionConstant();
            return correction + correction2 + rawScore * multiplier;
          }

          @Override
          public float scaleScore(float comparisonResult) {
            return configuredScorer.scaleVectorScore(comparisonResult);
          }
        };
      }
    };
  }

  public VectorSimilarity.VectorScorer getVectorScorer(
      QuantizedByteVectorProvider quantizedByteVectorProvider,
      byte[] quantizedQuery,
      float queryCorrection)
      throws IOException {
    final QuantizedByteVectorProvider providerCopy = quantizedByteVectorProvider.copy();
    final VectorSimilarity.VectorScorer rawComparator =
        configuredScorer.getVectorScorer(providerCopy, quantizedQuery);
    return new VectorSimilarity.VectorScorer() {
      @Override
      public float compare(int vectorOrd) throws IOException {
        float comparisonResult = rawComparator.compare(vectorOrd);
        providerCopy.vectorValue(vectorOrd);
        float correction = providerCopy.getScoreCorrectionConstant();
        return queryCorrection + correction + comparisonResult * multiplier;
      }

      @Override
      public float scaleScore(float comparisonResult) {
        return configuredScorer.scaleVectorScore(comparisonResult);
      }
    };
  }
}
