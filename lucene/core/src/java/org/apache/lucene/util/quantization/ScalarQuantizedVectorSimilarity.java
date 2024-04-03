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
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

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
    if (ScalarQuantizer.similarityRequiresNormalization(similarity)) {
      processedQuery = ArrayUtil.copyOfSubArray(query, 0, query.length);
      VectorUtil.l2normalize(processedQuery);
    }
    return scalarQuantizer.quantize(processedQuery, quantizedQuery, similarity);
  }

  private final VectorSimilarity configuredScorer;
  private final float multiplier;

  public ScalarQuantizedVectorSimilarity(
      VectorSimilarity configuredScorer, float multiplier, byte bits) {
    // This is a hack to avoid adding a new method to VectorSimilarity
    // Cosine is an exception as all quantization comparisons are dot products assuming normalized
    // vectors
    VectorSimilarity innerSimilarity = configuredScorer;
    if (configuredScorer.getName().equals(VectorSimilarity.CosineSimilarity.NAME)) {
      innerSimilarity = VectorSimilarity.DotProductSimilarity.INSTANCE;
    }
    if (bits <= 4
        && (innerSimilarity.getName().equals(VectorSimilarity.DotProductSimilarity.NAME)
            || innerSimilarity.getName().equals(VectorSimilarity.MaxInnerProductSimilarity.NAME))) {
      this.configuredScorer = new Int4OptimizedVectorSimilarity(innerSimilarity);
    } else {
      this.configuredScorer = innerSimilarity;
    }
    this.multiplier = multiplier;
  }

  public VectorSimilarity.VectorComparator getByteVectorComparator(
      RandomAccessQuantizedByteVectorValues quantizedByteVectorProvider) throws IOException {
    return new VectorSimilarity.VectorComparator() {
      private final RandomAccessQuantizedByteVectorValues provider =
          quantizedByteVectorProvider.copy();
      private final VectorSimilarity.VectorComparator rawComparator =
          configuredScorer.getByteVectorComparator(quantizedByteVectorProvider);

      @Override
      public float compare(int vectorOrd1, int vectorOrd2) throws IOException {
        float comparisonResult = rawComparator.compare(vectorOrd1, vectorOrd2);
        float correction1 = provider.getScoreCorrectionConstant(vectorOrd1);
        float correction2 = quantizedByteVectorProvider.getScoreCorrectionConstant(vectorOrd2);
        return correction1 + correction2 + comparisonResult * multiplier;
      }

      @Override
      public float scaleScore(float comparisonResult) {
        return configuredScorer.scaleVectorScore(comparisonResult);
      }

      @Override
      public VectorSimilarity.VectorScorer asScorer(int vectorOrd) throws IOException {
        VectorSimilarity.VectorScorer scorer = rawComparator.asScorer(vectorOrd);
        float correction = provider.getScoreCorrectionConstant(vectorOrd);

        return new VectorSimilarity.VectorScorer() {
          @Override
          public float compare(int vectorOrd) throws IOException {
            float rawScore = scorer.compare(vectorOrd);
            float correction2 = quantizedByteVectorProvider.getScoreCorrectionConstant(vectorOrd);
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
      RandomAccessQuantizedByteVectorValues quantizedByteVectorProvider,
      byte[] quantizedQuery,
      float queryCorrection)
      throws IOException {
    final RandomAccessQuantizedByteVectorValues providerCopy = quantizedByteVectorProvider.copy();
    final VectorSimilarity.VectorScorer rawComparator =
        configuredScorer.getVectorScorer(providerCopy, quantizedQuery);
    return new VectorSimilarity.VectorScorer() {
      @Override
      public float compare(int vectorOrd) throws IOException {
        float comparisonResult = rawComparator.compare(vectorOrd);
        float correction = providerCopy.getScoreCorrectionConstant(vectorOrd);
        return queryCorrection + correction + comparisonResult * multiplier;
      }

      @Override
      public float scaleScore(float comparisonResult) {
        return configuredScorer.scaleVectorScore(comparisonResult);
      }
    };
  }

  /** Compares two byte vectors */
  interface ByteVectorComparator {
    int compare(byte[] v1, byte[] v2);
  }

  private static final class Int4OptimizedVectorSimilarity extends VectorSimilarity {
    private final VectorSimilarity similarity;

    public Int4OptimizedVectorSimilarity(VectorSimilarity similarity) {
      super("Int4Optimized" + similarity.getName());
      this.similarity = similarity;
    }

    @Override
    public float scaleVectorScore(float score) {
      return similarity.scaleVectorScore(score);
    }

    @Override
    public VectorScorer getVectorScorer(
        RandomAccessVectorValues<float[]> vectorProvider, float[] target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public VectorComparator getFloatVectorComparator(
        RandomAccessVectorValues<float[]> vectorProvider) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public VectorScorer getVectorScorer(
        RandomAccessVectorValues<byte[]> byteVectorProvider, byte[] target) throws IOException {
      return new VectorScorer() {
        @Override
        public float scaleScore(float comparisonResult) {
          return similarity.scaleVectorScore(comparisonResult);
        }

        @Override
        public float compare(int targetVectorOrd) throws IOException {
          byte[] targetVector = byteVectorProvider.vectorValue(targetVectorOrd);
          return VectorUtil.int4DotProduct(target, targetVector);
        }
      };
    }

    @Override
    public VectorComparator getByteVectorComparator(
        RandomAccessVectorValues<byte[]> byteVectorProvider) throws IOException {
      return new VectorComparator() {
        private final RandomAccessVectorValues<byte[]> copy = byteVectorProvider.copy();

        @Override
        public float compare(int vectorOrd1, int vectorOrd2) throws IOException {
          return VectorUtil.int4DotProduct(
              copy.vectorValue(vectorOrd1), byteVectorProvider.vectorValue(vectorOrd2));
        }

        @Override
        public float scaleScore(float comparisonResult) {
          return similarity.scaleVectorScore(comparisonResult);
        }
      };
    }
  }
}
