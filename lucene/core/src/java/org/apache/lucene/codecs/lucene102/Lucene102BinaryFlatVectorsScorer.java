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
package org.apache.lucene.codecs.lucene102;

import static org.apache.lucene.codecs.lucene102.Lucene102BinaryQuantizedVectorsFormat.INDEX_BITS;
import static org.apache.lucene.codecs.lucene102.Lucene102BinaryQuantizedVectorsFormat.QUERY_BITS;
import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.util.quantization.OptimizedScalarQuantizer.transposeHalfByte;

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
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer.QuantizationResult;

/** Vector scorer over binarized vector values */
public class Lucene102BinaryFlatVectorsScorer implements FlatVectorsScorer {
  private final FlatVectorsScorer nonQuantizedDelegate;

  public Lucene102BinaryFlatVectorsScorer(FlatVectorsScorer nonQuantizedDelegate) {
    this.nonQuantizedDelegate = nonQuantizedDelegate;
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues)
      throws IOException {
    if (vectorValues instanceof BinarizedByteVectorValues) {
      throw new UnsupportedOperationException(
          "getRandomVectorScorerSupplier(VectorSimilarityFunction,RandomAccessVectorValues) not implemented for binarized format");
    }
    return nonQuantizedDelegate.getRandomVectorScorerSupplier(similarityFunction, vectorValues);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, float[] target)
      throws IOException {
    if (vectorValues instanceof BinarizedByteVectorValues binarizedVectors) {
      OptimizedScalarQuantizer quantizer = binarizedVectors.getQuantizer();
      float[] centroid = binarizedVectors.getCentroid();
      // We make a copy as the quantization process mutates the input
      float[] copy = ArrayUtil.copyOfSubArray(target, 0, target.length);
      if (similarityFunction == COSINE) {
        VectorUtil.l2normalize(copy);
      }
      target = copy;
      byte[] initial = new byte[target.length];
      byte[] quantized = new byte[QUERY_BITS * binarizedVectors.discretizedDimensions() / 8];
      OptimizedScalarQuantizer.QuantizationResult queryCorrections =
          quantizer.scalarQuantize(target, initial, (byte) 4, centroid);
      transposeHalfByte(initial, quantized);
      return new RandomVectorScorer.AbstractRandomVectorScorer(binarizedVectors) {
        private final OptimizedScalarQuantizedVectorSimilarity similarity =
            new OptimizedScalarQuantizedVectorSimilarity(
                similarityFunction,
                binarizedVectors.dimension(),
                binarizedVectors.getCentroidDP(),
                QUERY_BITS,
                INDEX_BITS);

        @Override
        public float score(int node) throws IOException {
          var indexVector = binarizedVectors.vectorValue(node);
          var indexCorrections = binarizedVectors.getCorrectiveTerms(node);
          float dotProduct = VectorUtil.int4BitDotProduct(quantized, indexVector);
          return similarity.score(dotProduct, queryCorrections, indexCorrections);
        }
      };
    }
    return nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, byte[] target)
      throws IOException {
    return nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction,
      Lucene102BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues scoringVectors,
      BinarizedByteVectorValues targetVectors)
      throws IOException {
    return new BinarizedRandomVectorScorerSupplier(
        scoringVectors, targetVectors, similarityFunction);
  }

  @Override
  public String toString() {
    return "Lucene102BinaryFlatVectorsScorer(nonQuantizedDelegate=" + nonQuantizedDelegate + ")";
  }

  /** Vector scorer supplier over binarized vector values */
  static class BinarizedRandomVectorScorerSupplier implements RandomVectorScorerSupplier {
    private final Lucene102BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues
        queryVectors;
    private final BinarizedByteVectorValues targetVectors;
    private final OptimizedScalarQuantizedVectorSimilarity similarity;

    BinarizedRandomVectorScorerSupplier(
        Lucene102BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues queryVectors,
        BinarizedByteVectorValues targetVectors,
        VectorSimilarityFunction similarityFunction)
        throws IOException {
      this.queryVectors = queryVectors;
      this.targetVectors = targetVectors;
      this.similarity =
          new OptimizedScalarQuantizedVectorSimilarity(
              similarityFunction,
              targetVectors.dimension(),
              targetVectors.getCentroidDP(),
              QUERY_BITS,
              INDEX_BITS);
    }

    BinarizedRandomVectorScorerSupplier(
        Lucene102BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues queryVectors,
        BinarizedByteVectorValues targetVectors,
        OptimizedScalarQuantizedVectorSimilarity similarity) {
      this.queryVectors = queryVectors;
      this.targetVectors = targetVectors;
      this.similarity = similarity;
    }

    @Override
    public UpdateableRandomVectorScorer scorer() throws IOException {
      final BinarizedByteVectorValues targetVectors = this.targetVectors.copy();
      final Lucene102BinaryQuantizedVectorsWriter.OffHeapBinarizedQueryVectorValues queryVectors =
          this.queryVectors.copy();
      return new UpdateableRandomVectorScorer.AbstractUpdateableRandomVectorScorer(targetVectors) {
        private QuantizationResult queryCorrections = null;
        private byte[] vector = null;

        @Override
        public void setScoringOrdinal(int node) throws IOException {
          queryCorrections = queryVectors.getCorrectiveTerms(node);
          vector = queryVectors.vectorValue(node);
        }

        @Override
        public float score(int node) throws IOException {
          if (vector == null || queryCorrections == null) {
            throw new IllegalStateException("setScoringOrdinal was not called");
          }
          var indexVector = targetVectors.vectorValue(node);
          var indexCorrections = targetVectors.getCorrectiveTerms(node);
          return similarity.score(
              (float) VectorUtil.int4BitDotProduct(vector, indexVector),
              queryCorrections,
              indexCorrections);
        }
      };
    }

    @Override
    public RandomVectorScorerSupplier copy() throws IOException {
      return new BinarizedRandomVectorScorerSupplier(
          queryVectors.copy(), targetVectors.copy(), similarity);
    }
  }
}
