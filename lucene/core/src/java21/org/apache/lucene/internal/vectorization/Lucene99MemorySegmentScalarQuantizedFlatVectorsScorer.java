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
package org.apache.lucene.internal.vectorization;

import static org.apache.lucene.codecs.hnsw.ScalarQuantizedVectorScorer.quantizeQuery;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.RandomAccessQuantizedByteVectorValues;

public class Lucene99MemorySegmentScalarQuantizedFlatVectorsScorer implements FlatVectorsScorer {

  private final FlatVectorsScorer delegate;

  public Lucene99MemorySegmentScalarQuantizedFlatVectorsScorer(FlatVectorsScorer delegate) {
    this.delegate = delegate;
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityType, RandomAccessVectorValues vectorValues)
      throws IOException {
    if (vectorValues instanceof RandomAccessQuantizedByteVectorValues quantizedByteVectorValues) {
      // Unoptimized edge case, we don't optimize compressed 4-bit quantization with Euclidean
      // similarity
      // So, we delegate to the default scorer
      if (quantizedByteVectorValues.getScalarQuantizer().getBits() == 4
          && similarityType == VectorSimilarityFunction.EUCLIDEAN
          // Indicates that the vector is compressed as the byte length is not equal to the
          // dimension count
          && (vectorValues.getVectorByteLength() - Float.BYTES) != vectorValues.dimension()) {
        return delegate.getRandomVectorScorerSupplier(similarityType, vectorValues);
      }
      var scalarQuantizer = quantizedByteVectorValues.getScalarQuantizer();
      var scalarScorerSupplier =
          Lucene99MemorySegmentScalarQuantizedVectorScorerSupplier.create(
              similarityType,
              scalarQuantizer.getBits(),
              scalarQuantizer.getConstantMultiplier(),
              quantizedByteVectorValues);
      if (scalarScorerSupplier.isPresent()) {
        return scalarScorerSupplier.get();
      }
    }
    return delegate.getRandomVectorScorerSupplier(similarityType, vectorValues);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityType,
      RandomAccessVectorValues vectorValues,
      float[] queryVector)
      throws IOException {
    if (vectorValues instanceof RandomAccessQuantizedByteVectorValues quantizedByteVectorValues) {
      // Unoptimized edge case, we don't optimize compressed 4-bit quantization with Euclidean
      // similarity
      // So, we delegate to the default scorer
      if (quantizedByteVectorValues.getScalarQuantizer().getBits() == 4
          && similarityType == VectorSimilarityFunction.EUCLIDEAN
          // Indicates that the vector is compressed as the byte length is not equal to the
          // dimension count
          && (vectorValues.getVectorByteLength() - Float.BYTES) != vectorValues.dimension()) {
        return delegate.getRandomVectorScorer(similarityType, vectorValues, queryVector);
      }
      checkDimensions(queryVector.length, vectorValues.dimension());
      var scalarQuantizer = quantizedByteVectorValues.getScalarQuantizer();
      byte[] targetBytes = new byte[queryVector.length];
      float offsetCorrection =
          quantizeQuery(queryVector, targetBytes, similarityType, scalarQuantizer);
      var scalarScorer =
          Lucene99MemorySegmentScalarQuantizedVectorScorer.create(
              similarityType,
              targetBytes,
              offsetCorrection,
              scalarQuantizer.getConstantMultiplier(),
              scalarQuantizer.getBits(),
              quantizedByteVectorValues);
      if (scalarScorer.isPresent()) {
        return scalarScorer.get();
      }
    }
    return delegate.getRandomVectorScorer(similarityType, vectorValues, queryVector);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityType,
      RandomAccessVectorValues vectorValues,
      byte[] queryVector)
      throws IOException {
    return delegate.getRandomVectorScorer(similarityType, vectorValues, queryVector);
  }

  static void checkDimensions(int queryLen, int fieldLen) {
    if (queryLen != fieldLen) {
      throw new IllegalArgumentException(
          "vector query dimension: " + queryLen + " differs from field dimension: " + fieldLen);
    }
  }

  @Override
  public String toString() {
    return "Lucene99MemorySegmentScalarQuantizedFlatVectorsScorer()";
  }
}
