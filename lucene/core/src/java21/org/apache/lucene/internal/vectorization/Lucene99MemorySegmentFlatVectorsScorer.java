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
import static org.apache.lucene.internal.vectorization.Lucene99MemorySegmentScalarQuantizedVectorScorer.ScalarQuantizedRandomVectorScorerSupplier;
import static org.apache.lucene.internal.vectorization.Lucene99MemorySegmentScalarQuantizedVectorScorer.fromVectorSimilarity;

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizer;

public class Lucene99MemorySegmentFlatVectorsScorer implements FlatVectorsScorer {

  public static final Lucene99MemorySegmentFlatVectorsScorer INSTANCE =
      new Lucene99MemorySegmentFlatVectorsScorer(DefaultFlatVectorScorer.INSTANCE);

  private final FlatVectorsScorer delegate;

  private Lucene99MemorySegmentFlatVectorsScorer(FlatVectorsScorer delegate) {
    this.delegate = delegate;
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityType, KnnVectorValues vectorValues) throws IOException {

    // Handle quantized vectors
    if (vectorValues instanceof QuantizedByteVectorValues quantizedByteVectorValues) {
      return new ScalarQuantizedRandomVectorScorerSupplier(
          quantizedByteVectorValues, similarityType);
    }

    // Handle binary vectors
    if (vectorValues instanceof ByteVectorValues bvv
        && bvv instanceof HasIndexSlice byteVectorValues
        && byteVectorValues.getSlice() != null) {
      var scorer =
          Lucene99MemorySegmentByteVectorScorerSupplier.create(
              similarityType, byteVectorValues.getSlice(), vectorValues);
      if (scorer.isPresent()) {
        return scorer.get();
      }
    }
    return delegate.getRandomVectorScorerSupplier(similarityType, vectorValues);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction, KnnVectorValues vectorValues, float[] target)
      throws IOException {
    if (vectorValues instanceof QuantizedByteVectorValues quantizedByteVectorValues) {
      ScalarQuantizer scalarQuantizer = quantizedByteVectorValues.getScalarQuantizer();
      final float constMultiplier = scalarQuantizer.getConstantMultiplier();
      final byte[] targetBytes = new byte[target.length];
      final float offsetCorrection =
          quantizeQuery(target, targetBytes, similarityFunction, scalarQuantizer);

      return fromVectorSimilarity(
          targetBytes,
          offsetCorrection,
          similarityFunction,
          constMultiplier,
          quantizedByteVectorValues);
    }
    // It is possible to get to this branch during initial indexing and flush
    return delegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityType, KnnVectorValues vectorValues, byte[] queryVector)
      throws IOException {
    checkDimensions(queryVector.length, vectorValues.dimension());
    // a quantized values here is a wrapping or delegation issue
    assert !(vectorValues instanceof QuantizedByteVectorValues);
    if (vectorValues instanceof ByteVectorValues bvv
        && bvv instanceof HasIndexSlice byteVectorValues
        && byteVectorValues.getSlice() != null) {
      var scorer =
          Lucene99MemorySegmentByteVectorScorer.create(
              similarityType, byteVectorValues.getSlice(), vectorValues, queryVector);
      if (scorer.isPresent()) {
        return scorer.get();
      }
    }
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
    return "Lucene99MemorySegmentFlatVectorsScorer(nonQuantizedDelegate=DefaultFlatVectorScorer())";
  }
}
