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

import java.io.IOException;
import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.RandomAccessQuantizedByteVectorValues;

public class Lucene99MemorySegmentFlatVectorsScorer implements FlatVectorsScorer {

  public static final Lucene99MemorySegmentFlatVectorsScorer INSTANCE =
      new Lucene99MemorySegmentFlatVectorsScorer(DefaultFlatVectorScorer.INSTANCE);

  private final FlatVectorsScorer delegate;

  private Lucene99MemorySegmentFlatVectorsScorer(FlatVectorsScorer delegate) {
    this.delegate = delegate;
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityType, RandomAccessVectorValues vectorValues)
      throws IOException {
    // a quantized values here is a wrapping or delegation issue
    assert !(vectorValues instanceof RandomAccessQuantizedByteVectorValues);
    // currently only supports binary vectors
    if (vectorValues instanceof RandomAccessVectorValues.Bytes && vectorValues.getSlice() != null) {
      var scorer =
          Lucene99MemorySegmentByteVectorScorerSupplier.create(
              similarityType, vectorValues.getSlice(), vectorValues);
      if (scorer.isPresent()) {
        return scorer.get();
      }
    }
    return delegate.getRandomVectorScorerSupplier(similarityType, vectorValues);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityType,
      RandomAccessVectorValues vectorValues,
      float[] target)
      throws IOException {
    // currently only supports binary vectors, so always delegate
    return delegate.getRandomVectorScorer(similarityType, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityType,
      RandomAccessVectorValues vectorValues,
      byte[] queryVector)
      throws IOException {
    checkDimensions(queryVector.length, vectorValues.dimension());
    // a quantized values here is a wrapping or delegation issue
    assert !(vectorValues instanceof RandomAccessQuantizedByteVectorValues);
    if (vectorValues instanceof RandomAccessVectorValues.Bytes && vectorValues.getSlice() != null) {
      var scorer =
          Lucene99MemorySegmentByteVectorScorer.create(
              similarityType, vectorValues.getSlice(), vectorValues, queryVector);
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
    return "Lucene99MemorySegmentFlatVectorsScorer()";
  }
}
