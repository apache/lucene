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
import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;

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
    return switch (vectorValues.getEncoding()) {
      case FLOAT32 -> getFloatScoringSupplier((FloatVectorValues) vectorValues, similarityType);
      case BYTE -> getByteScorerSupplier((ByteVectorValues) vectorValues, similarityType);
    };
  }

  private RandomVectorScorerSupplier getFloatScoringSupplier(
      FloatVectorValues vectorValues, VectorSimilarityFunction similarityType) throws IOException {
    if (vectorValues instanceof HasIndexSlice sliceableValues
        && sliceableValues.getSlice() != null) {
      var scorer =
          Lucene99MemorySegmentFloatVectorScorerSupplier.create(
              similarityType, sliceableValues.getSlice(), vectorValues);
      if (scorer.isPresent()) {
        return scorer.get();
      }
    }
    return delegate.getRandomVectorScorerSupplier(similarityType, vectorValues);
  }

  private RandomVectorScorerSupplier getByteScorerSupplier(
      ByteVectorValues vectorValues, VectorSimilarityFunction similarityType) throws IOException {
    // a quantized values here is a wrapping or delegation issue
    assert !(vectorValues instanceof QuantizedByteVectorValues);
    // currently only supports binary vectors
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
      VectorSimilarityFunction similarityType, KnnVectorValues vectorValues, float[] target)
      throws IOException {
    checkDimensions(target.length, vectorValues.dimension());
    if (vectorValues instanceof FloatVectorValues fvv
        && fvv instanceof HasIndexSlice floatVectorValues
        && floatVectorValues.getSlice() != null) {
      var scorer =
          Lucene99MemorySegmentFloatVectorScorer.create(
              similarityType, floatVectorValues.getSlice(), fvv, target);
      if (scorer.isPresent()) {
        return scorer.get();
      }
    }
    return delegate.getRandomVectorScorer(similarityType, vectorValues, target);
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
    return "Lucene99MemorySegmentFlatVectorsScorer()";
  }
}
