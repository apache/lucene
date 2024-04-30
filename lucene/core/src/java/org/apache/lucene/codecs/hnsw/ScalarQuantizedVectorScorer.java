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
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.RandomAccessQuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizedRandomVectorScorer;
import org.apache.lucene.util.quantization.ScalarQuantizedRandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.ScalarQuantizedVectorSimilarity;
import org.apache.lucene.util.quantization.ScalarQuantizer;

/**
 * Default scalar quantized implementation of {@link FlatVectorsScorer}.
 *
 * @lucene.experimental
 */
public class ScalarQuantizedVectorScorer implements FlatVectorsScorer {

  private final FlatVectorsScorer nonQuantizedDelegate;

  public ScalarQuantizedVectorScorer(FlatVectorsScorer flatVectorsScorer) {
    nonQuantizedDelegate = flatVectorsScorer;
  }

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, RandomAccessVectorValues vectorValues)
      throws IOException {
    if (vectorValues instanceof RandomAccessQuantizedByteVectorValues) {
      RandomAccessQuantizedByteVectorValues quantizedByteVectorValues =
          (RandomAccessQuantizedByteVectorValues) vectorValues;
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
      VectorSimilarityFunction similarityFunction,
      RandomAccessVectorValues vectorValues,
      float[] target)
      throws IOException {
    if (vectorValues instanceof RandomAccessQuantizedByteVectorValues) {
      RandomAccessQuantizedByteVectorValues quantizedByteVectorValues =
          (RandomAccessQuantizedByteVectorValues) vectorValues;
      ScalarQuantizer scalarQuantizer = quantizedByteVectorValues.getScalarQuantizer();
      byte[] targetBytes = new byte[target.length];
      float offsetCorrection =
          ScalarQuantizedRandomVectorScorer.quantizeQuery(
              target, targetBytes, similarityFunction, scalarQuantizer);
      ScalarQuantizedVectorSimilarity scalarQuantizedVectorSimilarity =
          ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
              similarityFunction,
              scalarQuantizer.getConstantMultiplier(),
              scalarQuantizer.getBits());
      return new ScalarQuantizedRandomVectorScorer(
          scalarQuantizedVectorSimilarity,
          quantizedByteVectorValues,
          targetBytes,
          offsetCorrection);
    }
    // It is possible to get to this branch during initial indexing and flush
    return nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction,
      RandomAccessVectorValues vectorValues,
      byte[] target)
      throws IOException {
    return nonQuantizedDelegate.getRandomVectorScorer(similarityFunction, vectorValues, target);
  }

  @Override
  public String toString() {
    return "ScalarQuantizedVectorScorer(" + "nonQuantizedDelegate=" + nonQuantizedDelegate + ')';
  }
}
