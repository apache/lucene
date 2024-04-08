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

package org.apache.lucene.codecs.lucene99;

import java.io.IOException;
import org.apache.lucene.codecs.FlatVectorsScorer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.RandomAccessQuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizedRandomVectorScorer;
import org.apache.lucene.util.quantization.ScalarQuantizedRandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.ScalarQuantizedVectorSimilarity;
import org.apache.lucene.util.quantization.ScalarQuantizer;

/** On-heap scalar quantized implementation of {@link FlatVectorsScorer}. */
public class OnHeapScalarQuantizedVectorScorer implements FlatVectorsScorer {

  public OnHeapScalarQuantizedVectorScorer() {}

  @Override
  public RandomVectorScorerSupplier getRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction, RandomAccessVectorValues vectorValues)
      throws IOException {
    assert vectorValues instanceof RandomAccessQuantizedByteVectorValues;
    if (vectorValues instanceof RandomAccessQuantizedByteVectorValues quantizedByteVectorValues) {
      return new ScalarQuantizedRandomVectorScorerSupplier(
          similarityFunction,
          quantizedByteVectorValues.getScalarQuantizer(),
          quantizedByteVectorValues);
    }
    throw new IllegalArgumentException(
        "vectorValues must be an instance of RandomAccessQuantizedByteVectorValues");
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction,
      RandomAccessVectorValues vectorValues,
      float[] target)
      throws IOException {
    assert vectorValues instanceof RandomAccessQuantizedByteVectorValues;
    if (vectorValues instanceof RandomAccessQuantizedByteVectorValues quantizedByteVectorValues) {
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
    throw new IllegalArgumentException(
        "vectorValues must be an instance of RandomAccessQuantizedByteVectorValues");
  }

  @Override
  public RandomVectorScorer getRandomVectorScorer(
      VectorSimilarityFunction similarityFunction,
      RandomAccessVectorValues vectorValues,
      byte[] target)
      throws IOException {
    throw new IllegalArgumentException("scalar quantization does not support byte[] targets");
  }
}
