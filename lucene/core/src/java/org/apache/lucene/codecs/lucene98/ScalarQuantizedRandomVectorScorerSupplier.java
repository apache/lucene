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
package org.apache.lucene.codecs.lucene98;

import java.io.IOException;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.ScalarQuantizedVectorSimilarity;
import org.apache.lucene.util.ScalarQuantizer;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;

/** Quantized vector scorer supplier */
final class ScalarQuantizedRandomVectorScorerSupplier implements RandomVectorScorerSupplier {

  private final float globalOffsetCorrection;
  private final RandomAccessQuantizedByteVectorValues values;
  private final ScalarQuantizedVectorSimilarity similarity;

  ScalarQuantizedRandomVectorScorerSupplier(
      VectorSimilarityFunction similarityFunction,
      ScalarQuantizer scalarQuantizer,
      RandomAccessQuantizedByteVectorValues values) {
    this.globalOffsetCorrection =
        switch (similarityFunction) {
          case EUCLIDEAN -> 0f;
          case COSINE, DOT_PRODUCT, MAXIMUM_INNER_PRODUCT -> values.dimension()
              * scalarQuantizer.getOffset()
              * scalarQuantizer.getOffset();
        };
    final float correctiveMultiplier = scalarQuantizer.getAlpha() * scalarQuantizer.getAlpha();
    this.similarity =
        ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
            similarityFunction, correctiveMultiplier);
    this.values = values;
  }

  @Override
  public RandomVectorScorer scorer(int ord) throws IOException {
    final RandomAccessQuantizedByteVectorValues vectorsCopy = values.copy();
    final byte[] queryVector = values.vectorValue(ord);
    final float queryOffset = values.getScoreCorrectionConstant();
    return new ScalarQuantizedRandomVectorScorer(
        similarity, vectorsCopy, globalOffsetCorrection, queryVector, queryOffset);
  }
}
