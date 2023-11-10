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
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ScalarQuantizedVectorSimilarity;
import org.apache.lucene.util.ScalarQuantizer;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;

/** Quantized vector scorer */
final class ScalarQuantizedRandomVectorScorer
    extends RandomVectorScorer.AbstractRandomVectorScorer<byte[]> {

  private static float quantizeQuery(
      float[] query,
      byte[] quantizedQuery,
      VectorSimilarityFunction similarityFunction,
      ScalarQuantizer scalarQuantizer) {
    float[] processedQuery = query;
    if (similarityFunction.equals(VectorSimilarityFunction.COSINE)) {
      float[] queryCopy = ArrayUtil.copyOfSubArray(query, 0, query.length);
      VectorUtil.l2normalize(queryCopy);
      processedQuery = queryCopy;
    }
    return scalarQuantizer.quantize(processedQuery, quantizedQuery, similarityFunction);
  }

  private final byte[] quantizedQuery;
  private final float queryOffset;
  private final RandomAccessQuantizedByteVectorValues values;
  private final ScalarQuantizedVectorSimilarity similarity;

  ScalarQuantizedRandomVectorScorer(
      ScalarQuantizedVectorSimilarity similarityFunction,
      RandomAccessQuantizedByteVectorValues values,
      byte[] query,
      float queryOffset) {
    super(values);
    this.quantizedQuery = query;
    this.queryOffset = queryOffset;
    this.similarity = similarityFunction;
    this.values = values;
  }

  ScalarQuantizedRandomVectorScorer(
      VectorSimilarityFunction similarityFunction,
      ScalarQuantizer scalarQuantizer,
      RandomAccessQuantizedByteVectorValues values,
      float[] query) {
    super(values);
    byte[] quantizedQuery = new byte[query.length];
    float correction = quantizeQuery(query, quantizedQuery, similarityFunction, scalarQuantizer);
    this.quantizedQuery = quantizedQuery;
    this.queryOffset = correction;
    this.similarity =
        ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
            similarityFunction, scalarQuantizer.getConstantMultiplier());
    this.values = values;
  }

  @Override
  public float score(int node) throws IOException {
    byte[] storedVectorValue = values.vectorValue(node);
    float storedVectorCorrection = values.getScoreCorrectionConstant();
    return similarity.score(
        quantizedQuery, this.queryOffset, storedVectorValue, storedVectorCorrection);
  }
}
