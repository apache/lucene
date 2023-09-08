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

import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ScalarQuantizedVectorSimilarity;
import org.apache.lucene.util.ScalarQuantizer;
import org.apache.lucene.util.VectorUtil;

/** Quantized vector scorer */
public interface QuantizedVectorScorer extends VectorScorer {

  static QuantizedVectorScorer fromFieldEntry(
      Lucene98ScalarQuantizedVectorsReader.FieldEntry fieldEntry,
      RandomAccessQuantizedByteVectorValues values,
      float[] query) {
    float[] processedQuery =
        switch (fieldEntry.similarityFunction) {
          case EUCLIDEAN, DOT_PRODUCT, MAXIMUM_INNER_PRODUCT -> query;
          case COSINE -> {
            float[] queryCopy = ArrayUtil.copyOfSubArray(query, 0, query.length);
            VectorUtil.l2normalize(queryCopy);
            yield queryCopy;
          }
        };
    final ScalarQuantizer scalarQuantizer = fieldEntry.scalarQuantizer;
    final byte[] quantizedQuery = new byte[query.length];
    scalarQuantizer.quantize(processedQuery, quantizedQuery);
    final float queryScoreCorrection =
        ScalarQuantizedVectorSimilarity.scoreCorrectiveOffset(
            fieldEntry.similarityFunction,
            quantizedQuery,
            scalarQuantizer.getAlpha(),
            scalarQuantizer.getOffset());
    final float globalOffsetCorrection =
        switch (fieldEntry.similarityFunction) {
          case EUCLIDEAN -> 0f;
          case COSINE, DOT_PRODUCT, MAXIMUM_INNER_PRODUCT -> fieldEntry.dimension
              * scalarQuantizer.getOffset()
              * scalarQuantizer.getOffset();
        };
    final float correctiveMultiplier = scalarQuantizer.getAlpha() * scalarQuantizer.getAlpha();
    ScalarQuantizedVectorSimilarity similarity =
        ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
            fieldEntry.similarityFunction, correctiveMultiplier);
    return vectorOrdinal -> {
      byte[] storedVectorValue = values.vectorValue(vectorOrdinal);
      float storedVectorCorrection = values.getScoreCorrectionConstant() + globalOffsetCorrection;
      return similarity.score(
          quantizedQuery, queryScoreCorrection, storedVectorValue, storedVectorCorrection);
    };
  }
}
