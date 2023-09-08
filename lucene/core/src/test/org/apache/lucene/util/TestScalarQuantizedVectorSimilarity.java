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
package org.apache.lucene.util;

import static org.apache.lucene.util.TestScalarQuantizer.fromFloats;
import static org.apache.lucene.util.TestScalarQuantizer.randomFloatArray;
import static org.apache.lucene.util.TestScalarQuantizer.randomFloats;

import java.io.IOException;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestScalarQuantizedVectorSimilarity extends LuceneTestCase {

  public void testToEuclidean() throws IOException {
    int dims = 128;
    int numVecs = 100;

    float[][] floats = randomFloats(numVecs, dims);
    for (int quantile : new int[] {90, 95, 99, 100}) {
      float error = Math.max((100 - quantile) * 0.01f, 0.01f);
      FloatVectorValues floatVectorValues = fromFloats(floats);
      ScalarQuantizer scalarQuantizer = ScalarQuantizer.fromVectors(floatVectorValues, quantile);
      byte[][] quantized = quantizeVectors(scalarQuantizer, floats);
      float globalOffset = scalarQuantizer.globalVectorOffset(dims);
      float[] query = ArrayUtil.copyOfSubArray(floats[0], 0, dims);
      ScalarQuantizedVectorSimilarity quantizedSimilarity =
          ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
              VectorSimilarityFunction.EUCLIDEAN, scalarQuantizer.getConstantMultiplier());
      assertQuantizedScores(
          floats,
          quantized,
          query,
          error,
          VectorSimilarityFunction.EUCLIDEAN,
          quantizedSimilarity,
          scalarQuantizer,
          globalOffset);
    }
  }

  public void testToCosine() throws IOException {
    int dims = 128;
    int numVecs = 100;

    float[][] floats = randomFloats(numVecs, dims);

    for (int quantile : new int[] {90, 95, 99, 100}) {
      float error = Math.max((100 - quantile) * 0.01f, 0.01f);
      FloatVectorValues floatVectorValues = fromFloatsNormalized(floats);
      ScalarQuantizer scalarQuantizer = ScalarQuantizer.fromVectors(floatVectorValues, quantile);
      byte[][] quantized = quantizeVectorsNormalized(scalarQuantizer, floats);
      float globalOffset = scalarQuantizer.globalVectorOffset(dims);
      float[] query = ArrayUtil.copyOfSubArray(floats[0], 0, dims);
      VectorUtil.l2normalize(query);
      ScalarQuantizedVectorSimilarity quantizedSimilarity =
          ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
              VectorSimilarityFunction.COSINE, scalarQuantizer.getConstantMultiplier());
      assertQuantizedScores(
          floats,
          quantized,
          query,
          error,
          VectorSimilarityFunction.COSINE,
          quantizedSimilarity,
          scalarQuantizer,
          globalOffset);
    }
  }

  public void testToDotProduct() throws IOException {
    int dims = 128;
    int numVecs = 100;

    float[][] floats = randomFloats(numVecs, dims);
    for (float[] fs : floats) {
      VectorUtil.l2normalize(fs);
    }
    for (int quantile : new int[] {90, 95, 99, 100}) {
      float error = Math.max((100 - quantile) * 0.01f, 0.01f);
      FloatVectorValues floatVectorValues = fromFloats(floats);
      ScalarQuantizer scalarQuantizer = ScalarQuantizer.fromVectors(floatVectorValues, quantile);
      byte[][] quantized = quantizeVectors(scalarQuantizer, floats);
      float globalOffset = scalarQuantizer.globalVectorOffset(dims);
      float[] query = randomFloatArray(dims);
      VectorUtil.l2normalize(query);
      ScalarQuantizedVectorSimilarity quantizedSimilarity =
          ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
              VectorSimilarityFunction.DOT_PRODUCT, scalarQuantizer.getConstantMultiplier());
      assertQuantizedScores(
          floats,
          quantized,
          query,
          error,
          VectorSimilarityFunction.DOT_PRODUCT,
          quantizedSimilarity,
          scalarQuantizer,
          globalOffset);
    }
  }

  public void testToMaxInnerProduct() throws IOException {
    int dims = 128;
    int numVecs = 100;

    float[][] floats = randomFloats(numVecs, dims);
    for (int quantile : new int[] {90, 95, 99, 100}) {
      float error = Math.max((100 - quantile) * 0.5f, 0.5f);
      FloatVectorValues floatVectorValues = fromFloats(floats);
      ScalarQuantizer scalarQuantizer = ScalarQuantizer.fromVectors(floatVectorValues, quantile);
      byte[][] quantized = quantizeVectors(scalarQuantizer, floats);
      float globalOffset = scalarQuantizer.globalVectorOffset(dims);
      float[] query = randomFloatArray(dims);
      ScalarQuantizedVectorSimilarity quantizedSimilarity =
          ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
              VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT,
              scalarQuantizer.getConstantMultiplier());
      assertQuantizedScores(
          floats,
          quantized,
          query,
          error,
          VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT,
          quantizedSimilarity,
          scalarQuantizer,
          globalOffset);
    }
  }

  private void assertQuantizedScores(
      float[][] floats,
      byte[][] quantized,
      float[] query,
      float error,
      VectorSimilarityFunction similarityFunction,
      ScalarQuantizedVectorSimilarity quantizedSimilarity,
      ScalarQuantizer scalarQuantizer,
      float globalOffset) {
    for (int i = 0; i < floats.length; i++) {
      float storedOffset =
          ScalarQuantizedVectorSimilarity.scoreCorrectiveOffset(
              similarityFunction,
              quantized[i],
              scalarQuantizer.getAlpha(),
              scalarQuantizer.getOffset());
      byte[] quantizedQuery = new byte[query.length];
      scalarQuantizer.quantize(query, quantizedQuery);
      float queryOffset =
          ScalarQuantizedVectorSimilarity.scoreCorrectiveOffset(
              similarityFunction,
              quantizedQuery,
              scalarQuantizer.getAlpha(),
              scalarQuantizer.getOffset());
      float original = similarityFunction.compare(query, floats[i]);
      float quantizedScore =
          quantizedSimilarity.score(
              quantizedQuery, queryOffset, quantized[i], globalOffset + storedOffset);
      assertEquals("Not within acceptable error [" + error + "]", original, quantizedScore, error);
    }
  }

  private static byte[][] quantizeVectors(ScalarQuantizer scalarQuantizer, float[][] floats) {
    byte[][] quantized = new byte[floats.length][];
    int i = 0;
    for (float[] v : floats) {
      quantized[i] = new byte[v.length];
      scalarQuantizer.quantize(v, quantized[i]);
      ++i;
    }
    return quantized;
  }

  private static byte[][] quantizeVectorsNormalized(
      ScalarQuantizer scalarQuantizer, float[][] floats) {
    byte[][] quantized = new byte[floats.length][];
    int i = 0;
    for (float[] f : floats) {
      float[] v = ArrayUtil.copyOfSubArray(f, 0, f.length);
      VectorUtil.l2normalize(v);
      quantized[i] = new byte[v.length];
      scalarQuantizer.quantizeTo(v, quantized[i]);
      ++i;
    }
    return quantized;
  }

  private static FloatVectorValues fromFloatsNormalized(float[][] floats) {
    return new TestScalarQuantizer.TestSimpleFloatVectorValues(floats) {
      @Override
      public float[] vectorValue() throws IOException {
        if (curDoc == -1 || curDoc >= floats.length) {
          throw new IOException("Current doc not set or too many iterations");
        }
        float[] v = ArrayUtil.copyOfSubArray(floats[curDoc], 0, floats[curDoc].length);
        VectorUtil.l2normalize(v);
        return v;
      }
    };
  }
}
