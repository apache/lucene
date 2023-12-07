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
    for (float confidenceInterval : new float[] {0.9f, 0.95f, 0.99f, (1 - 1f / (dims + 1)), 1f}) {
      float error = Math.max((100 - confidenceInterval) * 0.01f, 0.01f);
      FloatVectorValues floatVectorValues = fromFloats(floats);
      ScalarQuantizer scalarQuantizer =
          ScalarQuantizer.fromVectors(floatVectorValues, confidenceInterval);
      byte[][] quantized = new byte[floats.length][];
      float[] offsets =
          quantizeVectors(scalarQuantizer, floats, quantized, VectorSimilarityFunction.EUCLIDEAN);
      float[] query = ArrayUtil.copyOfSubArray(floats[0], 0, dims);
      ScalarQuantizedVectorSimilarity quantizedSimilarity =
          ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
              VectorSimilarityFunction.EUCLIDEAN, scalarQuantizer.getConstantMultiplier());
      assertQuantizedScores(
          floats,
          quantized,
          offsets,
          query,
          error,
          VectorSimilarityFunction.EUCLIDEAN,
          quantizedSimilarity,
          scalarQuantizer);
    }
  }

  public void testToCosine() throws IOException {
    int dims = 128;
    int numVecs = 100;

    float[][] floats = randomFloats(numVecs, dims);

    for (float confidenceInterval : new float[] {0.9f, 0.95f, 0.99f, (1 - 1f / (dims + 1)), 1f}) {
      float error = Math.max((100 - confidenceInterval) * 0.01f, 0.01f);
      FloatVectorValues floatVectorValues = fromFloatsNormalized(floats);
      ScalarQuantizer scalarQuantizer =
          ScalarQuantizer.fromVectors(floatVectorValues, confidenceInterval);
      byte[][] quantized = new byte[floats.length][];
      float[] offsets =
          quantizeVectorsNormalized(
              scalarQuantizer, floats, quantized, VectorSimilarityFunction.COSINE);
      float[] query = ArrayUtil.copyOfSubArray(floats[0], 0, dims);
      VectorUtil.l2normalize(query);
      ScalarQuantizedVectorSimilarity quantizedSimilarity =
          ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
              VectorSimilarityFunction.COSINE, scalarQuantizer.getConstantMultiplier());
      assertQuantizedScores(
          floats,
          quantized,
          offsets,
          query,
          error,
          VectorSimilarityFunction.COSINE,
          quantizedSimilarity,
          scalarQuantizer);
    }
  }

  public void testToDotProduct() throws IOException {
    int dims = 128;
    int numVecs = 100;

    float[][] floats = randomFloats(numVecs, dims);
    for (float[] fs : floats) {
      VectorUtil.l2normalize(fs);
    }
    for (float confidenceInterval : new float[] {0.9f, 0.95f, 0.99f, (1 - 1f / (dims + 1)), 1f}) {
      float error = Math.max((100 - confidenceInterval) * 0.01f, 0.01f);
      FloatVectorValues floatVectorValues = fromFloats(floats);
      ScalarQuantizer scalarQuantizer =
          ScalarQuantizer.fromVectors(floatVectorValues, confidenceInterval);
      byte[][] quantized = new byte[floats.length][];
      float[] offsets =
          quantizeVectors(scalarQuantizer, floats, quantized, VectorSimilarityFunction.DOT_PRODUCT);
      float[] query = randomFloatArray(dims);
      VectorUtil.l2normalize(query);
      ScalarQuantizedVectorSimilarity quantizedSimilarity =
          ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
              VectorSimilarityFunction.DOT_PRODUCT, scalarQuantizer.getConstantMultiplier());
      assertQuantizedScores(
          floats,
          quantized,
          offsets,
          query,
          error,
          VectorSimilarityFunction.DOT_PRODUCT,
          quantizedSimilarity,
          scalarQuantizer);
    }
  }

  public void testToMaxInnerProduct() throws IOException {
    int dims = 128;
    int numVecs = 100;

    float[][] floats = randomFloats(numVecs, dims);
    for (float confidenceInterval : new float[] {0.9f, 0.95f, 0.99f, (1 - 1f / (dims + 1)), 1f}) {
      float error = Math.max((100 - confidenceInterval) * 0.5f, 0.5f);
      FloatVectorValues floatVectorValues = fromFloats(floats);
      ScalarQuantizer scalarQuantizer =
          ScalarQuantizer.fromVectors(floatVectorValues, confidenceInterval);
      byte[][] quantized = new byte[floats.length][];
      float[] offsets =
          quantizeVectors(
              scalarQuantizer, floats, quantized, VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT);
      float[] query = randomFloatArray(dims);
      ScalarQuantizedVectorSimilarity quantizedSimilarity =
          ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
              VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT,
              scalarQuantizer.getConstantMultiplier());
      assertQuantizedScores(
          floats,
          quantized,
          offsets,
          query,
          error,
          VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT,
          quantizedSimilarity,
          scalarQuantizer);
    }
  }

  private void assertQuantizedScores(
      float[][] floats,
      byte[][] quantized,
      float[] storedOffsets,
      float[] query,
      float error,
      VectorSimilarityFunction similarityFunction,
      ScalarQuantizedVectorSimilarity quantizedSimilarity,
      ScalarQuantizer scalarQuantizer) {
    for (int i = 0; i < floats.length; i++) {
      float storedOffset = storedOffsets[i];
      byte[] quantizedQuery = new byte[query.length];
      float queryOffset = scalarQuantizer.quantize(query, quantizedQuery, similarityFunction);
      float original = similarityFunction.compare(query, floats[i]);
      float quantizedScore =
          quantizedSimilarity.score(quantizedQuery, queryOffset, quantized[i], storedOffset);
      assertEquals("Not within acceptable error [" + error + "]", original, quantizedScore, error);
    }
  }

  private static float[] quantizeVectors(
      ScalarQuantizer scalarQuantizer,
      float[][] floats,
      byte[][] quantized,
      VectorSimilarityFunction similarityFunction) {
    int i = 0;
    float[] offsets = new float[floats.length];
    for (float[] v : floats) {
      quantized[i] = new byte[v.length];
      offsets[i] = scalarQuantizer.quantize(v, quantized[i], similarityFunction);
      ++i;
    }
    return offsets;
  }

  private static float[] quantizeVectorsNormalized(
      ScalarQuantizer scalarQuantizer,
      float[][] floats,
      byte[][] quantized,
      VectorSimilarityFunction similarityFunction) {
    int i = 0;
    float[] offsets = new float[floats.length];
    for (float[] f : floats) {
      float[] v = ArrayUtil.copyOfSubArray(f, 0, f.length);
      VectorUtil.l2normalize(v);
      quantized[i] = new byte[v.length];
      offsets[i] = scalarQuantizer.quantize(v, quantized[i], similarityFunction);
      ++i;
    }
    return offsets;
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
