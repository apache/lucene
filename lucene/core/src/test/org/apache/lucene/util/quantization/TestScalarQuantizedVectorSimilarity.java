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
package org.apache.lucene.util.quantization;

import static org.apache.lucene.util.quantization.TestScalarQuantizer.fromFloats;
import static org.apache.lucene.util.quantization.TestScalarQuantizer.randomFloatArray;
import static org.apache.lucene.util.quantization.TestScalarQuantizer.randomFloats;

import java.io.IOException;
import java.util.Set;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;

public class TestScalarQuantizedVectorSimilarity extends LuceneTestCase {

  public void testNonZeroScores() {
    byte[][] quantized = new byte[2][32];
    for (VectorSimilarityFunction similarityFunction : VectorSimilarityFunction.values()) {
      float multiplier = random().nextFloat();
      if (random().nextBoolean()) {
        multiplier = -multiplier;
      }
      for (byte bits : new byte[] {4, 7}) {
        ScalarQuantizedVectorSimilarity quantizedSimilarity =
            ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
                similarityFunction, multiplier, bits);
        float negativeOffsetA = -(random().nextFloat() * (random().nextInt(10) + 1));
        float negativeOffsetB = -(random().nextFloat() * (random().nextInt(10) + 1));
        float score =
            quantizedSimilarity.score(quantized[0], negativeOffsetA, quantized[1], negativeOffsetB);
        assertTrue(score >= 0);
      }
    }
  }

  public void testToEuclidean() throws IOException {
    int dims = 128;
    int numVecs = 100;

    float[][] floats = randomFloats(numVecs, dims);
    for (float confidenceInterval : new float[] {0.9f, 0.95f, 0.99f, (1 - 1f / (dims + 1)), 1f}) {
      float error = Math.max((100 - confidenceInterval) * 0.01f, 0.01f);
      FloatVectorValues floatVectorValues = fromFloats(floats);
      ScalarQuantizer scalarQuantizer =
          ScalarQuantizer.fromVectors(
              floatVectorValues, confidenceInterval, floats.length, (byte) 7);
      byte[][] quantized = new byte[floats.length][];
      float[] offsets =
          quantizeVectors(scalarQuantizer, floats, quantized, VectorSimilarityFunction.EUCLIDEAN);
      float[] query = ArrayUtil.copyOfSubArray(floats[0], 0, dims);
      ScalarQuantizedVectorSimilarity quantizedSimilarity =
          ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
              VectorSimilarityFunction.EUCLIDEAN,
              scalarQuantizer.getConstantMultiplier(),
              scalarQuantizer.getBits());
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
      FloatVectorValues floatVectorValues = fromFloatsNormalized(floats, null);
      ScalarQuantizer scalarQuantizer =
          ScalarQuantizer.fromVectors(
              floatVectorValues, confidenceInterval, floats.length, (byte) 7);
      byte[][] quantized = new byte[floats.length][];
      float[] offsets =
          quantizeVectorsNormalized(
              scalarQuantizer, floats, quantized, VectorSimilarityFunction.COSINE);
      float[] query = ArrayUtil.copyOfSubArray(floats[0], 0, dims);
      VectorUtil.l2normalize(query);
      ScalarQuantizedVectorSimilarity quantizedSimilarity =
          ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
              VectorSimilarityFunction.COSINE,
              scalarQuantizer.getConstantMultiplier(),
              scalarQuantizer.getBits());
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
          ScalarQuantizer.fromVectors(
              floatVectorValues, confidenceInterval, floats.length, (byte) 7);
      byte[][] quantized = new byte[floats.length][];
      float[] offsets =
          quantizeVectors(scalarQuantizer, floats, quantized, VectorSimilarityFunction.DOT_PRODUCT);
      float[] query = randomFloatArray(dims);
      VectorUtil.l2normalize(query);
      ScalarQuantizedVectorSimilarity quantizedSimilarity =
          ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
              VectorSimilarityFunction.DOT_PRODUCT,
              scalarQuantizer.getConstantMultiplier(),
              scalarQuantizer.getBits());
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
          ScalarQuantizer.fromVectors(
              floatVectorValues, confidenceInterval, floats.length, (byte) 7);
      byte[][] quantized = new byte[floats.length][];
      float[] offsets =
          quantizeVectors(
              scalarQuantizer, floats, quantized, VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT);
      float[] query = randomFloatArray(dims);
      ScalarQuantizedVectorSimilarity quantizedSimilarity =
          ScalarQuantizedVectorSimilarity.fromVectorSimilarity(
              VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT,
              scalarQuantizer.getConstantMultiplier(),
              scalarQuantizer.getBits());
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
      float[] v = ArrayUtil.copyArray(f);
      VectorUtil.l2normalize(v);
      quantized[i] = new byte[v.length];
      offsets[i] = scalarQuantizer.quantize(v, quantized[i], similarityFunction);
      ++i;
    }
    return offsets;
  }

  private static FloatVectorValues fromFloatsNormalized(
      float[][] floats, Set<Integer> deletedVectors) {
    return new TestScalarQuantizer.TestSimpleFloatVectorValues(floats, deletedVectors) {
      @Override
      public float[] vectorValue() throws IOException {
        if (curDoc == -1 || curDoc >= floats.length) {
          throw new IOException("Current doc not set or too many iterations");
        }
        float[] v = ArrayUtil.copyArray(floats[curDoc]);
        VectorUtil.l2normalize(v);
        return v;
      }
    };
  }
}
