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
import org.apache.lucene.codecs.VectorSimilarity;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomAccessVectorValues;

public class TestScalarQuantizedVectorSimilarity extends LuceneTestCase {

  public void testToEuclidean() throws IOException {
    int dims = 128;
    int numVecs = 100;
    VectorSimilarity euclidean = VectorSimilarity.EuclideanDistanceSimilarity.INSTANCE;

    float[][] floats = randomFloats(numVecs, dims);
    for (float confidenceInterval : new float[] {0.9f, 0.95f, 0.99f, (1 - 1f / (dims + 1)), 1f}) {
      float error = Math.max((100 - confidenceInterval) * 0.01f, 0.01f);
      FloatVectorValues floatVectorValues = fromFloats(floats);
      ScalarQuantizer scalarQuantizer =
          ScalarQuantizer.fromVectors(
              floatVectorValues, confidenceInterval, floats.length, (byte) 7);
      byte[][] quantized = new byte[floats.length][];
      float[] offsets = quantizeVectors(scalarQuantizer, floats, quantized, euclidean);
      float[] query = ArrayUtil.copyOfSubArray(floats[0], 0, dims);
      ScalarQuantizedVectorSimilarity quantizedSimilarity =
          new ScalarQuantizedVectorSimilarity(
              euclidean, scalarQuantizer.getConstantMultiplier(), scalarQuantizer.getBits());
      assertQuantizedScores(
          floats,
          quantized,
          offsets,
          query,
          error,
          euclidean,
          quantizedSimilarity,
          scalarQuantizer);
    }
  }

  public void testToCosine() throws IOException {
    int dims = 128;
    int numVecs = 100;

    VectorSimilarity cosine = VectorSimilarity.CosineSimilarity.INSTANCE;
    float[][] floats = randomFloats(numVecs, dims);

    for (float confidenceInterval : new float[] {0.9f, 0.95f, 0.99f, (1 - 1f / (dims + 1)), 1f}) {
      float error = Math.max((100 - confidenceInterval) * 0.01f, 0.01f);
      FloatVectorValues floatVectorValues = fromFloatsNormalized(floats, null);
      ScalarQuantizer scalarQuantizer =
          ScalarQuantizer.fromVectors(
              floatVectorValues, confidenceInterval, floats.length, (byte) 7);
      byte[][] quantized = new byte[floats.length][];
      float[] offsets = quantizeVectorsNormalized(scalarQuantizer, floats, quantized, cosine);
      float[] query = ArrayUtil.copyOfSubArray(floats[0], 0, dims);
      VectorUtil.l2normalize(query);
      ScalarQuantizedVectorSimilarity quantizedSimilarity =
          new ScalarQuantizedVectorSimilarity(
              cosine, scalarQuantizer.getConstantMultiplier(), scalarQuantizer.getBits());
      assertQuantizedScores(
          floats, quantized, offsets, query, error, cosine, quantizedSimilarity, scalarQuantizer);
    }
  }

  public void testToDotProduct() throws IOException {
    int dims = 128;
    int numVecs = 100;
    VectorSimilarity dotProduct = VectorSimilarity.DotProductSimilarity.INSTANCE;

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
      float[] offsets = quantizeVectors(scalarQuantizer, floats, quantized, dotProduct);
      float[] query = randomFloatArray(dims);
      VectorUtil.l2normalize(query);
      ScalarQuantizedVectorSimilarity quantizedSimilarity =
          new ScalarQuantizedVectorSimilarity(
              dotProduct, scalarQuantizer.getConstantMultiplier(), scalarQuantizer.getBits());
      assertQuantizedScores(
          floats,
          quantized,
          offsets,
          query,
          error,
          dotProduct,
          quantizedSimilarity,
          scalarQuantizer);
    }
  }

  public void testToMaxInnerProduct() throws IOException {
    int dims = 128;
    int numVecs = 100;
    VectorSimilarity maxInnerProduct = VectorSimilarity.MaxInnerProductSimilarity.INSTANCE;
    float[][] floats = randomFloats(numVecs, dims);
    for (float confidenceInterval : new float[] {0.9f, 0.95f, 0.99f, (1 - 1f / (dims + 1)), 1f}) {
      float error = Math.max((100 - confidenceInterval) * 0.5f, 0.5f);
      FloatVectorValues floatVectorValues = fromFloats(floats);
      ScalarQuantizer scalarQuantizer =
          ScalarQuantizer.fromVectors(
              floatVectorValues, confidenceInterval, floats.length, (byte) 7);
      byte[][] quantized = new byte[floats.length][];
      float[] offsets = quantizeVectors(scalarQuantizer, floats, quantized, maxInnerProduct);
      float[] query = randomFloatArray(dims);
      ScalarQuantizedVectorSimilarity quantizedSimilarity =
          new ScalarQuantizedVectorSimilarity(
              maxInnerProduct, scalarQuantizer.getConstantMultiplier(), scalarQuantizer.getBits());
      assertQuantizedScores(
          floats,
          quantized,
          offsets,
          query,
          error,
          maxInnerProduct,
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
      VectorSimilarity similarityFunction,
      ScalarQuantizedVectorSimilarity quantizedSimilarity,
      ScalarQuantizer scalarQuantizer)
      throws IOException {
    RandomAccessQuantizedByteVectorValues quantizedByteVectorProvider =
        fromQuantized(quantized, storedOffsets);
    RandomAccessVectorValues<float[]> vectorValues = randomAccessVectorValues(floats);
    byte[] quantizedQuery = new byte[query.length];
    float queryOffset = scalarQuantizer.quantize(query, quantizedQuery, similarityFunction);
    VectorSimilarity.VectorScorer quantizedScorer =
        quantizedSimilarity.getVectorScorer(
            quantizedByteVectorProvider, quantizedQuery, queryOffset);
    VectorSimilarity.VectorScorer rawScorer =
        similarityFunction.getVectorScorer(vectorValues, query);
    for (int i = 0; i < floats.length; i++) {
      float original = rawScorer.score(i);
      float quantizedScore = quantizedScorer.score(i);
      assertEquals("Not within acceptable error [" + error + "]", original, quantizedScore, error);
    }
  }

  private static float[] quantizeVectors(
      ScalarQuantizer scalarQuantizer,
      float[][] floats,
      byte[][] quantized,
      VectorSimilarity similarityFunction) {
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
      VectorSimilarity similarityFunction) {
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

  private static FloatVectorValues fromFloatsNormalized(
      float[][] floats, Set<Integer> deletedVectors) {
    return new TestScalarQuantizer.TestSimpleFloatVectorValues(floats, deletedVectors) {
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

  private static TestQuantizedByteVectorProvider fromQuantized(
      byte[][] quantized, float[] offsets) {
    return new TestQuantizedByteVectorProvider(quantized, offsets);
  }

  private static RandomAccessVectorValues<float[]> randomAccessVectorValues(float[][] floats) {
    return new RandomAccessVectorValues<>() {
      @Override
      public float[] vectorValue(int targetOrd) {
        return floats[targetOrd];
      }

      @Override
      public int size() {
        return floats.length;
      }

      @Override
      public RandomAccessVectorValues<float[]> copy() {
        return this;
      }

      @Override
      public int dimension() {
        return floats[0].length;
      }
    };
  }

  private static class TestQuantizedByteVectorProvider
      implements RandomAccessQuantizedByteVectorValues {
    private final byte[][] quantized;
    private final float[] offsets;

    TestQuantizedByteVectorProvider(byte[][] quantized, float[] offsets) {
      this.quantized = quantized;
      this.offsets = offsets;
    }

    @Override
    public float getScoreCorrectionConstant(int targetOrd) {
      return offsets[targetOrd];
    }

    @Override
    public byte[] vectorValue(int targetOrd) {
      return quantized[targetOrd];
    }

    @Override
    public TestQuantizedByteVectorProvider copy() {
      return new TestQuantizedByteVectorProvider(quantized, offsets);
    }

    @Override
    public int size() {
      return quantized.length;
    }

    @Override
    public int dimension() {
      return quantized[0].length;
    }
  }
}
