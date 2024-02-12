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

import static org.apache.lucene.util.quantization.ScalarQuantizer.SCRATCH_SIZE;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.tests.util.LuceneTestCase;

public class TestScalarQuantizer extends LuceneTestCase {

  public void testQuantizeAndDeQuantize() throws IOException {
    int dims = 128;
    int numVecs = 100;
    VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.DOT_PRODUCT;

    float[][] floats = randomFloats(numVecs, dims);
    FloatVectorValues floatVectorValues = fromFloats(floats);
    ScalarQuantizer scalarQuantizer = ScalarQuantizer.fromVectors(floatVectorValues, 1, numVecs);
    float[] dequantized = new float[dims];
    byte[] quantized = new byte[dims];
    byte[] requantized = new byte[dims];
    for (int i = 0; i < numVecs; i++) {
      scalarQuantizer.quantize(floats[i], quantized, similarityFunction);
      scalarQuantizer.deQuantize(quantized, dequantized);
      scalarQuantizer.quantize(dequantized, requantized, similarityFunction);
      for (int j = 0; j < dims; j++) {
        assertEquals(dequantized[j], floats[i][j], 0.02);
        assertEquals(quantized[j], requantized[j]);
      }
    }
  }

  public void testQuantiles() {
    float[] percs = new float[1000];
    for (int i = 0; i < 1000; i++) {
      percs[i] = (float) i;
    }
    shuffleArray(percs);
    float[] upperAndLower = ScalarQuantizer.getUpperAndLowerQuantile(percs, 0.9f);
    assertEquals(50f, upperAndLower[0], 1e-7);
    assertEquals(949f, upperAndLower[1], 1e-7);
    shuffleArray(percs);
    upperAndLower = ScalarQuantizer.getUpperAndLowerQuantile(percs, 0.95f);
    assertEquals(25f, upperAndLower[0], 1e-7);
    assertEquals(974f, upperAndLower[1], 1e-7);
    shuffleArray(percs);
    upperAndLower = ScalarQuantizer.getUpperAndLowerQuantile(percs, 0.99f);
    assertEquals(5f, upperAndLower[0], 1e-7);
    assertEquals(994f, upperAndLower[1], 1e-7);
  }

  public void testEdgeCase() {
    float[] upperAndLower =
        ScalarQuantizer.getUpperAndLowerQuantile(new float[] {1.0f, 1.0f, 1.0f, 1.0f, 1.0f}, 0.9f);
    assertEquals(1f, upperAndLower[0], 1e-7f);
    assertEquals(1f, upperAndLower[1], 1e-7f);
  }

  public void testScalarWithSampling() throws IOException {
    int numVecs = random().nextInt(128) + 5;
    int dims = 64;
    float[][] floats = randomFloats(numVecs, dims);
    // Should not throw
    {
      TestSimpleFloatVectorValues floatVectorValues =
          fromFloatsWithRandomDeletions(floats, random().nextInt(numVecs - 1) + 1);
      ScalarQuantizer.fromVectors(
          floatVectorValues,
          0.99f,
          floatVectorValues.numLiveVectors,
          Math.max(floatVectorValues.numLiveVectors - 1, SCRATCH_SIZE + 1));
    }
    {
      TestSimpleFloatVectorValues floatVectorValues =
          fromFloatsWithRandomDeletions(floats, random().nextInt(numVecs - 1) + 1);
      ScalarQuantizer.fromVectors(
          floatVectorValues,
          0.99f,
          floatVectorValues.numLiveVectors,
          Math.max(floatVectorValues.numLiveVectors - 1, SCRATCH_SIZE + 1));
    }
    {
      TestSimpleFloatVectorValues floatVectorValues =
          fromFloatsWithRandomDeletions(floats, random().nextInt(numVecs - 1) + 1);
      ScalarQuantizer.fromVectors(
          floatVectorValues,
          0.99f,
          floatVectorValues.numLiveVectors,
          Math.max(floatVectorValues.numLiveVectors - 1, SCRATCH_SIZE + 1));
    }
    {
      TestSimpleFloatVectorValues floatVectorValues =
          fromFloatsWithRandomDeletions(floats, random().nextInt(numVecs - 1) + 1);
      ScalarQuantizer.fromVectors(
          floatVectorValues,
          0.99f,
          floatVectorValues.numLiveVectors,
          Math.max(random().nextInt(floatVectorValues.floats.length - 1) + 1, SCRATCH_SIZE + 1));
    }
  }

  static void shuffleArray(float[] ar) {
    for (int i = ar.length - 1; i > 0; i--) {
      int index = random().nextInt(i + 1);
      float a = ar[index];
      ar[index] = ar[i];
      ar[i] = a;
    }
  }

  static float[] randomFloatArray(int dims) {
    float[] arr = new float[dims];
    for (int j = 0; j < dims; j++) {
      arr[j] = random().nextFloat();
    }
    return arr;
  }

  static float[][] randomFloats(int num, int dims) {
    float[][] floats = new float[num][];
    for (int i = 0; i < num; i++) {
      floats[i] = randomFloatArray(dims);
    }
    return floats;
  }

  static FloatVectorValues fromFloats(float[][] floats) {
    return new TestSimpleFloatVectorValues(floats, null);
  }

  static TestSimpleFloatVectorValues fromFloatsWithRandomDeletions(
      float[][] floats, int numDeleted) {
    Set<Integer> deletedVectors = new HashSet<>();
    for (int i = 0; i < numDeleted; i++) {
      deletedVectors.add(random().nextInt(floats.length));
    }
    return new TestSimpleFloatVectorValues(floats, deletedVectors);
  }

  static class TestSimpleFloatVectorValues extends FloatVectorValues {
    protected final float[][] floats;
    protected final Set<Integer> deletedVectors;
    protected final int numLiveVectors;
    protected int curDoc = -1;

    TestSimpleFloatVectorValues(float[][] values, Set<Integer> deletedVectors) {
      this.floats = values;
      this.deletedVectors = deletedVectors;
      this.numLiveVectors =
          deletedVectors == null ? values.length : values.length - deletedVectors.size();
    }

    @Override
    public int dimension() {
      return floats[0].length;
    }

    @Override
    public int size() {
      return floats.length;
    }

    @Override
    public float[] vectorValue() throws IOException {
      if (curDoc == -1 || curDoc >= floats.length) {
        throw new IOException("Current doc not set or too many iterations");
      }
      return floats[curDoc];
    }

    @Override
    public int docID() {
      if (curDoc >= floats.length) {
        return NO_MORE_DOCS;
      }
      return curDoc;
    }

    @Override
    public int nextDoc() throws IOException {
      while (++curDoc < floats.length) {
        if (deletedVectors == null || !deletedVectors.contains(curDoc)) {
          return curDoc;
        }
      }
      return docID();
    }

    @Override
    public int advance(int target) throws IOException {
      curDoc = target - 1;
      return nextDoc();
    }
  }
}
