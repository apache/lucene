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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.FloatBuffer;
import org.apache.lucene.index.VectorValues;
import org.junit.Assert;

public class TestFullKnn extends LuceneTestCase {

  static class SimpleBufferProvider implements FullKnn.BufferProvider {
    private ByteBuffer buffer;

    SimpleBufferProvider(ByteBuffer buffer) {
      this.buffer = buffer;
    }

    @Override
    public ByteBuffer getBuffer(int offset, int blockSize) throws IOException {
      return buffer.position(offset).slice().order(ByteOrder.LITTLE_ENDIAN).limit(blockSize);
    }
  }

  FullKnn fullKnn;
  float[] vec0, vec1, vec2;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    fullKnn = new FullKnn(5, 2, VectorValues.SearchStrategy.DOT_PRODUCT_HNSW, true);
    vec0 = new float[] {1, 2, 3, 4, 5};
    VectorUtil.l2normalize(vec0);
    vec1 = new float[] {6, 7, 8, 9, 10};
    VectorUtil.l2normalize(vec1);
    vec2 = new float[] {1, 2, 3, 4, 6};
    VectorUtil.l2normalize(vec2);
  }

  private ByteBuffer floatArrayToByteBuffer(float[] floats) {
    ByteBuffer byteBuf =
        ByteBuffer.allocateDirect(floats.length * Float.BYTES); // 4 bytes per float
    byteBuf.order(ByteOrder.LITTLE_ENDIAN);
    FloatBuffer buffer = byteBuf.asFloatBuffer();
    buffer.put(floats);
    return byteBuf;
  }

  private void assertBufferEqualsArray(byte[] fa, ByteBuffer wholeBuffer) {
    final byte[] tempBufferFloats = new byte[wholeBuffer.remaining()];
    wholeBuffer.get(tempBufferFloats);
    Assert.assertArrayEquals(fa, tempBufferFloats);
  }

  public float[] joinArrays(float[]... args) {
    int length = 0;
    for (float[] arr : args) {
      length += arr.length;
    }

    float[] merged = new float[length];
    int i = 0;

    for (float[] arr : args) {
      for (float f : arr) {
        merged[i++] = f;
      }
    }

    return merged;
  }

  public void testSimpleFloatBufferProvider() throws IOException {
    byte[] fa = {0, 1, 2, 3, 4, 5, 6, 7, 8, 9};

    ByteBuffer fb = ByteBuffer.wrap(fa);

    final SimpleBufferProvider sf = new SimpleBufferProvider(fb);

    final ByteBuffer wholeBuffer = sf.getBuffer(0, 10);
    assertBufferEqualsArray(fa, wholeBuffer);

    final ByteBuffer fb5_5 = sf.getBuffer(5, 5);
    assertBufferEqualsArray(ArrayUtil.copyOfSubArray(fa, 5, 10), fb5_5);

    final ByteBuffer fb5_3 = sf.getBuffer(5, 3);
    assertBufferEqualsArray(ArrayUtil.copyOfSubArray(fa, 5, 8), fb5_3);

    final ByteBuffer fb2_12 = sf.getBuffer(5, 0);
    assertBufferEqualsArray(new byte[] {}, fb2_12);

    final ByteBuffer fb5_1 = sf.getBuffer(5, 1);
    assertBufferEqualsArray(ArrayUtil.copyOfSubArray(fa, 5, 6), fb5_1);
  }

  public void testSuccessFullKnn() throws IOException {
    float[] twoDocs = joinArrays(vec0, vec1);
    float[] threeDocs = joinArrays(vec0, vec1, vec2);
    float[] threeQueries = joinArrays(vec1, vec0, vec2);

    int[][] result =
        fullKnn.doFullKnn(
            2,
            1,
            new SimpleBufferProvider(floatArrayToByteBuffer(twoDocs)),
            new SimpleBufferProvider(floatArrayToByteBuffer(vec1)));

    Assert.assertArrayEquals(result[0], new int[] {1, 0});

    result =
        fullKnn.doFullKnn(
            2,
            1,
            new SimpleBufferProvider(floatArrayToByteBuffer(twoDocs)),
            new SimpleBufferProvider(floatArrayToByteBuffer(vec0)));

    Assert.assertArrayEquals(result[0], new int[] {0, 1});

    float[] twoQueries = joinArrays(vec1, vec0);
    result =
        fullKnn.doFullKnn(
            2,
            2,
            new SimpleBufferProvider(floatArrayToByteBuffer(twoDocs)),
            new SimpleBufferProvider(floatArrayToByteBuffer(twoQueries)));

    Assert.assertArrayEquals(result, new int[][] {{1, 0}, {0, 1}});

    result =
        fullKnn.doFullKnn(
            3,
            3,
            new SimpleBufferProvider(floatArrayToByteBuffer(threeDocs)),
            new SimpleBufferProvider(floatArrayToByteBuffer(threeQueries)));

    Assert.assertArrayEquals(new int[][] {{1, 0}, {0, 2}, {2, 0}}, result);

    FullKnn full3nn = new FullKnn(5, 3, VectorValues.SearchStrategy.DOT_PRODUCT_HNSW, true);
    result =
        full3nn.doFullKnn(
            3,
            3,
            new SimpleBufferProvider(floatArrayToByteBuffer(threeDocs)),
            new SimpleBufferProvider(floatArrayToByteBuffer(threeQueries)));

    Assert.assertArrayEquals(new int[][] {{1, 0, 2}, {0, 2, 1}, {2, 0, 1}}, result);
  }

  public void testExceptionFullKnn() {
    float[] twoDocs = joinArrays(vec0, vec1);
    FullKnn full3nn = new FullKnn(5, 3, VectorValues.SearchStrategy.DOT_PRODUCT_HNSW, true);

    expectThrows(
        IllegalArgumentException.class,
        () -> {
          full3nn.doFullKnn(
              2,
              1,
              new SimpleBufferProvider(floatArrayToByteBuffer(twoDocs)),
              new SimpleBufferProvider(floatArrayToByteBuffer(vec1)));
        });
  }

  public void testLargeVectors() throws IOException {
    int dim = 256;
    int noOfVectors = 1_000;
    int k = 3;
    float[] vectors = VectorUtil.randomVector(random(), dim * noOfVectors);
    final ByteBuffer byteBuffer = floatArrayToByteBuffer(vectors);

    FullKnn full3nn = new FullKnn(dim, k, VectorValues.SearchStrategy.DOT_PRODUCT_HNSW, true);
    final int[][] result =
        full3nn.doFullKnn(
            noOfVectors,
            noOfVectors,
            new SimpleBufferProvider(byteBuffer),
            new SimpleBufferProvider(byteBuffer.duplicate()));

    Assert.assertEquals("Result size did not match.", noOfVectors, result.length);
    for (int i = 0; i < noOfVectors; i++) {
      Assert.assertEquals("Did not return K results.", k, result[i].length);
      Assert.assertEquals("First result was not the same vector.", i, result[i][0]);
    }
  }
}
