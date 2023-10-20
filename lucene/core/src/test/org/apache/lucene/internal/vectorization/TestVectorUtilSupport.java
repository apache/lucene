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
package org.apache.lucene.internal.vectorization;

import static org.apache.lucene.codecs.lucene95.VectorValuesAccess.newDenseOffHeapFloatVectorValues;
import static org.apache.lucene.store.MMapDirectory.DEFAULT_MAX_CHUNK_SIZE;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.function.ToDoubleFunction;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSLockFactory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;

// @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 100)
public class TestVectorUtilSupport extends BaseVectorizationTestCase {

  private static final double DELTA = 1e-3;

  private static final int[] VECTOR_SIZES = {
    1, 4, 6, 8, 13, 16, 25, 32, 64, 100, 128, 207, 256, 300, 512, 702, 1024
  };

  private final int size;

  public TestVectorUtilSupport(int size) {
    this.size = size;
  }

  @ParametersFactory
  public static Iterable<Object[]> parametersFactory() {
    return () -> IntStream.of(VECTOR_SIZES).boxed().map(i -> new Object[] {i}).iterator();
  }

  public void testFloatVectors() throws Exception {
    var a = new float[size];
    var b = new float[size];
    for (int i = 0; i < size; ++i) {
      a[i] = random().nextFloat();
      b[i] = random().nextFloat();
    }
    testDotProduct(a, b);
    assertFloatReturningProviders(p -> p.squareDistance(a, b));
    assertFloatReturningProviders(p -> p.cosine(a, b));
  }

  public void testBinaryVectors() {
    var a = new byte[size];
    var b = new byte[size];
    random().nextBytes(a);
    random().nextBytes(b);
    assertIntReturningProviders(p -> p.dotProduct(a, b));
    assertIntReturningProviders(p -> p.squareDistance(a, b));
    assertFloatReturningProviders(p -> p.cosine(a, b));
  }

  public void testBinaryVectorsBoundaries() {
    var a = new byte[size];
    var b = new byte[size];

    Arrays.fill(a, Byte.MIN_VALUE);
    Arrays.fill(b, Byte.MIN_VALUE);
    assertIntReturningProviders(p -> p.dotProduct(a, b));
    assertIntReturningProviders(p -> p.squareDistance(a, b));
    assertFloatReturningProviders(p -> p.cosine(a, b));

    Arrays.fill(a, Byte.MAX_VALUE);
    Arrays.fill(b, Byte.MAX_VALUE);
    assertIntReturningProviders(p -> p.dotProduct(a, b));
    assertIntReturningProviders(p -> p.squareDistance(a, b));
    assertFloatReturningProviders(p -> p.cosine(a, b));

    Arrays.fill(a, Byte.MIN_VALUE);
    Arrays.fill(b, Byte.MAX_VALUE);
    assertIntReturningProviders(p -> p.dotProduct(a, b));
    assertIntReturningProviders(p -> p.squareDistance(a, b));
    assertFloatReturningProviders(p -> p.cosine(a, b));

    Arrays.fill(a, Byte.MAX_VALUE);
    Arrays.fill(b, Byte.MIN_VALUE);
    assertIntReturningProviders(p -> p.dotProduct(a, b));
    assertIntReturningProviders(p -> p.squareDistance(a, b));
    assertFloatReturningProviders(p -> p.cosine(a, b));
  }

  private void assertFloatReturningProviders(ToDoubleFunction<VectorUtilSupport> func) {
    assertEquals(
        func.applyAsDouble(LUCENE_PROVIDER.getVectorUtilSupport()),
        func.applyAsDouble(PANAMA_PROVIDER.getVectorUtilSupport()),
        DELTA);
  }

  private void assertIntReturningProviders(ToIntFunction<VectorUtilSupport> func) {
    assertEquals(
        func.applyAsInt(LUCENE_PROVIDER.getVectorUtilSupport()),
        func.applyAsInt(PANAMA_PROVIDER.getVectorUtilSupport()));
  }

  private void testDotProduct(float[] a, float[] b) throws IOException {
    assert a.length == b.length;
    assertFloatReturningProviders(p -> p.dotProduct(a, b));

    // variants that operate on RandomAccessVectorValues
    int bytesLength = a.length * Float.BYTES;
    int aPosition = randomIntBetween(0, 14); // element-wise vector position in the mmapped file
    int bPosition = randomIntBetween(0, 14);
    for (long chunkSize : List.of(DEFAULT_MAX_CHUNK_SIZE, (long) randomIntBetween(16, 64))) {
      var path = createTempDir("testDotProductFloat-" + chunkSize);
      try (Directory dir = new MMapDirectory(path, FSLockFactory.getDefault(), chunkSize);
          var aIndex = inputForFloats(a, aPosition, randomIntBetween(0, 7), dir, "vector-a");
          var bIndex = inputForFloats(b, bPosition, randomIntBetween(0, 7), dir, "vector-b")) {
        var v1 = newDenseOffHeapFloatVectorValues(a.length, aPosition + 1, aIndex, bytesLength);
        var v2 = newDenseOffHeapFloatVectorValues(b.length, bPosition + 1, bIndex, bytesLength);
        assertFloatReturningProviders(wrapd(p -> p.dotProduct(v1, aPosition, v2, bPosition)));
        assertFloatReturningProviders(wrapd(p -> p.dotProduct(a, v2, bPosition)));
      }
    }
  }

  /**
   * Creates an index input consisting of the given vector, at the vector element-wise position. The
   * initialBytes allows to start the vector data at an unaligned position.
   */
  static IndexInput inputForFloats(
      float[] vector, int vectorPosition, int initialBytes, Directory dir, String name)
      throws IOException {
    int vectorLenBytes = vector.length * Float.BYTES;
    try (var out = dir.createOutput(name + ".data", IOContext.DEFAULT)) {
      if (initialBytes != 0) {
        out.writeBytes(new byte[initialBytes], initialBytes);
      }
      if (vectorPosition != 0) {
        out.writeBytes(new byte[vectorPosition * vectorLenBytes], vectorPosition * vectorLenBytes);
      }
      writeFloat32(vector, out);
    }
    var in = dir.openInput(name + ".data", IOContext.DEFAULT);
    return in.slice(name, initialBytes, (long) (vectorPosition + 1) * vectorLenBytes);
  }

  static void writeFloat32(float[] arr, IndexOutput out) throws IOException {
    int lenBytes = arr.length * Float.BYTES;
    final ByteBuffer buffer = ByteBuffer.allocate(lenBytes).order(ByteOrder.LITTLE_ENDIAN);
    buffer.asFloatBuffer().put(arr);
    out.writeBytes(buffer.array(), lenBytes);
  }

  public static int randomIntBetween(int min, int max) {
    return RandomNumbers.randomIntBetween(random(), min, max);
  }

  interface ThrowingToIntFunction<T> {
    int applyAsInt(T value) throws IOException;
  }

  interface ThrowingToDoubleFunction<T> {
    double applyAsDouble(T value) throws IOException;
  }

  static <T> ToIntFunction<T> wrap(ThrowingToIntFunction<T> function) {
    return t -> {
      try {
        return function.applyAsInt(t);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }

  static <T> ToDoubleFunction<T> wrapd(ThrowingToDoubleFunction<T> function) {
    return t -> {
      try {
        return function.applyAsDouble(t);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    };
  }
}
