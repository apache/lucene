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

import static java.lang.foreign.ValueLayout.JAVA_BYTE;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.BeforeClass;

public abstract class BaseVectorizationTestCase extends LuceneTestCase {

  protected static final VectorizationProvider LUCENE_PROVIDER = defaultProvider();
  protected static final VectorizationProvider PANAMA_PROVIDER = maybePanamaProvider();
  protected static final MethodHandle NATIVE_DOT_PRODUCT = nativeDotProductHandle("dotProduct");
  protected static final MethodHandle NATIVE_DOT_PRODUCT_ON_AND_OFFHEAP =
      nativeDotProductHandleOnAndOffHeap("dotProduct");

  /**
   * Copy input byte[] to off-heap MemorySegment
   *
   * @param byteVector to be copied off-heap
   * @return MemorySegment with byteVector copied to off-heap
   */
  public static MemorySegment offHeapByteVector(byte[] byteVector) {
    MemorySegment offHeapByteVector =
        Arena.ofAuto().allocate(byteVector.length, JAVA_BYTE.byteAlignment());
    MemorySegment.copy(byteVector, 0, offHeapByteVector, JAVA_BYTE, 0, byteVector.length);
    return offHeapByteVector;
  }

  /**
   * Used to get a MethodHandle of PanamaVectorUtilSupport.dotProduct(MemorySegment a, MemorySegment
   * b). The method above will use a native C implementation of dotProduct if it is enabled via
   * {@link org.apache.lucene.util.Constants#NATIVE_DOT_PRODUCT_ENABLED}. A reflection based
   * approach is necessary to avoid taking a direct dependency on preview APIs in Panama which may
   * be blocked at compile time.
   *
   * @return MethodHandle PanamaVectorUtilSupport.DotProduct(MemorySegment a, MemorySegment b)
   */
  private static MethodHandle nativeDotProductHandle(String methodName) {
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      // A method type that computes dot-product between two off-heap vectors
      // provided as native MemorySegment and returns an int score.
      final var MemorySegment = "java.lang.foreign.MemorySegment";
      final var methodType =
          MethodType.methodType(
              int.class, lookup.findClass(MemorySegment), lookup.findClass(MemorySegment));
      var mh =
          lookup.findStatic(
              PANAMA_PROVIDER.getVectorUtilSupport().getClass(), methodName, methodType);
      return mh;
    } catch (ClassNotFoundException | IllegalAccessException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Used to get a MethodHandle of PanamaVectorUtilSupport.dotProduct(byte[] a, MemorySegment b).
   * The method above will use a native C implementation of dotProduct if it is enabled via {@link
   * org.apache.lucene.util.Constants#NATIVE_DOT_PRODUCT_ENABLED}. A reflection based approach is
   * necessary to avoid taking a direct dependency on preview APIs in Panama which may be blocked at
   * compile time.
   *
   * @return MethodHandle PanamaVectorUtilSupport.DotProduct(byte[] a, MemorySegment b)
   */
  private static MethodHandle nativeDotProductHandleOnAndOffHeap(String methodName) {
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      // A method type that computes dot-product between one off-heap vector
      // as native MemorySegment and other one on heap and returns an int score.
      final var MemorySegment = "java.lang.foreign.MemorySegment";
      final var methodType =
          MethodType.methodType(int.class, byte[].class, lookup.findClass(MemorySegment));
      var mh =
          lookup.findStatic(
              PANAMA_PROVIDER.getVectorUtilSupport().getClass(), methodName, methodType);
      return mh;
    } catch (ClassNotFoundException | IllegalAccessException | NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
    assumeTrue(
        "Test only works when JDK's vector incubator module is enabled.",
        PANAMA_PROVIDER.getClass() != LUCENE_PROVIDER.getClass());
  }

  public static VectorizationProvider defaultProvider() {
    return new DefaultVectorizationProvider();
  }

  public static VectorizationProvider maybePanamaProvider() {
    return VectorizationProvider.lookup(true);
  }
}
