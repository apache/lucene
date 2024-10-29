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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.junit.BeforeClass;

public abstract class BaseVectorizationTestCase extends LuceneTestCase {

  protected static final VectorizationProvider LUCENE_PROVIDER = new DefaultVectorizationProvider();
  protected static final VectorizationProvider PANAMA_PROVIDER = VectorizationProvider.lookup(true);
  protected static final MethodHandle NATIVE_DOT_PRODUCT = nativeDotProductHandle("dotProduct");

  /**
   * Copy input byte[] to off-heap MemorySegment
   *
   * @param byteVector to be copied off-heap
   * @return Object MemorySegment
   */
  protected static Object offHeap(byte[] byteVector) {
    try {
      MethodHandles.Lookup lookup = MethodHandles.lookup();
      // A method type that copies input byte[] to an off-heap MemorySegment
      final var methodType =
          MethodType.methodType(lookup.findClass("java.lang.foreign.MemorySegment"), byte[].class);
      Class<?> vectorUtilSupportClass = PANAMA_PROVIDER.getVectorUtilSupport().getClass();
      final MethodHandle offHeapByteVector =
          lookup.findStatic(vectorUtilSupportClass, "offHeapByteVector", methodType);
      return offHeapByteVector.invoke(byteVector);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  /**
   * Used to get a MethodHandle of PanamaVectorUtilSupport.dotProduct(MemorySegment a, MemorySegment
   * b). The method above will use a native C implementation of dotProduct if it is enabled via
   * {@link org.apache.lucene.util.Constants#NATIVE_DOT_PRODUCT_ENABLED} AND both MemorySegment
   * arguments are backed by off-heap memory. A reflection based approach is necessary to avoid
   * taking a direct dependency on preview APIs in Panama which may be blocked at compile time.
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
      // Erase the type of receiver to Object so that mh.invokeExact(a, b) does not throw
      // WrongMethodException.
      // Here 'a' and 'b' are off-heap vectors of type MemorySegment constructed via reflection
      // API.
      // This minimizes the reflection overhead and brings us very close to the performance of
      // direct method invocation.
      mh = mh.asType(mh.type().changeParameterType(0, Object.class));
      mh = mh.asType(mh.type().changeParameterType(1, Object.class));
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
}
