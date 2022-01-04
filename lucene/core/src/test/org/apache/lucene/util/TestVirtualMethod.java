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

import org.apache.lucene.tests.util.LuceneTestCase;

public class TestVirtualMethod extends LuceneTestCase {
  private static final VirtualMethod<Base> publicTestMethod =
      new VirtualMethod<>(Base.class, "publicTest", String.class);
  private static final VirtualMethod<Base> protectedTestMethod =
      new VirtualMethod<>(Base.class, "protectedTest", int.class);

  static class Base {
    public void publicTest(String test) {}

    protected void protectedTest(int test) {}
  }

  static class Nested1 extends Base {
    @Override
    public void publicTest(String test) {}

    @Override
    protected void protectedTest(int test) {}
  }

  static class Nested2 extends Nested1 {
    @Override // make it public here
    public void protectedTest(int test) {}
  }

  static class Nested3 extends Nested2 {
    @Override
    public void publicTest(String test) {}
  }

  static class Nested4 extends Base {}

  static class Nested5 extends Nested4 {}

  public void testGeneral() {
    assertEquals(0, publicTestMethod.getImplementationDistance(Base.class));
    assertEquals(1, publicTestMethod.getImplementationDistance(Nested1.class));
    assertEquals(1, publicTestMethod.getImplementationDistance(Nested2.class));
    assertEquals(3, publicTestMethod.getImplementationDistance(Nested3.class));
    assertFalse(publicTestMethod.isOverriddenAsOf(Nested4.class));
    assertFalse(publicTestMethod.isOverriddenAsOf(Nested5.class));

    assertEquals(0, protectedTestMethod.getImplementationDistance(Base.class));
    assertEquals(1, protectedTestMethod.getImplementationDistance(Nested1.class));
    assertEquals(2, protectedTestMethod.getImplementationDistance(Nested2.class));
    assertEquals(2, protectedTestMethod.getImplementationDistance(Nested3.class));
    assertFalse(protectedTestMethod.isOverriddenAsOf(Nested4.class));
    assertFalse(protectedTestMethod.isOverriddenAsOf(Nested5.class));

    assertTrue(
        VirtualMethod.compareImplementationDistance(
                Nested3.class, publicTestMethod, protectedTestMethod)
            > 0);
    assertEquals(
        0,
        VirtualMethod.compareImplementationDistance(
            Nested5.class, publicTestMethod, protectedTestMethod));
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public void testExceptions() {
    // Object is not a subclass and can never override publicTest(String)
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          publicTestMethod.getImplementationDistance((Class) Object.class);
        });

    // Method bogus() does not exist, so IAE should be thrown
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new VirtualMethod<>(Base.class, "bogus");
        });

    // Method publicTest(String) is not declared in TestClass2, so IAE should be thrown
    expectThrows(
        IllegalArgumentException.class,
        () -> {
          new VirtualMethod<>(Nested2.class, "publicTest", String.class);
        });

    // try to create a second instance of the same baseClass / method combination
    expectThrows(
        UnsupportedOperationException.class,
        () -> {
          new VirtualMethod<>(Base.class, "publicTest", String.class);
        });
  }
}
